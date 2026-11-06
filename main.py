# -*- coding: utf-8 -*-
"""Bot Telegram para ToDus S3 + manifiestos UDYAT experimentales con chunks.

IMPORTANTE: el esquema start/end de los chunks es experimental; debe probarse
con UDYAT Downloader. No se afirma compatibilidad hasta que la app lo acepte.
"""
import asyncio
import json
import hashlib
import logging
import os
import re
import shutil
import socket
import tempfile
import threading
import time
import unicodedata
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit
import xml.etree.ElementTree as ET

import httpx
import requests
import urllib3
from telethon import TelegramClient, events
from telethon.sessions import MemorySession

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("todus-bot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ALLOWED = {int(x) for x in os.getenv("ALLOWED_IDS", "").replace(" ", "").split(",") if x}
MAX_MB = int(os.getenv("MAX_MB", "0") or 0)
S3_TIMEOUT = int(os.getenv("S3_TIMEOUT", "3600"))
S3_RETRIES = max(1, int(os.getenv("S3_RETRIES", "3")))
DOWNLOAD_THREADS = max(1, min(8, int(os.getenv("DOWNLOAD_THREADS", "4"))))
DK_BASE = os.getenv("DK_BASE", "https://dk-todus-backend.fastapicloud.dev").rstrip("/")
UDYAT_IPS = [x.strip() for x in os.getenv("UDYAT_IPS", "152.207.206.86,152.207.206.88").split(",") if x.strip()]
S3_BASE = "https://s3.todus.cu/stream"
IO_CHUNK = 1024 * 1024
STATUS = {"telegram": "desconectado", "todus": "sin comprobar"}
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# Configuración UDYAT: por defecto 2 MB; /chunk permite de 2 a 20 MB.
UDYAT_CHUNK_CONFIG = Path(os.getenv("UDYAT_CHUNK_CONFIG", "udyat_chunk_size.json"))
UDYAT_CHUNK_MB = 2
try:
    if UDYAT_CHUNK_CONFIG.exists():
        saved = json.loads(UDYAT_CHUNK_CONFIG.read_text(encoding="utf-8"))
        UDYAT_CHUNK_MB = max(2, min(20, int(saved.get("mb", 2))))
except Exception as exc:
    log.warning("No se pudo leer configuración UDYAT; uso 2 MB: %s", exc)


class ToDusError(Exception):
    pass


def safe_name(name):
    name = unicodedata.normalize("NFKD", name or "archivo")
    name = "".join(c for c in name if not unicodedata.combining(c))
    base, ext = os.path.splitext(os.path.basename(name))
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("._") or "archivo"
    ext = re.sub(r"[^A-Za-z0-9.]", "", ext)
    return (base[:100] + ext[:20])[:120]


def fmt_size(n):
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.2f} {unit}"
        value /= 1024


def build_url(key):
    return S3_BASE + "/" + "/".join(quote(p, safe="") for p in key.strip("/").split("/"))


class ToDusS3:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "ToDusTelegramBot/2.0"

    @staticmethod
    def validate(url):
        p = urlsplit(url)
        if p.scheme != "https" or p.hostname != "s3.todus.cu" or not p.path.startswith("/stream/"):
            raise ValueError("Solo se aceptan enlaces HTTPS s3.todus.cu/stream/.")

    def head(self, url):
        self.validate(url)
        headers = {"Host": "s3.todus.cu", "User-Agent": "Wget/1.21.3",
                   "Accept": "*/*", "Accept-Encoding": "identify"}
        try:
            with httpx.Client(timeout=30.0, follow_redirects=False) as client:
                r = client.head(url, headers=headers,
                                extensions={"sni_hostname": "s3.todus.cu"})
        except httpx.HTTPError as exc:
            raise ToDusError(f"Error en HEAD httpx: {exc}") from exc
        if r.status_code == 404:
            return {"exists": False, "status": 404, "size": 0, "etag": None,
                    "ranges": False, "headers": dict(r.headers)}
        if r.status_code != 200:
            raise ToDusError(f"HEAD HTTP {r.status_code}: {dict(r.headers)}")
        return {"exists": True, "status": r.status_code,
                "size": int(r.headers.get("Content-Length", 0) or 0),
                "etag": r.headers.get("ETag"),
                "ranges": "bytes" in r.headers.get("Accept-Ranges", "").lower(),
                "last_modified": r.headers.get("Last-Modified"),
                "headers": dict(r.headers)}

    def upload(self, path, uid, filename=None):
        name = safe_name(filename or os.path.basename(path))
        size = os.path.getsize(path)
        url = build_url(name)
        last = None
        for attempt in range(1, S3_RETRIES + 1):
            started = time.monotonic()
            try:
                with open(path, "rb") as f:
                    r = self.session.put(url, data=f, headers={
                        "Content-Length": str(size),
                        "Content-Type": "application/octet-stream"}, timeout=S3_TIMEOUT)
                if r.status_code == 429 or r.status_code >= 500:
                    last = f"HTTP {r.status_code}"
                    if attempt < S3_RETRIES:
                        time.sleep(min(30, 2 ** (attempt - 1)))
                        continue
                elif r.status_code not in (200, 201, 204):
                    raise ToDusError(f"PUT HTTP {r.status_code}: {r.text[:200]}")
                else:
                    meta = self.head(url)
                    if not meta["exists"]:
                        raise ToDusError("PUT aceptado pero HEAD no encuentra el archivo en /stream/<nombre>.")
                    elapsed = max(.001, time.monotonic() - started)
                    return {"url": url, "name": name, "size": size, "seconds": elapsed,
                            "speed": size / elapsed / 1048576, "etag": meta["etag"],
                            "rate_remaining": r.headers.get("x-ratelimit-remaining")}
            except (requests.ConnectionError, requests.Timeout) as exc:
                last = str(exc)
                if attempt < S3_RETRIES:
                    time.sleep(min(30, 2 ** (attempt - 1)))
                    continue
            if attempt == S3_RETRIES:
                break
        raise ToDusError(f"No se pudo subir tras {S3_RETRIES} intentos: {last}")

    def calculate_md5(self, url):
        """Calcula el MD5 REAL del objeto completo; ETag no siempre es MD5."""
        self.validate(url)
        digest = hashlib.md5()
        try:
            with self.session.get(url, stream=True, timeout=S3_TIMEOUT) as response:
                if response.status_code != 200:
                    raise ToDusError(f"GET para calcular MD5 devolvió HTTP {response.status_code}")
                for block in response.iter_content(chunk_size=1024 * 1024):
                    if block:
                        digest.update(block)
        except requests.RequestException as exc:
            raise ToDusError(f"No se pudo calcular el MD5 real: {exc}") from exc
        return digest.hexdigest()

    def list_user(self, uid):
        token, items = None, []
        for _ in range(100):
            params = {"list-type": "2", "max-keys": 1000}
            if token:
                params["continuation-token"] = token
            r = self.session.get(S3_BASE, params=params, timeout=60)
            if r.status_code != 200:
                raise ToDusError(f"LIST HTTP {r.status_code}: {r.text[:200]}")
            root = ET.fromstring(r.text)
            for obj in root.findall("s3:Contents", S3_NS):
                key = obj.findtext("s3:Key", "", S3_NS)
                items.append({"key": key, "name": key.rsplit("/", 1)[-1],
                              "size": int(obj.findtext("s3:Size", "0", S3_NS)),
                              "url": build_url(key),
                              "modified": obj.findtext("s3:LastModified", "", S3_NS),
                              "etag": obj.findtext("s3:ETag", "", S3_NS).strip('"')})
            if root.findtext("s3:IsTruncated", "false", S3_NS).lower() != "true":
                break
            token = root.findtext("s3:NextContinuationToken", "", S3_NS)
            if not token:
                break
        return items

    def delete(self, url):
        self.validate(url)
        r = self.session.delete(url, timeout=30, allow_redirects=False)
        if r.status_code not in (200, 204, 404):
            raise ToDusError(f"DELETE HTTP {r.status_code}")
        return True

    def download(self, url, path, threads=1):
        self.validate(url)
        meta = self.head(url)
        if not meta["exists"]:
            raise ToDusError("El archivo no existe.")
        if MAX_MB and meta["size"] > MAX_MB * 1048576:
            raise ToDusError(f"Supera el límite de {MAX_MB} MB.")
        size = meta["size"]
        threads = max(1, min(8, int(threads)))
        if threads > 1 and meta["ranges"] and size > IO_CHUNK:
            import concurrent.futures
            count = min(threads, max(1, (size + IO_CHUNK - 1) // IO_CHUNK))
            span = (size + count - 1) // count
            parts = [path + f".part{i}" for i in range(count)]

            def get_part(i, start, end):
                with self.session.get(url, headers={"Range": f"bytes={start}-{end}"},
                                      stream=True, timeout=S3_TIMEOUT) as r:
                    if r.status_code != 206:
                        raise ToDusError(f"Range HTTP {r.status_code}")
                    with open(parts[i], "wb") as out:
                        for data in r.iter_content(IO_CHUNK):
                            if data:
                                out.write(data)

            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
                    futures = [pool.submit(get_part, i, i * span, min(size - 1, (i + 1) * span - 1))
                               for i in range(count)]
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                with open(path, "wb") as out:
                    for part in parts:
                        with open(part, "rb") as src:
                            shutil.copyfileobj(src, out, IO_CHUNK)
            finally:
                for part in parts:
                    try:
                        os.remove(part)
                    except FileNotFoundError:
                        pass
        else:
            with self.session.get(url, stream=True, timeout=S3_TIMEOUT) as r:
                if r.status_code != 200:
                    raise ToDusError(f"GET HTTP {r.status_code}")
                total = 0
                with open(path, "wb") as out:
                    for data in r.iter_content(IO_CHUNK):
                        if data:
                            total += len(data)
                            if MAX_MB and total > MAX_MB * 1048576:
                                raise ToDusError(f"Supera el límite de {MAX_MB} MB.")
                            out.write(data)
        return path


s3 = ToDusS3()


class DKClient:
    def __init__(self):
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "ToDusTelegramBot/2.0"

    def request(self, method, path, token=None, **kwargs):
        headers = kwargs.pop("headers", {})
        if token:
            headers["Authorization"] = "Bearer " + token
        r = self.http.request(method, DK_BASE + path, headers=headers, timeout=25, **kwargs)
        if r.status_code >= 400:
            raise ToDusError(f"DK backend HTTP {r.status_code}: {r.text[:250]}")
        try:
            return r.json()
        except ValueError:
            return {"raw": r.text}

    def stats(self):
        return self.request("GET", "/api/v1/stats")

    def health(self):
        return self.request("GET", "/api/v1/health-db")


dk = DKClient()


def make_udyat(url, meta, ip, real_md5):
    """Crea manifiesto experimental: un elemento 'c' por rango de bytes.

    ATENCIÓN: los campos start/end son tentativos. El repositorio original solo
    documentaba url/size en cada elemento; la app debe confirmar este esquema.
    """
    p = urlsplit(url)
    filename = safe_name(unquote(os.path.basename(p.path)) or "archivo")
    total = int(meta.get("size", 0) or 0)
    if total <= 0:
        raise ToDusError("El servidor no informó el tamaño del archivo; no puedo dividirlo en chunks.")
    chunk_bytes = UDYAT_CHUNK_MB * 1024 * 1024
    chunks = []
    offset = 0
    while offset < total:
        last = min(total - 1, offset + chunk_bytes - 1)
        chunks.append({"url": f"https://{ip}{p.path}", "size": last - offset + 1,
                       "start": offset, "end": last})
        offset = last + 1
    return {"f": filename, "s": total,
            "md5": real_md5,
            "c": chunks, "h": ip, "original_host": "s3.todus.cu",
            "platform": "android"}


def allowed(e):
    return not ALLOWED or e.sender_id in ALLOWED


def check_todus():
    started = time.monotonic()
    try:
        with socket.create_connection(("s3.todus.cu", 443), timeout=8):
            STATUS["todus"] = f"✅ Accesible ({time.monotonic() - started:.1f}s)"
    except Exception as exc:
        STATUS["todus"] = f"❌ Sin conexión: {exc}"


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = ("ToDus Telegram Bot\nTelegram: " + STATUS["telegram"] +
                "\nToDus: " + STATUS["todus"] + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def start_web():
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "10000"))), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


async def bg(fn, *args):
    return await asyncio.to_thread(fn, *args)


async def main():
    start_web()
    await bg(check_todus)
    client = TelegramClient(MemorySession(), API_ID, API_HASH)

    async def guard(e):
        if not allowed(e):
            await e.reply("⛔ No estás autorizado.")
            return False
        return True

    @client.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$"))
    async def start(e):
        if await guard(e):
            await e.reply(
                "👋 **Bot ToDus**\nEnvíame un archivo para subirlo automáticamente a S3.\n\n"
                "/lista — listar archivos\n/borrar NOMBRE — borrar archivo\n"
                "/verificar URL — comprobar enlace\n/descargar URL — descargar y reenviar\n"
                "/udyat URL — crear manifiesto UDYAT experimental\n"
                "/chunk — ver tamaño actual\n/chunk 2..20 — cambiar tamaño de chunk en MB\n"
                "/dkstats — estadísticas DK\n/dkhealth — estado DK\n/id /estado /ayuda")

    @client.on(events.NewMessage(pattern=r"^/ayuda$"))
    async def help_cmd(e):
        if await guard(e):
            await start(e)

    @client.on(events.NewMessage(pattern=r"^/id$"))
    async def id_cmd(e):
        if await guard(e):
            await e.reply(f"Tu ID: `{e.sender_id}`")

    @client.on(events.NewMessage(pattern=r"^/estado$"))
    async def state_cmd(e):
        if await guard(e):
            await bg(check_todus)
            await e.reply(f"Telegram: {STATUS['telegram']}\nToDus S3: {STATUS['todus']}")

    @client.on(events.NewMessage(pattern=r"^/chunk(?:\s+(\d+))?$"))
    async def chunk_cmd(e):
        global UDYAT_CHUNK_MB
        if not await guard(e):
            return
        value = e.pattern_match.group(1)
        if value is None:
            return await e.reply(f"Tamaño actual: {UDYAT_CHUNK_MB} MB.\nUsa /chunk 2 hasta /chunk 20.")
        mb = int(value)
        if not 2 <= mb <= 20:
            return await e.reply("El tamaño permitido es de 2 a 20 MB. Ejemplo: /chunk 5")
        UDYAT_CHUNK_MB = mb
        try:
            UDYAT_CHUNK_CONFIG.write_text(json.dumps({"mb": mb}), encoding="utf-8")
            note = "Configuración guardada."
        except Exception as exc:
            log.warning("No se pudo guardar la configuración: %s", exc)
            note = "Configuración aplicada en memoria; Render podría perderla al reiniciar."
        await e.reply(f"✅ Chunk configurado en {mb} MB. {note}")

    @client.on(events.NewMessage(pattern=r"^/lista$"))
    async def list_cmd(e):
        if not await guard(e):
            return
        msg = await e.reply("📂 Consultando ToDus...")
        try:
            items = await bg(s3.list_user, e.sender_id)
            if not items:
                return await msg.edit("No hay archivos.")
            lines = [f"**Archivos ({len(items)}):**"]
            lines += [f"• `{x['name']}` — {fmt_size(x['size'])}" for x in items[:40]]
            if len(items) > 40:
                lines.append(f"... y {len(items) - 40} más.")
            await msg.edit("\n".join(lines))
        except Exception as exc:
            await msg.edit(f"Error: {exc}")

    @client.on(events.NewMessage(pattern=r"^/borrar\s+(.+)$"))
    async def delete_cmd(e):
        if not await guard(e):
            return
        name = safe_name(e.pattern_match.group(1))
        msg = await e.reply("Buscando archivo...")
        try:
            items = await bg(s3.list_user, e.sender_id)
            item = next((x for x in items if x["name"] == name), None)
            if not item:
                return await msg.edit("No existe en tu lista.")
            await bg(s3.delete, item["url"])
            await msg.edit("🗑️ Archivo borrado.")
        except Exception as exc:
            await msg.edit(f"Error: {exc}")

    @client.on(events.NewMessage(pattern=r"^/verificar\s+(https?://\S+)$"))
    async def verify_cmd(e):
        if not await guard(e):
            return
        try:
            info = await bg(s3.head, e.pattern_match.group(1).rstrip(".,)"))
            await e.reply(f"Existe: {info['exists']}\nTamaño: {fmt_size(info['size'])}\n"
                          f"Acepta rangos: {info['ranges']}\nETag: {info['etag']}")
        except Exception as exc:
            await e.reply(f"Error: {exc}")

    @client.on(events.NewMessage(pattern=r"^/descargar\s+(https?://\S+)$"))
    async def download_cmd(e):
        if not await guard(e):
            return
        url = e.pattern_match.group(1).rstrip(".,)")
        msg = await e.reply("⬇️ Descargando...")
        tmp = tempfile.mkdtemp()
        try:
            name = safe_name(unquote(os.path.basename(urlsplit(url).path)) or "archivo")
            path = os.path.join(tmp, name)
            await bg(s3.download, url, path, DOWNLOAD_THREADS)
            await client.send_file(e.chat_id, path,
                                   caption=f"{name} — {fmt_size(os.path.getsize(path))}")
            await msg.delete()
        except Exception as exc:
            await msg.edit(f"Error: {exc}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @client.on(events.NewMessage(pattern=r"^/udyat\s+(https?://\S+)$"))
    async def udyat_cmd(e):
        if not await guard(e):
            return
        url = e.pattern_match.group(1).rstrip(".,)")
        msg = await e.reply("🔎 Comprobando enlace y creando manifiesto UDYAT...")
        tmp = tempfile.mkdtemp()
        try:
            info = await bg(s3.head, url)
            if not info["exists"]:
                return await msg.edit("El archivo no existe.")
            if not info["ranges"]:
                return await msg.edit("⚠️ El servidor no anuncia Accept-Ranges: bytes. No puedo asegurar que los chunks funcionen.")
            ip = UDYAT_IPS[0] if UDYAT_IPS else "152.207.206.86"
            await msg.edit("🧮 Calculando MD5 real del video (esto lee el archivo completo una vez)...")
            real_md5 = await bg(s3.calculate_md5, url)
            manifest = make_udyat(url, info, ip, real_md5)
            filename = safe_name(os.path.basename(urlsplit(url).path)) + ".udyat"
            path = os.path.join(tmp, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            await client.send_file(e.chat_id, path, caption=(
                f"📄 Manifiesto UDYAT experimental.\nTamaño de chunk: {UDYAT_CHUNK_MB} MB\n"
                f"Cantidad: {len(manifest['c'])}\n"
                "⚠️ Los campos start/end son experimentales: importa primero un archivo pequeño "
                "y confirma que UDYAT Downloader realmente descarga y une los rangos. "
                "MD5 calculado leyendo el archivo completo. La ruta gratuita depende de ETECSA."))
            await msg.delete()
        except Exception as exc:
            await msg.edit(f"UDYAT: {exc}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @client.on(events.NewMessage(pattern=r"^/dklogin$"))
    async def dk_login(e):
        if await guard(e):
            await e.reply(
                "DK ToDus requiere su flujo oficial de autorización. "
                "Esta versión no inventa ni suplanta esa autenticación. "
                "Las funciones S3 y UDYAT trabajan directamente desde este bot."
            )

    @client.on(events.NewMessage(pattern=r"^/(?:dkme|dkuploads)$"))
    async def dk_account_unavailable(e):
        if await guard(e):
            await e.reply("Esta función requiere una sesión DK oficial; no se puede crear una sesión independiente con la documentación disponible.")

    @client.on(events.NewMessage(pattern=r"^/dkstats$"))
    async def dk_stats(e):
        if not await guard(e):
            return
        try:
            data = await bg(dk.stats)
            await e.reply("**Estadísticas públicas DK:**\n`" +
                          json.dumps(data, ensure_ascii=False, indent=2)[:3000] + "`")
        except Exception as exc:
            await e.reply(f"DK stats: {exc}")

    @client.on(events.NewMessage(pattern=r"^/dkhealth$"))
    async def dk_health(e):
        if not await guard(e):
            return
        try:
            data = await bg(dk.health)
            await e.reply("**Estado público DK:**\n`" +
                          json.dumps(data, ensure_ascii=False, indent=2)[:2000] + "`")
        except Exception as exc:
            await e.reply(f"DK health: {exc}")

    @client.on(events.NewMessage(func=lambda ev: bool(ev.file)))
    async def upload_file(e):
        if not await guard(e):
            return
        if MAX_MB and e.file.size and e.file.size > MAX_MB * 1048576:
            return await e.reply(f"Archivo supera {MAX_MB} MB.")
        name = safe_name(e.file.name or f"{e.id}{e.file.ext or ''}")
        msg = await e.reply(f"⬇️ Recibiendo {name}...")
        tmp = tempfile.mkdtemp()
        try:
            path = os.path.join(tmp, name)
            await e.download_media(file=path)
            size = os.path.getsize(path)
            if MAX_MB and size > MAX_MB * 1048576:
                raise ToDusError("Supera MAX_MB.")
            await msg.edit(f"⬆️ Subiendo {fmt_size(size)} a S3...")
            result = await bg(s3.upload, path, e.sender_id, name)
            await msg.edit("✅ **Subido y verificado.**\n"
                           f"📄 `{result['name']}`\n📦 {fmt_size(result['size'])}\n"
                           f"⚡ {result['speed']:.2f} MB/s\n🔗 {result['url']}")
        except Exception as exc:
            log.exception("Upload failed")
            await msg.edit(f"❌ Error: {exc}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    STATUS["telegram"] = f"conectado como @{me.username or me.id}"
    log.info("Bot conectado: %s", STATUS["telegram"])
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
