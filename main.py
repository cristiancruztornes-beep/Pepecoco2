# -*- coding: utf-8 -*-
"""Bot Telegram para ToDus S3 + integración opcional DK ToDus + manifiestos UDYAT.

DK auth solo se completa después de que el usuario abra el enlace y el endpoint
/auth/check indique estado verificado. Nunca se inventa el Telegram ID ni se
desactiva TLS para el backend DK.
"""
import asyncio
import json
import logging
import os
import re
import shutil
import socket
import tempfile
import threading
import time
import unicodedata
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit
import xml.etree.ElementTree as ET

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
DK_AUTH_WAIT = int(os.getenv("DK_AUTH_WAIT", "180"))
UDYAT_IPS = [x.strip() for x in os.getenv("UDYAT_IPS", "152.207.206.86,152.207.206.88").split(",") if x.strip()]

S3_BASE = "https://s3.todus.cu/stream"
CHUNK = 1024 * 1024
STATUS = {"telegram": "desconectado", "todus": "sin comprobar"}
DK_SESSIONS = {}  # tokens en RAM: se pierden al reiniciar el servicio
DK_PENDING = {}
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


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
        r = self.session.head(url, timeout=30, allow_redirects=False)
        if r.status_code == 404:
            return {"exists": False, "size": 0, "etag": None, "ranges": False}
        if r.status_code != 200:
            raise ToDusError(f"HEAD HTTP {r.status_code}")
        return {"exists": True, "size": int(r.headers.get("Content-Length", 0) or 0),
                "etag": r.headers.get("ETag"), "ranges": r.headers.get("Accept-Ranges") == "bytes",
                "last_modified": r.headers.get("Last-Modified")}

    def upload(self, path, uid, filename=None):
        name = safe_name(filename or os.path.basename(path))
        size = os.path.getsize(path)
        url = build_url(name)
        last = None
        for attempt in range(1, S3_RETRIES + 1):
            start = time.monotonic()
            try:
                with open(path, "rb") as f:
                    r = self.session.put(url, data=f, headers={
                        "Content-Length": str(size),
                        "Content-Type": "application/octet-stream"
                    }, timeout=S3_TIMEOUT)
                if r.status_code in (429,) or r.status_code >= 500:
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
                    elapsed = max(.001, time.monotonic() - start)
                    return {"url": url, "name": name, "size": size, "seconds": elapsed,
                            "speed": size / elapsed / 1048576, "etag": meta["etag"],
                            "rate_remaining": r.headers.get("x-ratelimit-remaining")}
            except (requests.ConnectionError, requests.Timeout) as e:
                last = str(e)
                if attempt < S3_RETRIES:
                    time.sleep(min(30, 2 ** (attempt - 1)))
                    continue
            if attempt == S3_RETRIES:
                break
        raise ToDusError(f"No se pudo subir tras {S3_RETRIES} intentos: {last}")

    def list_user(self, uid):
        prefix, token, items = "", None, []
        for _ in range(100):
            params = {"list-type": "2", "max-keys": 1000, "prefix": prefix}
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
        if threads > 1 and meta["ranges"] and size > CHUNK:
            import concurrent.futures
            count = min(threads, max(1, (size + CHUNK - 1) // CHUNK))
            chunk = (size + count - 1) // count
            parts = [path + f".part{i}" for i in range(count)]
            def get_part(i, a, b):
                with self.session.get(url, headers={"Range": f"bytes={a}-{b}"},
                                      stream=True, timeout=S3_TIMEOUT) as r:
                    if r.status_code != 206:
                        raise ToDusError(f"Range HTTP {r.status_code}")
                    with open(parts[i], "wb") as out:
                        for data in r.iter_content(CHUNK):
                            if data:
                                out.write(data)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
                    futures = [pool.submit(get_part, i, i*chunk, min(size-1, (i+1)*chunk-1))
                               for i in range(count)]
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
                with open(path, "wb") as out:
                    for part in parts:
                        with open(part, "rb") as src:
                            shutil.copyfileobj(src, out, CHUNK)
            finally:
                for part in parts:
                    try: os.remove(part)
                    except FileNotFoundError: pass
        else:
            with self.session.get(url, stream=True, timeout=S3_TIMEOUT) as r:
                if r.status_code != 200:
                    raise ToDusError(f"GET HTTP {r.status_code}")
                total = 0
                with open(path, "wb") as out:
                    for data in r.iter_content(CHUNK):
                        if data:
                            total += len(data)
                            if MAX_MB and total > MAX_MB * 1048576:
                                raise ToDusError(f"Supera el límite de {MAX_MB} MB.")
                            out.write(data)
        return path


s3 = ToDusS3()


class DKClient:
    """Cliente opcional del backend DK documentado en el repo."""
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

    def init(self):
        return self.request("POST", "/api/v1/auth/init", json={})

    def check(self, temp_token):
        from urllib.parse import urlencode
        return self.request("GET", "/api/v1/auth/check?" + urlencode({"token": temp_token}))

    def verify(self, temp_token, uid, username):
        return self.request("POST", "/api/v1/auth/verify",
                            json={"temp_token": temp_token, "telegram_id": uid, "username": username or ""})

    def me(self, token):
        return self.request("GET", "/api/v1/auth/me", token=token)

    def uploads(self, token):
        return self.request("GET", "/api/v1/uploads?limit=50", token=token)

    def register_upload(self, token, data):
        return self.request("POST", "/api/v1/uploads", token=token, json=data)

    def stats(self):
        return self.request("GET", "/api/v1/stats")

    def health(self):
        return self.request("GET", "/api/v1/health-db")


dk = DKClient()


def status_ip(ip):
    # UDYAT TLS self-signed: verificación desactivada SOLO para esta IP configurada,
    # con Host/SNI explícito. Nunca se usa para el endpoint S3 normal ni DK.
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.head(f"https://{ip}/", headers={"Host": "s3.todus.cu",
                          "User-Agent": "Wget/1.21.3"}, timeout=3, verify=False)
        return r.status_code in (200, 403, 404), r.status_code
    except requests.RequestException as e:
        return False, str(e)


def make_udyat(url, meta, ip):
    p = urlsplit(url)
    filename = safe_name(unquote(os.path.basename(p.path)) or "archivo")
    return {
        "f": filename,
        "s": meta["size"],
        "md5": meta.get("etag", "").strip('"'),
        "c": [{"url": f"https://{ip}{p.path}", "size": meta["size"]}],
        "h": ip,
        "original_host": "s3.todus.cu",
        "platform": "android",
    }


def allowed(e):
    return not ALLOWED or e.sender_id in ALLOWED


def check_todus():
    t = time.monotonic()
    try:
        with socket.create_connection(("s3.todus.cu", 443), timeout=8):
            STATUS["todus"] = f"✅ Accesible ({time.monotonic()-t:.1f}s)"
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
    def log_message(self, *args): pass


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
                "👋 **Bot ToDus completo**\n"
                "Envíame un archivo para subirlo a tu carpeta persistente S3.\n\n"
                "/lista — tus archivos\n"
                "/borrar NOMBRE — borrar archivo\n"
                "/verificar URL — comprobar enlace\n"
                "/descargar URL — descargar y reenviar aquí\n"
                "/udyat URL — crear manifiesto .udyat para IroS3Dl\n"
                "/dklogin — información sobre la limitación de autenticación DK\n"
                "/dkme — perfil DK\n/dkuploads — subidas registradas en DK\n"
                "/dkstats — estadísticas públicas DK\n/dkhealth — salud de BD DK\n"
                "/id /estado /ayuda"
            )

    @client.on(events.NewMessage(pattern=r"^/(?:ayuda)$"))
    async def help_cmd(e):
        if await guard(e): await start(e)

    @client.on(events.NewMessage(pattern=r"^/id$"))
    async def id_cmd(e):
        if await guard(e): await e.reply(f"Tu ID: `{e.sender_id}`")

    @client.on(events.NewMessage(pattern=r"^/estado$"))
    async def state_cmd(e):
        if await guard(e):
            await bg(check_todus)
            await e.reply(f"Telegram: {STATUS['telegram']}\nToDus S3: {STATUS['todus']}")

    @client.on(events.NewMessage(pattern=r"^/lista$"))
    async def list_cmd(e):
        if not await guard(e): return
        m = await e.reply("📂 Consultando ToDus...")
        try:
            items = await bg(s3.list_user, e.sender_id)
            if not items: return await m.edit("No tienes archivos.")
            lines = [f"**Tus archivos ({len(items)}):**"]
            lines += [f"• `{x['name']}` — {fmt_size(x['size'])}" for x in items[:40]]
            if len(items) > 40: lines.append(f"... y {len(items)-40} más.")
            await m.edit("\n".join(lines))
        except Exception as ex: await m.edit(f"Error: {ex}")

    @client.on(events.NewMessage(pattern=r"^/borrar\s+(.+)$"))
    async def delete_cmd(e):
        if not await guard(e): return
        name = safe_name(e.pattern_match.group(1))
        m = await e.reply("Buscando archivo...")
        try:
            items = await bg(s3.list_user, e.sender_id)
            item = next((x for x in items if x["name"] == name), None)
            if not item: return await m.edit("No existe en tu carpeta.")
            await bg(s3.delete, item["url"])
            await m.edit("🗑️ Archivo borrado.")
        except Exception as ex: await m.edit(f"Error: {ex}")

    @client.on(events.NewMessage(pattern=r"^/verificar\s+(https?://\S+)$"))
    async def verify_s3(e):
        if not await guard(e): return
        try:
            info = await bg(s3.head, e.pattern_match.group(1).rstrip(".,)"))
            await e.reply(f"Existe: {info['exists']}\nTamaño: {fmt_size(info['size'])}\nETag: {info['etag']}")
        except Exception as ex: await e.reply(f"Error: {ex}")

    @client.on(events.NewMessage(pattern=r"^/descargar\s+(https?://\S+)$"))
    async def download_cmd(e):
        if not await guard(e): return
        url = e.pattern_match.group(1).rstrip(".,)")
        m = await e.reply("⬇️ Descargando...")
        tmp = tempfile.mkdtemp()
        try:
            name = safe_name(unquote(os.path.basename(urlsplit(url).path)) or "archivo")
            path = os.path.join(tmp, name)
            await bg(s3.download, url, path, DOWNLOAD_THREADS)
            await client.send_file(e.chat_id, path, caption=f"{name} — {fmt_size(os.path.getsize(path))}")
            await m.delete()
        except Exception as ex: await m.edit(f"Error: {ex}")
        finally: shutil.rmtree(tmp, ignore_errors=True)

    @client.on(events.NewMessage(pattern=r"^/udyat\s+(https?://\S+)$"))
    async def udyat_cmd(e):
        if not await guard(e): return
        url = e.pattern_match.group(1).rstrip(".,)")
        m = await e.reply("🔎 Comprobando enlace y disponibilidad UDYAT...")
        try:
            info = await bg(s3.head, url)
            if not info["exists"]: return await m.edit("El archivo no existe.")
            # Render está fuera de la red ETECSA; comprobar la IP desde Render
            # no permite saber si el teléfono en Cuba podrá usar la ruta gratuita.
            # Generamos el manifiesto con la IP clásica documentada y dejamos
            # que UDYAT Downloader la resuelva desde el dispositivo del usuario.
            free_ip = UDYAT_IPS[0] if UDYAT_IPS else "152.207.206.86"
            manifest = make_udyat(url, info, free_ip)
            filename = safe_name(os.path.basename(urlsplit(url).path)) + ".udyat"
            path = os.path.join(tempfile.mkdtemp(), filename)
            with open(path, "w", encoding="utf-8") as f: json.dump(manifest, f, ensure_ascii=False, indent=2)
            await client.send_file(e.chat_id, path, caption=(
                "📄 Manifiesto UDYAT creado para IroS3Dl. Impórtalo desde la app. "
                "La descarga gratuita solo puede funcionar desde una conexión ETECSA compatible en Cuba. "
                "Render no puede verificar esa conexión."
            ))
            await m.delete()
            shutil.rmtree(os.path.dirname(path), ignore_errors=True)
        except Exception as ex: await m.edit(f"UDYAT: {ex}")

    @client.on(events.NewMessage(pattern=r"^/dklogin$"))
    async def dk_login(e):
        if await guard(e):
            await e.reply(
                "DK ToDus no tiene en el repositorio un flujo de autenticación independiente. "
                "Su API documentada exige autorización mediante @DkTodusbot. "
                "Para evitar depender de un bot ajeno, esta función queda desactivada. "
                "Las funciones S3 y UDYAT de este bot sí trabajan directamente."
            )

    @client.on(events.NewMessage(pattern=r"^/(?:dkme|dkuploads)$"))
    async def dk_account_unavailable(e):
        if await guard(e):
            await e.reply("Esta función requiere una sesión DK oficial; no se puede crear una sesión independiente con la documentación disponible.")

    @client.on(events.NewMessage(pattern=r"^/dkstats$"))
    async def dk_stats(e):
        if not await guard(e): return
        try:
            data = await bg(dk.stats)
            await e.reply("**Estadísticas públicas DK:**\n`" + json.dumps(data, ensure_ascii=False, indent=2)[:3000] + "`")
        except Exception as ex: await e.reply(f"DK stats: {ex}")

    @client.on(events.NewMessage(pattern=r"^/dkhealth$"))
    async def dk_health(e):
        if not await guard(e): return
        try:
            data = await bg(dk.health)
            await e.reply("**Estado público de DK:**\n`" + json.dumps(data, ensure_ascii=False, indent=2)[:2000] + "`")
        except Exception as ex: await e.reply(f"DK health: {ex}")

    @client.on(events.NewMessage(func=lambda ev: bool(ev.file)))
    async def upload_file(e):
        if not await guard(e): return
        if MAX_MB and e.file.size and e.file.size > MAX_MB * 1048576:
            return await e.reply(f"Archivo supera {MAX_MB} MB.")
        name = safe_name(e.file.name or f"{e.id}{e.file.ext or ''}")
        m = await e.reply(f"⬇️ Recibiendo {name}...")
        tmp = tempfile.mkdtemp()
        try:
            path = os.path.join(tmp, name)
            await e.download_media(file=path)
            size = os.path.getsize(path)
            if MAX_MB and size > MAX_MB * 1048576: raise ToDusError("Supera MAX_MB.")
            await m.edit(f"⬆️ Subiendo {fmt_size(size)} a S3...")
            result = await bg(s3.upload, path, e.sender_id, name)
            await m.edit(
                "✅ **Subido y verificado.**\n"
                f"📄 `{result['name']}`\n📦 {fmt_size(result['size'])}\n"
                f"⚡ {result['speed']:.2f} MB/s\n🔗 {result['url']}"
            )
        except Exception as ex:
            log.exception("Upload failed")
            await m.edit(f"❌ Error: {ex}")
        finally: shutil.rmtree(tmp, ignore_errors=True)

    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    STATUS["telegram"] = f"conectado como @{me.username or me.id}"
    log.info("Bot conectado: %s", STATUS["telegram"])
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
