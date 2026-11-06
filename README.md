# Bot Telegram ToDus S3 + UDYAT chunks (experimental)

## Incluye
- Recepción automática de archivos por Telegram y subida a `https://s3.todus.cu/stream/<nombre>`.
- `/lista`, `/borrar NOMBRE`, `/verificar URL`, `/descargar URL`, `/udyat URL`.
- `/chunk` consulta el tamaño configurado (por defecto 2 MB).
- `/chunk 2`, `/chunk 5`, `/chunk 10`, `/chunk 15`, `/chunk 20` cambia el tamaño usado al crear manifiestos UDYAT.
- `/estado`, `/id`, `/ayuda`, `/dkstats`, `/dkhealth`.
- Servicio HTTP de salud en el puerto `PORT` (por defecto 10000).

## Variables de entorno en Render
Obligatorias:
- `API_ID`: API ID de Telegram.
- `API_HASH`: API hash de Telegram.
- `BOT_TOKEN`: token del bot.
- `ALLOWED_IDS`: tu ID numérico de Telegram. Recomendado para restringir el uso.

Opcionales:
- `MAX_MB=0` (0 = sin límite de tamaño local configurado).
- `S3_TIMEOUT=3600`
- `S3_RETRIES=3`
- `DOWNLOAD_THREADS=4`
- `UDYAT_IPS=152.207.206.86,152.207.206.88`
- `UDYAT_CHUNK_CONFIG=udyat_chunk_size.json`

## Despliegue
1. Haz copia del proyecto actual.
2. Sube `main.py`, `requirements.txt`, `render.yaml` y este README a la raíz de tu repositorio GitHub.
3. En Render, comprueba que el servicio ejecute `python main.py` y que las variables anteriores estén configuradas.
4. Haz deploy.
5. En Telegram, envía `/chunk` y luego prueba `/chunk 2`.
6. Sube un archivo pequeño, copia el enlace devuelto y envía `/udyat <enlace>`.
7. Importa el `.udyat` en UDYAT Downloader y comprueba que descargue el archivo completo y lo reconstruya correctamente.

## Aviso técnico MUY IMPORTANTE sobre UDYAT
El manifiesto original de este repositorio tenía un único elemento dentro de `c`, con los campos `url` y `size`. Esta versión genera varios elementos con `url`, `size`, `start` y `end` (rangos inclusivos de bytes). No se encontró una especificación oficial verificable que confirme que la versión concreta de UDYAT Downloader que usas reconoce `start` y `end`. Por eso esta función es EXPERIMENTAL: puede que la app rechace el archivo o ignore los rangos. No se debe asumir que la descarga paralela funciona hasta probarla con un archivo pequeño.

Los chunks no hacen que la conexión sea automáticamente más rápida. La aplicación tiene que solicitar cada rango HTTP (`Range: bytes=inicio-fin`) y ensamblarlos en orden. Además, el servidor debe responder `206 Partial Content` y `Content-Range` correctamente.

## Seguridad y límites
- Si `ALLOWED_IDS` está vacío, cualquier usuario que encuentre el bot podrá usarlo. Configura tu ID para restringirlo.
- El manifiesto usa una IP UDYAT configurada y una URL de archivo bajo `/stream/`.
- El filesystem de Render puede ser efímero; el ajuste de `/chunk` guardado en JSON puede perderse al reiniciar o desplegar. Si sucede, vuelve a enviar el comando.
- No incluyas tokens ni claves en GitHub.
