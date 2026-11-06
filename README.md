# ToDus Telegram Bot — S3 + UDYAT + estado público DK

## Funciones implementadas directamente
- S3 anónimo: PUT, HEAD, GET, DELETE y LIST paginado.
- Los archivos se guardan directamente en la raíz de `stream`: `https://s3.todus.cu/stream/<nombre-del-archivo>`.
- Reintentos de subida, verificación HEAD y descargas por rangos cuando el servidor las admite.
- Recepción de archivos de Telegram y subida automática.
- `/lista`, `/borrar`, `/verificar`, `/descargar`, `/udyat`, `/estado`, `/id`, `/ayuda`.
- `/udyat URL` genera un manifiesto JSON `.udyat` para importar en IroS3Dl usando la IP UDYAT clásica documentada.
- `/dkstats` y `/dkhealth` consultan únicamente información pública de DK.

## Límite real de DK ToDus
La documentación del repositorio indica que el inicio de sesión DK devuelve un enlace de autorización asociado a `@DkTodusbot`. Por tanto, no existe en esa documentación un método para crear una sesión DK completa de forma independiente. Para no pedir al usuario que autorice un bot de terceros, `/dklogin`, `/dkme` y `/dkuploads` informan que esa función no está disponible y no intentan autenticarse. Tampoco se falsifican Telegram IDs ni se fabrican JWT.

Las funciones S3 y UDYAT no dependen de la cuenta DK. Los enlaces S3 directos son públicos según el repositorio; no subas archivos sensibles.

## UDYAT: limitación de red
El manifiesto usa `152.207.206.86` como IP gratuita clásica descrita por el repositorio. Render está fuera de la red ETECSA, así que una prueba desde Render no puede confirmar si el teléfono del usuario tendrá acceso gratuito. La descarga gratuita solo puede funcionar desde una conexión ETECSA compatible en Cuba. La IP puede cambiar o dejar de responder.

El manifiesto `.udyat` se construye con los campos documentados (`f`, `s`, `md5`, `c`, `h`, `original_host`, `platform`). El valor ETag se usa como `md5` porque el API expone ese dato; para archivos grandes o multipart ETag puede no ser un MD5 criptográfico real.

## Despliegue
Build: `pip install -r requirements.txt`
Start: `python main.py`

Variables obligatorias: `API_ID`, `API_HASH`, `BOT_TOKEN`.
Opcionales: `ALLOWED_IDS`, `MAX_MB`, `S3_TIMEOUT`, `S3_RETRIES`, `DOWNLOAD_THREADS`, `UDYAT_IPS`.

## Seguridad
- El bucket S3 es público y anónimo: cualquiera que conozca o enumere enlaces puede leer, borrar o listar objetos según los permisos del servidor.
- `ALLOWED_IDS` restringe el uso del bot, pero no convierte los enlaces S3 en privados.
- La verificación TLS se mantiene activa para S3. No se desactiva TLS para peticiones normales.
