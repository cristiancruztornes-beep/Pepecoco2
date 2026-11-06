# ToDus Telegram Bot — S3 + DK ToDus + UDYAT

## Funciones
- S3: PUT, HEAD, GET, DELETE, LIST paginado.
- Subida con streaming, reintentos 429/5xx/red, comprobación HEAD, ETag y rate-limit.
- Descargas con rangos y varios hilos.
- Prefijo persistente recomendado: `users/df/<telegram_id>/`.
- DK backend: auth init/check/verify, perfil, listado de subidas, registro de subidas, stats y health.
- UDYAT: detección de IP y generación de manifiesto `.udyat` para importar en IroS3Dl.
- Página de salud para Render.

## Despliegue
Build: `pip install -r requirements.txt`
Start: `python main.py`

Variables obligatorias: `API_ID`, `API_HASH`, `BOT_TOKEN`.
Opcionales: `ALLOWED_IDS`, `MAX_MB`, `S3_TIMEOUT`, `S3_RETRIES`, `DOWNLOAD_THREADS`,
`DK_BASE`, `DK_AUTH_WAIT`, `UDYAT_IPS`.

## Comandos
`/start`, `/ayuda`, `/id`, `/estado`, `/lista`, `/borrar nombre`,
`/verificar URL`, `/descargar URL`, `/udyat URL`,
`/dklogin`, `/dkme`, `/dkuploads`, `/dkstats`, `/dkhealth`.

## Importante: DK ToDus
El repositorio documenta que `/api/v1/auth/verify` acepta un Telegram ID arbitrario si solo se le entrega un temp_token. Por eso el bot NO completa el login directamente: espera a que `/api/v1/auth/check` informe explícitamente `verified`, `authenticated`, `completed` o `success`. El comportamiento real del backend puede cambiar; si su respuesta no expone uno de esos estados, el inicio de sesión fallará de forma cerrada. No se implementan endpoints administrativos de ban/unban ni se falsifica la identidad de otro usuario.
El JWT se guarda solo en memoria; al reiniciar Render, el usuario debe volver a ejecutar `/dklogin`. No guardar JWT en el repositorio ni en logs.

## Importante: UDYAT
UDYAT depende de que la conexión salga por la red ETECSA en Cuba. Un bot alojado en Render hace las peticiones desde los centros de datos de Render, no desde el teléfono del usuario en Cuba. Por tanto, el bot solo genera el manifiesto `.udyat` y prueba la disponibilidad desde su propio servidor; eso NO demuestra que la IP sea gratuita desde la conexión del usuario. El `.udyat` debe importarse en IroS3Dl desde una conexión ETECSA compatible.
La IP UDYAT usa un certificado autofirmado. La verificación TLS se desactiva únicamente para el preflight de las IPs UDYAT indicadas, nunca para el S3 normal ni para DK. No se debe usar esta opción con hosts arbitrarios.

## Seguridad y límites
- S3 es público y anónimo según el repo: cualquiera con el enlace puede leerlo; cualquiera puede intentar borrar/listar objetos. No subir datos sensibles.
- `ALLOWED_IDS` limita quién usa el bot, pero no vuelve privados los enlaces S3.
- La API/servidor, Telegram y Render imponen límites propios. `MAX_MB=0` solo elimina el límite local configurado.
- El formato de enlace cifrado `dktodus.wev.cu/download/<token>` no se genera: el repositorio describe cifrado ligado a dispositivo/JWT, pero no documenta una implementación interoperable completa ni claves. No se intenta romper ese cifrado.
