# Telegram Publisher Web

## Inicio con Docker Compose

1. Copiá `.env.example` como `.env` y definí una contraseña maestra fuerte.
2. Ejecutá `docker compose up -d --build`.
3. Abrí `http://IP_DEL_SERVIDOR:8501` desde el teléfono o la computadora.
4. Revisá el worker con `docker compose logs -f telegram-publisher`.
5. Actualizá con `docker compose pull` (si usás una imagen publicada) o `docker compose up -d --build`.

El volumen `telegram_publisher_data` conserva `publisher.db`, sesiones de Telethon, medios y reportes aunque el contenedor se recree. Hacé copias periódicas con `docker run --rm -v telegram_publisher_data:/data -v "$PWD":/backup alpine tar czf /backup/telegram-data.tgz -C /data .`.

## Portainer

En **Stacks > Add stack**, pegá el contenido de `docker-compose.yml`, cargá `APP_PASSWORD` en las variables de entorno y desplegá. Para conservar datos, no elimines el volumen al actualizar el stack.

## Railway / Render

Usá el `Dockerfile`, exponé el puerto `8501` y definí `APP_PASSWORD`. Activá un volumen persistente (Railway Volume o Render Persistent Disk) montado en `/data`; sin disco persistente se perderán las sesiones y plantillas al redeploy.

## VPS Ubuntu

Instalá Docker y Compose, copiá el proyecto, creá `.env`, y ejecutá:

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f telegram-publisher
```

Para producción, colocá un proxy HTTPS (Caddy/Nginx) delante de `8501` y no expongas el puerto sin autenticación. Autorizá previamente las cuentas en los grupos; la app no se une automáticamente a comunidades mediante enlaces.
