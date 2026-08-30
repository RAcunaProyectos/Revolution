"""Telegram Publisher Web — Streamlit, Telethon y worker asyncio persistente.

El worker vive en un hilo/event loop del proceso Streamlit; no depende de la
conexión del navegador. Para producción, ejecutar detrás de HTTPS y definir
APP_PASSWORD en el entorno.
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import json
import os
import queue
import sqlite3
import threading
import time
import uuid
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st
from PIL import Image, ImageOps
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    ChatWriteForbiddenError,
    FloodWaitError,
    PeerIdInvalidError,
    SessionPasswordNeededError,
    UserBannedInChannelError,
    UserNotParticipantError,
    UsernameNotOccupiedError,
)

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # Permite ejecutar sin auto-refresh durante desarrollo.
    def st_autorefresh(**_: Any) -> int:
        return 0


DATA_DIR = Path(os.getenv("DATA_DIR", "/data" if Path("/data").exists() else ".data"))
DB_PATH = DATA_DIR / "publisher.db"
LEGACY_FILE = Path(__file__).resolve().parent / "publisher_data.json"
SESSION_DIR = DATA_DIR / "sessions"
MEDIA_DIR = DATA_DIR / "media"
REPORT_DIR = DATA_DIR / "reports"
RESOLVE_TIMEOUT = 8


class Database:
    """SQLite pequeño y transaccional para cuentas, plantillas, logs y reportes."""

    def __init__(self, path: Path = DB_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self) -> None:
        with self.lock, self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, api_id INTEGER NOT NULL,
                    api_hash TEXT NOT NULL, phone TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'disconnected', detail TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS templates (
                    name TEXT PRIMARY KEY, body TEXT NOT NULL DEFAULT '', image_path TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
                    level TEXT NOT NULL, message TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL, cycle INTEGER,
                    account TEXT, target TEXT, status TEXT, detail TEXT
                );
            """)
        self._migrate_legacy_json()

    def _migrate_legacy_json(self) -> None:
        """Conserva las plantillas y la cuenta de la aplicación de escritorio."""
        with self.lock:
            has_data = bool(self.conn.execute("SELECT 1 FROM accounts LIMIT 1").fetchone() or self.conn.execute("SELECT 1 FROM templates LIMIT 1").fetchone())
        if has_data or not LEGACY_FILE.is_file():
            return
        try:
            legacy = json.loads(LEGACY_FILE.read_text(encoding="utf-8"))
            credentials = legacy.get("credentials", {})
            if credentials.get("api_id") and credentials.get("api_hash"):
                self.upsert_account({"id": uuid.uuid4().hex, "name": "Cuenta principal", "api_id": credentials["api_id"], "api_hash": credentials["api_hash"], "phone": credentials.get("phone_number", ""), "enabled": True})
            for name, template in legacy.get("templates", {}).items():
                self.save_template(name, template.get("text", ""), template.get("image_path", ""))
            self.log("Datos migrados desde publisher_data.json.")
        except (OSError, ValueError, TypeError) as exc:
            self.log(f"No se pudo migrar el JSON anterior: {exc}", "WARN")

    def accounts(self) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(row) for row in self.conn.execute("SELECT * FROM accounts ORDER BY name")]

    def account(self, account_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
            return dict(row) if row else None

    def upsert_account(self, account: dict[str, Any]) -> None:
        with self.lock, self.conn:
            self.conn.execute("""INSERT INTO accounts(id,name,api_id,api_hash,phone,enabled,status,detail)
                VALUES(:id,:name,:api_id,:api_hash,:phone,:enabled,:status,:detail)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name,api_id=excluded.api_id,
                api_hash=excluded.api_hash,phone=excluded.phone,enabled=excluded.enabled""", {
                "id": account.get("id", uuid.uuid4().hex), "name": account["name"],
                "api_id": int(account["api_id"]), "api_hash": account["api_hash"],
                "phone": account["phone"], "enabled": int(account.get("enabled", True)),
                "status": account.get("status", "disconnected"), "detail": account.get("detail", ""),
            })

    def set_account_status(self, account_id: str, status: str, detail: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("UPDATE accounts SET status=?,detail=? WHERE id=?", (status, detail, account_id))

    def templates(self) -> list[dict[str, str]]:
        with self.lock:
            return [dict(row) for row in self.conn.execute("SELECT * FROM templates ORDER BY name")]

    def save_template(self, name: str, body: str, image_path: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("INSERT INTO templates(name,body,image_path) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET body=excluded.body,image_path=excluded.image_path", (name, body, image_path))

    def delete_template(self, name: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM templates WHERE name=?", (name,))

    def log(self, message: str, level: str = "INFO") -> None:
        with self.lock, self.conn:
            self.conn.execute("INSERT INTO logs(created_at,level,message) VALUES(?,?,?)", (datetime.now().isoformat(timespec="seconds"), level, message))

    def logs(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(row) for row in self.conn.execute("SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,))][::-1]

    def result(self, cycle: int, account: str, target: str, status: str, detail: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("INSERT INTO results(created_at,cycle,account,target,status,detail) VALUES(?,?,?,?,?,?)", (datetime.now().isoformat(timespec="seconds"), cycle, account, target, status, detail))

    def results(self) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(row) for row in self.conn.execute("SELECT * FROM results ORDER BY id")]


class CampaignService:
    """Event loop dedicado y cola asyncio que sobrevive a los reruns de Streamlit."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._loop_main, name="telegram-worker", daemon=True)
        self.thread.start()
        self.stop_flag = threading.Event()
        self.resume_flag = threading.Event(); self.resume_flag.set()
        self.task: Future[Any] | None = None
        self.pending_auth: dict[str, TelegramClient] = {}
        self.auth_state: dict[str, str] = {}
        self.clients: dict[str, TelegramClient] = {}

    def _loop_main(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def call(self, coroutine: Any) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=90)

    def emit(self, message: str, level: str = "INFO") -> None:
        self.db.log(message, level)

    def begin_auth(self, account: dict[str, Any]) -> str:
        try:
            self.call(self._begin_auth(account))
            return self.auth_state.get(account["id"], "error")
        except Exception as exc:
            self.emit(f"Error al pedir código para {account['name']}: {exc}", "ERROR")
            return "error"

    async def _begin_auth(self, account: dict[str, Any]) -> None:
        SESSION_DIR.mkdir(parents=True, exist_ok=True)
        client = TelegramClient(str(SESSION_DIR / account["id"]), int(account["api_id"]), account["api_hash"], connection_retries=1, request_retries=1, flood_sleep_threshold=0)
        await client.connect()
        if await client.is_user_authorized():
            self.clients[account["id"]] = client; self.auth_state[account["id"]] = "authorized"; self.db.set_account_status(account["id"], "connected", "Autorizada"); return
        await client.send_code_request(account["phone"])
        self.pending_auth[account["id"]] = client; self.auth_state[account["id"]] = "code_required"; self.db.set_account_status(account["id"], "waiting", "Código enviado")

    def complete_auth(self, account: dict[str, Any], code: str, password: str = "") -> str:
        try:
            return self.call(self._complete_auth(account, code, password))
        except Exception as exc:
            self.emit(f"Error de autenticación en {account['name']}: {exc}", "ERROR"); return "error"

    async def _complete_auth(self, account: dict[str, Any], code: str, password: str) -> str:
        client = self.pending_auth.get(account["id"])
        if not client: return "error"
        try:
            await client.sign_in(phone=account["phone"], code=code)
        except SessionPasswordNeededError:
            if not password: self.auth_state[account["id"]] = "password_required"; return "password_required"
            await client.sign_in(password=password)
        self.clients[account["id"]] = client; self.pending_auth.pop(account["id"], None); self.auth_state[account["id"]] = "authorized"; self.db.set_account_status(account["id"], "connected", "Autorizada"); self.emit(f"Cuenta {account['name']} autorizada."); return "authorized"

    def start(self, accounts: list[dict[str, Any]], payload: dict[str, str], settings: dict[str, Any]) -> bool:
        if self.running: return False
        self.stop_flag.clear(); self.resume_flag.set(); self.task = asyncio.run_coroutine_threadsafe(self._campaign(accounts, payload, settings), self.loop); return True

    @property
    def running(self) -> bool:
        return bool(self.task and not self.task.done())

    def pause(self) -> None:
        self.resume_flag.clear(); self.emit("Campaña pausada por el usuario.", "WARN")

    def resume(self) -> None:
        self.resume_flag.set(); self.emit("Campaña reanudada.")

    def stop(self) -> None:
        self.stop_flag.set(); self.resume_flag.set(); self.emit("Detención solicitada.", "WARN")

    async def _campaign(self, accounts: list[dict[str, Any]], payload: dict[str, str], settings: dict[str, Any]) -> None:
        connected: list[tuple[dict[str, Any], TelegramClient]] = []
        try:
            for account in accounts:
                client = self.clients.get(account["id"])
                if client and client.is_connected(): connected.append((account, client))
                else: self.emit(f"{account['name']} no está autorizada; se omite.", "WARN")
            if not connected: self.emit("No hay cuentas autorizadas conectadas.", "ERROR"); return
            scheduled = str(settings.get("scheduled", "")).strip()
            if scheduled:
                try:
                    wait = (datetime.strptime(scheduled, "%Y-%m-%d %H:%M") - datetime.now()).total_seconds()
                    if wait > 0:
                        self.emit(f"Inicio programado para {scheduled}.")
                        await self._wait(wait, "Inicio programado")
                except ValueError:
                    self.emit("Fecha programada inválida; se inicia ahora.", "WARN")
            repeat = max(30, int(settings.get("repeat", 30))); cycle = 0
            while not self.stop_flag.is_set():
                cycle += 1; started = time.monotonic(); self.emit(f"Ciclo {cycle} iniciado.")
                await self._cycle(connected, payload, float(settings.get("delay", 2)), cycle)
                if self.stop_flag.is_set(): break
                remaining = max(0, repeat * 60 - (time.monotonic() - started))
                await self._wait(remaining, "Próxima publicación")
        except Exception as exc: self.emit(f"Error de campaña: {type(exc).__name__}: {exc}", "ERROR")
        finally: self.emit("Campaña finalizada.")

    async def _cycle(self, clients: list[tuple[dict[str, Any], TelegramClient]], payload: dict[str, str], delay: float, cycle: int) -> None:
        pending: asyncio.Queue[str | int] = asyncio.Queue(); targets = self.targets(payload["destinations"])
        for target in targets: pending.put_nowait(target)
        async def worker(account: dict[str, Any], client: TelegramClient) -> None:
            while not self.stop_flag.is_set():
                await self._wait_resume()
                try: target = pending.get_nowait()
                except asyncio.QueueEmpty: return
                ok, detail = await self._send(client, target, payload); self.db.result(cycle, account["name"], str(target), "success" if ok else "failed", detail); self.emit(f"{'✓' if ok else '✗'} {target}: {detail}", "INFO" if ok else "WARN"); pending.task_done()
                await self._wait(delay, "Pausa entre destinos")
        await asyncio.gather(*(worker(account, client) for account, client in clients))

    async def _send(self, client: TelegramClient, target: str | int, payload: dict[str, str]) -> tuple[bool, str]:
        temporary: Path | None = None
        try:
            entity = await asyncio.wait_for(client.get_entity(target), timeout=RESOLVE_TIMEOUT); image = payload.get("image", "")
            if image: image, temporary = self._photo(image); await client.send_file(entity, image, caption=payload.get("text") or None, force_document=False)
            else: await client.send_message(entity, payload["text"])
            return True, "Enviado"
        except FloodWaitError as exc: return False, f"FloodWait ({exc.seconds}s), omitido"
        except asyncio.TimeoutError: return False, "Tiempo agotado, omitido"
        except (UsernameNotOccupiedError, ChannelPrivateError, PeerIdInvalidError, ValueError): return False, "No encontrado o inaccesible"
        except (ChatWriteForbiddenError, UserBannedInChannelError, UserNotParticipantError): return False, "Sin permiso"
        except Exception as exc: return False, f"{type(exc).__name__}: {exc}"
        finally:
            if temporary: temporary.unlink(missing_ok=True)

    async def _wait_resume(self) -> None:
        while not self.resume_flag.is_set() and not self.stop_flag.is_set(): self.emit("Campaña pausada.", "WARN"); await asyncio.sleep(1)

    async def _wait(self, seconds: float, label: str) -> None:
        left = max(0, int(seconds))
        while left and not self.stop_flag.is_set():
            await self._wait_resume(); self.emit(f"{label}: {left // 60:02}:{left % 60:02}", "INFO"); await asyncio.sleep(1); left -= 1

    async def scan(self, account: dict[str, Any], targets: list[str | int]) -> list[str]:
        client = self.clients.get(account["id"]); valid: list[str] = []
        if not client: self.emit("La cuenta debe autorizarse antes de escanear.", "ERROR"); return valid
        for target in targets:
            try:
                entity = await asyncio.wait_for(client.get_entity(target), timeout=RESOLVE_TIMEOUT); perm = await client.get_permissions(entity, "me")
                if getattr(perm, "send_messages", True) and getattr(perm, "send_media", True): valid.append(str(target)); self.emit(f"✓ {target}: apto para texto y medios")
                else: self.emit(f"✗ {target}: sin permisos", "WARN")
            except Exception as exc: self.emit(f"✗ {target}: {type(exc).__name__}", "WARN")
        return valid

    @staticmethod
    def targets(value: str) -> list[str | int]:
        out, seen = [], set()
        for item in value.replace(",", "\n").splitlines():
            item = item.strip()
            if item and not item.startswith("#") and item not in seen: seen.add(item); out.append(int(item) if item.lstrip("-").isdigit() else item)
        return out

    @staticmethod
    def _photo(path: str) -> tuple[str, Path | None]:
        source = Path(path)
        if source.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}: return str(source), None
        result = Path(tempfile.gettempdir()) / f"telegram_{uuid.uuid4().hex}.jpg"
        with Image.open(source) as original:
            image = ImageOps.exif_transpose(original)
            if image.mode != "RGB": image = image.convert("RGB")
            image.save(result, "JPEG", quality=92)
        return str(result), result


@st.cache_resource(show_spinner=False)
def resources() -> tuple[Database, CampaignService]:
    db = Database(); return db, CampaignService(db)


def authenticated() -> bool:
    if st.session_state.get("authenticated"): return True
    st.title("Telegram Publisher")
    st.caption("Acceso protegido")
    configured = os.getenv("APP_PASSWORD", "")
    if not configured:
        # Streamlit Community Cloud expone los valores configurados en
        # Settings → Secrets a través de st.secrets, no de os.environ.
        try:
            configured = str(st.secrets.get("APP_PASSWORD", ""))
        except Exception:
            configured = ""
    if not configured:
        st.error("Definí APP_PASSWORD en el entorno antes de usar la aplicación.")
        return False
    with st.form("login"):
        password = st.text_input("Contraseña maestra", type="password")
        if st.form_submit_button("Entrar", use_container_width=True):
            if hmac.compare_digest(password, configured): st.session_state.authenticated = True; st.rerun()
            else: st.error("Contraseña incorrecta.")
    return False


def save_uploaded(uploaded: Any) -> str:
    MEDIA_DIR.mkdir(parents=True, exist_ok=True); suffix = Path(uploaded.name).suffix.lower() or ".bin"; path = MEDIA_DIR / f"{uuid.uuid4().hex}{suffix}"; path.write_bytes(uploaded.getbuffer()); return str(path)


def render_accounts(db: Database, service: CampaignService) -> None:
    st.subheader("Cuentas y autenticación")
    with st.expander("Agregar / editar cuenta", expanded=not bool(db.accounts())):
        with st.form("account_form"):
            name = st.text_input("Nombre visible"); api_id = st.number_input("API ID", min_value=1, step=1); api_hash = st.text_input("API Hash", type="password"); phone = st.text_input("Teléfono internacional", placeholder="+549..."); enabled = st.checkbox("Cuenta activa", True)
            if st.form_submit_button("Guardar cuenta", use_container_width=True):
                db.upsert_account({"id": uuid.uuid4().hex, "name": name.strip(), "api_id": api_id, "api_hash": api_hash.strip(), "phone": phone.strip(), "enabled": enabled}); st.success("Cuenta guardada."); st.rerun()
    for account in db.accounts():
        state = account["status"]; color = {"connected": "🟢", "waiting": "🟡", "error": "🔴"}.get(state, "⚪")
        st.markdown(f"**{color} {account['name']}** · {account['phone']} · {account['detail'] or 'Sin autorizar'}")
        c1, c2 = st.columns(2)
        if c1.button("Pedir código", key=f"auth_{account['id']}", use_container_width=True): service.begin_auth(account); st.rerun()
        if service.auth_state.get(account["id"]) in {"code_required", "password_required"}:
            code = st.text_input("Código recibido", key=f"code_{account['id']}")
            password = st.text_input("Contraseña 2FA (si la solicita)", type="password", key=f"pass_{account['id']}")
            if c2.button("Validar", key=f"verify_{account['id']}", use_container_width=True): service.complete_auth(account, code, password); st.rerun()


def render_content(db: Database) -> tuple[str, str]:
    st.subheader("Contenido y plantillas")
    templates = db.templates(); names = [t["name"] for t in templates] or ["Nueva plantilla"]
    selected = st.selectbox("Plantilla", names)
    current = next((t for t in templates if t["name"] == selected), {"name": selected, "body": "", "image_path": ""})
    body = st.text_area("Mensaje", current["body"], height=220, placeholder="Texto que acompañará la foto")
    uploaded = st.file_uploader("Foto (opcional)", type=["jpg", "jpeg", "png", "jfif", "webp", "gif"])
    image_path = current.get("image_path", "")
    if uploaded:
        image_path = save_uploaded(uploaded); st.image(Image.open(uploaded), caption="Vista previa", use_container_width=True)
    elif image_path and Path(image_path).is_file(): st.image(image_path, caption="Imagen guardada", use_container_width=True)
    c1, c2 = st.columns(2)
    if c1.button("Guardar plantilla", use_container_width=True):
        name = selected if selected != "Nueva plantilla" else f"Plantilla {len(templates)+1}"; db.save_template(name, body, image_path); st.success("Plantilla guardada."); st.rerun()
    if c2.button("Eliminar plantilla", use_container_width=True) and selected != "Nueva plantilla": db.delete_template(selected); st.rerun()
    return body, image_path


def render_destinations(db: Database, service: CampaignService) -> str:
    st.subheader("Destinos")
    default = st.session_state.get("destinations", "")
    uploaded = st.file_uploader("Importar TXT o CSV", type=["txt", "csv"])
    if uploaded:
        if uploaded.name.lower().endswith(".csv"): default = "\n".join(row[0].strip() for row in csv.reader(uploaded.getvalue().decode("utf-8-sig").splitlines()) if row and row[0].strip())
        else: default = uploaded.getvalue().decode("utf-8")
        st.session_state.destinations = default
    value = st.text_area("Un @username, enlace o ID por línea", default, height=220); st.session_state.destinations = value
    if st.button("Escanear permisos con la primera cuenta", use_container_width=True):
        accounts = [a for a in db.accounts() if a["enabled"]]
        if accounts:
            valid = service.call(service.scan(accounts[0], CampaignService.targets(value))); st.session_state.destinations = "\n".join(valid); st.success(f"Conservados {len(valid)} destinos aptos."); st.rerun()
    return value


def render_control(db: Database, service: CampaignService, body: str, image: str, destinations: str) -> None:
    st.subheader("Control de campaña")
    delay = st.slider("Pausa entre destinos (segundos)", 1.0, 30.0, 2.0, 0.5); repeat = st.number_input("Repetir tanda cada (minutos)", min_value=30, value=30, step=5)
    scheduled = st.text_input("Inicio programado opcional (AAAA-MM-DD HH:MM)")
    c1, c2, c3 = st.columns(3)
    accounts = [a for a in db.accounts() if a["enabled"]]
    if c1.button("▶ Iniciar envíos", type="primary", use_container_width=True):
        if not accounts or not destinations.strip() or not (body.strip() or image): st.error("Necesitás cuenta autorizada, destinos y contenido.")
        else: service.start(accounts, {"text": body.strip(), "image": image, "destinations": destinations}, {"delay": delay, "repeat": repeat, "scheduled": scheduled}); st.success("Worker iniciado; podés cerrar el navegador.")
    if service.resume_flag.is_set():
        if c2.button("⏸ Pausar", use_container_width=True): service.pause()
    elif c2.button("▶ Reanudar", use_container_width=True): service.resume()
    if c3.button("■ Detener", use_container_width=True): service.stop()
    st.metric("Estado", "Ejecutando" if service.running else "Detenido")


def export_report(db: Database) -> None:
    rows = db.results()
    if not rows: st.info("Todavía no hay resultados."); return
    REPORT_DIR.mkdir(parents=True, exist_ok=True); path = REPORT_DIR / f"reporte_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["created_at", "cycle", "account", "target", "status", "detail"]); writer.writeheader(); writer.writerows(rows)
    st.download_button("Descargar reporte CSV", path.read_bytes(), file_name=path.name, mime="text/csv", use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Telegram Publisher", page_icon="✈️", layout="wide", initial_sidebar_state="expanded")
    st.markdown("<style>div.stButton > button {min-height: 44px;} .block-container {max-width: 1200px; padding-top: 1rem;}</style>", unsafe_allow_html=True)
    if not authenticated(): return
    db, service = resources(); st_autorefresh(interval=2000, key="publisher_refresh")
    st.sidebar.title("✈️ Publisher Pro"); st.sidebar.caption("Worker persistente 24/7")
    if st.sidebar.button("Cerrar sesión", use_container_width=True): st.session_state.authenticated = False; st.rerun()
    page = st.sidebar.radio("Navegación", ["Dashboard", "Contenido", "Destinos", "Cuentas", "Control"])
    if page == "Dashboard":
        st.title("Dashboard"); results = db.results(); logs = db.logs(12); c1,c2,c3,c4=st.columns(4); c1.metric("Enviados", sum(r["status"]=="success" for r in results)); c2.metric("Fallidos", sum(r["status"]=="failed" for r in results)); c3.metric("Cuentas", len([a for a in db.accounts() if a["enabled"]])); c4.metric("Worker", "Activo" if service.running else "En espera"); st.subheader("Actividad reciente"); st.code("\n".join(f"[{x['created_at']}] {x['message']}" for x in logs) or "Sin actividad", language=None)
        if results:
            import io
            output = io.StringIO(); writer = csv.DictWriter(output, fieldnames=["created_at", "cycle", "account", "target", "status", "detail"]); writer.writeheader(); writer.writerows(results)
            st.download_button("Descargar reporte CSV", output.getvalue(), "telegram_report.csv", "text/csv", use_container_width=True)
    elif page == "Contenido": st.session_state.content = render_content(db)
    elif page == "Destinos": render_destinations(db, service)
    elif page == "Cuentas": render_accounts(db, service)
    else:
        body, image = st.session_state.get("content", ("", "")); destinations = st.session_state.get("destinations", ""); render_control(db, service, body, image, destinations); st.divider(); export_report(db)


if __name__ == "__main__":
    main()
