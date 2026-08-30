"""RevolutionPM — Streamlit, Telethon y worker asyncio persistente.

El worker vive en un hilo/event loop del proceso Streamlit; no depende de la
conexión del navegador. Para producción, ejecutar detrás de HTTPS y definir
APP_PASSWORD en el entorno.
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import html
import json
import os
import queue
import sqlite3
import threading
import time
import tempfile
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
        self.progress_lock = threading.Lock()
        self.next_run_at: float | None = None
        self.current_cycle = 0
        self.current_done = 0
        self.current_total = 0

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

    def status_snapshot(self) -> dict[str, Any]:
        with self.progress_lock:
            return {
                "running": self.running,
                "paused": not self.resume_flag.is_set(),
                "next_run_at": self.next_run_at,
                "cycle": self.current_cycle,
                "done": self.current_done,
                "total": self.current_total,
            }

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
                        self.next_run_at = time.time() + wait
                        await self._wait(wait, "Inicio programado")
                except ValueError:
                    self.emit("Fecha programada inválida; se inicia ahora.", "WARN")
            repeat = max(30, int(settings.get("repeat", 30))); cycle = 0
            while not self.stop_flag.is_set():
                cycle += 1; self.current_cycle = cycle; self.next_run_at = None; started = time.monotonic(); self.emit(f"Ciclo {cycle} iniciado.")
                await self._cycle(connected, payload, float(settings.get("delay", 2)), cycle)
                if self.stop_flag.is_set(): break
                remaining = max(0, repeat * 60 - (time.monotonic() - started))
                self.next_run_at = time.time() + remaining
                await self._wait(remaining, "Próxima publicación")
        except Exception as exc: self.emit(f"Error de campaña: {type(exc).__name__}: {exc}", "ERROR")
        finally:
            self.next_run_at = None
            self.emit("Campaña finalizada.")

    async def _cycle(self, clients: list[tuple[dict[str, Any], TelegramClient]], payload: dict[str, str], delay: float, cycle: int) -> None:
        pending: asyncio.Queue[str | int] = asyncio.Queue(); targets = self.targets(payload["destinations"])
        with self.progress_lock:
            self.current_done = 0
            self.current_total = len(targets)
        for target in targets: pending.put_nowait(target)
        async def worker(account: dict[str, Any], client: TelegramClient) -> None:
            while not self.stop_flag.is_set():
                await self._wait_resume()
                try: target = pending.get_nowait()
                except asyncio.QueueEmpty: return
                ok, detail = await self._send(client, target, payload); self.db.result(cycle, account["name"], str(target), "success" if ok else "failed", detail); self.emit(f"{'✓' if ok else '✗'} {target}: {detail}", "INFO" if ok else "WARN"); pending.task_done()
                with self.progress_lock:
                    self.current_done += 1
                await self._wait(delay, "Pausa entre destinos")
        await asyncio.gather(*(worker(account, client) for account, client in clients))

    async def _send(self, client: TelegramClient, target: str | int, payload: dict[str, str]) -> tuple[bool, str]:
        temporary: Path | None = None
        try:
            entity = await asyncio.wait_for(client.get_entity(target), timeout=RESOLVE_TIMEOUT); image = payload.get("image", "")
            message = self.expand_spintax(payload.get("text", ""))
            if image: image, temporary = self._photo(image); await client.send_file(entity, image, caption=message or None, force_document=False)
            else: await client.send_message(entity, message)
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
    def expand_spintax(value: str) -> str:
        """Expande expresiones simples como {Hola|Buenas} justo antes de enviar."""
        import random
        import re
        return re.sub(r"\{([^{}|]+(?:\|[^{}|]+)+)\}", lambda match: random.choice(match.group(1).split("|")), value)

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


def inject_theme() -> None:
    """Design system de RevolutionPM: dark glassmorphism, responsive y táctil."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');
        :root { --rpm-bg:#05070f; --rpm-panel:rgba(15,23,42,.68); --rpm-cyan:#00e5ff; --rpm-violet:#8a2be2; --rpm-green:#00ff66; --rpm-red:#ff0055; --rpm-text:#e6f7ff; --rpm-muted:#87a0b8; }
        .stApp { background: radial-gradient(circle at 12% 0%, rgba(0,229,255,.11), transparent 34%), radial-gradient(circle at 90% 18%, rgba(138,43,226,.14), transparent 30%), var(--rpm-bg); color:var(--rpm-text); font-family:'Inter',sans-serif; }
        .stApp header { background:transparent; }
        .block-container { max-width:1500px; padding:1.25rem 2rem 3rem; }
        section[data-testid="stSidebar"] { background:linear-gradient(180deg,rgba(7,12,27,.98),rgba(5,7,15,.96)); border-right:1px solid rgba(0,229,255,.16); }
        section[data-testid="stSidebar"] > div { padding:1.25rem .9rem; }
        h1,h2,h3 { font-family:'Space Grotesk','Inter',sans-serif; letter-spacing:-.025em; }
        .rpm-hero { display:flex; align-items:center; justify-content:space-between; gap:1rem; padding:1.5rem 1.8rem; margin-bottom:1rem; border:1px solid rgba(0,229,255,.24); border-radius:20px; background:linear-gradient(135deg,rgba(15,23,42,.82),rgba(10,13,30,.68)); box-shadow:0 8px 32px rgba(0,0,0,.37), inset 0 0 42px rgba(0,229,255,.035); animation:fadeInUp .55s ease both; }
        .rpm-kicker { color:var(--rpm-cyan); font-size:.7rem; font-weight:700; letter-spacing:.18em; margin-bottom:.35rem; }
        .rpm-brand { margin:0; font-family:'Space Grotesk',sans-serif; font-size:clamp(2rem,4vw,3.4rem); font-weight:700; text-shadow:0 0 10px rgba(0,229,255,.8),0 0 20px rgba(0,229,255,.4); }
        .rpm-brand span { color:var(--rpm-cyan); }
        .rpm-tagline { margin:.35rem 0 0; color:var(--rpm-muted); font-size:.85rem; }
        .rpm-online { display:flex; align-items:center; gap:.55rem; color:var(--rpm-green); font-size:.76rem; font-weight:700; letter-spacing:.1em; white-space:nowrap; }
        .rpm-pulse { width:10px; height:10px; border-radius:50%; background:var(--rpm-green); box-shadow:0 0 8px var(--rpm-green),0 0 18px var(--rpm-green); animation:neonPulse 1.6s ease-in-out infinite; }
        .rpm-kpis { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.8rem; margin-bottom:1.1rem; }
        .rpm-kpi { min-height:92px; padding:1rem 1.15rem; border:1px solid rgba(0,229,255,.15); border-radius:15px; background:rgba(15,23,42,.65); backdrop-filter:blur(16px); box-shadow:0 8px 32px rgba(0,0,0,.25); animation:fadeInUp .6s ease both; }
        .rpm-kpi:nth-child(2){border-color:rgba(255,0,85,.3)} .rpm-kpi:nth-child(3){border-color:rgba(138,43,226,.3)} .rpm-kpi:nth-child(4){border-color:rgba(0,229,255,.3)}
        .rpm-kpi-label { color:var(--rpm-muted); font-size:.67rem; font-weight:700; letter-spacing:.12em; }
        .rpm-kpi-value { display:block; margin-top:.25rem; font-family:'Space Grotesk',sans-serif; font-size:1.8rem; font-weight:700; background:linear-gradient(90deg,#fff,var(--rpm-cyan)); -webkit-background-clip:text; background-clip:text; color:transparent; }
        .rpm-kpi-sub { color:#6b849c; font-size:.72rem; }
        .rpm-panel-title { display:flex; align-items:center; justify-content:space-between; margin:.25rem 0 .75rem; }
        .rpm-panel-title h3 { margin:0; font-size:1.05rem; }
        .rpm-panel-title small { color:var(--rpm-muted); }
        .rpm-terminal { overflow:hidden; margin:.2rem 0 1rem; border:1px solid rgba(0,229,255,.35); border-radius:14px; background:#020408; box-shadow:0 0 24px rgba(0,229,255,.1); }
        .rpm-terminal-head { display:flex; align-items:center; gap:.4rem; padding:.6rem .8rem; border-bottom:1px solid rgba(0,229,255,.16); color:#7e9bae; font:600 .68rem 'JetBrains Mono','Fira Code',monospace; }
        .rpm-dot { width:9px; height:9px; border-radius:50%; } .rpm-dot.red{background:#ff5577}.rpm-dot.yellow{background:#ffd166}.rpm-dot.green{background:#00ff66}
        .rpm-led { width:7px; height:7px; margin-left:auto; border-radius:50%; background:var(--rpm-cyan); box-shadow:0 0 10px var(--rpm-cyan); animation:neonPulse 1.2s ease-in-out infinite; }
        .rpm-terminal pre { min-height:230px; max-height:340px; overflow:auto; margin:0; padding:1rem; color:#9fffe1; white-space:pre-wrap; font: .72rem/1.55 'JetBrains Mono','Fira Code',monospace; }
        .rpm-cursor { animation:blink 1s steps(2,start) infinite; color:var(--rpm-cyan); }
        .rpm-account { display:flex; align-items:center; gap:.7rem; padding:.7rem .85rem; margin:.45rem 0; border:1px solid rgba(0,229,255,.12); border-radius:11px; background:rgba(15,23,42,.55); }
        .rpm-account-name { flex:1; font-size:.83rem; font-weight:600; } .rpm-account-detail { color:var(--rpm-muted); font-size:.7rem; }
        .rpm-badge { padding:.22rem .48rem; border-radius:999px; font-size:.6rem; font-weight:700; letter-spacing:.06em; }
        .rpm-badge.connected{color:#001b0c;background:var(--rpm-green)} .rpm-badge.waiting{color:#281800;background:#ffd166} .rpm-badge.error{color:#fff;background:var(--rpm-red)} .rpm-badge.disconnected{color:#d6e4ed;background:#30465a}
        .rpm-progress-wrap { padding:1rem; margin-bottom:1rem; border:1px solid rgba(0,229,255,.16); border-radius:14px; background:rgba(15,23,42,.58); }
        .rpm-progress-top { display:flex; justify-content:space-between; color:var(--rpm-muted); font-size:.76rem; margin-bottom:.55rem; }
        .rpm-progress-bar { height:9px; overflow:hidden; border-radius:20px; background:#111d31; } .rpm-progress-fill { height:100%; border-radius:20px; background:linear-gradient(90deg,var(--rpm-cyan),var(--rpm-violet)); box-shadow:0 0 15px rgba(0,229,255,.55); transition:width .35s ease; }
        .rpm-note { padding:.7rem .85rem; border-left:3px solid var(--rpm-cyan); border-radius:0 9px 9px 0; background:rgba(0,229,255,.07); color:#a6c3d4; font-size:.76rem; }
        .rpm-empty { display:grid; place-items:center; min-height:120px; border:1px dashed rgba(0,229,255,.2); border-radius:12px; color:#607d92; font-size:.8rem; text-align:center; }
        div[data-testid="stButton"] > button { min-height:45px; border:1px solid rgba(0,229,255,.27); border-radius:10px; background:linear-gradient(135deg,rgba(0,229,255,.16),rgba(124,77,255,.2)); color:#eaffff; font-weight:700; transition:all .3s cubic-bezier(.4,0,.2,1); }
        div[data-testid="stButton"] > button:hover { transform:translateY(-3px); border-color:var(--rpm-cyan); box-shadow:0 0 20px rgba(0,229,255,.6); }
        div[data-testid="stButton"] > button[kind="primary"] { border:0; background:linear-gradient(135deg,#00e5ff 0%,#7c4dff 100%); color:#03101a; box-shadow:0 0 14px rgba(0,229,255,.28); }
        div[data-testid="stTextInput"] input, div[data-testid="stTextArea"] textarea, div[data-testid="stNumberInput"] input { border:1px solid rgba(0,229,255,.2); border-radius:9px; background:rgba(2,4,8,.6); color:var(--rpm-text); }
        div[data-testid="stTextInput"] input:focus, div[data-testid="stTextArea"] textarea:focus, div[data-testid="stNumberInput"] input:focus { border-color:var(--rpm-cyan); box-shadow:0 0 0 .12rem rgba(0,229,255,.18),0 0 15px rgba(0,229,255,.18); }
        div[data-testid="stFileUploader"] section { border:1px dashed rgba(0,229,255,.3); border-radius:12px; background:rgba(0,229,255,.035); }
        div[data-testid="stVerticalBlockBorderWrapper"] { border-color:rgba(0,229,255,.16); border-radius:16px; background:rgba(15,23,42,.36); }
        div[data-testid="stExpander"] { border-color:rgba(0,229,255,.16); background:rgba(15,23,42,.32); border-radius:13px; }
        div[data-testid="stMetric"] { border:1px solid rgba(0,229,255,.14); border-radius:12px; background:rgba(15,23,42,.52); padding:.7rem; }
        [data-testid="stSidebar"] .stRadio label { border-radius:9px; padding:.4rem .55rem; }
        [data-testid="stSidebar"] .stRadio label:hover { background:rgba(0,229,255,.08); }
        @keyframes fadeInUp { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:translateY(0)} }
        @keyframes neonPulse { 0%,100%{opacity:.55;transform:scale(.9)} 50%{opacity:1;transform:scale(1.15)} }
        @keyframes blink { 50%{opacity:0} }
        @media (max-width:800px) { .block-container{padding:1rem .8rem 2rem}.rpm-hero{align-items:flex-start;flex-direction:column;padding:1.15rem}.rpm-kpis{grid-template-columns:repeat(2,minmax(0,1fr));gap:.55rem}.rpm-kpi{min-height:78px;padding:.75rem}.rpm-kpi-value{font-size:1.45rem}.rpm-terminal pre{min-height:190px}.rpm-brand{font-size:2.15rem} }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _countdown(epoch: float | None) -> str:
    if not epoch:
        return "—"
    left = max(0, int(epoch - time.time()))
    return f"{left // 3600:02}:{left % 3600 // 60:02}:{left % 60:02}"


def _service_snapshot(service: CampaignService) -> dict[str, Any]:
    """Lee el estado incluso si Streamlit conserva un worker de una versión anterior."""
    reader = getattr(service, "status_snapshot", None)
    if callable(reader):
        return reader()
    return {
        "running": bool(getattr(service, "running", False)),
        "paused": not getattr(service, "resume_flag", threading.Event()).is_set(),
        "next_run_at": getattr(service, "next_run_at", None),
        "cycle": getattr(service, "current_cycle", 0),
        "done": getattr(service, "current_done", 0),
        "total": getattr(service, "current_total", 0),
    }


def authenticated() -> bool:
    if st.session_state.get("authenticated"):
        return True
    st.markdown('<div class="rpm-hero"><div><div class="rpm-kicker">SECURE CONTROL PLANE</div><h1 class="rpm-brand">Revolution<span>PM</span></h1><p class="rpm-tagline">NEXT-GEN TELEGRAM AUTOMATION ENGINE — V2.0 PRO</p></div><div class="rpm-online"><span class="rpm-pulse"></span>SYSTEM ONLINE</div></div>', unsafe_allow_html=True)
    st.caption("Acceso protegido")
    configured = os.getenv("APP_PASSWORD", "")
    if not configured:
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
            if hmac.compare_digest(password, configured):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Contraseña incorrecta.")
    return False


def save_uploaded(uploaded: Any) -> str:
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    suffix = Path(uploaded.name).suffix.lower() or ".bin"
    path = MEDIA_DIR / f"{uuid.uuid4().hex}{suffix}"
    path.write_bytes(uploaded.getbuffer())
    return str(path)


def render_brand_header(db: Database, service: CampaignService) -> None:
    results = db.results()
    sent = sum(row["status"] == "success" for row in results)
    failed = sum(row["status"] == "failed" for row in results)
    accounts = [account for account in db.accounts() if account["enabled"]]
    snapshot = _service_snapshot(service)
    worker = "PAUSED" if snapshot["paused"] else ("RUNNING" if snapshot["running"] else "STANDBY")
    worker_color = "#ffd166" if snapshot["paused"] else ("#00ff66" if snapshot["running"] else "#00e5ff")
    st.markdown(f'<div class="rpm-hero"><div><div class="rpm-kicker">NEXT-GEN TELEGRAM AUTOMATION ENGINE — V2.0 PRO</div><h1 class="rpm-brand">Revolution<span>PM</span></h1><p class="rpm-tagline">Control inteligente de campañas, presets y rotación multicuenta.</p></div><div class="rpm-online"><span class="rpm-pulse"></span>SYSTEM ONLINE</div></div><div class="rpm-kpis"><div class="rpm-kpi"><span class="rpm-kpi-label">MESSAGES SENT</span><strong class="rpm-kpi-value">{sent:,}</strong><span class="rpm-kpi-sub">entregas confirmadas</span></div><div class="rpm-kpi"><span class="rpm-kpi-label">FAILED / SKIPPED</span><strong class="rpm-kpi-value">{failed:,}</strong><span class="rpm-kpi-sub">sin reintentos eternos</span></div><div class="rpm-kpi"><span class="rpm-kpi-label">ACTIVE ACCOUNTS</span><strong class="rpm-kpi-value">{len(accounts):,}</strong><span class="rpm-kpi-sub">credenciales habilitadas</span></div><div class="rpm-kpi"><span class="rpm-kpi-label">NEXT PUBLICATION</span><strong class="rpm-kpi-value" style="color:{worker_color}">{_countdown(snapshot["next_run_at"])}</strong><span class="rpm-kpi-sub">worker: {worker}</span></div></div>', unsafe_allow_html=True)


def render_terminal(db: Database, limit: int = 18) -> None:
    rows = db.logs(limit)
    text = "\n".join(f"[{row['created_at']}] {row['message']}" for row in rows) or "Esperando actividad..."
    st.markdown(f'<div class="rpm-terminal"><div class="rpm-terminal-head"><span class="rpm-dot red"></span><span class="rpm-dot yellow"></span><span class="rpm-dot green"></span><span style="margin-left:.35rem">root@revolution-pm:~# execution_stream.log</span><span class="rpm-led"></span></div><pre>{_esc(text)}\n<span class="rpm-cursor">▋</span></pre></div>', unsafe_allow_html=True)


def render_progress(service: CampaignService) -> None:
    snapshot = _service_snapshot(service)
    total = int(snapshot["total"] or 0)
    done = min(total, int(snapshot["done"] or 0))
    percent = 0 if not total else int(done * 100 / total)
    state = "PAUSED" if snapshot["paused"] else ("RUNNING" if snapshot["running"] else "STANDBY")
    next_run = _countdown(snapshot["next_run_at"])
    st.markdown(f'<div class="rpm-progress-wrap"><div class="rpm-progress-top"><span>CYCLE {snapshot["cycle"] or 0} · {state}</span><span>{done}/{total} destinos · próximo {next_run}</span></div><div class="rpm-progress-bar"><div class="rpm-progress-fill" style="width:{percent}%"></div></div></div>', unsafe_allow_html=True)


def render_account_rotation(db: Database) -> None:
    st.markdown('<div class="rpm-panel-title"><h3>Account rotation</h3><small>estado de sesión</small></div>', unsafe_allow_html=True)
    accounts = db.accounts()
    if not accounts:
        st.markdown('<div class="rpm-empty">Agregá una cuenta para habilitar la rotación.</div>', unsafe_allow_html=True)
        return
    labels = {"connected": ("CONNECTED", "connected"), "waiting": ("WAITING", "waiting"), "error": ("ERROR", "error")}
    for account in accounts:
        label, css = labels.get(account["status"], ("DISCONNECTED", "disconnected"))
        detail = account["detail"] or account["phone"] or "Sin autorizar"
        st.markdown(f'<div class="rpm-account"><span class="rpm-badge {css}">{label}</span><span class="rpm-account-name">{_esc(account["name"])}</span><span class="rpm-account-detail">{_esc(detail)}</span></div>', unsafe_allow_html=True)


def render_composer(db: Database, compact: bool = False) -> tuple[str, str]:
    templates = db.templates()
    names = [template["name"] for template in templates]
    new_label = "＋ NUEVO PRESET"
    options = names + [new_label]
    selected_key = "template_select"
    if st.session_state.get(selected_key) not in options:
        st.session_state[selected_key] = options[0] if names else new_label
    selected = st.selectbox("Preset activo", options, key=selected_key)
    if templates:
        st.caption("Presets rápidos")
        quick_columns = st.columns(min(4, len(templates)))
        for index, template in enumerate(templates[:4]):
            if quick_columns[index % len(quick_columns)].button(template["name"], key=f"quick_{hashlib.md5(template['name'].encode()).hexdigest()[:10]}", use_container_width=True):
                st.session_state[selected_key] = template["name"]
                st.rerun()
    current = next((template for template in templates if template["name"] == selected), {"name": selected, "body": "", "image_path": ""})
    if st.session_state.get("_last_template") != selected:
        st.session_state["composer_message"] = current.get("body", "")
        st.session_state["composer_image_path"] = current.get("image_path", "")
        st.session_state["_last_template"] = selected
    body = st.text_area("Mensaje / Spintax", height=170 if compact else 220, key="composer_message", placeholder="Escribí el mensaje. Ejemplo: {Hola|Buenas} comunidad...")
    st.caption("Spintax disponible: {Hola|Buenas|Qué tal}. Se elige una variante por destino.")
    upload_key = f"composer_upload_{hashlib.md5(selected.encode()).hexdigest()[:10]}"
    uploaded = st.file_uploader("Media visual (JPG, PNG, JFIF, WEBP)", type=["jpg", "jpeg", "png", "jfif", "webp", "gif"], key=upload_key)
    if uploaded:
        signature = f"{uploaded.name}:{uploaded.size}"
        if st.session_state.get("_upload_signature") != signature:
            st.session_state["composer_image_path"] = save_uploaded(uploaded)
            st.session_state["_upload_signature"] = signature
    image_path = st.session_state.get("composer_image_path", "")
    if image_path and Path(image_path).is_file():
        try:
            st.image(Image.open(image_path), caption="PREVIEW / MEDIA READY", width=300)
        except (OSError, ValueError):
            st.warning("No se pudo previsualizar el archivo seleccionado.")
    else:
        st.markdown('<div class="rpm-empty">Arrastrá una imagen para preparar el envío multimedia.</div>', unsafe_allow_html=True)
    if selected == new_label:
        template_name = st.text_input("Nombre del nuevo preset", value=f"Preset {len(templates) + 1}", key="new_template_name")
    else:
        template_name = selected
    c1, c2 = st.columns(2)
    if c1.button("Guardar preset", key="save_template", use_container_width=True):
        if not template_name.strip():
            st.error("Elegí un nombre para el preset.")
        else:
            db.save_template(template_name.strip(), body, image_path)
            st.session_state[selected_key] = template_name.strip()
            st.success("Preset guardado.")
            st.rerun()
    if c2.button("＋ Nuevo preset", key="new_template", use_container_width=True):
        st.session_state[selected_key] = new_label
        st.rerun()
    st.session_state.content = (body, image_path)
    return body, image_path


def render_content(db: Database) -> tuple[str, str]:
    st.markdown('<div class="rpm-panel-title"><h3>Content lab</h3><small>presets + media + spintax</small></div>', unsafe_allow_html=True)
    return render_composer(db)


def render_destinations(db: Database, service: CampaignService) -> str:
    st.markdown('<div class="rpm-panel-title"><h3>Destination matrix</h3><small>TXT, CSV, enlaces o IDs</small></div>', unsafe_allow_html=True)
    if "destinations" not in st.session_state:
        st.session_state.destinations = ""
    uploaded = st.file_uploader("Importar lista masiva", type=["txt", "csv"], key="destinations_upload")
    if uploaded:
        try:
            if uploaded.name.lower().endswith(".csv"):
                imported = "\n".join(row[0].strip() for row in csv.reader(uploaded.getvalue().decode("utf-8-sig").splitlines()) if row and row[0].strip())
            else:
                imported = uploaded.getvalue().decode("utf-8")
            if st.session_state.get("_dest_upload") != f"{uploaded.name}:{uploaded.size}":
                st.session_state.destinations = imported
                st.session_state["_dest_upload"] = f"{uploaded.name}:{uploaded.size}"
        except UnicodeDecodeError:
            st.error("El archivo debe estar codificado en UTF-8.")
    if "destinations_editor" not in st.session_state:
        st.session_state.destinations_editor = st.session_state.destinations
    elif st.session_state.destinations_editor != st.session_state.destinations and st.session_state.get("_dest_upload"):
        st.session_state.destinations_editor = st.session_state.destinations
    value = st.text_area("Un @username, enlace o ID por línea", height=250, key="destinations_editor", placeholder="@grupo_1\nhttps://t.me/grupo_2\n-1001234567890")
    st.session_state.destinations = value
    count = len(CampaignService.targets(value))
    st.markdown(f'<div class="rpm-note">{count} destinos únicos cargados. El verificador omitirá grupos inaccesibles o sin permisos.</div>', unsafe_allow_html=True)
    if st.button("Escanear permisos con la primera cuenta", key="scan_destinations", use_container_width=True):
        accounts = [account for account in db.accounts() if account["enabled"]]
        if not accounts:
            st.error("Agregá y autorizá una cuenta antes de escanear.")
        else:
            valid = service.call(service.scan(accounts[0], CampaignService.targets(value)))
            st.session_state.destinations = "\n".join(valid)
            st.session_state.destinations_editor = st.session_state.destinations
            st.success(f"Conservados {len(valid)} destinos aptos.")
            st.rerun()
    return value


def render_accounts(db: Database, service: CampaignService) -> None:
    st.markdown('<div class="rpm-panel-title"><h3>Accounts & authentication</h3><small>sesiones Telethon protegidas</small></div>', unsafe_allow_html=True)
    with st.expander("＋ Agregar cuenta", expanded=not bool(db.accounts())):
        with st.form("account_form"):
            name = st.text_input("Nombre visible", placeholder="Cuenta principal")
            api_id = st.number_input("API ID", min_value=1, step=1)
            api_hash = st.text_input("API Hash", type="password")
            phone = st.text_input("Teléfono internacional", placeholder="+549...")
            enabled = st.checkbox("Cuenta activa", True)
            if st.form_submit_button("Guardar cuenta", use_container_width=True):
                if not name.strip() or not api_hash.strip() or not phone.strip():
                    st.error("Completá nombre, API Hash y teléfono.")
                else:
                    db.upsert_account({"id": uuid.uuid4().hex, "name": name.strip(), "api_id": api_id, "api_hash": api_hash.strip(), "phone": phone.strip(), "enabled": enabled})
                    st.success("Cuenta guardada.")
                    st.rerun()
    for account in db.accounts():
        state = account["status"]
        label, css = {"connected": ("CONNECTED", "connected"), "waiting": ("WAITING", "waiting"), "error": ("ERROR", "error")}.get(state, ("DISCONNECTED", "disconnected"))
        st.markdown(f'<div class="rpm-account"><span class="rpm-badge {css}">{label}</span><span class="rpm-account-name">{_esc(account["name"])}</span><span class="rpm-account-detail">{_esc(account["phone"])} · {_esc(account["detail"] or "Pendiente de autorización")}</span></div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        if c1.button("Pedir código", key=f"auth_{account['id']}", use_container_width=True):
            service.begin_auth(account)
            st.rerun()
        if service.auth_state.get(account["id"]) in {"code_required", "password_required"}:
            code = st.text_input("Código recibido", key=f"code_{account['id']}")
            password = st.text_input("Contraseña 2FA (si la solicita)", type="password", key=f"pass_{account['id']}")
            if c2.button("Validar", key=f"verify_{account['id']}", use_container_width=True):
                service.complete_auth(account, code, password)
                st.rerun()


def _campaign_accounts(db: Database) -> list[dict[str, Any]]:
    enabled = [account for account in db.accounts() if account["enabled"]]
    options = [account["name"] for account in enabled]
    selected = st.multiselect("Cuentas para esta campaña", options, default=options, key="campaign_accounts")
    return [account for account in enabled if account["name"] in selected]


def render_actions(db: Database, service: CampaignService, body: str, image: str, destinations: str, compact: bool = False) -> None:
    if not compact:
        st.markdown('<div class="rpm-panel-title"><h3>Mission control</h3><small>pipeline asíncrono 24/7</small></div>', unsafe_allow_html=True)
    accounts = _campaign_accounts(db)
    c1, c2 = st.columns(2)
    delay = c1.slider("Pausa entre destinos (segundos)", 1.0, 30.0, 2.0, 0.5, key="delay_seconds")
    repeat = c2.number_input("Repetir tanda cada (minutos)", min_value=30, value=30, step=5, key="repeat_minutes")
    scheduled = st.text_input("Inicio programado opcional (AAAA-MM-DD HH:MM)", key="scheduled_start", placeholder="2026-08-30 22:30")
    st.markdown('<div class="rpm-note">Mínimo recomendado: 1–2 segundos entre destinos. FloodWait y grupos inaccesibles se omiten automáticamente.</div>', unsafe_allow_html=True)
    b1, b2, b3 = st.columns(3)
    if b1.button("▶ Iniciar envíos", type="primary", key="start_campaign", use_container_width=True):
        if not accounts or not destinations.strip() or not (body.strip() or image):
            st.error("Necesitás cuenta autorizada, destinos y contenido.")
        elif not any(account["id"] in service.clients for account in accounts):
            st.error("Autorizá al menos una cuenta antes de iniciar.")
        elif not service.start(accounts, {"text": body.strip(), "image": image, "destinations": destinations}, {"delay": delay, "repeat": repeat, "scheduled": scheduled}):
            st.warning("Ya hay una campaña ejecutándose.")
        else:
            st.success("Worker iniciado. Podés cerrar el navegador sin detenerlo.")
    if service.resume_flag.is_set():
        if b2.button("⏸ Pausar", key="pause_campaign", use_container_width=True):
            service.pause()
            st.rerun()
    elif b2.button("▶ Reanudar", key="resume_campaign", use_container_width=True):
        service.resume()
        st.rerun()
    if b3.button("■ Detener", key="stop_campaign", use_container_width=True):
        service.stop()
        st.rerun()
    render_progress(service)


def export_report(db: Database) -> None:
    rows = db.results()
    if not rows:
        st.info("Todavía no hay resultados para exportar.")
        return
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"reporte_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["created_at", "cycle", "account", "target", "status", "detail"])
        writer.writeheader()
        writer.writerows(rows)
    st.download_button("Descargar reporte CSV", path.read_bytes(), file_name=path.name, mime="text/csv", use_container_width=True)


def render_dashboard(db: Database, service: CampaignService) -> None:
    render_brand_header(db, service)
    st.markdown('<div class="rpm-panel-title"><h3>Workspace central</h3><small>todos los módulos en una sola vista</small></div>', unsafe_allow_html=True)
    with st.container(border=True):
        render_destinations(db, service)
    left, right = st.columns([1.05, 1], gap="large")
    with left:
        st.markdown('<div class="rpm-panel-title"><h3>Control & presets</h3><small>composición rápida</small></div>', unsafe_allow_html=True)
        body, image = render_composer(db, compact=True)
        destinations = st.session_state.get("destinations", "")
        st.markdown(f'<div class="rpm-note">Destination matrix: <strong>{len(CampaignService.targets(destinations))}</strong> grupos listos. Configurá la lista completa desde Destinos.</div>', unsafe_allow_html=True)
        render_actions(db, service, body, image, destinations, compact=True)
    with right:
        st.markdown('<div class="rpm-panel-title"><h3>Live monitor</h3><small>output en tiempo real</small></div>', unsafe_allow_html=True)
        render_progress(service)
        render_terminal(db)
        render_account_rotation(db)
    st.divider()
    st.markdown('<div class="rpm-panel-title"><h3>Export center</h3><small>reporte de éxitos, fallos y FloodWait</small></div>', unsafe_allow_html=True)
    export_report(db)


def main() -> None:
    st.set_page_config(page_title="RevolutionPM", page_icon="✦", layout="wide", initial_sidebar_state="expanded")
    inject_theme()
    if not authenticated():
        return
    db, service = resources()
    st_autorefresh(interval=2000, key="revolution_refresh")
    st.sidebar.markdown('<div class="rpm-kicker">AUTOMATION CONTROL PLANE</div><div class="rpm-brand" style="font-size:1.75rem">Revolution<span>PM</span></div>', unsafe_allow_html=True)
    st.sidebar.caption("Worker persistente · asyncio + Telethon")
    if st.sidebar.button("Cerrar sesión", key="logout", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()
    st.sidebar.markdown("---")
    st.sidebar.markdown('<div class="rpm-note">Dashboard unificado con acceso directo a la gestión de cuentas.</div>', unsafe_allow_html=True)
    st.sidebar.markdown('<div class="rpm-note" style="margin-top:1rem">La pestaña puede cerrarse: el worker continúa mientras el servidor permanezca activo.</div>', unsafe_allow_html=True)
    page = st.sidebar.radio("Navegación", ["Dashboard", "Cuentas"], key="navigation")
    if page == "Cuentas":
        render_accounts(db, service)
    else:
        render_dashboard(db, service)


if __name__ == "__main__":
    main()
