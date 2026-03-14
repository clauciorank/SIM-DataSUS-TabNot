"""
Armazenamento criptografado de chaves API por provedor.
Arquivo data/llm_secrets.enc (não versionado); chave em data/.key ou TABNOT_SECRET.
"""
import json
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SECRETS_FILE = DATA_DIR / "llm_secrets.enc"
KEY_FILE = DATA_DIR / ".key"

# Provedores que usam api_key
KEY_PROVIDERS = ("gemini", "anthropic", "openai", "generic")


def _get_fernet_key() -> bytes:
    """Obtém 32 bytes para Fernet: TABNOT_SECRET ou arquivo .key."""
    env_secret = os.environ.get("TABNOT_SECRET", "").strip()
    if env_secret:
        from hashlib import sha256
        return sha256(env_secret.encode()).digest()
    if KEY_FILE.exists():
        return KEY_FILE.read_bytes()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    key = os.urandom(32)
    KEY_FILE.write_bytes(key)
    return key


def _get_fernet():
    """Retorna Fernet com a chave do projeto."""
    try:
        from cryptography.fernet import Fernet
        from base64 import urlsafe_b64encode
    except ImportError:
        return None
    raw = _get_fernet_key()
    fernet_key = urlsafe_b64encode(raw)
    return Fernet(fernet_key)


def load_api_keys() -> dict:
    """Carrega chaves por provedor (decifradas). Retorna { provider: api_key }."""
    fernet = _get_fernet()
    if not fernet or not SECRETS_FILE.exists():
        return {}
    try:
        cipher = SECRETS_FILE.read_bytes()
        data = fernet.decrypt(cipher).decode()
        return json.loads(data)
    except Exception:
        return {}


def save_api_key(provider: str, api_key: str) -> None:
    """Salva a chave do provedor (cifrada). Mantém as demais."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    keys = load_api_keys()
    if provider in KEY_PROVIDERS:
        keys[provider] = (api_key or "").strip()
    fernet = _get_fernet()
    if not fernet:
        raise RuntimeError("Instale cryptography para salvar chaves criptografadas: pip install cryptography")
    plain = json.dumps(keys).encode()
    SECRETS_FILE.write_bytes(fernet.encrypt(plain))
