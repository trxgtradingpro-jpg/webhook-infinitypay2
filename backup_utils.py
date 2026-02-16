import hashlib
import json
import os
import tarfile
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from database import exportar_snapshot_publico

BACKUP_MAGIC = b"TRXBK1"
PBKDF2_ITERATIONS = 390000

EXCLUDED_DIRS = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".venv",
    "venv",
    "node_modules",
    "auth",
    "saida",
    "backups",
}

EXCLUDED_FILES = {
    ".DS_Store",
    "Thumbs.db",
}


def _derive_key(password, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
    )
    return kdf.derive(password.encode("utf-8"))


def _encrypt_bytes(plaintext, password):
    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = _derive_key(password=password, salt=salt)
    ciphertext = AESGCM(key).encrypt(nonce=nonce, data=plaintext, associated_data=None)
    return BACKUP_MAGIC + salt + nonce + ciphertext


def _iter_project_files(project_root):
    root = Path(project_root).resolve()
    for path in root.rglob("*"):
        if not path.is_file():
            continue

        rel = path.relative_to(root)
        rel_parts = set(rel.parts)
        if rel_parts & EXCLUDED_DIRS:
            continue

        if path.name in EXCLUDED_FILES:
            continue

        yield path, rel


def _write_database_snapshot_json(target_path):
    snapshot = exportar_snapshot_publico()
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


def _build_plain_backup_tar(project_root, tar_output_path):
    with tempfile.TemporaryDirectory(prefix="trxbkp-db-") as tmp_dir:
        db_snapshot_path = Path(tmp_dir) / "database_snapshot.json"
        _write_database_snapshot_json(db_snapshot_path)

        with tarfile.open(tar_output_path, "w:gz") as tar:
            tar.add(db_snapshot_path, arcname="database_snapshot.json")
            for abs_path, rel_path in _iter_project_files(project_root):
                tar.add(abs_path, arcname=str(rel_path).replace("\\", "/"))


def criar_backup_criptografado(project_root, output_dir, password):
    if not password or len(password.strip()) < 10:
        raise ValueError("Senha de backup ausente ou muito curta (minimo 10 caracteres).")

    root = Path(project_root).resolve()
    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    backup_base_name = f"trxpro-backup-{stamp}"

    with tempfile.TemporaryDirectory(prefix="trxbkp-") as tmp_dir:
        tar_path = Path(tmp_dir) / f"{backup_base_name}.tar.gz"
        _build_plain_backup_tar(project_root=root, tar_output_path=tar_path)

        with open(tar_path, "rb") as f:
            plain_data = f.read()

    encrypted_data = _encrypt_bytes(plaintext=plain_data, password=password)
    encrypted_path = out_dir / f"{backup_base_name}.enc"

    with open(encrypted_path, "wb") as f:
        f.write(encrypted_data)
    try:
        os.chmod(encrypted_path, 0o600)
    except Exception:
        pass

    sha256_hash = hashlib.sha256(encrypted_data).hexdigest()

    return {
        "path": str(encrypted_path),
        "filename": encrypted_path.name,
        "size_bytes": encrypted_path.stat().st_size,
        "sha256": sha256_hash,
        "created_at_utc": stamp,
    }


def remover_backups_antigos(output_dir, keep_days=15):
    removed = []
    if keep_days <= 0:
        return removed

    out_dir = Path(output_dir)
    if not out_dir.exists():
        return removed

    cutoff = datetime.utcnow() - timedelta(days=keep_days)

    for file_path in out_dir.glob("trxpro-backup-*.enc"):
        try:
            mtime = datetime.utcfromtimestamp(file_path.stat().st_mtime)
            if mtime < cutoff:
                file_path.unlink(missing_ok=True)
                removed.append(str(file_path))
        except Exception:
            continue

    return removed
