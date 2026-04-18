"""Wrapper na komendy HDFS uruchamiane przez `docker exec master hdfs dfs ...`.

Orkiestrator pipeline'u żyje na hoście (venv Python), ale HDFS jest dostępny tylko
z wnętrza kontenera master. Wszystkie operacje na HDFS są tu routowane przez
`subprocess` + `docker exec`.
"""
from __future__ import annotations

import logging
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

MASTER_CONTAINER = "master"
DEFAULT_REPLICATION = 3


class HdfsError(RuntimeError):
    pass


@dataclass
class CmdResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def _run(cmd: list[str], stdin_bytes: Optional[bytes] = None, timeout: Optional[int] = None) -> CmdResult:
    proc = subprocess.run(
        cmd,
        input=stdin_bytes,
        capture_output=True,
        timeout=timeout,
    )
    return CmdResult(
        returncode=proc.returncode,
        stdout=proc.stdout.decode("utf-8", errors="replace"),
        stderr=proc.stderr.decode("utf-8", errors="replace"),
    )


def _docker_exec(args: list[str], stdin_bytes: Optional[bytes] = None, timeout: Optional[int] = None) -> CmdResult:
    cmd = ["docker", "exec"]
    if stdin_bytes is not None:
        cmd.append("-i")
    cmd.append(MASTER_CONTAINER)
    cmd.extend(args)
    return _run(cmd, stdin_bytes=stdin_bytes, timeout=timeout)


def check_cluster_alive() -> None:
    """Sanity check — master musi odpowiadać i HDFS musi być w stanie RUNNING."""
    r = _docker_exec(["hdfs", "dfsadmin", "-report"], timeout=30)
    if not r.ok:
        raise HdfsError(
            f"Klaster HDFS nie odpowiada. Sprawdź `docker ps` i `./hadoop-start.sh restart`.\n"
            f"stderr: {r.stderr.strip()}"
        )
    if "Live datanodes" not in r.stdout:
        raise HdfsError(f"Raport HDFS nie zawiera 'Live datanodes':\n{r.stdout[:500]}")


def mkdir_p(path: str) -> None:
    r = _docker_exec(["hdfs", "dfs", "-mkdir", "-p", path])
    if not r.ok:
        raise HdfsError(f"mkdir -p {path} FAIL: {r.stderr.strip()}")


def exists(path: str) -> bool:
    r = _docker_exec(["hdfs", "dfs", "-test", "-e", path])
    return r.ok


def ls(path: str) -> str:
    r = _docker_exec(["hdfs", "dfs", "-ls", "-R", path])
    if not r.ok and "No such file" not in r.stderr:
        raise HdfsError(f"ls {path} FAIL: {r.stderr.strip()}")
    return r.stdout


def rm(path: str, recursive: bool = False) -> None:
    flags = ["-rm"]
    if recursive:
        flags = ["-rm", "-r"]
    r = _docker_exec(["hdfs", "dfs", *flags, "-f", path])
    if not r.ok:
        raise HdfsError(f"rm {path} FAIL: {r.stderr.strip()}")


def put(local_path: Path, hdfs_path: str, logger: logging.Logger) -> int:
    """Wrzuca lokalny plik do HDFS. Zwraca rozmiar wgranego pliku w bajtach."""
    local_path = Path(local_path)
    if not local_path.is_file():
        raise HdfsError(f"put: lokalny plik nie istnieje: {local_path}")

    size = local_path.stat().st_size
    parent = hdfs_path.rsplit("/", 1)[0]
    if parent:
        mkdir_p(parent)

    # Pipe przez stdin, żeby nie musieć `docker cp` pliku do kontenera.
    with local_path.open("rb") as fh:
        data = fh.read()
    r = _docker_exec(["hdfs", "dfs", "-put", "-f", "-", hdfs_path], stdin_bytes=data, timeout=600)
    if not r.ok:
        raise HdfsError(f"put {local_path} → {hdfs_path} FAIL: {r.stderr.strip()}")
    logger.debug("hdfs put OK: %s → %s (%d B)", local_path, hdfs_path, size)
    return size


def put_bytes(data: bytes, hdfs_path: str, logger: logging.Logger) -> int:
    """Wrzuca bufor w pamięci do HDFS (dla manifestu, logów)."""
    parent = hdfs_path.rsplit("/", 1)[0]
    if parent:
        mkdir_p(parent)
    r = _docker_exec(["hdfs", "dfs", "-put", "-f", "-", hdfs_path], stdin_bytes=data, timeout=120)
    if not r.ok:
        raise HdfsError(f"put_bytes → {hdfs_path} FAIL: {r.stderr.strip()}")
    logger.debug("hdfs put_bytes OK: %s (%d B)", hdfs_path, len(data))
    return len(data)


def cat_bytes(hdfs_path: str) -> bytes:
    """Czyta plik z HDFS do pamięci."""
    r = _docker_exec(["hdfs", "dfs", "-cat", hdfs_path], timeout=120)
    if not r.ok:
        raise HdfsError(f"cat {hdfs_path} FAIL: {r.stderr.strip()}")
    # stdout jest już zdekodowane; ale dla bezpieczeństwa powracamy do bajtów
    return r.stdout.encode("utf-8", errors="replace")


def get(hdfs_path: str, local_path: Path, logger: logging.Logger) -> int:
    """Pobiera plik z HDFS do lokalnego FS (przez stdout kontenera)."""
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    r = _docker_exec(["hdfs", "dfs", "-cat", hdfs_path], timeout=600)
    if not r.ok:
        raise HdfsError(f"get {hdfs_path} FAIL: {r.stderr.strip()}")
    # Dla plików binarnych (ZIP) nie możemy polegać na r.stdout (str). Powtórz z bajtami.
    proc = subprocess.run(
        ["docker", "exec", MASTER_CONTAINER, "hdfs", "dfs", "-cat", hdfs_path],
        capture_output=True,
        timeout=600,
    )
    if proc.returncode != 0:
        raise HdfsError(f"get {hdfs_path} FAIL: {proc.stderr.decode('utf-8', 'replace').strip()}")
    local_path.write_bytes(proc.stdout)
    size = local_path.stat().st_size
    logger.debug("hdfs get OK: %s → %s (%d B)", hdfs_path, local_path, size)
    return size


def setrep(path: str, replication: int = DEFAULT_REPLICATION, wait: bool = True, recursive: bool = False) -> None:
    """Ustawia replikację; z wait=True blokuje aż do osiągnięcia docelowego stanu."""
    args = ["hdfs", "dfs", "-setrep"]
    if wait:
        args.append("-w")
    if recursive:
        args.append("-R")
    args.extend([str(replication), path])
    r = _docker_exec(args, timeout=600)
    if not r.ok:
        raise HdfsError(f"setrep {replication} {path} FAIL: {r.stderr.strip()}")


def fsck(path: str) -> str:
    r = _docker_exec(["hdfs", "fsck", path, "-files", "-blocks"], timeout=120)
    if not r.ok:
        raise HdfsError(f"fsck {path} FAIL: {r.stderr.strip()}")
    return r.stdout


def du_h(path: str) -> str:
    r = _docker_exec(["hdfs", "dfs", "-du", "-h", path])
    if not r.ok:
        return ""
    return r.stdout
