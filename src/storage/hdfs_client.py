"""
EduGuard Hadoop HDFS Client Module
Provides native WebHDFS integration for storing student datasets, snapshots,
and prediction records with atomic writes and host-redirection handling.
"""

import json
import logging
import os
import urllib.parse
import uuid
from typing import Any, Dict, List, Optional, Union

import requests

from src.config import (
    HDFS_INCOMING_DIR,
    HDFS_MODEL_DIR,
    HDFS_PREDICTIONS_DIR,
    HDFS_PROCESSED_DIR,
    HDFS_ROOT_DIR,
    HDFS_URL,
    HDFS_USER,
)

logger = logging.getLogger("eduguard.hdfs")


class HDFSClient:
    """
    Client for interacting with Hadoop HDFS via the WebHDFS REST API.
    Handles NameNode 307 redirects to DataNodes across Docker network boundaries.
    """

    def __init__(
        self,
        base_url: str = HDFS_URL,
        user: str = HDFS_USER,
        datanode_host_override: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.user = user
        # In Docker desktop port mapping, DataNodes advertise their internal container host
        # We override the netloc to localhost:9864 (or configured datanode host)
        self.datanode_host_override = datanode_host_override or os.getenv(
            "HDFS_DATANODE_OVERRIDE", "localhost:9864"
        )
        self.timeout = timeout

    def _resolve_datanode_url(self, redirect_url: str) -> str:
        """Translates internal container hostname in WebHDFS redirect to accessible host address."""
        if not self.datanode_host_override:
            return redirect_url
        parsed = urllib.parse.urlparse(redirect_url)
        # Preserve path, query, schema, but replace netloc
        return parsed._replace(netloc=self.datanode_host_override).geturl()

    def check_health(self) -> Dict[str, Any]:
        """Checks NameNode status and cluster directory availability."""
        url = f"{self.base_url}/webhdfs/v1{HDFS_ROOT_DIR}?op=GETFILESTATUS&user.name={self.user}"
        try:
            r = requests.get(url, timeout=self.timeout)
            if r.status_code == 200:
                return {"status": "HEALTHY", "reachable": True, "message": "HDFS is online and accessible"}
            elif r.status_code == 404:
                return {"status": "HEALTHY", "reachable": True, "message": f"HDFS reachable, {HDFS_ROOT_DIR} not yet created"}
            else:
                return {"status": "UNHEALTHY", "reachable": False, "message": f"HTTP {r.status_code}: {r.text}"}
        except Exception as e:
            logger.warning("HDFS health check failed: %s", e)
            return {"status": "UNREACHABLE", "reachable": False, "message": str(e)}

    def mkdirs(self, hdfs_path: str) -> bool:
        """Creates directory and any missing parents in HDFS."""
        path = "/" + hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1{path}?op=MKDIRS&user.name={self.user}"
        try:
            r = requests.put(url, timeout=self.timeout)
            if r.status_code in (200, 201):
                data = r.json()
                return bool(data.get("boolean", True))
            logger.error("Failed to create HDFS dir %s: %s", path, r.text)
            return False
        except Exception as e:
            logger.error("Error creating HDFS dir %s: %s", path, e)
            raise

    def exists(self, hdfs_path: str) -> bool:
        """Checks if a file or directory exists in HDFS."""
        path = "/" + hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1{path}?op=GETFILESTATUS&user.name={self.user}"
        try:
            r = requests.get(url, timeout=self.timeout)
            return r.status_code == 200
        except Exception:
            return False

    def write_file(
        self,
        hdfs_path: str,
        data: Union[str, bytes],
        overwrite: bool = True,
        atomic: bool = True,
    ) -> bool:
        """
        Writes data to HDFS via WebHDFS protocol.
        If atomic=True, writes to a temporary file first, then issues WebHDFS RENAME.
        """
        path = "/" + hdfs_path.strip("/")
        payload = data.encode("utf-8") if isinstance(data, str) else data

        target_write_path = f"{path}.tmp.{uuid.uuid4().hex}" if atomic else path

        # Step 1: Send CREATE to NameNode (gets 307 Redirect to DataNode)
        create_url = (
            f"{self.base_url}/webhdfs/v1{target_write_path}"
            f"?op=CREATE&overwrite={str(overwrite).lower()}&user.name={self.user}"
        )
        try:
            r1 = requests.put(create_url, allow_redirects=False, timeout=self.timeout)
            if r1.status_code not in (307, 200, 201):
                raise IOError(f"NameNode CREATE rejected: HTTP {r1.status_code} - {r1.text}")

            upload_url = r1.headers.get("Location")
            if not upload_url:
                raise IOError("NameNode did not provide DataNode redirect Location header")

            # Translate internal DataNode host if needed
            resolved_upload_url = self._resolve_datanode_url(upload_url)

            # Step 2: Stream payload to DataNode
            headers = {"Content-Type": "application/octet-stream"}
            r2 = requests.put(resolved_upload_url, data=payload, headers=headers, timeout=self.timeout)
            if r2.status_code not in (200, 201):
                raise IOError(f"DataNode PUT rejected: HTTP {r2.status_code} - {r2.text}")

            # Step 3: Atomic rename if applicable
            if atomic:
                rename_ok = self.rename(target_write_path, path)
                if not rename_ok:
                    raise IOError(f"Failed to atomically rename {target_write_path} to {path}")

            logger.info("Successfully wrote %d bytes to HDFS at %s", len(payload), path)
            return True

        except Exception as e:
            logger.error("HDFS write failed for %s: %s", path, e)
            if atomic:
                try:
                    self.delete(target_write_path)
                except Exception:
                    pass
            raise

    def read_file(self, hdfs_path: str) -> bytes:
        """Reads binary file contents from HDFS."""
        path = "/" + hdfs_path.strip("/")
        open_url = f"{self.base_url}/webhdfs/v1{path}?op=OPEN&user.name={self.user}"
        try:
            r1 = requests.get(open_url, allow_redirects=False, timeout=self.timeout)
            if r1.status_code == 307:
                read_url = self._resolve_datanode_url(r1.headers["Location"])
                r2 = requests.get(read_url, timeout=self.timeout)
                if r2.status_code != 200:
                    raise IOError(f"Failed to read from DataNode: HTTP {r2.status_code} - {r2.text}")
                return r2.content
            elif r1.status_code == 200:
                return r1.content
            else:
                raise IOError(f"NameNode OPEN returned HTTP {r1.status_code}: {r1.text}")
        except Exception as e:
            logger.error("HDFS read failed for %s: %s", path, e)
            raise

    def read_text(self, hdfs_path: str, encoding: str = "utf-8") -> str:
        """Reads file contents as text from HDFS."""
        content = self.read_file(hdfs_path)
        return content.decode(encoding)

    def write_json(self, hdfs_path: str, obj: Any, overwrite: bool = True, atomic: bool = True) -> bool:
        """Writes JSON-serializable object to HDFS."""
        payload = json.dumps(obj, indent=2, default=str)
        return self.write_file(hdfs_path, payload, overwrite=overwrite, atomic=atomic)

    def read_json(self, hdfs_path: str) -> Any:
        """Reads and parses JSON object from HDFS."""
        text = self.read_text(hdfs_path)
        return json.loads(text)

    def rename(self, src_path: str, dest_path: str) -> bool:
        """Renames a file or directory in HDFS."""
        src = "/" + src_path.strip("/")
        dest = "/" + dest_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1{src}?op=RENAME&destination={dest}&user.name={self.user}"
        try:
            r = requests.put(url, timeout=self.timeout)
            if r.status_code == 200:
                data = r.json()
                return bool(data.get("boolean", True))
            return False
        except Exception as e:
            logger.error("Error renaming HDFS %s -> %s: %s", src, dest, e)
            return False

    def delete(self, hdfs_path: str, recursive: bool = False) -> bool:
        """Deletes a file or directory from HDFS."""
        path = "/" + hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1{path}?op=DELETE&recursive={str(recursive).lower()}&user.name={self.user}"
        try:
            r = requests.delete(url, timeout=self.timeout)
            return r.status_code == 200
        except Exception:
            return False

    def list_status(self, hdfs_path: str) -> List[Dict[str, Any]]:
        """Lists directory contents in HDFS."""
        path = "/" + hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1{path}?op=LISTSTATUS&user.name={self.user}"
        try:
            r = requests.get(url, timeout=self.timeout)
            if r.status_code == 200:
                data = r.json()
                return data.get("FileStatuses", {}).get("FileStatus", [])
            return []
        except Exception as e:
            logger.warning("Error listing HDFS %s: %s", path, e)
            return []

    def ensure_directories(self) -> None:
        """Ensures all standard EduGuard HDFS directories exist."""
        for d in [HDFS_ROOT_DIR, HDFS_INCOMING_DIR, HDFS_PROCESSED_DIR, HDFS_PREDICTIONS_DIR, HDFS_MODEL_DIR]:
            if not self.exists(d):
                self.mkdirs(d)


# Default singleton instance
_DEFAULT_CLIENT = None


def get_hdfs_client() -> HDFSClient:
    """Returns singleton HDFS client instance."""
    global _DEFAULT_CLIENT
    if _DEFAULT_CLIENT is None:
        _DEFAULT_CLIENT = HDFSClient()
    return _DEFAULT_CLIENT
