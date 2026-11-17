"""
StorageClient — orchestrator → storage HTTP boundary.

All httpx calls to the storage service live here. Services depend on this
class, not on httpx. Mirrors the gateway's StorageClient pattern.

Methods limited to what orchestrator actually calls:
  - presign_upload     (FileProxyService / upload-job handler)
  - save_crops         (PipelineService after media worker)
  - save_output        (PipelineService after burner)
  - list_files         (download lookups)
  - presign_download   (download lookups)
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from app.Config import STORAGE_SERVICE_URL
from app.Exceptions import NotFound, StorageError, StorageUnreachable
from app.Security.ServiceAuth import sign_request

logger = logging.getLogger("orchestrator.storage-client")


class StorageClient:
    def __init__(self, http:httpx.AsyncClient,base_url:str = STORAGE_SERVICE_URL):
        self.http = http
        self.base_url = base_url

    # ── Presign ──────────────────────────────────────
    async def presign_client(self,
                             session_id:str,
                             file_type:str,
                             mime_type:str,
                             original_filename:Optional[str]=None,
                             user_id:Optional[str]=None,
                             )-> dict:
        body: dict[str,Any] = {
            "session_id": session_id,
            "file_type": file_type,
            "mime_type": mime_type,
        }
        if original_filename is not None:
            body["original_filename"] = original_filename
        if user_id is not None:
            body["user_id"] = user_id
        return await self._post("/internal/presign/upload", json=body)
    async def presign_download(self, file_id: str) -> dict:
        return await self._post(
            "/internal/presign/download",
            json={"file_id": file_id},
        )
    # ── Save ─────────────────────────────────────────
    async def save_crops(
            self, session_id: str, frame_index: int, detection_index: int, crops_b64: dict
    ) -> dict:
        """Hand base64 JPEG crops to storage. Returns {file_ids: {...}}."""
        return await self._post(
            "/internal/save-crops",
            json={
                "session_id": session_id,
                "frame_index": frame_index,
                "detection_index": detection_index,
                "crops": crops_b64,
            },
        )

    async def save_output(
            self,
            session_id: str,
            data_b64: str,
            mime_type: str,
            file_type: str = "burned",
    ) -> dict:
        """Hand the burned output bytes (base64) to storage."""
        return await self._post(
            "/internal/save-output",
            json={
                "session_id": session_id,
                "data": data_b64,
                "mime_type": mime_type,
                "file_type": file_type,
            },
        )

    # ── Files ────────────────────────────────────────

    async def list_files(
            self,
            session_id: Optional[str] = None,
            user_id: Optional[str] = None,
            category: Optional[str] = None,
    ) -> list[dict]:
        params: dict[str, str] = {}
        if session_id is not None:
            params["session_id"] = session_id
        if user_id is not None:
            params["user_id"] = user_id
        if category is not None:
            params["category"] = category
        data = await self._get("/internal/files", params=params)
        return data if isinstance(data, list) else []

    # ── Internal ─────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> Any:
        try:
            resp = await self.http.get(f"{self.base_url}{path}", params=params, headers=sign_request({}))
        except httpx.RequestError as e:
            logger.error("GET %s unreachable: %s", path, e)
            raise StorageUnreachable()
        if resp.status_code == 404:
            raise NotFound()
        if resp.status_code != 200:
            logger.error("GET %s -> %s: %s", path, resp.status_code, resp.text)
            raise StorageError()
        return resp.json()

    async def _post(self, path: str, json: dict) -> Any:
        try:
            resp = await self.http.post(f"{self.base_url}{path}", json=json, headers=sign_request({}))
        except httpx.RequestError as e:
            logger.error("POST %s unreachable: %s", path, e)
            raise StorageUnreachable()
        if resp.status_code == 404:
            raise NotFound()
        if resp.status_code != 200:
            logger.error("POST %s -> %s: %s", path, resp.status_code, resp.text)
            raise StorageError()
        return resp.json()




