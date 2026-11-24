"""备份导入与导入重放接口：上传归档包、登记本地归档卷、按批次申请与确认重放。

这里不使用 multipart 表单，上传接口直接读取原始请求体，文件类型由扩展名与内容自校验，
既避免额外依赖，也让调用方（网页、脚本、巡检）都能用同一种方式提交压缩包。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.schemas import ActionResult
from app.services import (
    BackupImportService,
    ExchangeReplayService,
    TransferLineageService,
)

router = APIRouter(prefix="/api/import-replay", tags=["备份导入与导入重放"])


def _backup() -> BackupImportService:
    return BackupImportService()


def _replay() -> ExchangeReplayService:
    return ExchangeReplayService()


@router.post("/packages", response_model=dict)
async def upload_package(
    request: Request,
    filename: str = Query(default="backup.zip"),
    label: str | None = Query(default=None),
    requested_by: str | None = Query(default=None),
) -> dict[str, Any]:
    """上传外部归档数据包：读取原始请求体，检查包内批次与分片后登记为待核验材料。

    包损坏或结构不安全时返回 ``ok=false`` 与具体原因，不会写入任何可用来源材料，
    也不会影响已有的本地归档卷。
    """
    data = await request.body()
    result = _backup().import_package(
        data or None,
        filename=filename,
        label=label,
        requested_by=requested_by,
    )
    return result


@router.post("/packages/base64", response_model=dict)
def upload_package_base64(payload: dict[str, Any]) -> dict[str, Any]:
    """上传接口的 JSON 版本：以 base64 携带压缩包内容，便于脚本调用。"""
    import base64
    import binascii

    content = payload.get("content_base64")
    if not isinstance(content, str) or not content.strip():
        raise HTTPException(status_code=400, detail="缺少 content_base64")
    try:
        data = base64.b64decode(content, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"content_base64 无法解码：{exc}") from exc
    return _backup().import_package(
        data,
        filename=str(payload.get("filename") or "backup.zip"),
        label=payload.get("label"),
        requested_by=payload.get("requested_by"),
    )


@router.post("/local-volumes", response_model=dict)
def register_local_volume(payload: dict[str, Any]) -> dict[str, Any]:
    """把恢复可读的本地归档卷登记为来源材料（与离线包相互独立）。"""
    archive_code = payload.get("archive_code")
    path = payload.get("path")
    if not archive_code and not path:
        raise HTTPException(status_code=400, detail="必须提供 archive_code 或 path")
    return _backup().register_local_volume(
        archive_code=archive_code,
        path=path,
        label=payload.get("label"),
    )


@router.get("/materials/{material_id}", response_model=dict)
def get_material(material_id: str) -> dict[str, Any]:
    """查看一份来源材料的批次、分片与版本清单。"""
    manifest = _backup().get_material(material_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"材料 {material_id} 不存在")
    return manifest


@router.post("/materials/{material_id}/verify", response_model=dict)
def verify_material(material_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """核验一份来源材料；核验不会改动已确认重放的交付记录。"""
    payload = payload or {}
    return _backup().verify_material(
        material_id,
        accepted=bool(payload.get("accepted", True)),
        remark=str(payload.get("remark") or ""),
    )


@router.get("/batches/{batch_code}/sources", response_model=dict)
def batch_sources(batch_code: str) -> dict[str, Any]:
    """查看批次的全部候选来源、分片版本冲突与若确认将采用的来源预览。"""
    return _backup().get_batch_sources(batch_code)


@router.get("/batches/{batch_code}/scope", response_model=dict)
def confirmed_scope(batch_code: str) -> dict[str, Any]:
    """查看该批次已经确认的导入重放交付范围。"""
    return _backup().get_confirmed_replay_scope(batch_code)


@router.get("/batches/{batch_code}/deliveries", response_model=dict)
def batch_deliveries(batch_code: str) -> dict[str, Any]:
    """查看该批次在传输血缘中保存的全部导入重放交付记录。"""
    return TransferLineageService().query_replay_deliveries(batch_code)


@router.post("/replays", response_model=dict)
def request_replay(payload: dict[str, Any]) -> dict[str, Any]:
    """按批次申请历史重放：只登记待重放任务并预览来源，不对外交付。"""
    batch_code = str(payload.get("batch_code") or "").strip()
    if not batch_code:
        raise HTTPException(status_code=400, detail="缺少 batch_code")
    return _replay().request_batch_replay(
        batch_code,
        replay_range=payload.get("replay_range"),
        mode=str(payload.get("mode") or "append"),
        requested_by=payload.get("requested_by"),
        archive_code=payload.get("archive_code"),
    )


@router.post("/replays/{replay_id}/confirm", response_model=dict)
def confirm_replay(replay_id: int) -> dict[str, Any]:
    """确认重放：逐分片选定来源、落重放产物，并由传输血缘写入交付记录。"""
    return _replay().confirm_batch_replay(replay_id)


@router.get("/replays/{replay_id}/delivery", response_model=dict)
def replay_delivery(replay_id: int) -> dict[str, Any]:
    """查看一次重放实际采用的来源、逐分片明细与待复核差异。"""
    return _replay().get_replay_delivery(replay_id)


@router.post("/replays/{replay_id}/action", response_model=ActionResult)
def replay_action(replay_id: int, payload: dict[str, Any]) -> ActionResult:
    """对重放任务执行显式动作：确认（逐片取源）或取消。"""
    action = str(payload.get("action") or "").strip()
    if action in ("确认重放", "confirm", "确认"):
        result = _replay().confirm_batch_replay(replay_id)
        return ActionResult(ok=bool(result.get("ok")), message=str(result.get("message") or result.get("error") or ""))
    if action in ("取消重放", "cancel", "取消"):
        entry, message = _replay().run_action(replay_id, "取消重放")
        return ActionResult(ok=entry is not None, message=message, entry=entry)
    raise HTTPException(status_code=400, detail=f"不支持的重放动作：{action}")
