"""重放交付记录领域服务：保存一次历史重放实际采用的来源与逐分片交付明细。

重放确认后由传输血缘调用本服务落一条交付记录，记录本次重放用了哪些备份材料、
每个分片取自分片版本、以及尚未消除、留给后续人工复核的差异。
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService, _ensure_dir
from app.services.sharding import safe_token
from app.store import store


class ReplayDeliveryService(BaseModuleService):
    MODULE = "replay_delivery"
    ENTITY = "重放交付记录"
    OPERATION_TERM = "交付"
    LIST_FIELDS = [
        "交付编号", "重放编号", "批次编号", "重放范围",
        "采用来源", "分片总数", "差异分片", "复核状态", "交付时间",
    ]
    REQUIRED_FIELDS = ["交付编号", "重放编号", "批次编号"]
    STATUS_ORDER = ["草稿", "已交付", "已撤回"]
    ACTION_RULES = {"确认交付": "已交付", "撤回交付": "已撤回"}
    NEGATIVE_ACTIONS = ["撤回交付"]

    # -- 交付记录落库 --

    def record_delivery(
        self,
        *,
        replay_code: str,
        batch_code: str,
        replay_range: str,
        sources: list[dict[str, Any]],
        shard_provenance: list[dict[str, Any]],
        pending_review: list[dict[str, Any]],
        lineage_code: str,
        artifact_path: str | None = None,
    ) -> dict[str, Any]:
        """写入一条重放交付记录，并落地逐分片来源明细。

        ``shard_provenance`` 中每一项至少包含 ``shard_index`` 与 ``material_id``，
        这样确认后的重放可以逐片追溯“这一片实际取自哪份材料”。
        """
        delivery_code = f"DELI-{safe_token(batch_code, limit=32)}-{int(time.time())}-{uuid.uuid4().hex[:4]}"
        material_ids = sorted({str(item["material_id"]) for item in shard_provenance})
        values = {
            "交付编号": delivery_code,
            "重放编号": replay_code,
            "批次编号": batch_code,
            "重放范围": replay_range,
            "采用来源": "、".join(material_ids) or "无",
            "分片总数": str(len(shard_provenance)),
            "差异分片": str(len(pending_review)),
            "复核状态": "待复核" if pending_review else "无需复核",
            "交付时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建重放交付记录失败: {missing}")

        # create_entry 一律从草稿开始；交付记录是在重放确认动作内生成的，直接进入已交付。
        entry["status"] = "已交付"
        entry["pending"] = bool(pending_review)
        entry["abnormal"] = False
        entry["_lineage_code"] = lineage_code
        entry["_sources"] = material_ids
        entry["_pending_review"] = len(pending_review)

        detail = {
            "delivery_code": delivery_code,
            "replay_code": replay_code,
            "batch_code": batch_code,
            "replay_range": replay_range,
            "lineage_code": lineage_code,
            "sources": sources,
            "shards": shard_provenance,
            "pending_review": pending_review,
            "artifact_path": artifact_path,
            "delivered_at": time.time(),
        }
        detail_file = self._data_dir("deliveries") / f"{delivery_code}.json"
        self._write_json(detail_file, detail)
        self._write_jsonl(
            self._data_dir("events") / "replay_delivery_log.jsonl",
            [{
                "action": "record_delivery",
                "delivery_code": delivery_code,
                "replay_code": replay_code,
                "batch_code": batch_code,
                "sources": material_ids,
                "shard_count": len(shard_provenance),
                "pending_review": len(pending_review),
                "at": time.time(),
            }],
        )
        return {
            "delivery_code": delivery_code,
            "delivery_id": entry["id"],
            "detail_path": str(detail_file),
            "shard_count": len(shard_provenance),
            "pending_review": len(pending_review),
            "status": "已交付",
        }

    def get_delivery_detail(self, delivery_code: str) -> dict[str, Any] | None:
        """读取交付明细（逐分片来源与未消除差异），不存在返回 None。"""
        token = safe_token(delivery_code, limit=80)
        for path in (
            self._data_dir("deliveries") / f"{delivery_code}.json",
            self._data_dir("deliveries") / f"{token}.json",
        ):
            if path.exists():
                return self._read_json(path)
        # 允许按重放编号反查
        for path in self._data_dir("deliveries").glob("*.json"):
            data = self._read_json(path)
            if data.get("replay_code") == delivery_code:
                return data
        return None

    def get_delivery_by_replay(self, replay_code: str) -> dict[str, Any] | None:
        """按重放编号查找对应的交付明细。"""
        for path in sorted(self._data_dir("deliveries").glob("*.json")):
            data = self._read_json(path)
            if data.get("replay_code") == replay_code:
                return data
        return None

    # -- 小工具 --

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        """原子写入 JSON 文件，避免中途失败留下半个文件。"""
        _ensure_dir(path.parent)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        tmp.replace(path)

    def _read_json(self, path: Path) -> dict[str, Any]:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
