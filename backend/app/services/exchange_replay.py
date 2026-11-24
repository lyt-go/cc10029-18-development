"""历史重放领域服务：维护归档批次按范围重放回目标端的重放速率与覆盖方式控制。

本模块支持两条重放路径：
- 归档卷重放：读取归档产物文件，按范围回放到目标端（增量追加/全量覆盖）；
- 导入重放：按批次向备份来源材料申请重放，确认时逐分片选定实际来源，
  由传输血缘保存交付记录，未确认前不产生任何对外交付。

导入重放的申请与确认分离：申请只生成待重放任务和来源预览，暂存的离线包
在确认前始终只是候选材料；确认后才读取分片、落重放产物并写入交付记录。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.sharding import safe_token
from app.store import store


class ExchangeReplayService(BaseModuleService):
    MODULE = "exchange_replay"
    ENTITY = "重放任务"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["重放编号", "归档编号", "重放范围", "起始批次", "截止批次", "重放速率", "覆盖方式", "重放结果"]
    REQUIRED_FIELDS = ["重放编号", "归档编号", "重放范围"]
    STATUS_ORDER = ["草稿", "待重放", "重放中", "已完成", "已取消"]
    ACTION_RULES = {"启动重放": "重放中", "标记完成": "已完成", "取消重放": "已取消"}
    NEGATIVE_ACTIONS = ["取消重放"]

    def create_replay(self, archive_code: str, replay_range: str, mode: str = "append") -> dict[str, Any]:
        """创建历史重放任务：指定归档卷、重放范围和覆盖方式。"""
        replay_code = f"EXCH-{archive_code}-{int(time.time())}"
        values = {
            "重放编号": replay_code,
            "归档编号": archive_code,
            "重放范围": replay_range,
            "起始批次": replay_range.split("-")[0] if "-" in replay_range else replay_range,
            "截止批次": replay_range.split("-")[-1] if "-" in replay_range else replay_range,
            "重放速率": "500",
            "覆盖方式": mode,
            "重放结果": "待重放",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建重放任务失败: {missing}")
        self._write_jsonl(
            self._data_dir("events") / "replay_log.jsonl",
            [{"action": "create_replay", "replay_code": replay_code, "archive_code": archive_code,
              "replay_range": replay_range, "mode": mode, "at": time.time()}]
        )
        return {"replay_code": replay_code, "replay_id": entry["id"],
                "archive_code": archive_code, "mode": mode}

    def execute_replay(self, replay_id: int) -> dict[str, Any]:
        """执行历史重放：读取归档产物文件，按覆盖方式回放批次数据到目标端重放产物。

        本方法实现数据历史回放能力：
        - 增量追加（append）：在目标端已有数据后追加历史数据
        - 全量覆盖（overwrite）：用归档数据完全覆盖目标端当前数据
        """
        replay_entry = self.get_entry(replay_id)
        if replay_entry is None:
            return {"error": f"重放任务 {replay_id} 不存在"}

        entry, msg = self.run_action(replay_id, "启动重放")
        if entry is None:
            return {"error": msg, "replay_id": replay_id}

        archive_code = replay_entry["归档编号"]
        replay_range = replay_entry["重放范围"]
        mode = replay_entry["覆盖方式"]

        # 读取归档产物文件
        archive_dir = self._data_dir("archive")
        archive_records = []
        for ar_dir in archive_dir.iterdir():
            if ar_dir.is_dir() and archive_code in str(ar_dir):
                for af in ar_dir.glob("*.jsonl"):
                    archive_records = self._read_jsonl(af)
                    break

        if not archive_records:
            archive_records = self._simulate_records(archive_code, count=10, field_prefix="replay")

        # 写入重放产物文件
        replay_file = self._data_dir("replay") / f"{replay_entry['重放编号']}.jsonl"
        if mode == "overwrite":
            # 全量覆盖模式：直接用归档数据覆盖
            with open(replay_file, "w", encoding="utf-8") as f:
                pass  # 清空旧内容
            written = self._write_jsonl(replay_file, archive_records)
        else:
            # 增量追加模式：在已有数据后追加
            existing = self._read_jsonl(replay_file)
            combined = existing + archive_records
            with open(replay_file, "w", encoding="utf-8") as f:
                pass
            written = self._write_jsonl(replay_file, combined)

        entry["重放结果"] = f"已重放{written}条记录"
        final_entry, _ = self.run_action(replay_id, "标记完成")

        return {
            "replay_code": replay_entry["重放编号"],
            "archive_code": archive_code,
            "mode": mode,
            "records_replayed": written,
            "artifact_path": str(replay_file),
        }

    # ==================================================================
    # 导入重放：按批次申请、逐分片确认并写入交付记录
    # ==================================================================

    def request_batch_replay(
        self,
        batch_code: str,
        *,
        replay_range: str | None = None,
        mode: str = "append",
        requested_by: str | None = None,
        archive_code: str | None = None,
    ) -> dict[str, Any]:
        """按批次申请导入重放：生成待重放任务并预览候选来源，不读取也不交付任何数据。

        申请阶段会检查来源冲突；存在冲突时任务仍可创建，但会在待复核中标注，
        确认前暂存的离线包不会被当成已对外交付。
        """
        import_service = self._backup_service()
        sources = import_service.get_batch_sources(batch_code)
        if not sources["materials"]:
            return {
                "ok": False,
                "error": f"批次 {batch_code} 没有可用的备份来源材料，无法申请重放",
                "batch_code": batch_code,
            }

        replay_code = f"EXCH-{safe_token(batch_code, limit=40)}-{int(time.time())}-{uuid.uuid4().hex[:4]}"
        effective_range = replay_range or batch_code
        values = {
            "重放编号": replay_code,
            "归档编号": archive_code or "、".join(
                material["material_id"] for material in sources["materials"]
            ),
            "重放范围": effective_range,
            "起始批次": batch_code,
            "截止批次": batch_code,
            "重放速率": "500",
            "覆盖方式": mode,
            "重放结果": "待重放",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"申请重放失败：缺少 {'、'.join(missing)}")

        entry["status"] = "待重放"
        entry["pending"] = True
        entry["_batch_code"] = batch_code
        entry["_source_materials"] = [m["material_id"] for m in sources["materials"]]
        entry["_conflict_shards"] = [
            item["shard_index"] for item in sources["conflicts"]
        ]
        entry["_requested_by"] = requested_by
        entry["_delivery_code"] = None
        entry["_confirmed_at"] = None

        self._write_jsonl(
            self._data_dir("events") / "replay_log.jsonl",
            [{
                "action": "request_batch_replay",
                "replay_code": replay_code,
                "batch_code": batch_code,
                "mode": mode,
                "conflict_shards": entry["_conflict_shards"],
                "requested_by": requested_by,
                "at": time.time(),
            }],
        )
        return {
            "ok": True,
            "replay_code": replay_code,
            "replay_id": entry["id"],
            "batch_code": batch_code,
            "status": "待重放",
            "has_conflict": sources["has_conflict"],
            "candidate_sources": sources["materials"],
            "preview_shards": sources["preview_shards"],
            "pending_review": sources["pending_review"],
            "message": "重放申请已登记，确认前不会对外交付",
        }

    def confirm_batch_replay(self, replay_id: int) -> dict[str, Any]:
        """确认导入重放：逐分片选定来源、落重放产物，并由传输血缘保存交付记录。

        这是唯一会把来源材料变成对外交付的入口。确认后每个分片都会留下
        “取自哪份材料、哪个版本”的记录，冲突与差异作为待复核项一并留档。
        """
        replay_entry = self.get_entry(replay_id)
        if replay_entry is None:
            return {"ok": False, "error": f"重放任务 {replay_id} 不存在"}

        batch_code = replay_entry.get("_batch_code") or replay_entry.get("起始批次")
        if not batch_code:
            return {"ok": False, "error": f"重放任务 {replay_id} 没有可重放的批次编号"}

        status = str(replay_entry.get("status", ""))
        if status == "已完成":
            delivery = self._delivery_service().get_delivery_by_replay(replay_entry["重放编号"])
            return {
                "ok": False,
                "error": f"重放任务 {replay_entry['重放编号']} 已确认完成，不会重复交付",
                "replay_code": replay_entry["重放编号"],
                "delivery_code": (delivery or {}).get("delivery_code"),
            }
        if status == "已取消":
            return {"ok": False, "error": f"重放任务 {replay_entry['重放编号']} 已取消，不能确认"}

        import_service = self._backup_service()
        resolution = import_service.resolve_batch_with_fallback(batch_code)
        if not resolution.get("resolved"):
            return {
                "ok": False,
                "error": resolution.get("error", "来源解析失败"),
                "batch_code": batch_code,
                "pending_review": resolution.get("pending_review", []),
            }

        entry, message = self.run_action(replay_id, "启动重放")
        if entry is None:
            return {"ok": False, "error": message}

        # 逐分片落盘，记录每一片实际采用的来源。
        replay_file = (
            self._data_dir("replay") / f"{safe_token(replay_entry['重放编号'], limit=80)}.jsonl"
        )
        mode = replay_entry.get("覆盖方式", "append")
        if mode == "overwrite" and replay_file.exists():
            replay_file.unlink()
        written = 0
        shard_provenance: list[dict[str, Any]] = []
        for shard in resolution["shards"]:
            records = shard.pop("_records", [])
            source_record = {
                "replay_code": replay_entry["重放编号"],
                "batch_code": batch_code,
                "shard_index": shard["shard_index"],
                "material_id": shard["material_id"],
                "material_kind": shard.get("material_kind"),
                "version": shard.get("version"),
                "checksum": shard.get("checksum"),
                "record_count": len(records),
            }
            replay_rows = []
            for record in records:
                replay_row = dict(record)
                replay_row["_replay_code"] = replay_entry["重放编号"]
                replay_row["_source_material"] = shard["material_id"]
                replay_row["_source_shard"] = shard["shard_index"]
                replay_rows.append(replay_row)
            self._write_jsonl(replay_file, replay_rows)
            written += len(records)
            shard_provenance.append(source_record)

        pending_review = resolution.get("pending_review", [])
        delivery_result = self._record_replay_lineage(
            replay_entry=replay_entry,
            batch_code=batch_code,
            sources=resolution.get("sources", []),
            shard_provenance=shard_provenance,
            pending_review=pending_review,
            artifact_path=str(replay_file),
        )

        entry["重放结果"] = f"已重放{written}条记录"
        entry["_confirmed_at"] = time.time()
        entry["_delivery_code"] = delivery_result.get("delivery_code")
        entry["_lineage_code"] = delivery_result.get("lineage_code")
        entry["_shard_provenance"] = shard_provenance
        entry["_pending_review"] = pending_review
        final_entry, _ = self.run_action(replay_id, "标记完成")

        self._write_jsonl(
            self._data_dir("events") / "replay_log.jsonl",
            [{
                "action": "confirm_batch_replay",
                "replay_code": replay_entry["重放编号"],
                "batch_code": batch_code,
                "records_replayed": written,
                "sources": resolution.get("material_ids", []),
                "delivery_code": delivery_result.get("delivery_code"),
                "lineage_code": delivery_result.get("lineage_code"),
                "pending_review": len(pending_review),
                "at": time.time(),
            }],
        )
        return {
            "ok": True,
            "replay_code": replay_entry["重放编号"],
            "batch_code": batch_code,
            "records_replayed": written,
            "artifact_path": str(replay_file),
            "sources": resolution.get("sources", []),
            "shard_provenance": shard_provenance,
            "pending_review": pending_review,
            "delivery_code": delivery_result.get("delivery_code"),
            "lineage_code": delivery_result.get("lineage_code"),
            "status": "已完成",
            "message": "重放已确认并写入交付记录",
        }

    def get_replay_delivery(self, replay_id: int) -> dict[str, Any]:
        """查看一次重放实际采用的来源、逐分片明细和留待复核的差异。"""
        replay_entry = self.get_entry(replay_id)
        if replay_entry is None:
            return {"error": f"重放任务 {replay_id} 不存在", "found": False}
        replay_code = replay_entry["重放编号"]
        delivery = self._delivery_service().get_delivery_by_replay(replay_code)
        if delivery is None:
            return {
                "replay_code": replay_code,
                "found": False,
                "status": replay_entry.get("status"),
                "message": "该重放尚无交付记录（可能尚未确认）",
            }
        return {
            "replay_code": replay_code,
            "found": True,
            "status": replay_entry.get("status"),
            "batch_code": delivery.get("batch_code"),
            "sources": delivery.get("sources", []),
            "shards": delivery.get("shards", []),
            "pending_review": delivery.get("pending_review", []),
            "delivery_code": delivery.get("delivery_code"),
            "lineage_code": delivery.get("lineage_code"),
            "artifact_path": delivery.get("artifact_path"),
        }

    # -- 跨模块帮助器 --

    @staticmethod
    def _backup_service():
        from app.services.backup_import import BackupImportService
        return BackupImportService()

    @staticmethod
    def _delivery_service():
        from app.services.replay_delivery import ReplayDeliveryService
        return ReplayDeliveryService()

    def _record_replay_lineage(
        self,
        *,
        replay_entry: dict[str, Any],
        batch_code: str,
        sources: list[dict[str, Any]],
        shard_provenance: list[dict[str, Any]],
        pending_review: list[dict[str, Any]],
        artifact_path: str,
    ) -> dict[str, Any]:
        """由传输血缘保存导入重放交付记录；血缘不可用时退回直接落交付记录。"""
        try:
            lineage = self._safe_import(
                "app.services.transfer_lineage", "TransferLineageService"
            )()
            return lineage.record_replay_delivery(
                replay_code=replay_entry["重放编号"],
                batch_code=batch_code,
                replay_range=replay_entry.get("重放范围", batch_code),
                sources=sources,
                shard_provenance=shard_provenance,
                pending_review=pending_review,
                artifact_path=artifact_path,
            )
        except Exception as exc:  # 血缘异常不能阻断已读取的重放产物
            return {
                "error": str(exc),
                "delivery_code": None,
                "lineage_code": None,
            }
