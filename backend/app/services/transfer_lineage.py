"""传输血缘领域服务：维护源端经抽取窗口、传输批次到目标端的交换链路血缘追溯。

本模块记录每一次数据交换的完整链路信息（源端→窗口→批次→目标端），
生成血缘链路产物文件，供下游数据追溯和质量审计使用。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.sharding import safe_token
from app.store import store


class TransferLineageService(BaseModuleService):
    MODULE = "transfer_lineage"
    ENTITY = "血缘链路"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["链路编号", "源端编号", "抽取窗口", "传输批次", "目标编号", "字段契约", "血缘版本", "采集时间"]
    REQUIRED_FIELDS = ["链路编号", "源端编号", "抽取窗口"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def record_lineage(self, batch_code: str, is_balanced: bool) -> dict[str, Any]:
        """记录传输批次的血缘链路：从源端经抽取窗口到目标端的完整数据交换链路。

        本方法遍历 store 中与批次关联的源端、抽取窗口、目标端记录，
        构建完整血缘链路并写入血缘产物文件，供数据追溯和质量审计。
        """
        # 从 store 中查找关联的源端登记、抽取窗口和目标端记录
        source_info = {}
        target_info = {}
        window_info = {}
        contract_info = {}

        for s in store.rows("source_register"):
            if s.get("status") == "已登记":
                source_info = s
                break
        for t in store.rows("target_register"):
            if t.get("status") == "已登记":
                target_info = t
                break
        for w in store.rows("extract_window"):
            if w.get("窗口编号", "").startswith("EXTR-"):
                window_info = w
                break
        for c in store.rows("field_contract"):
            if c.get("status") == "已生效":
                contract_info = c
                break

        lineage_code = f"TRAN-{batch_code[:8]}-{int(time.time())}"
        source_code = source_info.get("源端编号", f"SOUR-{batch_code[:8]}")
        window_code = window_info.get("窗口编号", f"EXTR-{batch_code[:8]}")
        target_code = target_info.get("目标编号", f"TARG-{batch_code[:8]}")
        contract_code = contract_info.get("契约编号", f"FIEL-{batch_code[:8]}")

        values = {
            "链路编号": lineage_code,
            "源端编号": source_code,
            "抽取窗口": window_code,
            "传输批次": batch_code,
            "目标编号": target_code,
            "字段契约": contract_code,
            "血缘版本": "v1.0",
            "采集时间": time.strftime("%Y-%m-%d", time.localtime(time.time())),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建血缘链路失败: {missing}")

        # 写入血缘产物文件——完整链路追溯
        lineage_file = self._data_dir("lineage") / f"{lineage_code}.jsonl"
        self._write_jsonl(lineage_file, [{
            "lineage_code": lineage_code,
            "source_code": source_code,
            "source_system": source_info.get("系统名称", "unknown"),
            "extract_window": window_code,
            "transfer_batch": batch_code,
            "target_code": target_code,
            "target_system": target_info.get("系统名称", "unknown"),
            "field_contract": contract_code,
            "balanced": is_balanced,
            "recorded_at": time.time(),
        }])

        final_entry, _ = self.run_action(entry["id"], "确认生效")

        return {
            "lineage_code": lineage_code,
            "batch_code": batch_code,
            "source": source_code,
            "target": target_code,
            "balanced": is_balanced,
        }

    def query_lineage(self, batch_code: str) -> dict[str, Any]:
        """查询指定传输批次的血缘链路，追溯数据从源端到目标端的完整流转路径。"""
        lineages = []
        lineage_dir = self._data_dir("lineage")
        for lf in lineage_dir.glob(f"*{batch_code[:8]}*.jsonl"):
            lineages.extend(self._read_jsonl(lf))
        return {"batch_code": batch_code, "lineages": lineages, "count": len(lineages)}

    def get_active_lineages(self) -> list[dict[str, Any]]:
        """获取所有已生效的血缘链路列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]
    # ==================================================================
    # 导入重放交付记录：由血缘保存本次重放实际采用的来源与逐分片明细
    # ==================================================================

    def record_replay_delivery(
        self,
        *,
        replay_code: str,
        batch_code: str,
        replay_range: str,
        sources: list[dict[str, Any]],
        shard_provenance: list[dict[str, Any]],
        pending_review: list[dict[str, Any]],
        artifact_path: str | None = None,
    ) -> dict[str, Any]:
        """在传输血缘中保存一次导入重放的交付记录，并落盘逐分片来源明细。

        血缘链路把这些交付关联到对应批次的抽取窗口与目标端；后续补处理的人
        可以通过 ``query_replay_deliveries`` 看到这次从哪份材料取了哪些分片，
        以及还有哪些差异留待复核。
        """
        source_info = {}
        for row in store.rows("source_register"):
            if row.get("status") == "已登记":
                source_info = row
                break
        target_info = {}
        for row in store.rows("target_register"):
            if row.get("status") == "已登记":
                target_info = row
                break
        window_info = {}
        for row in store.rows("extract_window"):
            if str(row.get("窗口编号", "")).startswith("EXTR-"):
                window_info = row
                break
        contract_info = {}
        for row in store.rows("field_contract"):
            if row.get("status") == "已生效":
                contract_info = row
                break

        lineage_code = f"TRAN-RPL-{safe_token(batch_code, limit=32)}-{uuid.uuid4().hex[:6]}"
        source_code = source_info.get("源端编号", f"SOUR-{safe_token(batch_code, limit=8)}")
        window_code = window_info.get("窗口编号", f"EXTR-{safe_token(batch_code, limit=8)}")
        target_code = target_info.get("目标编号", f"TARG-{safe_token(batch_code, limit=8)}")
        contract_code = contract_info.get("契约编号", f"FIEL-{safe_token(batch_code, limit=8)}")

        material_ids = sorted({str(item["material_id"]) for item in shard_provenance})
        values = {
            "链路编号": lineage_code,
            "源端编号": source_code,
            "抽取窗口": window_code,
            "传输批次": batch_code,
            "目标编号": target_code,
            "字段契约": contract_code,
            "血缘版本": "v1.0-import",
            "采集时间": time.strftime("%Y-%m-%d", time.localtime(time.time())),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建重放血缘链路失败: {missing}")
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["_replay_code"] = replay_code
        entry["_delivery_materials"] = material_ids
        entry["_pending_review"] = len(pending_review)

        # 逐分片来源明细落到血缘产物，供补处理时直接查阅。
        lineage_file = self._data_dir("lineage") / f"{lineage_code}_replay_delivery.jsonl"
        self._write_jsonl(lineage_file, [{
            "lineage_code": lineage_code,
            "replay_code": replay_code,
            "batch_code": batch_code,
            "replay_range": replay_range,
            "sources": sources,
            "shards": shard_provenance,
            "pending_review": pending_review,
            "artifact_path": artifact_path,
            "recorded_at": time.time(),
        }])

        delivery = self._safe_import(
            "app.services.replay_delivery", "ReplayDeliveryService"
        )().record_delivery(
            replay_code=replay_code,
            batch_code=batch_code,
            replay_range=replay_range,
            sources=sources,
            shard_provenance=shard_provenance,
            pending_review=pending_review,
            lineage_code=lineage_code,
            artifact_path=artifact_path,
        )

        entry["_delivery_code"] = delivery.get("delivery_code")
        return {
            "lineage_code": lineage_code,
            "batch_code": batch_code,
            "replay_code": replay_code,
            "delivery_code": delivery.get("delivery_code"),
            "delivery_id": delivery.get("delivery_id"),
            "sources": material_ids,
            "shard_count": len(shard_provenance),
            "pending_review": len(pending_review),
            "lineage_path": str(lineage_file),
        }

    def query_replay_deliveries(self, batch_code: str) -> dict[str, Any]:
        """查询某批次的导入重放交付记录，返回逐分片来源与待复核差异。"""
        deliveries = []
        lineage_dir = self._data_dir("lineage")
        for lf in sorted(lineage_dir.glob("*_replay_delivery.jsonl")):
            for record in self._read_jsonl(lf):
                if record.get("batch_code") != batch_code:
                    continue
                deliveries.append(record)
        return {
            "batch_code": batch_code,
            "deliveries": deliveries,
            "count": len(deliveries),
        }
