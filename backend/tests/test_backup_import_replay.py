"""备份包导入与导入重放链路测试。

覆盖任务要求的关键场景：
- 导入时检查压缩包内的批次与分片；
- 同一批次存在离线包与本地归档卷两个合法来源、分片版本不一致时的来源冲突；
- 重放申请确认前暂存离线包不产生对外交付；
- 确认后逐分片关联实际采用的来源，并由传输血缘保存交付记录；
- 坏包不影响已有本地归档的独立读取；
- 重复上传与坏包上传后，已确认重放范围与本地卷读取保持不变。
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from app.services import BackupImportService, ExchangeReplayService, TransferLineageService
from app.services import base as base_module
from app.services import backup_import as backup_module
from app.services.backup_import import create_package_bytes
from app.services.base import DATA_ROOT
from app.services.replay_delivery import ReplayDeliveryService
from app.store import store


@pytest.fixture(autouse=True)
def isolated_data_root(tmp_path, monkeypatch):
    """把运行期产物目录重定向到临时目录，避免测试之间互相污染。"""
    monkeypatch.setattr(base_module, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(backup_module, "DATA_ROOT", tmp_path)
    return tmp_path


def _records(
    batch: str, count: int, *, version: str = "v1", prefix: str = "r", shard_size: int = 3,
) -> list[dict]:
    return [
        {
            "batch_code": batch,
            "chunk_index": index // max(1, shard_size),
            "id": index,
            "value": f"{prefix}-{index}",
            "version": version,
        }
        for index in range(count)
    ]


def _import(service: BackupImportService, batches: dict[str, list[dict]], **kwargs):
    data = create_package_bytes(
        batches, manifest_versions={code: recs[0].get("version", "v1")
                                      for code, recs in batches.items()},
    )
    return service.import_package(data, filename=kwargs.pop("filename", "package.zip"), **kwargs)


def _register_local(tmp_path: Path, archive_code: str, batches: dict[str, list[dict]]):
    """在临时数据根下造一个本地归档卷目录并登记。"""
    volume = tmp_path / "archive" / archive_code
    volume.mkdir(parents=True, exist_ok=True)
    for batch, records in batches.items():
        (volume / f"{batch}_bundle.jsonl").write_text(
            "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
            encoding="utf-8",
        )
    return BackupImportService().register_local_volume(archive_code)


class TestPackageImport:
    def test_import_scans_batches_and_shards(self):
        service = BackupImportService()
        records = _records("BATCH-IMP-1", 6)
        result = _import(service, {"BATCH-IMP-1": records})

        assert result["ok"] is True
        assert result["duplicate"] is False
        assert result["status"] == "待核验"
        assert "BATCH-IMP-1" in result["batches"]
        batch = result["batches"]["BATCH-IMP-1"]
        assert batch["record_count"] == 6
        assert batch["versions"] == ["v1"]
        assert [shard["shard_index"] for shard in batch["shards"]] == [1, 2]
        assert sum(shard["record_count"] for shard in batch["shards"]) == 6

        # 材料清单可以独立回查，分片内容可读取且校验一致。
        manifest = service.get_material(result["material_id"])
        assert manifest["kind"] == "离线包"
        stored, error = service.read_shard_records(result["material_id"], "BATCH-IMP-1", 1)
        assert error is None
        assert len(stored) == 3

    def test_import_without_manifest_recognises_batch_from_path(self):
        service = BackupImportService()
        buffer = io.BytesIO()
        rows = [{"id": index, "value": f"row-{index}"} for index in range(4)]
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(
                "BATCH-PATH/shard-0001.jsonl",
                "\n".join(json.dumps(row, ensure_ascii=False) for row in rows),
            )
        result = service.import_package(buffer.getvalue(), filename="no-manifest.zip")
        assert result["ok"] is True
        assert "BATCH-PATH" in result["batches"]

    def test_broken_package_is_rejected(self):
        service = BackupImportService()
        result = service.import_package(b"this is not a zip", filename="broken.zip")
        assert result["ok"] is False
        assert result["material_id"] is None
        assert "损坏" in result["error"] or "无法打开" in str(result["errors"])
        assert service.list_materials() == []

    def test_package_with_traversal_member_is_rejected(self):
        service = BackupImportService()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("../../escape.jsonl", '{"batch_code": "B"}\n')
        result = service.import_package(buffer.getvalue(), filename="evil.zip")
        assert result["ok"] is False
        assert any("上级目录" in error for error in result["errors"])

    def test_manifest_checksum_mismatch_is_rejected(self):
        service = BackupImportService()
        buffer = io.BytesIO()
        manifest = {"batches": [{
            "batch_code": "BATCH-BAD-SUM",
            "shards": [{
                "shard_index": 1,
                "entry": "BATCH-BAD-SUM/shard-0001.jsonl",
                "checksum": "SHA256:" + "0" * 64,
                "record_count": 1,
            }],
        }]}
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            archive.writestr("BATCH-BAD-SUM/shard-0001.jsonl", '{"batch_code": "BATCH-BAD-SUM"}\n')
        result = service.import_package(buffer.getvalue(), filename="bad-sum.zip")
        assert result["ok"] is False, result
        assert any("校验值" in error and "不符" in error for error in result["errors"]), result["errors"]

    def test_duplicate_upload_reuses_material(self):
        service = BackupImportService()
        payload = create_package_bytes({"BATCH-DUP": _records("BATCH-DUP", 3)})
        first = service.import_package(payload, filename="dup.zip")
        second = service.import_package(payload, filename="dup-again.zip")
        assert first["ok"] and second["ok"]
        assert second["duplicate"] is True
        assert second["material_id"] == first["material_id"]
        assert len(service.list_materials(include_rejected=True)) == 1


class TestSourceConflictAndProvenance:
    def test_two_legal_sources_with_different_versions_conflict(self, tmp_path):
        service = BackupImportService()
        batch = "BATCH-CONFLICT"
        offline_records = _records(batch, 4, version="v2", prefix="offline")
        local_records = _records(batch, 4, version="v1", prefix="local")
        imported = _import(service, {batch: offline_records}, filename="offline.zip")
        local = _register_local(tmp_path, "VOL-CONFLICT", {batch: local_records})
        assert imported["ok"] and local["ok"]

        snapshot = service.get_batch_sources(batch)
        assert snapshot["has_conflict"] is True
        assert snapshot["conflicts"][0]["versions"] == ["v1", "v2"]
        kinds = {material["kind"] for material in snapshot["materials"]}
        assert kinds == {"离线包", "本地归档卷"}

    def test_request_does_not_deliver_until_confirmed(self, tmp_path):
        service = BackupImportService()
        replay = ExchangeReplayService()
        lineage = TransferLineageService()
        batch = "BATCH-PENDING"
        _import(service, {batch: _records(batch, 4, version="v2")}, filename="pending.zip")
        _register_local(tmp_path, "VOL-PENDING", {batch: _records(batch, 4, version="v1")})

        request = replay.request_batch_replay(batch, requested_by="operator")
        assert request["ok"] is True
        assert request["status"] == "待重放"
        assert request["has_conflict"] is True

        replay_id = request["replay_id"]
        # 申请后、确认前：没有交付记录，材料仍是待核验，血缘里也没有交付。
        assert replay.get_replay_delivery(replay_id)["found"] is False
        assert service.get_confirmed_replay_scope(batch)["count"] == 0
        assert lineage.query_replay_deliveries(batch)["count"] == 0
        statuses = {m["kind"]: m["status"] for m in service.list_materials(batch)}
        assert statuses["离线包"] == "待核验"
        # 暂存的离线包没有被当成对外交付。
        assert service.get_confirmed_replay_scope(batch)["confirmed_replays"] == []

    def test_confirm_records_per_shard_source_and_pending_review(self, tmp_path):
        service = BackupImportService()
        replay = ExchangeReplayService()
        lineage = TransferLineageService()
        batch = "BATCH-CONFIRM"
        offline_records = _records(batch, 4, version="v2", prefix="offline")
        local_records = _records(batch, 4, version="v1", prefix="local")
        imported = _import(service, {batch: offline_records}, filename="first.zip")
        local = _register_local(tmp_path, "VOL-CONFIRM", {batch: local_records})

        request = replay.request_batch_replay(batch)
        result = replay.confirm_batch_replay(request["replay_id"])

        assert result["ok"] is True
        assert result["records_replayed"] == 4
        assert result["delivery_code"]
        assert result["lineage_code"]
        # 本地归档卷优先，第一次真正取到的分片来自本地卷 v1。
        chosen = result["shard_provenance"]
        assert chosen and all(item["material_id"] == local["material_id"] for item in chosen)
        assert all(item["version"] == "v1" for item in chosen)
        assert all(item["material_kind"] == "本地归档卷" for item in chosen)
        # 离线包的差异留待复核，且记录了两边的材料与版本。
        review = result["pending_review"]
        assert any(item["kind"] == "来源冲突" for item in review)
        conflict_review = next(item for item in review if item["kind"] == "来源冲突")
        assert conflict_review["alternate_material_id"] == imported["material_id"]
        assert conflict_review["chosen_version"] == "v1"
        assert conflict_review["alternate_version"] == "v2"

        # 重放产物里每条记录都能追溯到材料与分片。
        artifact = Path(result["artifact_path"])
        rows = [json.loads(line) for line in artifact.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 4
        assert {row["_source_material"] for row in rows} == {local["material_id"]}

        # 传输血缘保存了交付记录：从哪份材料取了哪些分片、还有哪些差异。
        deliveries = lineage.query_replay_deliveries(batch)
        assert deliveries["count"] == 1
        record = deliveries["deliveries"][0]
        assert record["replay_code"] == request["replay_code"]
        assert {source["material_id"] for source in record["sources"]} == {local["material_id"]}
        assert [shard["material_id"] for shard in record["shards"]] == [
            item["material_id"] for item in result["shard_provenance"]
        ]
        assert record["pending_review"]

        # 重放任务详情的交付查询可直接用于补处理。
        detail = replay.get_replay_delivery(request["replay_id"])
        assert detail["found"] is True
        assert detail["delivery_code"] == result["delivery_code"]
        assert detail["pending_review"]

    def test_confirm_twice_is_refused(self, tmp_path):
        service = BackupImportService()
        replay = ExchangeReplayService()
        batch = "BATCH-TWICE"
        _import(service, {batch: _records(batch, 2)})
        request = replay.request_batch_replay(batch)
        assert replay.confirm_batch_replay(request["replay_id"])["ok"] is True
        second = replay.confirm_batch_replay(request["replay_id"])
        assert second["ok"] is False
        assert "已确认" in second["error"]
        assert service.get_confirmed_replay_scope(batch)["count"] == 1

    def test_fallback_when_preferred_source_is_unreadable(self, tmp_path):
        service = BackupImportService()
        replay = ExchangeReplayService()
        batch = "BATCH-FALLBACK"
        offline = _import(service, {batch: _records(batch, 2, version="v9", prefix="offline")})
        local = _register_local(tmp_path, "VOL-FALLBACK", {batch: _records(batch, 2, version="v1")})

        # 模拟本地归档卷在登记后又损坏：首选来源不可读，应退到离线包。
        manifest = service.get_material(local["material_id"])
        shard = manifest["batches"][batch]["shards"][0]
        (service._material_dir(local["material_id"]) / shard["entry_path"]).unlink()

        request = replay.request_batch_replay(batch)
        result = replay.confirm_batch_replay(request["replay_id"])
        assert result["ok"] is True
        assert result["shard_provenance"][0]["material_id"] == offline["material_id"]
        assert any(item["kind"] == "来源回退" for item in result["pending_review"])


class TestIsolationAfterBadUploads:
    def test_bad_package_does_not_break_local_volume(self, tmp_path):
        service = BackupImportService()
        batch = "BATCH-ISOLATED"
        local = _register_local(tmp_path, "VOL-ISOLATED", {batch: _records(batch, 3)})
        assert local["ok"] is True

        rejected = service.import_package(b"definitely not a zip", filename="broken.zip")
        assert rejected["ok"] is False

        # 本地归档仍可独立取回。
        records, error = service.read_shard_records(local["material_id"], batch, 1)
        assert error is None
        assert len(records) == 3
        sources = service.get_batch_sources(batch)
        assert [material["material_id"] for material in sources["materials"]] == [local["material_id"]]

    def test_duplicate_upload_keeps_confirmed_scope_and_local_read(self, tmp_path):
        service = BackupImportService()
        replay = ExchangeReplayService()
        batch = "BATCH-STABLE"
        payload = create_package_bytes({"BATCH-STABLE": _records(batch, 3, version="v2")})
        imported = service.import_package(payload, filename="stable.zip")
        local = _register_local(tmp_path, "VOL-STABLE", {batch: _records(batch, 3, version="v1")})

        request = replay.request_batch_replay(batch)
        confirmed = replay.confirm_batch_replay(request["replay_id"])
        assert confirmed["ok"] is True
        scope_before = service.get_confirmed_replay_scope(batch)
        assert scope_before["count"] == 1

        # 重复上传同一份包：复用材料，已确认重放范围不变。
        duplicate = service.import_package(payload, filename="stable-again.zip")
        assert duplicate["duplicate"] is True
        assert duplicate["material_id"] == imported["material_id"]
        assert service.get_confirmed_replay_scope(batch) == scope_before

        # 重复上传后再来一个坏包：本地归档卷读取与已确认范围都不受影响。
        assert service.import_package(b"broken-bytes", filename="bad.zip")["ok"] is False
        records, error = service.read_shard_records(local["material_id"], batch, 1)
        assert error is None and len(records) == 3
        assert service.get_confirmed_replay_scope(batch) == scope_before
        delivery = ReplayDeliveryService().get_delivery_by_replay(request["replay_code"])
        assert delivery is not None
        assert len(delivery["shards"]) == len(confirmed["shard_provenance"])

    def test_request_without_materials_is_refused(self):
        replay = ExchangeReplayService()
        result = replay.request_batch_replay("BATCH-NO-SOURCE")
        assert result["ok"] is False
        assert "没有可用的备份来源" in result["error"]
