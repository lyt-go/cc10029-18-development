"""备份包导入领域服务：接收外部归档数据包、检查包内批次与分片，登记为待核验来源材料。

导入只负责“把材料收进来并看清里面有什么”：
- 校验压缩包结构、成员路径与校验值，损坏包直接拒收，不影响已有本地归档卷；
- 扫描包内批次、分片、分片版本和条数，形成材料清单；
- 同一批次允许多份合法材料（离线包、本地归档卷），版本不一致时记为来源冲突；
- 暂存材料在重放确认之前只是候选来源，不会被当作已对外交付。

重放时按确定的优先级逐分片选取来源：本地归档卷优先于离线包，同类型内已核验优先于待核验，
再按材料编号排序保证结果可复现。
"""
from __future__ import annotations

import hashlib
import io
import json
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService, DATA_ROOT, _ensure_dir
from app.services.sharding import (
    DEFAULT_SHARD_SIZE,
    batch_code_of,
    checksum_matches,
    checksum_of_records,
    normalize_version,
    safe_token,
    shards_from_records,
    strip_shard_records,
    version_of_records,
)
from app.store import store

# 压缩包扫描上限，防止异常包把内存打满。
MAX_MEMBERS = 4096
MAX_MEMBER_BYTES = 64 * 1024 * 1024
MAX_TOTAL_BYTES = 256 * 1024 * 1024

KIND_OFFLINE = "离线包"
KIND_LOCAL = "本地归档卷"

# 来源优先级：数值越小越优先。本地归档卷是原始读取，优先于离线包；
# 同类型内按核验状态（已核验/可读优先于待核验），最后按材料编号排序，
# 保证同一输入始终选出同一来源。
_KIND_RANK = {KIND_LOCAL: 0, KIND_OFFLINE: 1}
_STATUS_RANK = {"已核验": 0, "可读": 0, "待核验": 1}

MATERIALS_DIR = "backup_import/materials"


class BackupImportError(Exception):
    """导入被拒时抛出，携带可读原因列表。"""

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or [message]


class BackupImportService(BaseModuleService):
    MODULE = "backup_import"
    ENTITY = "备份包"
    OPERATION_TERM = "核验"
    LIST_FIELDS = ["包编号", "批次范围", "来源类型", "分片版本", "包内批次", "分片数", "校验状态", "核验状态"]
    REQUIRED_FIELDS = ["包编号", "批次范围", "来源类型"]
    STATUS_ORDER = ["待核验", "可读", "已核验", "已拒收"]
    ACTION_RULES = {"确认核验": "已核验", "标记可读": "可读", "标记拒收": "已拒收"}
    NEGATIVE_ACTIONS = ["标记拒收"]

    # ------------------------------------------------------------------
    # 材料目录与清单
    # ------------------------------------------------------------------

    def _materials_root(self) -> Path:
        path = DATA_ROOT / MATERIALS_DIR
        _ensure_dir(path)
        return path

    def _material_dir(self, material_id: str) -> Path:
        return self._materials_root() / safe_token(material_id, limit=96)

    def _material_manifest_path(self, material_id: str) -> Path:
        return self._material_dir(material_id) / "manifest.json"

    def get_material(self, material_id: str) -> dict[str, Any] | None:
        """按材料编号读取材料清单，不存在返回 None。"""
        path = self._material_manifest_path(material_id)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def list_materials(
        self, batch_code: str | None = None, *, include_rejected: bool = False,
    ) -> list[dict[str, Any]]:
        """列出材料清单；可按批次过滤，默认排除已拒收材料。"""
        materials = []
        root = self._materials_root()
        for manifest_path in sorted(root.glob("*/manifest.json")):
            try:
                with open(manifest_path, "r", encoding="utf-8") as handle:
                    manifest = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            if not include_rejected and manifest.get("status") == "已拒收":
                continue
            if batch_code and batch_code not in manifest.get("batches", {}):
                continue
            materials.append(manifest)
        materials.sort(key=lambda item: (str(item.get("imported_at", "")), str(item.get("material_id"))))
        return materials

    def _save_material(self, manifest: dict[str, Any]) -> Path:
        path = self._material_manifest_path(manifest["material_id"])
        _ensure_dir(path.parent)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2, default=str)
        tmp.replace(path)
        return path

    def _material_records_dir(self, material_id: str, batch_code: str) -> Path:
        path = self._material_dir(material_id) / "records" / safe_token(batch_code)
        _ensure_dir(path)
        return path

    def _store_shards(
        self, material_id: str, batch_code: str, shards: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """把分片记录写到材料目录，返回可持久化的分片清单（含相对文件位置）。"""
        directory = self._material_records_dir(material_id, batch_code)
        stored: list[dict[str, Any]] = []
        for shard in shards:
            index = int(shard["shard_index"])
            filename = f"shard-{index:04d}.jsonl"
            self._write_jsonl(directory / filename, shard["records"])
            record = {
                "shard_index": index,
                "version": normalize_version(shard.get("version")),
                "checksum": str(shard.get("checksum", "")),
                "record_count": int(shard.get("record_count", 0)),
                "entry_path": f"records/{safe_token(batch_code)}/{filename}",
                "entry_size": sum(len(str(r).encode("utf-8")) for r in shard["records"]),
            }
            stored.append(record)
        return stored

    # ------------------------------------------------------------------
    # 导入离线压缩包
    # ------------------------------------------------------------------

    def import_package(
        self,
        data: bytes | None = None,
        *,
        filename: str = "backup.zip",
        file_path: str | Path | None = None,
        label: str | None = None,
        requested_by: str | None = None,
        shard_size: int = DEFAULT_SHARD_SIZE,
    ) -> dict[str, Any]:
        """导入一个外部归档压缩包，扫描其中的批次与分片并登记为待核验材料。

        导入成功只代表材料已暂存、可供重放候选；不会生成任何交付记录。
        压缩包损坏、成员越界或校验值不符时整体拒收，且不触碰已有本地归档材料。
        """
        if data is None and file_path is not None:
            try:
                data = Path(file_path).read_bytes()
            except OSError as exc:
                return self._rejected_result(
                    f"备份包读取失败：{exc}", [f"无法读取文件 {file_path}：{exc}"],
                    filename=str(file_path), requested_by=requested_by,
                )
        if data is None:
            return self._rejected_result(
                "未收到备份包内容", ["备份包内容为空"], filename=filename, requested_by=requested_by,
            )
        if not data:
            return self._rejected_result(
                "备份包内容为空", ["备份包内容为空"], filename=filename, requested_by=requested_by,
            )

        package_checksum = f"SHA256:{hashlib.sha256(data).hexdigest()}"

        # 重复上传：同一份内容只登记一次，已有交付与确认范围不受影响。
        existing = self._find_material_by_checksum(package_checksum, kind=KIND_OFFLINE)
        if existing is not None:
            return {
                "ok": True,
                "duplicate": True,
                "material_id": existing["material_id"],
                "status": existing.get("status"),
                "batches": sorted(existing.get("batches", {})),
                "package_checksum": package_checksum,
                "message": "相同备份包已导入过，复用原材料，不重复登记",
            }

        try:
            batches, warnings = self._scan_zip(data, shard_size=shard_size)
        except BackupImportError as exc:
            return self._rejected_result(
                str(exc), exc.errors, filename=filename,
                package_checksum=package_checksum, requested_by=requested_by,
            )

        if not batches:
            return self._rejected_result(
                "压缩包内未找到可识别的批次或分片",
                warnings or ["没有匹配到批次记录"],
                filename=filename, package_checksum=package_checksum, requested_by=requested_by,
            )

        material_id = f"MAT-OFF-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        manifest = self._build_manifest(
            material_id=material_id,
            kind=KIND_OFFLINE,
            label=label or filename,
            source_path=str(file_path) if file_path else filename,
            status="待核验",
            batches=batches,
            package_checksum=package_checksum,
            warnings=warnings,
            requested_by=requested_by,
        )
        return self._persist_import(manifest)

    def _persist_import(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """落盘材料清单并同步登记到模块列表，返回导入摘要。"""
        stored_batches: dict[str, Any] = {}
        for batch_code, batch in manifest["batches"].items():
            shards = self._store_shards(manifest["material_id"], batch_code, batch["_shards"])
            stored_batches[batch_code] = {
                "batch_code": batch_code,
                "record_count": int(batch.get("record_count", 0)),
                "versions": sorted({
                    normalize_version(shard["version"]) for shard in shards
                }),
                "shards": shards,
            }
        manifest["batches"] = stored_batches
        manifest["record_count"] = sum(b["record_count"] for b in stored_batches.values())
        manifest["shard_count"] = sum(len(b["shards"]) for b in stored_batches.values())
        self._save_material(manifest)
        self._register_row(manifest)
        self._write_jsonl(
            self._data_dir("events") / "backup_import_log.jsonl",
            [{
                "action": "import_package",
                "material_id": manifest["material_id"],
                "kind": manifest["kind"],
                "batches": sorted(stored_batches),
                "shard_count": manifest["shard_count"],
                "status": manifest["status"],
                "package_checksum": manifest.get("package_checksum"),
                "at": time.time(),
            }],
        )
        return {
            "ok": True,
            "duplicate": False,
            "material_id": manifest["material_id"],
            "status": manifest["status"],
            "package_checksum": manifest.get("package_checksum"),
            "batches": {
                code: {
                    "record_count": batch["record_count"],
                    "versions": batch["versions"],
                    "shards": [
                        {key: shard[key] for key in
                         ("shard_index", "version", "checksum", "record_count")}
                        for shard in batch["shards"]
                    ],
                }
                for code, batch in stored_batches.items()
            },
            "warnings": manifest.get("warnings", []),
            "message": "备份包已暂存为待核验材料，尚未对外交付",
        }

    def _build_manifest(
        self,
        *,
        material_id: str,
        kind: str,
        label: str,
        source_path: str,
        status: str,
        batches: dict[str, Any],
        package_checksum: str | None = None,
        warnings: list[str] | None = None,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for batch_code, batch in batches.items():
            normalized[batch_code] = {
                "batch_code": batch_code,
                "record_count": int(batch.get("record_count", 0)),
                "shards": strip_shard_records(batch.get("shards", [])),
                "_shards": batch.get("shards", []),
            }
        return {
            "material_id": material_id,
            "kind": kind,
            "label": label,
            "source_path": source_path,
            "status": status,
            "package_checksum": package_checksum,
            "imported_at": time.time(),
            "requested_by": requested_by,
            "batches": normalized,
            "warnings": warnings or [],
        }

    def _register_row(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """把材料同步到 backup_import 列表行，供通用工作台查询与核验。"""
        batches = manifest.get("batches", {})
        versions = sorted({
            version for batch in batches.values() for version in batch.get("versions", [])
        })
        values = {
            "包编号": manifest["material_id"],
            "批次范围": "、".join(sorted(batches)) or "无",
            "来源类型": manifest["kind"],
            "分片版本": "、".join(versions) or "未声明",
            "包内批次": str(len(batches)),
            "分片数": str(manifest.get("shard_count", 0)),
            "校验状态": "通过" if manifest.get("status") != "已拒收" else "失败",
            "核验状态": manifest.get("status", "待核验"),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise BackupImportError(f"登记材料列表失败：缺少 {'、'.join(missing)}")
        entry["status"] = manifest.get("status", "待核验")
        entry["pending"] = entry["status"] == "待核验"
        entry["abnormal"] = entry["status"] == "已拒收"
        entry["_material_id"] = manifest["material_id"]
        entry["_kind"] = manifest["kind"]
        entry["_package_checksum"] = manifest.get("package_checksum")
        return entry

    def _rejected_result(
        self,
        message: str,
        errors: list[str],
        *,
        filename: str,
        package_checksum: str | None = None,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        """拒收结果：记录审计行，但不产生任何材料或交付。"""
        material_id = f"MAT-REJ-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        values = {
            "包编号": material_id,
            "批次范围": "无",
            "来源类型": KIND_OFFLINE,
            "分片版本": "未声明",
            "包内批次": "0",
            "分片数": "0",
            "校验状态": "失败",
            "核验状态": "已拒收",
        }
        entry, _ = self.create_entry(values)
        if entry is not None:
            entry["status"] = "已拒收"
            entry["pending"] = False
            entry["abnormal"] = True
            entry["_errors"] = errors
            entry["_source_file"] = filename
        self._write_jsonl(
            self._data_dir("events") / "backup_import_log.jsonl",
            [{
                "action": "reject_package",
                "material_id": material_id,
                "source_file": filename,
                "package_checksum": package_checksum,
                "errors": errors,
                "requested_by": requested_by,
                "at": time.time(),
            }],
        )
        return {
            "ok": False,
            "duplicate": False,
            "material_id": None,
            "rejected_material_id": material_id,
            "error": message,
            "errors": errors,
            "package_checksum": package_checksum,
            "message": "备份包已拒收，未登记为可重放来源",
        }

    def _find_material_by_checksum(self, checksum: str, *, kind: str | None = None) -> dict[str, Any] | None:
        if not checksum:
            return None
        for manifest in self.list_materials(include_rejected=False):
            if kind and manifest.get("kind") != kind:
                continue
            if manifest.get("package_checksum") == checksum:
                return manifest
        return None

    # ------------------------------------------------------------------
    # 压缩包扫描
    # ------------------------------------------------------------------

    def _scan_zip(self, data: bytes, *, shard_size: int) -> tuple[dict[str, Any], list[str]]:
        try:
            archive = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile as exc:
            raise BackupImportError(f"压缩包已损坏：{exc}", [f"无法打开压缩包：{exc}"]) from exc
        except OSError as exc:
            raise BackupImportError(f"压缩包读取失败：{exc}", [str(exc)]) from exc

        warnings: list[str] = []
        members: dict[str, bytes] = {}
        manifest_doc: dict[str, Any] | None = None
        total_bytes = 0
        try:
            infos = archive.infolist()
            if len(infos) > MAX_MEMBERS:
                raise BackupImportError(
                    f"压缩包成员过多（{len(infos)}）", [f"成员数超过上限 {MAX_MEMBERS}"]
                )
            for info in infos:
                name = info.filename
                if info.is_dir():
                    continue
                # 先做路径安全检查，再跳过系统自带的隐藏文件，避免越界成员被静默忽略。
                error = self._unsafe_member_reason(name)
                if error:
                    raise BackupImportError(f"压缩包包含不安全路径：{name}", [error])
                if name.startswith("__MACOSX/") or name.rsplit("/", 1)[-1].startswith("."):
                    continue
                if info.flag_bits & 0x1:
                    raise BackupImportError(f"压缩包成员已加密：{name}", [f"不支持加密成员 {name}"])
                if info.file_size > MAX_MEMBER_BYTES:
                    raise BackupImportError(
                        f"压缩包成员过大：{name}", [f"{name} 超过单文件上限 {MAX_MEMBER_BYTES} 字节"]
                    )
                total_bytes += info.file_size
                if total_bytes > MAX_TOTAL_BYTES:
                    raise BackupImportError("压缩包解压后体积过大", [f"总解压体积超过 {MAX_TOTAL_BYTES} 字节"])
                try:
                    content = archive.read(info)
                except (zipfile.BadZipFile, OSError, RuntimeError) as exc:
                    raise BackupImportError(
                        f"压缩包成员校验失败：{name}", [f"{name} 读取失败：{exc}"]
                    ) from exc
                lower = name.lower()
                if lower.endswith("manifest.json"):
                    try:
                        manifest_doc = json.loads(content.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise BackupImportError(
                            f"清单文件无法解析：{name}", [f"{name} JSON 解析失败：{exc}"]
                        ) from exc
                    continue
                if lower.endswith((".jsonl", ".json", ".ndjson")):
                    members[name] = content
        finally:
            archive.close()

        if manifest_doc is not None:
            batches, manifest_warnings = self._batches_from_manifest(manifest_doc, members, shard_size)
            warnings.extend(manifest_warnings)
        else:
            batches = self._batches_from_members(members, shard_size)
            if not batches and members:
                warnings.append("压缩包内有数据文件，但未能识别批次编号")

        return batches, warnings

    @staticmethod
    def _unsafe_member_reason(name: str) -> str | None:
        """检查压缩成员路径是否越界（绝对路径、上级目录、盘符、反斜杠）。"""
        normalized = name.replace("\\", "/")
        if normalized.startswith("/") or (len(normalized) > 1 and normalized[1] == ":"):
            return f"成员使用绝对路径：{name}"
        parts = [part for part in normalized.split("/") if part not in ("", ".")]
        if any(part == ".." for part in parts):
            return f"成员路径包含上级目录：{name}"
        return None

    def _batches_from_manifest(
        self, manifest: dict[str, Any], members: dict[str, bytes], shard_size: int,
    ) -> tuple[dict[str, Any], list[str]]:
        """按清单声明的批次/分片读取成员，并校验声明校验值与实际内容。"""
        warnings: list[str] = []
        batches: dict[str, Any] = {}
        declared = manifest.get("batches")
        entries: list[dict[str, Any]] = []
        if isinstance(declared, list):
            entries = [item for item in declared if isinstance(item, dict)]
        elif isinstance(declared, dict):
            for code, value in declared.items():
                if isinstance(value, dict):
                    entry = dict(value)
                    entry.setdefault("batch_code", code)
                    entries.append(entry)
                elif isinstance(value, list):
                    entries.append({"batch_code": code, "shards": value})

        used_members: set[str] = set()
        for entry in entries:
            batch_code = str(entry.get("batch_code") or entry.get("batch") or "").strip()
            shards_decl = entry.get("shards") or entry.get("chunks") or []
            if not batch_code:
                warnings.append("清单中有批次缺少 batch_code，已跳过")
                continue
            shards: list[dict[str, Any]] = []
            for position, shard_decl in enumerate(shards_decl, start=1):
                if not isinstance(shard_decl, dict):
                    continue
                member = str(
                    shard_decl.get("entry") or shard_decl.get("member")
                    or shard_decl.get("path") or ""
                ).strip()
                if not member:
                    warnings.append(f"{batch_code} 的分片缺少成员路径，已跳过")
                    continue
                if member not in members:
                    raise BackupImportError(
                        f"清单声明的成员不存在：{member}",
                        [f"{batch_code} 的分片成员 {member} 不在压缩包内"],
                    )
                used_members.add(member)
                records = self._parse_records(members[member], member)
                if records is None:
                    raise BackupImportError(
                        f"分片成员无法解析：{member}", [f"{member} 不是合法的 JSON/JSONL"]
                    )
                shard_index = int(shard_decl.get("shard_index") or shard_decl.get("index") or position)
                version = normalize_version(
                    shard_decl.get("version") or version_of_records(records)
                )
                computed = checksum_of_records(records)
                declared_checksum = shard_decl.get("checksum") or shard_decl.get("sha256") or ""
                if declared_checksum and not checksum_matches(str(declared_checksum), computed):
                    raise BackupImportError(
                        f"分片校验值不符：{batch_code} 分片 {shard_index}",
                        [f"{batch_code} 分片 {shard_index} 声明校验值 {declared_checksum} 与实际 {computed} 不符"],
                    )
                declared_count = shard_decl.get("record_count") or shard_decl.get("count")
                if declared_count is not None and int(declared_count) != len(records):
                    raise BackupImportError(
                        f"分片条数不符：{batch_code} 分片 {shard_index}",
                        [f"{batch_code} 分片 {shard_index} 声明 {declared_count} 条，实际 {len(records)} 条"],
                    )
                shards.append({
                    "shard_index": shard_index,
                    "records": records,
                    "record_count": len(records),
                    "version": version,
                    "checksum": computed,
                })
            if not shards:
                warnings.append(f"{batch_code} 未声明任何分片，已跳过")
                continue
            shards.sort(key=lambda item: item["shard_index"])
            batches[batch_code] = {
                "batch_code": batch_code,
                "record_count": sum(shard["record_count"] for shard in shards),
                "shards": shards,
            }

        # 清单外的数据成员也尝试识别，避免漏掉批次。
        remaining = {name: content for name, content in members.items() if name not in used_members}
        if remaining:
            extra = self._batches_from_members(remaining, shard_size)
            for code, batch in extra.items():
                if code in batches:
                    warnings.append(f"{code} 同时出现在清单与独立成员中，采用清单定义")
                else:
                    batches[code] = batch
        return batches, warnings

    def _batches_from_members(
        self, members: dict[str, bytes], shard_size: int,
    ) -> dict[str, Any]:
        """没有清单时，从数据成员反推批次与分片：优先记录内批次字段，其次用文件名/目录。"""
        grouped: dict[str, list[dict[str, Any]]] = {}
        files_with_index: dict[str, list[tuple[str, list[dict[str, Any]]]]] = {}
        for name in sorted(members):
            records = self._parse_records(members[name], name)
            if not records:
                continue
            # 记录内明确写了批次编号的，直接按批次聚合。
            explicit = [batch_code_of(record) for record in records]
            if any(explicit):
                for record, code in zip(records, explicit):
                    if not code:
                        code = self._batch_code_from_path(name)
                    grouped.setdefault(code, []).append(record)
            else:
                code = self._batch_code_from_path(name)
                files_with_index.setdefault(code, []).append((name, records))

        batches: dict[str, Any] = {}
        for code, records in grouped.items():
            shards = shards_from_records(records, shard_size=shard_size)
            batches[code] = {
                "batch_code": code,
                "record_count": len(records),
                "shards": shards,
            }
        for code, files in files_with_index.items():
            records: list[dict[str, Any]] = []
            for _, file_records in files:
                records.extend(file_records)
            shards = shards_from_records(records, shard_size=shard_size)
            if code in batches:
                batches[code]["shards"].extend(shards)
                batches[code]["record_count"] += len(records)
            else:
                batches[code] = {
                    "batch_code": code,
                    "record_count": len(records),
                    "shards": shards,
                }
        return batches

    @staticmethod
    def _batch_code_from_path(name: str) -> str:
        """按成员路径推断批次编号：取目录段或文件名主干。"""
        parts = [part for part in name.replace("\\", "/").split("/") if part]
        if len(parts) >= 2:
            return parts[-2]
        stem = Path(parts[-1]).stem if parts else "batch"
        for suffix in ("_bundle", ".jsonl"):
            stem = stem.replace(suffix, "")
        return stem or "batch"

    @staticmethod
    def _parse_records(content: bytes, name: str) -> list[dict[str, Any]] | None:
        """解析成员内容为记录列表，支持 JSONL、单对象 JSON 和数组 JSON。"""
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            return None
        stripped = text.strip()
        if not stripped:
            return []
        lower = name.lower()
        if lower.endswith((".json", ".ndjson")) and not lower.endswith(".jsonl"):
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, list):
                return [item for item in payload if isinstance(item, dict)]
            if isinstance(payload, dict):
                if isinstance(payload.get("records"), list):
                    return [item for item in payload["records"] if isinstance(item, dict)]
                return [payload]
            if payload is not None:
                return None
        records = []
        for line in stripped.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                return None
            if isinstance(item, dict):
                records.append(item)
        return records

    # ------------------------------------------------------------------
    # 登记本地归档卷
    # ------------------------------------------------------------------

    def register_local_volume(
        self,
        archive_code: str | None = None,
        *,
        path: str | Path | None = None,
        label: str | None = None,
        shard_size: int = DEFAULT_SHARD_SIZE,
    ) -> dict[str, Any]:
        """把恢复可读的本地归档卷登记为来源材料。

        与离线包相互独立：离线包损坏或拒收都不会影响这里已登记的材料。
        """
        if path is None:
            if not archive_code:
                return {"ok": False, "error": "必须提供归档编号或本地路径", "errors": ["缺少归档编号"]}
            path = DATA_ROOT / "archive" / archive_code
        path = Path(path)
        if not path.exists():
            return {
                "ok": False,
                "error": f"本地归档卷不可读：{path}",
                "errors": [f"路径不存在：{path}"],
                "archive_code": archive_code,
            }

        # 重复登记：同一归档编号且内容一致时复用，避免制造额外候选来源。
        raw = self._read_volume_bytes(path)
        package_checksum = f"SHA256:{hashlib.sha256(raw).hexdigest()}" if raw is not None else None
        if archive_code:
            existing = self._find_local_material_by_archive(archive_code, path)
            if existing is not None and existing.get("package_checksum") == package_checksum:
                return {
                    "ok": True,
                    "duplicate": True,
                    "material_id": existing["material_id"],
                    "status": existing.get("status"),
                    "batches": sorted(existing.get("batches", {})),
                    "message": "该本地归档卷已登记，复用原材料",
                }

        warnings: list[str] = []
        batches: dict[str, Any] = {}
        try:
            if path.is_file() and path.suffix.lower() == ".zip":
                if raw is None:
                    raw = path.read_bytes()
                batches, warnings = self._scan_zip(raw, shard_size=shard_size)
            else:
                members = self._read_volume_members(path)
                if not members:
                    return {
                        "ok": False,
                        "error": f"本地归档卷内没有可读数据：{path}",
                        "errors": [f"{path} 下未找到 JSON/JSONL 数据文件"],
                        "archive_code": archive_code,
                    }
                batches = self._batches_from_members(members, shard_size)
        except BackupImportError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "errors": exc.errors,
                "archive_code": archive_code,
            }
        except OSError as exc:
            return {
                "ok": False,
                "error": f"本地归档卷读取失败：{exc}",
                "errors": [str(exc)],
                "archive_code": archive_code,
            }

        if not batches:
            return {
                "ok": False,
                "error": "本地归档卷内未识别出批次",
                "errors": warnings or ["没有匹配到批次记录"],
                "archive_code": archive_code,
            }

        material_id = f"MAT-LOC-{safe_token(archive_code or path.stem, limit=32)}-{int(time.time())}"
        manifest = self._build_manifest(
            material_id=material_id,
            kind=KIND_LOCAL,
            label=label or archive_code or path.name,
            source_path=str(path),
            status="可读",
            batches=batches,
            package_checksum=package_checksum,
            warnings=warnings,
        )
        manifest["archive_code"] = archive_code
        persisted = self._persist_import(manifest)
        persisted["archive_code"] = archive_code
        persisted["message"] = "本地归档卷已恢复可读并登记为来源材料"
        return persisted

    def _find_local_material_by_archive(self, archive_code: str, path: Path) -> dict[str, Any] | None:
        for manifest in self.list_materials(include_rejected=True):
            if manifest.get("kind") != KIND_LOCAL:
                continue
            if manifest.get("archive_code") == archive_code or manifest.get("source_path") == str(path):
                if manifest.get("status") != "已拒收":
                    return manifest
        return None

    @staticmethod
    def _read_volume_bytes(path: Path) -> bytes | None:
        """把归档卷内容规整成一段可校验字节流；目录按文件名顺序拼接。"""
        try:
            if path.is_file():
                return path.read_bytes()
            chunks = []
            for file in sorted(path.rglob("*")):
                if file.is_file() and file.suffix.lower() in (".jsonl", ".json", ".ndjson"):
                    chunks.append(file.name.encode("utf-8") + b"\0" + file.read_bytes())
            return b"\n".join(chunks) if chunks else None
        except OSError:
            return None

    def _read_volume_members(self, path: Path) -> dict[str, bytes]:
        members: dict[str, bytes] = {}
        files = [path] if path.is_file() else sorted(
            file for file in path.rglob("*") if file.is_file()
        )
        for file in files:
            if file.suffix.lower() not in (".jsonl", ".json", ".ndjson"):
                continue
            try:
                members[str(file.relative_to(path.parent if path.is_file() else path))] = file.read_bytes()
            except OSError:
                continue
        return members

    # ------------------------------------------------------------------
    # 来源解析与冲突识别
    # ------------------------------------------------------------------

    def _priority_key(self, material: dict[str, Any], shard: dict[str, Any]) -> tuple[int, int, str, int]:
        return (
            _KIND_RANK.get(str(material.get("kind")), 9),
            _STATUS_RANK.get(str(material.get("status")), 5),
            str(material.get("material_id")),
            int(shard.get("shard_index", 0)),
        )

    def detect_source_conflicts(self, batch_code: str) -> dict[str, Any]:
        """识别同一批次在多份材料间分片版本或校验值不一致的情况。"""
        materials = self.list_materials(batch_code)
        by_shard = self._collect_shards(materials, batch_code)
        conflicts = []
        for index in sorted(by_shard):
            candidates = by_shard[index]
            versions = sorted({normalize_version(c["shard"].get("version")) for c in candidates})
            checksums = sorted({str(c["shard"].get("checksum")) for c in candidates})
            if len(candidates) > 1 and (len(versions) > 1 or len(checksums) > 1):
                conflicts.append({
                    "batch_code": batch_code,
                    "shard_index": index,
                    "versions": versions,
                    "materials": [
                        {
                            "material_id": c["material"]["material_id"],
                            "kind": c["material"].get("kind"),
                            "status": c["material"].get("status"),
                            "version": normalize_version(c["shard"].get("version")),
                            "checksum": c["shard"].get("checksum"),
                            "record_count": c["shard"].get("record_count"),
                        }
                        for c in candidates
                    ],
                })
        return {
            "batch_code": batch_code,
            "has_conflict": bool(conflicts),
            "conflict_shards": [item["shard_index"] for item in conflicts],
            "conflicts": conflicts,
            "materials": [
                {
                    "material_id": material["material_id"],
                    "kind": material.get("kind"),
                    "status": material.get("status"),
                    "versions": material.get("batches", {}).get(batch_code, {}).get("versions", []),
                    "label": material.get("label"),
                }
                for material in materials
            ],
        }

    def _collect_shards(
        self, materials: list[dict[str, Any]], batch_code: str,
    ) -> dict[int, list[dict[str, Any]]]:
        by_shard: dict[int, list[dict[str, Any]]] = {}
        for material in materials:
            batch = material.get("batches", {}).get(batch_code)
            if not batch:
                continue
            for shard in batch.get("shards", []):
                by_shard.setdefault(int(shard["shard_index"]), []).append(
                    {"material": material, "shard": shard}
                )
        for index in by_shard:
            by_shard[index].sort(key=lambda c: self._priority_key(c["material"], c["shard"]))
        return by_shard

    def resolve_batch(self, batch_code: str) -> dict[str, Any]:
        """按确定性优先级逐分片选定来源，并给出待复核差异。

        选择规则：本地归档卷优先于离线包；同类型中已核验/可读优先于待核验；
        其余按材料编号排序。每个分片都会在结果中标注实际采用的来源。
        """
        materials = self.list_materials(batch_code)
        by_shard = self._collect_shards(materials, batch_code)
        shard_provenance: list[dict[str, Any]] = []
        pending_review: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        used_batches: dict[str, set[int]] = {}

        if not by_shard:
            return {
                "batch_code": batch_code,
                "resolved": False,
                "error": f"批次 {batch_code} 没有可用来源材料",
                "shards": [],
                "sources": [],
                "conflicts": [],
                "pending_review": [],
            }

        for index in sorted(by_shard):
            candidates = by_shard[index]
            chosen = candidates[0]
            versions = sorted({normalize_version(c["shard"].get("version")) for c in candidates})
            checksums = sorted({str(c["shard"].get("checksum")) for c in candidates})
            conflict = len(candidates) > 1 and (len(versions) > 1 or len(checksums) > 1)
            provenance = {
                "shard_index": index,
                "material_id": chosen["material"]["material_id"],
                "material_kind": chosen["material"].get("kind"),
                "material_label": chosen["material"].get("label"),
                "material_status": chosen["material"].get("status"),
                "version": normalize_version(chosen["shard"].get("version")),
                "checksum": chosen["shard"].get("checksum"),
                "record_count": int(chosen["shard"].get("record_count", 0)),
                "conflict": conflict,
                "alternates": [
                    {
                        "material_id": c["material"]["material_id"],
                        "kind": c["material"].get("kind"),
                        "status": c["material"].get("status"),
                        "version": normalize_version(c["shard"].get("version")),
                        "checksum": c["shard"].get("checksum"),
                    }
                    for c in candidates[1:]
                ],
            }
            shard_provenance.append(provenance)
            used_batches.setdefault(provenance["material_id"], set()).add(index)

            if conflict:
                conflicts.append({
                    "shard_index": index,
                    "versions": versions,
                    "chosen_material_id": provenance["material_id"],
                    "candidate_material_ids": [c["material"]["material_id"] for c in candidates],
                })
                for alternate in provenance["alternates"]:
                    pending_review.append({
                        "kind": "来源冲突",
                        "shard_index": index,
                        "chosen_material_id": provenance["material_id"],
                        "chosen_version": provenance["version"],
                        "alternate_material_id": alternate["material_id"],
                        "alternate_version": alternate["version"],
                        "reason": (
                            f"分片 {index} 在 {provenance['material_id']}({provenance['version']}) 与 "
                            f"{alternate['material_id']}({alternate['version']}) 间版本/校验值不一致，"
                            f"本次采用 {provenance['material_id']}，差异留待复核"
                        ),
                    })
            if provenance["material_status"] == "待核验":
                pending_review.append({
                    "kind": "来源待核验",
                    "shard_index": index,
                    "chosen_material_id": provenance["material_id"],
                    "reason": f"分片 {index} 取自尚未核验的材料 {provenance['material_id']}",
                })

        sources = []
        for material in materials:
            used = used_batches.get(material["material_id"])
            if not used:
                continue
            sources.append({
                "material_id": material["material_id"],
                "kind": material.get("kind"),
                "label": material.get("label"),
                "status": material.get("status"),
                "archive_code": material.get("archive_code"),
                "shards": sorted(used),
            })

        return {
            "batch_code": batch_code,
            "resolved": True,
            "shards": shard_provenance,
            "sources": sources,
            "conflicts": conflicts,
            "pending_review": pending_review,
            "material_ids": [item["material_id"] for item in sources],
        }

    # ------------------------------------------------------------------
    # 分片记录读取与完整性校验
    # ------------------------------------------------------------------

    def read_shard_records(
        self, material_id: str, batch_code: str, shard_index: int,
    ) -> tuple[list[dict[str, Any]] | None, str | None]:
        """读取指定材料的分片记录；文件缺失或校验值不符时返回错误原因。"""
        manifest = self.get_material(material_id)
        if manifest is None:
            return None, f"材料 {material_id} 不存在"
        batch = manifest.get("batches", {}).get(batch_code)
        if not batch:
            return None, f"材料 {material_id} 不含批次 {batch_code}"
        shard = next(
            (item for item in batch.get("shards", []) if int(item["shard_index"]) == int(shard_index)),
            None,
        )
        if shard is None:
            return None, f"材料 {material_id} 不含分片 {shard_index}"
        path = self._material_dir(material_id) / str(shard.get("entry_path", ""))
        if not path.exists():
            return None, f"分片文件缺失：{path}"
        try:
            records = self._read_jsonl(path)
        except OSError as exc:
            return None, f"分片文件读取失败：{exc}"
        if len(records) != int(shard.get("record_count", len(records))):
            return None, (
                f"分片条数不符：声明 {shard.get('record_count')} 条，实际 {len(records)} 条"
            )
        computed = checksum_of_records(records)
        if shard.get("checksum") and computed != shard.get("checksum"):
            return None, f"分片校验值不符：声明 {shard.get('checksum')}，实际 {computed}"
        return records, None

    def resolve_batch_with_fallback(self, batch_code: str) -> dict[str, Any]:
        """逐分片选取可用来源；首选材料损坏时自动退到下一候选并在待复核中标注。"""
        resolution = self.resolve_batch(batch_code)
        if not resolution.get("resolved"):
            return resolution

        materials = {m["material_id"]: m for m in self.list_materials(batch_code)}
        by_shard = self._collect_shards(list(materials.values()), batch_code)
        final_shards: list[dict[str, Any]] = []
        extra_review: list[dict[str, Any]] = resolution.get("pending_review", [])
        for provenance in resolution["shards"]:
            index = provenance["shard_index"]
            candidates = by_shard.get(index, [])
            # 把首选来源及后续候选依次尝试，直到读到且校验通过。
            ordered = sorted(
                candidates,
                key=lambda c: (
                    0 if c["material"]["material_id"] == provenance["material_id"] else 1,
                    self._priority_key(c["material"], c["shard"]),
                ),
            )
            chosen_records = None
            for candidate in ordered:
                material_id = candidate["material"]["material_id"]
                records, error = self.read_shard_records(material_id, batch_code, index)
                if records is None:
                    if material_id == provenance["material_id"]:
                        extra_review.append({
                            "kind": "来源不可读",
                            "shard_index": index,
                            "chosen_material_id": material_id,
                            "reason": f"首选来源读取失败（{error}），已尝试后备材料",
                        })
                    continue
                if material_id != provenance["material_id"]:
                    extra_review.append({
                        "kind": "来源回退",
                        "shard_index": index,
                        "chosen_material_id": provenance["material_id"],
                        "alternate_material_id": material_id,
                        "reason": f"分片 {index} 原定来源不可用，改用 {material_id}",
                    })
                    provenance = dict(provenance)
                    provenance.update({
                        "material_id": material_id,
                        "material_kind": candidate["material"].get("kind"),
                        "material_label": candidate["material"].get("label"),
                        "material_status": candidate["material"].get("status"),
                        "version": normalize_version(candidate["shard"].get("version")),
                        "checksum": candidate["shard"].get("checksum"),
                        "record_count": int(candidate["shard"].get("record_count", 0)),
                        "fallback": True,
                    })
                chosen_records = records
                break
            if chosen_records is None:
                return {
                    "batch_code": batch_code,
                    "resolved": False,
                    "error": f"批次 {batch_code} 的分片 {index} 在所有来源中均不可读",
                    "shards": final_shards,
                    "sources": [],
                    "conflicts": resolution.get("conflicts", []),
                    "pending_review": extra_review,
                }
            final_shards.append({**provenance, "_records": chosen_records})

        used: dict[str, set[int]] = {}
        for item in final_shards:
            used.setdefault(item["material_id"], set()).add(item["shard_index"])
        sources = []
        for material_id, indices in used.items():
            material = materials.get(material_id, {})
            sources.append({
                "material_id": material_id,
                "kind": material.get("kind"),
                "label": material.get("label"),
                "status": material.get("status"),
                "archive_code": material.get("archive_code"),
                "shards": sorted(indices),
            })

        return {
            "batch_code": batch_code,
            "resolved": True,
            "shards": final_shards,
            "sources": sources,
            "material_ids": [source["material_id"] for source in sources],
            "conflicts": resolution.get("conflicts", []),
            "pending_review": extra_review,
        }

    # ------------------------------------------------------------------
    # 核验与范围查询
    # ------------------------------------------------------------------

    def verify_material(self, material_id: str, *, accepted: bool = True, remark: str = "") -> dict[str, Any]:
        """核验一份来源材料；核验不改变已确认重放的交付记录。"""
        manifest = self.get_material(material_id)
        if manifest is None:
            return {"ok": False, "error": f"材料 {material_id} 不存在"}
        new_status = "已核验" if accepted else "已拒收"
        manifest["status"] = new_status
        manifest["verified_at"] = time.time()
        manifest["verify_remark"] = remark
        self._save_material(manifest)
        for row in store.rows(self.MODULE):
            if row.get("_material_id") == material_id:
                row["核验状态"] = new_status
                row["status"] = "已核验" if accepted else "已拒收"
                row["pending"] = False
                row["abnormal"] = not accepted
        self._write_jsonl(
            self._data_dir("events") / "backup_import_log.jsonl",
            [{
                "action": "verify_material",
                "material_id": material_id,
                "accepted": accepted,
                "remark": remark,
                "at": time.time(),
            }],
        )
        return {"ok": True, "material_id": material_id, "status": new_status}

    def get_batch_sources(self, batch_code: str) -> dict[str, Any]:
        """汇总一个批次的全部候选来源与冲突情况，供重放前人工核对。"""
        conflicts = self.detect_source_conflicts(batch_code)
        preview = self.resolve_batch(batch_code)
        return {
            "batch_code": batch_code,
            "materials": conflicts["materials"],
            "has_conflict": conflicts["has_conflict"],
            "conflicts": conflicts["conflicts"],
            "preview_shards": [
                {
                    "shard_index": item["shard_index"],
                    "material_id": item["material_id"],
                    "version": item["version"],
                    "checksum": item["checksum"],
                    "record_count": item["record_count"],
                    "conflict": item["conflict"],
                }
                for item in preview.get("shards", [])
            ],
            "pending_review": preview.get("pending_review", []),
        }

    def get_confirmed_replay_scope(self, batch_code: str) -> dict[str, Any]:
        """查询该批次已经确认的导入重放交付范围，用于重复上传/坏包后的一致性核对。"""
        deliveries = []
        directory = DATA_ROOT / "deliveries"
        if directory.exists():
            for path in sorted(directory.glob("*.json")):
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        detail = json.load(handle)
                except (OSError, json.JSONDecodeError):
                    continue
                if detail.get("batch_code") != batch_code:
                    continue
                deliveries.append({
                    "delivery_code": detail.get("delivery_code"),
                    "replay_code": detail.get("replay_code"),
                    "sources": [source.get("material_id") for source in detail.get("sources", [])],
                    "shard_count": len(detail.get("shards", [])),
                    "pending_review": len(detail.get("pending_review", [])),
                    "artifact_path": detail.get("artifact_path"),
                })
        return {
            "batch_code": batch_code,
            "confirmed_replays": deliveries,
            "count": len(deliveries),
        }

    def remote_batch_snapshot(self, batch_code: str) -> dict[str, Any]:
        """输出批次当前的候选来源快照，便于在重复上传前后比对是否被扰动。"""
        return {
            "batch_code": batch_code,
            "materials": [
                {
                    "material_id": material["material_id"],
                    "kind": material.get("kind"),
                    "status": material.get("status"),
                    "package_checksum": material.get("package_checksum"),
                }
                for material in self.list_materials(batch_code, include_rejected=True)
            ],
        }


def create_package_bytes(
    batches: dict[str, list[dict[str, Any]]],
    *,
    manifest_versions: dict[str, str] | None = None,
    include_manifest: bool = True,
) -> bytes:
    """辅助工具：把批次记录打包成可导入的 zip 字节流（便于自测与演示）。

    ``batches`` 形如 ``{"BATCH-A": [record, ...]}``；``manifest_versions`` 可为每个
    批次指定分片版本，默认从记录字段读取或使用 v1。
    """
    manifest_versions = manifest_versions or {}
    buffer = io.BytesIO()
    manifest_batches = []
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for batch_code, records in batches.items():
            version = normalize_version(manifest_versions.get(batch_code))
            shards = shards_from_records(records)
            shard_entries = []
            for shard in shards:
                member = f"{safe_token(batch_code)}/shard-{int(shard['shard_index']):04d}.jsonl"
                payload = "".join(
                    json.dumps(record, ensure_ascii=False, default=str) + "\n"
                    for record in shard["records"]
                )
                archive.writestr(member, payload.encode("utf-8"))
                shard_entries.append({
                    "shard_index": int(shard["shard_index"]),
                    "version": version,
                    "entry": member,
                    "checksum": shard["checksum"],
                    "record_count": shard["record_count"],
                })
            manifest_batches.append({"batch_code": batch_code, "shards": shard_entries})
        if include_manifest:
            archive.writestr(
                "manifest.json",
                json.dumps({"batches": manifest_batches}, ensure_ascii=False).encode("utf-8"),
            )
    return buffer.getvalue()
