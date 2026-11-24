"""分片工具：批次记录的分片划分、版本识别与规范化校验值计算。

备份包导入、本地归档卷恢复和历史重放三处都需要用同一套口径把批次记录切成
分片，并计算稳定的校验值，否则同一批数据在离线包与本地卷之间会被判成不同版本。
本模块只依赖标准库，供各领域服务复用。
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable

# 批次编号在记录里可能出现的字段名，按优先级排列。
BATCH_CODE_KEYS = ("batch_code", "batch_key", "批次编号", "batch")
# 分片版本在记录里可能出现的字段名，按优先级排列。
VERSION_KEYS = ("version", "shard_version", "分片版本", "_version")
# 记录内分片序号字段。
CHUNK_INDEX_KEYS = ("chunk_index", "chunk", "分片序号")

DEFAULT_SHARD_SIZE = 100
DEFAULT_VERSION = "v1"


def batch_code_of(record: dict[str, Any]) -> str:
    """从一条记录中识别所属批次编号，缺失时返回空串。"""
    for key in BATCH_CODE_KEYS:
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def version_of_records(records: Iterable[dict[str, Any]]) -> str | None:
    """识别一组记录共同声明的分片版本；没有声明或声明不一致时返回混合值。

    记录里可能用 version / shard_version / 分片版本 / _version 等字段表达版本，
    这里取每条记录命中的第一个非空值，再去重比较。
    """
    values: list[str] = []
    for record in records:
        for key in VERSION_KEYS:
            value = record.get(key)
            if value is not None and str(value).strip():
                values.append(str(value).strip())
                break
    distinct = sorted({value.casefold() for value in values})
    if not distinct:
        return None
    if len(distinct) == 1:
        # 保留原始大小写中的第一个写法，方便展示。
        first = next(iter(values))
        return first
    return "mixed:" + "|".join(distinct)


def normalize_version(value: str | None) -> str:
    """把版本声明规范成可比较的字符串；空值按默认版本处理。"""
    if value is None or not str(value).strip():
        return DEFAULT_VERSION
    return str(value).strip()


def canonical_json(records: Any) -> str:
    """生成稳定的 JSON 文本，用于计算与字段顺序无关的分片校验值。"""
    return json.dumps(
        records, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=str,
    )


def checksum_of_records(records: list[dict[str, Any]]) -> str:
    """计算一组记录的分片校验值，形如 ``SHA256:<hex>``。"""
    digest = hashlib.sha256(canonical_json(records).encode("utf-8")).hexdigest()
    return f"SHA256:{digest}"


def _as_index(value: Any) -> int | None:
    try:
        index = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return index if index >= 0 else None


def _chunk_index_of(record: dict[str, Any]) -> int | None:
    for key in CHUNK_INDEX_KEYS:
        if key in record:
            index = _as_index(record.get(key))
            if index is not None:
                return index
    return None


def _normalize_indices(shards: list[dict[str, Any]]) -> None:
    """把 0 起算的分片集合统一平移成 1 起算，保证跨来源比较口径一致。"""
    indices = [int(shard["shard_index"]) for shard in shards]
    if indices and min(indices) == 0:
        for shard in shards:
            shard["shard_index"] = int(shard["shard_index"]) + 1


def shards_from_records(
    records: list[dict[str, Any]], *, shard_size: int = DEFAULT_SHARD_SIZE,
) -> list[dict[str, Any]]:
    """把批次记录切成带校验值、版本和条数的分片列表。

    若记录自带 chunk_index（传输批次产物就是这种形态），按该字段分组；
    否则按 ``shard_size`` 顺序切块。分片序号统一为 1 起算。
    """
    if not records:
        return []
    chunk_indices = [_chunk_index_of(record) for record in records]
    shards: list[dict[str, Any]] = []
    if all(index is not None for index in chunk_indices):
        grouped: dict[int, list[dict[str, Any]]] = {}
        for record, index in zip(records, chunk_indices):
            grouped.setdefault(int(index), []).append(record)  # type: ignore[arg-type]
        for index in sorted(grouped):
            shard_records = grouped[index]
            shards.append({
                "shard_index": index,
                "records": shard_records,
                "record_count": len(shard_records),
                "version": normalize_version(version_of_records(shard_records)),
                "checksum": checksum_of_records(shard_records),
            })
    else:
        for offset in range(0, len(records), max(1, shard_size)):
            shard_records = records[offset:offset + max(1, shard_size)]
            shards.append({
                "shard_index": offset // max(1, shard_size),
                "records": shard_records,
                "record_count": len(shard_records),
                "version": normalize_version(version_of_records(shard_records)),
                "checksum": checksum_of_records(shard_records),
            })
    _normalize_indices(shards)
    return shards


def strip_shard_records(shards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把分片列表转换成可持久化的清单，不携带完整记录内容。"""
    cleaned = []
    for shard in shards:
        cleaned.append({
            "shard_index": int(shard["shard_index"]),
            "version": normalize_version(shard.get("version")),
            "checksum": str(shard.get("checksum", "")),
            "record_count": int(shard.get("record_count", 0)),
            **{
                key: shard[key]
                for key in ("entry_path", "entry_size")
                if key in shard
            },
        })
    return cleaned


def safe_token(value: str, *, limit: int = 48) -> str:
    """把批次/包编号转成适合做文件名或路径片段的 token。"""
    token = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(value)).strip("._-")
    return (token or "batch")[:limit]


def parse_checksum_hex(value: str) -> str:
    """从 ``SHA256:abcd`` 之类的声明中取出小写十六进制摘要。"""
    text = str(value or "").strip()
    if ":" in text:
        text = text.split(":", 1)[1]
    return re.sub(r"[^0-9a-fA-F]", "", text).lower()


def checksum_matches(declared: str, computed: str) -> bool:
    """比较声明校验值与实际校验值，允许声明使用截断摘要。"""
    declared_hex = parse_checksum_hex(declared)
    computed_hex = parse_checksum_hex(computed)
    if not declared_hex or not computed_hex:
        return False
    if len(declared_hex) >= 16:
        return computed_hex.startswith(declared_hex) or declared_hex.startswith(computed_hex)
    return False
