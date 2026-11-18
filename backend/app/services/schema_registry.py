"""结构定义业务规则：管理表的字段结构，版本化管理。

转换节点输出数据集的结构注册遵循两条硬约束：
1. 结构注册与输出生成在同一事务内完成，输出已写而结构未登记的情况会整体回滚；
2. 只读取上游「已经登记」的字段，依赖节点中断恢复后也不会把未登记字段带进新版本。
"""
from __future__ import annotations

from typing import Any

from app.store import store, StoreTransactionError

MODULE = "schema_registry"
LIST_FIELDS = ["定义编号", "表名称", "字段名称", "字段类型", "可空标识", "默认值", "定义版本", "生效批次"]
REQUIRED_FIELDS = ["定义编号", "表名称", "字段名称"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记废弃": "已废弃"}
NEGATIVE_ACTIONS = []


def _split_fields(text: Any) -> list[str]:
    return [part.strip() for part in str(text or "").split(",") if part.strip()]


def _next_schema_version(table_name: str) -> str:
    versions: list[int] = []
    for row in store.rows(MODULE):
        if row.get("表名称") != table_name:
            continue
        raw = str(row.get("定义版本", "")).lstrip("vV")
        if raw.isdigit():
            versions.append(int(raw))
    return f"v{max(versions, default=0) + 1}"


class SchemaRegistryService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("定义编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"结构定义 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于结构注册可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        store.save_version(MODULE, entry_id, entry)
        return entry, f"结构定义已{action}"

    def get_schema_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return store.find_all_by_field(MODULE, "表名称", table_name)

    def get_latest_schema(self, table_name: str) -> dict[str, Any] | None:
        schemas = self.get_schema_for_table(table_name)
        if not schemas:
            return None
        # 找版本号最大的
        latest = max(schemas, key=lambda s: s.get("定义版本", "v0"))
        return latest

    def derive_schema_from_lineage(self, table_name: str) -> dict[str, Any] | None:
        """从血缘关系推导字段变更：找上游表最新schema并合并。"""
        # 查找血缘中该table_name作为下游的边
        upstream_tables = []
        for edge in store.get_lineage_edges():
            if edge.get("下游表名") == table_name:
                up = edge.get("上游表名", "")
                if up:
                    upstream_tables.append(up)
        if not upstream_tables:
            return None
        # 取第一个上游的最新schema
        up_schema = self.get_latest_schema(upstream_tables[0])
        if not up_schema:
            return None
        latest = self.get_latest_schema(table_name)
        new_version = "v2"
        if latest:
            try:
                vnum = int(latest.get("定义版本", "v1").replace("v", ""))
                new_version = f"v{vnum + 1}"
            except Exception:
                new_version = "v2"
        # 生成新schema定义
        new_entry = {
            "定义编号": f"SCH-DERIVED-{table_name}",
            "表名称": table_name,
            "字段名称": up_schema.get("字段名称", ""),
            "字段类型": up_schema.get("字段类型", ""),
            "可空标识": up_schema.get("可空标识", ""),
            "默认值": up_schema.get("默认值", ""),
            "定义版本": new_version,
            "生效批次": up_schema.get("生效批次", ""),
        }
        return self.create_entry(new_entry)[0]

    # ---- 转换节点输出数据集结构注册（跨模块同步） ----

    def registered_fields(self, table_name: str) -> dict[str, dict[str, Any]]:
        """只读取已经登记且未废弃的字段；中断恢复后新增但未登记的字段不会出现。"""
        fields: dict[str, dict[str, Any]] = {}
        for row in store.rows(MODULE):
            if row.get("表名称") != table_name or row.get("status") == "已废弃":
                continue
            names = _split_fields(row.get("字段名称"))
            types = _split_fields(row.get("字段类型"))
            nullable = _split_fields(row.get("可空标识"))
            defaults = _split_fields(row.get("默认值"))
            for index, name in enumerate(names):
                fields[name] = {
                    "字段名称": name,
                    "字段类型": types[index] if index < len(types) else "STRING",
                    "可空标识": nullable[index] if index < len(nullable) else "Y",
                    "默认值": defaults[index] if index < len(defaults) else "",
                    "来源定义编号": row.get("定义编号", ""),
                    "定义版本": row.get("定义版本", ""),
                }
        return fields

    def register_transform_output(
        self,
        *,
        output_table: str,
        batch_id: str,
        output_rows: list[dict[str, Any]],
        upstream_tables: list[str],
        node_code: str,
    ) -> dict[str, Any]:
        """为转换节点输出数据集登记结构，并同步到结构注册模块。

        字段来源严格限定为上游已登记字段；未在上游登记的输出字段不会冒充已确认来源，
        而是以「待补登记」草稿字段单独标注。整个方法运行在转换执行事务内，
        登记失败抛出 ``StoreTransactionError`` 触发整体回滚。
        """
        if not output_rows:
            raise StoreTransactionError(
                f"输出表 {output_table} 无数据行，无法登记结构（禁止半成品结构注册）")
        output_fields = list(output_rows[0].keys())
        confirmed: dict[str, dict[str, Any]] = {}
        unconfirmed: list[str] = []
        for upstream in upstream_tables:
            for name, meta in self.registered_fields(upstream).items():
                if name in output_fields and name not in confirmed:
                    confirmed[name] = meta
        for name in output_fields:
            if name not in confirmed:
                unconfirmed.append(name)
        if not confirmed and not unconfirmed:
            raise StoreTransactionError(f"输出表 {output_table} 没有可登记字段")
        version = _next_schema_version(output_table)
        entry = {
            "定义编号": f"SCH-TRANS-{output_table}",
            "表名称": output_table,
            "字段名称": ",".join(list(confirmed.keys()) + unconfirmed),
            "字段类型": ",".join(
                [confirmed[name]["字段类型"] for name in confirmed]
                + ["STRING"] * len(unconfirmed)),
            "可空标识": ",".join(
                [confirmed[name]["可空标识"] for name in confirmed]
                + ["Y"] * len(unconfirmed)),
            "默认值": ",".join(
                [confirmed[name]["默认值"] for name in confirmed]
                + [""] * len(unconfirmed)),
            "定义版本": version,
            "生效批次": batch_id,
        }
        created, missing = self.create_entry(entry)
        if missing or created is None:
            raise StoreTransactionError(
                f"输出表 {output_table} 结构登记失败，缺少字段：{'、'.join(missing)}")
        created["status"] = "已生效"
        created["pending"] = False
        created["来源转换节点"] = node_code
        created["已确认字段"] = ",".join(confirmed.keys())
        created["待补登记字段"] = ",".join(unconfirmed)
        created["字段来源"] = "；".join(
            f"{name}<-{meta['来源定义编号']}({meta['定义版本']})"
            for name, meta in confirmed.items())
        store.save_version(MODULE, created["id"], created)
        store.index_schema_sync(output_table, created["id"])
        return created

    def list_transform_sync(self) -> list[dict[str, Any]]:
        """结构注册侧的转换节点输出数据集同步视图。"""
        result: list[dict[str, Any]] = []
        for output in store.list_transform_outputs():
            synced_ids = store.get_schema_sync_index(output["dataset"])
            schema = next(
                (row for row in store.rows(MODULE) if row.get("id") == (synced_ids[-1] if synced_ids else None)),
                None,
            )
            result.append({
                "输出数据集": output["dataset"],
                "来源转换节点": output["node"],
                "输出批次": output["batch_id"],
                "输出版本": output["version"],
                "结构版本": schema.get("定义版本") if schema else None,
                "结构状态": schema.get("status") if schema else "未登记",
                "已确认字段": schema.get("已确认字段", "") if schema else "",
                "待补登记字段": schema.get("待补登记字段", "") if schema else "",
                "结构注册id": synced_ids[-1] if synced_ids else None,
                "同步次数": len(synced_ids),
            })
        return result
