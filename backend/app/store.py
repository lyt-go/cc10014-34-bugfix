"""内存数据仓库：支持模块间关联、版本管理、模拟表数据、血缘记录与状态流转。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

关键一致性约定（缺陷修复后）：
- 转换节点的「依赖状态校验 → 输出数据集写入 → 结构注册」必须通过 ``transaction()``
  在同一事务里完成，任一步失败整体回滚，不会留下半成品输出。
- 转换节点输出数据集（``_transform_outputs``）按输出表保存完整版本链，同一转换批次
  重复执行只保留该批次的一个完整版本，历史批次版本不丢失。
"""
from __future__ import annotations

import copy
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from app.seed import SEED_ROWS


class StoreTransactionError(RuntimeError):
    """事务内任何一步失败时抛出，事务上下文据此回滚全部写入。"""


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 模拟真实表数据（分层存储的实际行数据）
        self._table_data: dict[str, list[dict[str, Any]]] = {}
        # 版本历史: "module:id" -> [{version, timestamp, data}, ...]
        self._versions: dict[str, list[dict[str, Any]]] = {}
        # 血缘边记录
        self._lineage_edges: list[dict[str, Any]] = []
        # 质量稽核结果
        self._quality_results: dict[str, list[dict[str, Any]]] = {}
        # 快照冻结数据: "snapshot_id" -> {table_name, rows, frozen_at}
        self._snapshot_data: dict[str, dict[str, Any]] = {}
        # 归档记录
        self._archive_records: list[dict[str, Any]] = []
        # 补数区间记录
        self._backfill_partitions: dict[str, list[dict[str, Any]]] = {}
        # 转换节点输出数据集：输出表名 -> 版本链（每个元素为一个完整版本快照）
        self._transform_outputs: dict[str, list[dict[str, Any]]] = {}
        # 转换节点当前执行批次：节点编号 -> 批次号
        self._transform_batches: dict[str, str] = {}
        # 批次自增序列：节点编号 -> 已分配批次数，保证同秒多次开批次也不撞号
        self._batch_seq: dict[str, int] = {}
        # 结构注册跨模块同步记录：输出表名 -> 结构注册条目 id 列表
        self._schema_sync_index: dict[str, list[int]] = {}
        # 事务嵌套计数与回滚快照
        self._tx_depth = 0
        self._tx_snapshot: dict[str, Any] | None = None
        self._migrated = False
        self.migrate()

    # ---- 事务：依赖状态、输出批次、结构注册必须同一事务完成 ----

    def _snapshot_state(self) -> dict[str, Any]:
        return {
            "_tables": copy.deepcopy(self._tables),
            "_table_data": copy.deepcopy(self._table_data),
            "_versions": copy.deepcopy(self._versions),
            "_lineage_edges": copy.deepcopy(self._lineage_edges),
            "_quality_results": copy.deepcopy(self._quality_results),
            "_snapshot_data": copy.deepcopy(self._snapshot_data),
            "_archive_records": copy.deepcopy(self._archive_records),
            "_backfill_partitions": copy.deepcopy(self._backfill_partitions),
            "_transform_outputs": copy.deepcopy(self._transform_outputs),
            "_transform_batches": copy.deepcopy(self._transform_batches),
            "_batch_seq": copy.deepcopy(self._batch_seq),
            "_schema_sync_index": copy.deepcopy(self._schema_sync_index),
        }

    def _restore_snapshot(self, snapshot: dict[str, Any]) -> None:
        for key, value in snapshot.items():
            setattr(self, key, copy.deepcopy(value))

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """统一事务边界：最外层提交、任一步异常整体回滚。

        嵌套调用复用最外层事务，保证转换执行内部的多次写入要么全部生效，
        要么全部不生效（例如输出已写入但结构注册失败时，输出一并回滚）。
        """
        if self._tx_depth == 0:
            self._tx_snapshot = self._snapshot_state()
        self._tx_depth += 1
        try:
            yield
        except Exception:
            if self._tx_depth == 1:
                assert self._tx_snapshot is not None
                self._restore_snapshot(self._tx_snapshot)
                self._tx_snapshot = None
            self._tx_depth -= 1
            raise
        else:
            self._tx_depth -= 1
            if self._tx_depth == 0:
                self._tx_snapshot = None

    # ---- 迁移：覆盖内存库、部署环境与历史数据 ----

    def migrate(self) -> None:
        """幂等迁移：为历史数据补齐输出数据集/批次/同步索引所需容器。

        真实项目里对应数据库 DDL 与数据回填脚本；内存实现里保证重复调用安全，
        本地开发、容器部署与上线后首次启动走的是同一条迁移路径。
        """
        if self._migrated:
            return
        for name in ("_transform_outputs", "_transform_batches", "_batch_seq", "_schema_sync_index"):
            if not hasattr(self, name) or getattr(self, name) is None:
                setattr(self, name, {})
        if not hasattr(self, "_tx_depth"):
            self._tx_depth = 0
            self._tx_snapshot = None
        # 回填历史输出：已写入模拟表但没有输出数据集版本的转换结果，登记为 v1 历史版本
        for row in self._tables.get("transform_dag", []):
            output_table = str(row.get("输出表名", ""))
            node_code = str(row.get("节点编号", ""))
            if not output_table or output_table in self._transform_outputs:
                continue
            legacy_rows = self._table_data.get(output_table)
            if legacy_rows:
                self._transform_outputs[output_table] = [{
                    "dataset": output_table,
                    "node": node_code,
                    "node_id": row.get("id"),
                    "version": "v1",
                    "batch_id": f"{node_code}-legacy",
                    "complete": True,
                    "rows": copy.deepcopy(legacy_rows),
                    "row_count": len(legacy_rows),
                    "schema_version": None,
                    "dependencies": [],
                    "deactivated_dependencies": [],
                    "boundary": "wide",
                    "created_at": datetime.now().isoformat(),
                    "retained": False,
                }]
        self._migrated = True

    def reset_all(self) -> None:
        """恢复到种子数据状态，供测试与本地开发重置使用。"""
        self._tables = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._table_data.clear()
        self._versions.clear()
        self._lineage_edges.clear()
        self._quality_results.clear()
        self._snapshot_data.clear()
        self._archive_records.clear()
        self._backfill_partitions.clear()
        self._transform_outputs.clear()
        self._transform_batches.clear()
        self._batch_seq.clear()
        self._schema_sync_index.clear()
        self._tx_depth = 0
        self._tx_snapshot = None

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by_field(self, module: str, field: str, value: Any) -> dict[str, Any] | None:
        for row in self.rows(module):
            if row.get(field) == value:
                return row
        return None

    def find_all_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get(field) == value]

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ---- 模拟真实表数据 ----

    def table_rows(self, table_name: str) -> list[dict[str, Any]]:
        return self._table_data.setdefault(table_name, [])

    def set_table_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._table_data[table_name] = rows

    def append_table_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._table_data.setdefault(table_name, []).extend(rows)

    # ---- 版本管理 ----

    def _version_key(self, module: str, entry_id: int) -> str:
        return f"{module}:{entry_id}"

    def save_version(self, module: str, entry_id: int, data: dict[str, Any]) -> dict[str, Any]:
        key = self._version_key(module, entry_id)
        versions = self._versions.setdefault(key, [])
        version = {
            "version": len(versions) + 1,
            "timestamp": datetime.now().isoformat(),
            "data": dict(data),
        }
        versions.append(version)
        return version

    def get_versions(self, module: str, entry_id: int) -> list[dict[str, Any]]:
        key = self._version_key(module, entry_id)
        return list(self._versions.get(key, []))

    def get_latest_version(self, module: str, entry_id: int) -> dict[str, Any] | None:
        versions = self.get_versions(module, entry_id)
        return versions[-1] if versions else None

    # ---- 血缘 ----

    def add_lineage(self, upstream: str, downstream: str, field_map: str,
                    transform_logic: str = "", batch_id: str = "") -> dict[str, Any]:
        edge = {
            "id": len(self._lineage_edges) + 1,
            "上游表名": upstream,
            "下游表名": downstream,
            "来源字段": field_map,
            "目标字段": field_map,
            "转换逻辑": transform_logic,
            "采集批次": batch_id,
            "链路版本": f"v{len(self._lineage_edges) + 1}",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        self._lineage_edges.append(edge)
        # 同时写入 lineage_track 模块表供查询
        self.rows("lineage_track").append(edge)
        return edge

    def get_lineage_edges(self) -> list[dict[str, Any]]:
        return list(self._lineage_edges)

    # ---- 质量稽核结果 ----

    def save_quality_result(self, rule_id: str, result: dict[str, Any]) -> None:
        self._quality_results.setdefault(rule_id, []).append(result)

    def get_quality_results(self, rule_id: str) -> list[dict[str, Any]]:
        return list(self._quality_results.get(rule_id, []))

    # ---- 快照 ----

    def save_snapshot_data(self, snapshot_id: str, table_name: str, rows: list[dict[str, Any]]) -> None:
        self._snapshot_data[snapshot_id] = {
            "table_name": table_name,
            "rows": [dict(r) for r in rows],
            "frozen_at": datetime.now().isoformat(),
            "row_count": len(rows),
        }

    def get_snapshot_data(self, snapshot_id: str) -> dict[str, Any] | None:
        return self._snapshot_data.get(snapshot_id)

    # ---- 归档 ----

    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_records.append(record)

    def get_archive_records(self) -> list[dict[str, Any]]:
        return list(self._archive_records)

    # ---- 补数分区 ----

    def save_backfill_partition(self, job_id: str, partition: dict[str, Any]) -> None:
        self._backfill_partitions.setdefault(job_id, []).append(partition)

    def get_backfill_partitions(self, job_id: str) -> list[dict[str, Any]]:
        return list(self._backfill_partitions.get(job_id, []))

    # ---- 转换节点输出数据集（版本化、按批次去重） ----

    def get_output_versions(self, output_table: str) -> list[dict[str, Any]]:
        """返回某输出表的完整版本链，历史转换节点输出数据集不会丢失。"""
        return [copy.deepcopy(v) for v in self._transform_outputs.get(output_table, [])]

    def list_transform_outputs(self) -> list[dict[str, Any]]:
        """列出全部转换节点输出数据集（每个输出表只暴露最新完整版本摘要）。"""
        result: list[dict[str, Any]] = []
        for output_table, versions in self._transform_outputs.items():
            if not versions:
                continue
            latest = copy.deepcopy(versions[-1])
            latest["history_versions"] = len(versions)
            result.append(latest)
        return result

    def find_output_version(self, output_table: str, version: str) -> dict[str, Any] | None:
        for item in self._transform_outputs.get(output_table, []):
            if item.get("version") == version:
                return copy.deepcopy(item)
        return None

    def get_latest_output(self, output_table: str) -> dict[str, Any] | None:
        versions = self._transform_outputs.get(output_table, [])
        return copy.deepcopy(versions[-1]) if versions else None

    def get_current_batch(self, node_code: str) -> str | None:
        return self._transform_batches.get(node_code)

    def set_current_batch(self, node_code: str, batch_id: str) -> None:
        self._transform_batches[node_code] = batch_id

    def next_batch_id(self, node_code: str) -> str:
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        seq = self._batch_seq.get(node_code, 0) + 1
        self._batch_seq[node_code] = seq
        return f"{node_code}-B{stamp}-{seq:03d}"

    def save_transform_output(
        self,
        *,
        output_table: str,
        node_code: str,
        node_id: int | None,
        batch_id: str,
        rows: list[dict[str, Any]],
        dependencies: list[dict[str, Any]],
        boundary: str,
        schema_version: str | None,
        deactivated_dependencies: list[dict[str, Any]] | None = None,
        retained: bool = False,
    ) -> dict[str, Any]:
        """写入一个完整输出版本。

        同一转换批次重复执行时，覆盖该批次已有版本，保证一个批次只有一个完整版本；
        不同批次追加新版本，历史版本链完整保留。
        """
        chain = self._transform_outputs.setdefault(output_table, [])
        existing_idx = next(
            (i for i, item in enumerate(chain) if item.get("batch_id") == batch_id), None)
        if existing_idx is not None:
            version = chain[existing_idx]["version"]
        else:
            version = f"v{len(chain) + 1}"
        record = {
            "dataset": output_table,
            "node": node_code,
            "node_id": node_id,
            "version": version,
            "batch_id": batch_id,
            "complete": True,
            "rows": copy.deepcopy(rows),
            "row_count": len(rows),
            "schema_version": schema_version,
            "dependencies": copy.deepcopy(dependencies),
            "deactivated_dependencies": copy.deepcopy(deactivated_dependencies or []),
            "boundary": boundary,
            "retained": retained,
            "created_at": datetime.now().isoformat(),
        }
        if existing_idx is not None:
            chain[existing_idx] = record
        else:
            chain.append(record)
        return copy.deepcopy(record)

    def mark_output_retained(
        self, output_table: str, batch_id: str,
        deactivated_dependencies: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """缩窄边界：不产生新版本，仅在当前版本上记录停用依赖清单并标记保持旧版本。"""
        chain = self._transform_outputs.get(output_table, [])
        for item in reversed(chain):
            if item.get("batch_id") == batch_id or item is chain[-1]:
                item["retained"] = True
                item["deactivated_dependencies"] = copy.deepcopy(deactivated_dependencies)
                item["retained_at"] = datetime.now().isoformat()
                return copy.deepcopy(item)
        return None

    def index_schema_sync(self, output_table: str, schema_id: int) -> None:
        """登记结构注册跨模块同步关系：输出表 -> 已同步的结构定义 id。"""
        self._schema_sync_index.setdefault(output_table, []).append(schema_id)

    def get_schema_sync_index(self, output_table: str) -> list[int]:
        return list(self._schema_sync_index.get(output_table, []))

    def bind_output_version(
        self, output_table: str, node_code: str, version: str, batch_id: str,
    ) -> None:
        """版本切换后重写实时表与当前批次绑定，使输出数据集只暴露指定完整版本。"""
        target = self.find_output_version(output_table, version)
        if target is None:
            raise StoreTransactionError(
                f"版本切换失败：{output_table} 不存在完整版本 {version}")
        self.set_table_rows(output_table, copy.deepcopy(target["rows"]))
        self.set_current_batch(node_code, batch_id)

    def bind_dependency_version(
        self, downstream_table: str, dep_node: str, version: str, batch_id: str,
    ) -> None:
        """沿调用链修正下游输出数据集里绑定的依赖节点结果引用。"""
        chain = self._transform_outputs.get(downstream_table, [])
        if not chain:
            return
        latest = chain[-1]
        updated = False
        for dep in latest.get("dependencies", []):
            if dep.get("依赖节点") == dep_node:
                dep["引用版本"] = version
                dep["引用批次"] = batch_id
                updated = True
        if not updated:
            latest.setdefault("dependencies", []).append({
                "依赖节点": dep_node,
                "依赖状态": "已生效",
                "引用版本": version,
                "引用批次": batch_id,
            })


store = Store()
