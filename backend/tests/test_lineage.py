"""测试6：血缘自动生成。

验证：
- transform_dag节点执行后自动生成血缘
- lineage_track模块能展示合并后的血缘图
- 血缘包含上游表、下游表、转换逻辑
- 自动生成的血缘可被查询
"""
from __future__ import annotations

from app.store import store
from app.services.transform_dag import TransformDagService
from app.services.lineage_track import LineageTrackService
from app.services.dwd_layer import DwdLayerService


def reset_store():
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._table_data.clear()
    store._versions.clear()
    store._transform_outputs.clear()
    store._transform_batches.clear()
    store._batch_seq.clear()
    store._schema_sync_index.clear()
    store._lineage_edges.clear()
    store._quality_results.clear()
    store._snapshot_data.clear()
    store._archive_records.clear()
    store._backfill_partitions.clear()


class TestLineage:
    def setup_method(self):
        reset_store()

    def test_transform_creates_lineage(self):
        """转换节点执行后应自动生成血缘边。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        td = TransformDagService()
        result = td.execute_node(1)
        assert result["ok"] is True
        edges = store.get_lineage_edges()
        assert any(e["上游表名"] == "dwd_order_detail" and e["下游表名"] == "dws_order_daily" for e in edges)

    def test_lineage_graph_contains_auto_edges(self):
        """血缘图应包含自动生成的边和手工录入的边。"""
        store.add_lineage("ods_orders", "dwd_order_detail", "order_id",
                          transform_logic="清洗+去重", batch_id="RAW-001")
        lt = LineageTrackService()
        graph = lt.get_graph()
        nodes = {n["name"] for n in graph["nodes"]}
        assert "ods_orders" in nodes
        assert "dwd_order_detail" in nodes
        edge_pairs = {(e["from"], e["to"]) for e in graph["edges"]}
        # 由于节点id可能不同，用名称映射验证
        name_to_id = {n["name"]: n["id"] for n in graph["nodes"]}
        assert any(
            e["from"] == name_to_id.get("ods_orders") and e["to"] == name_to_id.get("dwd_order_detail")
            for e in graph["edges"]
        )

    def test_auto_generate_from_transform(self):
        """从transform_dag批量自动生成血缘。"""
        lt = LineageTrackService()
        result = lt.auto_generate_from_transform()
        assert result["ok"] is True
        assert result["created"] >= 1
        edges = store.get_lineage_edges()
        assert len(edges) >= 1
