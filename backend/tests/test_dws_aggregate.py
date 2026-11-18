"""测试3：汇总表消费DWD数据。

验证：
- DWS服务能读取DWD表数据
- 按汇总维度正确聚合
- 聚合结果写入DWS模拟表
- 血缘关系自动生成
"""
from __future__ import annotations

from app.store import store
from app.services.dws_layer import DwsLayerService
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


class TestDwsAggregate:
    def setup_method(self):
        reset_store()

    def test_dws_sum_aggregate(self):
        """DWS应按dt,order_status维度SUM聚合order_amount。"""
        # 准备DWD数据
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100.00", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200.00", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O003", "order_amount": "50.00", "order_status": "待支付", "dt": "2026-11-01"},
        ])
        dws = DwsLayerService()
        result = dws.aggregate_from_dwd(1)
        assert result["ok"] is True
        rows = result.get("rows", [])
        assert len(rows) == 2  # 已完成 + 待支付
        completed = next((r for r in rows if r.get("order_status") == "已完成"), None)
        assert completed is not None
        assert completed.get("order_amount_agg") == 300.0

    def test_dws_count_aggregate(self):
        """DWS应按dt,user_level维度COUNT聚合user_count。"""
        store.set_table_rows("dwd_user_detail", [
            {"user_id": "U001", "user_level": "VIP", "dt": "2026-11-01"},
            {"user_id": "U002", "user_level": "VIP", "dt": "2026-11-01"},
            {"user_id": "U003", "user_level": "普通", "dt": "2026-11-01"},
        ])
        dws = DwsLayerService()
        result = dws.aggregate_from_dwd(3)
        assert result["ok"] is True
        rows = result.get("rows", [])
        vip = next((r for r in rows if r.get("user_level") == "VIP"), None)
        assert vip is not None
        assert vip.get("user_count_agg") == 2

    def test_dws_lineage_created(self):
        """聚合后应自动生成DWD到DWS的血缘。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100.00", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        dws = DwsLayerService()
        dws.aggregate_from_dwd(1)
        edges = store.get_lineage_edges()
        assert any(e["上游表名"] == "dwd_order_detail" and e["下游表名"] == "dws_order_daily" for e in edges)
