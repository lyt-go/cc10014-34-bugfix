"""测试8：发布版本与转换节点关联。

验证：
- data_publish确认发布时读取transform_dag最新输出
- 发布版本的数据与转换节点输出一致
- 发布版本支持查询关联数据
"""
from __future__ import annotations

from app.store import store
from app.services.data_publish import DataPublishService
from app.services.transform_dag import TransformDagService


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


class TestPublishTransform:
    def setup_method(self):
        reset_store()

    def test_publish_attaches_transform_output(self):
        """确认发布时应关联转换节点的最新输出。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        td = TransformDagService()
        td.execute_node(1)
        dp = DataPublishService()
        updated, msg = dp.run_action(1, "确认发布")
        assert updated is not None
        assert updated["status"] == "已发布"
        # 验证发布数据
        pub_data = dp.get_published_data(1)
        assert pub_data is not None
        assert pub_data["table"] == "dws_order_daily"
        assert len(pub_data["rows"]) >= 1

    def test_publish_data_matches_transform_output(self):
        """发布版本的数据应与转换节点输出一致。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        td = TransformDagService()
        td.execute_node(1)
        transform_rows = store.table_rows("dws_order_daily")
        dp = DataPublishService()
        dp.run_action(1, "确认发布")
        pub_data = dp.get_published_data(1)
        assert pub_data is not None
        assert len(pub_data["rows"]) == len(transform_rows)
