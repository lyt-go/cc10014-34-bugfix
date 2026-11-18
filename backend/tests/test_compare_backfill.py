"""测试7：表级比对→补数触发。

验证：
- 比对任务发现差异后自动触发补数任务
- 补数任务作用于具体表并修改分区水位
- 比对结果包含差异数量
"""
from __future__ import annotations

from app.store import store
from app.services.table_compare import TableCompareService
from app.services.backfill_job import BackfillJobService


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


class TestCompareBackfill:
    def setup_method(self):
        reset_store()

    def test_compare_finds_diff_and_triggers_backfill(self):
        """比对发现差异后应触发补数任务。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200", "dt": "2026-11-01"},
        ])
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "dt": "2026-11-01"},
        ])
        tc = TableCompareService()
        updated, msg = tc.run_action(1, "启动比对")
        assert updated is not None
        result = updated.get("_compare_result", {})
        assert result.get("diff_count", 0) > 0
        # 验证补数任务已创建
        bf = BackfillJobService()
        jobs = [r for r in store.rows("backfill_job") if r.get("任务编号", "").startswith("BF-CMP")]
        assert len(jobs) >= 1

    def test_backfill_modifies_partition_watermark(self):
        """补数任务应修改目标表的分区水位。"""
        bf = BackfillJobService()
        updated, msg = bf.run_action(3, "启动任务")
        assert updated is not None
        assert updated["status"] == "运行中"
        # 验证补数分区已记录
        partitions = store.get_backfill_partitions("BF-USR-001")
        assert len(partitions) >= 1
        # 验证表数据已更新
        data = store.table_rows("ods_users")
        assert len(data) >= 1
        assert data[0].get("_backfilled") is True
