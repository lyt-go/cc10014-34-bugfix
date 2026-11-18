"""测试4：快照版本化与分区归档。

验证：
- 创建快照时冻结目标数据集当前状态
- 快照数据可独立读取
- 归档策略执行后写入归档记录
- 补数分区可被记录
"""
from __future__ import annotations

from app.store import store
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.cold_archive import ColdArchiveService


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


class TestSnapshotArchive:
    def setup_method(self):
        reset_store()

    def test_snapshot_freezes_data(self):
        """创建快照动作应冻结当前表数据。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200", "dt": "2026-11-01"},
        ])
        snap = SnapshotMgrService()
        entry = snap.get_entry(1)
        updated, msg = snap.run_action(1, "创建快照")
        assert updated is not None
        assert updated["status"] == "创建中"
        # 验证冻结数据
        frozen = store.get_snapshot_data("SNAP-20261101-A")
        assert frozen is not None
        assert frozen["row_count"] == 2
        assert frozen["table_name"] == "ods_orders"

    def test_archive_executes(self):
        """归档策略执行后应写入归档记录。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100", "dt": "2026-11-01"},
        ])
        arc = ColdArchiveService()
        result = arc.execute_archive(1)
        assert result["ok"] is True
        assert result["record"]["row_count"] == 1
        records = store.get_archive_records()
        assert len(records) >= 1

    def test_archive_with_snapshot(self):
        """归档可消费快照数据而非当前表。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100", "dt": "2026-11-01"},
        ])
        snap = SnapshotMgrService()
        snap.run_action(1, "创建快照")
        arc = ColdArchiveService()
        result = arc.execute_archive(1, snapshot_id="SNAP-20261101-A")
        assert result["ok"] is True
