"""测试5：质量稽核规则执行。

验证：
- 稽核规则能读取目标表数据
- 校验表达式正确判定通过/失败
- 失败结果被持久化
- 阈值边界正确生效
"""
from __future__ import annotations

from app.store import store
from app.services.quality_check import QualityCheckService


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


class TestQualityCheck:
    def setup_method(self):
        reset_store()

    def test_check_passes_with_valid_data(self):
        """所有数据满足阈值时稽核应全部通过。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100.00", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200.00", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        qc = QualityCheckService()
        result = qc.execute_check(1)
        assert result["ok"] is True
        assert result["result"]["failed"] == 0
        assert result["result"]["passed"] == 2

    def test_check_fails_with_invalid_data(self):
        """存在越界值时稽核应标记失败。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "-10.00", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "1000000.00", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        qc = QualityCheckService()
        result = qc.execute_check(1)
        assert result["ok"] is True
        assert result["result"]["failed"] == 2
        assert result["result"]["passed"] == 0

    def test_check_enum_validation(self):
        """枚举值校验规则应拒绝非法值。"""
        store.set_table_rows("ods_users", [
            {"user_id": "U001", "user_level": "VIP", "dt": "2026-11-01"},
            {"user_id": "U002", "user_level": "非法值", "dt": "2026-11-01"},
        ])
        qc = QualityCheckService()
        result = qc.execute_check(3)
        assert result["ok"] is True
        assert result["result"]["failed"] == 1
        assert result["result"]["passed"] == 1

    def test_result_persisted(self):
        """稽核结果应被持久化到store。"""
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100.00", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        qc = QualityCheckService()
        qc.execute_check(1)
        persisted = store.get_quality_results("QC-ORD-001")
        assert len(persisted) >= 1
