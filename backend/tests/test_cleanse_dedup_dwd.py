"""测试2：清洗+去重→DWD表链路。

验证：
- DWD服务能消费ODS数据
- 清洗规则应用到数据行
- 去重规则按业务键去重
- 结果写入DWD模拟表并生成血缘
"""
from __future__ import annotations

from app.store import store
from app.services.dwd_layer import DwdLayerService
from app.services.cleanse_rule import CleanseRuleService
from app.services.dedup_config import DedupConfigService


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


class TestCleanseDedupDwd:
    def setup_method(self):
        reset_store()

    def test_cleanse_applies_regex(self):
        """清洗规则应将不符合正则的值替换为默认值。"""
        cs = CleanseRuleService()
        data = [
            {"order_id": "O001", "order_amount": "100.50"},
            {"order_id": "O002", "order_amount": "-10.00"},
            {"order_id": "O003", "order_amount": ""},
        ]
        result = cs.apply_cleanse(1, data)
        assert result[0]["order_amount"] == "100.50"
        assert result[1]["order_amount"] == "0.00"
        assert result[2]["order_amount"] == "0.00"

    def test_dedup_removes_duplicates(self):
        """去重规则应按业务键去重保留最新。"""
        ds = DedupConfigService()
        data = [
            {"order_id": "O001", "order_amount": "100"},
            {"order_id": "O001", "order_amount": "200"},
            {"order_id": "O002", "order_amount": "50"},
        ]
        result = ds.apply_dedup(1, data)
        assert len(result) == 2
        keys = {r["order_id"] for r in result}
        assert keys == {"O001", "O002"}

    def test_dwd_build_from_ods(self):
        """DWD构建应消费ODS数据并生成模拟表和血缘。"""
        dwd = DwdLayerService()
        # 先给ODS表注入数据
        store.set_table_rows("ods_orders", [
            {"order_id": "O001", "order_amount": "100.50", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200.00", "order_status": "待支付", "dt": "2026-11-01"},
            {"order_id": "O001", "order_amount": "150.00", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        result = dwd.build_dwd_from_ods(1)
        assert result["ok"] is True
        dwd_data = store.table_rows("dwd_order_detail")
        assert len(dwd_data) >= 1
        # 验证血缘已生成
        edges = store.get_lineage_edges()
        assert any(e["上游表名"] == "ods_orders" and e["下游表名"] == "dwd_order_detail" for e in edges)
