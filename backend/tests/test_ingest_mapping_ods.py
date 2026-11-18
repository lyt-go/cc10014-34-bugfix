"""测试1：入库→映射→ODS表链路。

验证：
- 入库批次确认后自动触发字段映射规则创建
- 字段映射规则与入库批次关联
- ODS表被自动创建/更新
- schema_registry同步注册
"""
from __future__ import annotations

import pytest
from app.store import store
from app.services.raw_ingest import RawIngestService
from app.services.column_mapping import ColumnMappingService
from app.services.ods_layer import OdsLayerService
from app.services.schema_registry import SchemaRegistryService


def reset_store():
    """重置内存仓库到初始seed状态。"""
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


class TestIngestMappingOds:
    def setup_method(self):
        reset_store()

    def test_ingest_confirm_triggers_mapping_and_ods(self):
        """确认入库后应自动创建字段映射和ODS表。"""
        ri = RawIngestService()
        entry, missing = ri.create_entry({
            "批次编号": "RAW-20261115",
            "来源系统": "订单系统",
            "文件格式": "CSV",
            "原始行数": "50000",
            "入库时间": "2026-11-15T02:00:00",
            "字符编码": "UTF-8",
            "校验摘要": "abc123",
            "落地路径": "/data/raw/orders/20261115.csv",
        })
        assert entry is not None
        assert entry["status"] == "草稿"

        # 执行确认入库动作
        updated, msg = ri.run_action(entry["id"], "确认入库")
        assert updated is not None
        assert updated["status"] == "已入库"

        # 验证字段映射已创建
        cms = ColumnMappingService()
        mappings = cms.get_mappings_by_batch("RAW-20261115")
        assert len(mappings) >= 1
        mapping = mappings[0]
        assert "order_id" in mapping.get("源字段名", "")
        assert "RAW-20261115" == mapping.get("来源批次", "")

        # 验证ODS表已创建
        ods = OdsLayerService()
        ods_entry = ods.get_by_table_name("ods_orders")
        assert ods_entry is not None
        assert ods_entry.get("来源系统") == "订单系统"
        assert "RAW-20261115" == ods_entry.get("创建批次", "")

        # 验证Schema已注册
        sch = SchemaRegistryService()
        schemas = sch.get_schema_for_table("ods_orders")
        assert len(schemas) >= 1
        latest = sch.get_latest_schema("ods_orders")
        assert latest is not None
        assert "order_id" in latest.get("字段名称", "")

        # 验证版本记录
        versions = store.get_versions("raw_ingest", entry["id"])
        assert len(versions) >= 1
        assert versions[-1]["data"]["status"] == "已入库"

    def test_ingest_batch_linkage(self):
        """验证入库批次与ODS表的双向关联。"""
        ods = OdsLayerService()
        batches = ods.get_batches_for_table("ods_orders")
        assert len(batches) >= 1
        assert batches[0].get("来源系统") == "订单系统"
