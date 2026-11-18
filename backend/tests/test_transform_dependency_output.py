"""转换节点依赖校验与输出数据集一致性测试。

覆盖缺陷修复的全部验收点：
1. 依赖缺失/未生效/无完整结果时阻断执行，不产生残缺数据；
2. 依赖状态、输出批次、结构注册同一事务，失败整体回滚；
3. 同批次重复执行只保留一个完整版本，历史批次不丢失；
4. 依赖停用冲突：宽边界出新版本并记录停用时间，缩窄边界保持旧版本并展示停用清单；
5. 冲突无法裁定时 fail-closed；
6. 结构注册只读取上游已登记字段，并跨模块同步；
7. 版本切换沿调用链修正下游引用；
8. 残留/旧值在编排清单与结构注册两侧均可复现。
"""
from __future__ import annotations

from app.store import store, StoreTransactionError
from app.services.schema_registry import SchemaRegistryService
from app.services.transform_dag import TransformDagService

DWD_ROWS = [
    {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
    {"order_id": "O002", "order_amount": "200", "order_status": "已完成", "dt": "2026-11-01"},
    {"order_id": "O003", "order_amount": "50", "order_status": "待支付", "dt": "2026-11-01"},
]
GROUP_SCRIPT = ("SELECT dt,order_status,SUM(order_amount),COUNT(*) "
                "FROM dwd_order_detail GROUP BY dt,order_status")
DOWN_SCRIPT = ("SELECT order_status,COUNT(order_amount_agg) "
               "FROM mid_table GROUP BY order_status")


def _reset() -> None:
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._table_data.clear()
    store._versions.clear()
    store._lineage_edges.clear()
    store._quality_results.clear()
    store._snapshot_data.clear()
    store._archive_records.clear()
    store._backfill_partitions.clear()
    store._transform_outputs.clear()
    store._transform_batches.clear()
    store._batch_seq.clear()
    store._schema_sync_index.clear()
    # 测试内登记 dwd_order_detail 的上游结构，用于验证「只读取已登记字段」
    schema_rows = store.rows("schema_registry")
    schema_rows.append({
        "id": max((int(r.get("id", 0)) for r in schema_rows), default=0) + 1,
        "status": "已生效", "pending": False, "abnormal": False,
        "定义编号": "SCH-DWD-ORD-T", "表名称": "dwd_order_detail",
        "字段名称": "order_id,order_amount,order_status,dt",
        "字段类型": "STRING,DECIMAL,STRING,STRING", "可空标识": "N,N,N,N",
        "默认值": ",,,", "定义版本": "v1", "生效批次": "RAW-TEST",
    })


def _make(td: TransformDagService, values: dict, *, activate: bool = True) -> int:
    entry, missing = td.create_entry(values)
    assert not missing
    node_id = entry["id"]
    if activate:
        td.run_action(node_id, "提交校验")
        td.run_action(node_id, "标记生效")
    return node_id


class TestTransformDependencyGuard:
    def setup_method(self):
        _reset()
        self.td = TransformDagService()

    def _upstream(self, *, activate: bool = True, code: str = "DEP-001",
                  output_table: str = "mid_table") -> int:
        return _make(self.td, {
            "节点编号": code, "输入表名": "dwd_order_detail",
            "输出表名": output_table, "转换脚本": GROUP_SCRIPT,
            "执行顺序": "1", "依赖节点": "",
        }, activate=activate)

    def _downstream(self, dep_code: str = "DEP-001", code: str = "DOWN-001",
                    input_table: str = "mid_table",
                    output_table: str = "down_table",
                    script: str = DOWN_SCRIPT) -> int:
        return _make(self.td, {
            "节点编号": code, "输入表名": input_table,
            "输出表名": output_table, "转换脚本": script,
            "执行顺序": "2", "依赖节点": dep_code,
        })

    def test_missing_dependency_blocks_execution(self):
        """依赖节点不存在：阻断且不写任何输出。"""
        down_id = self._downstream(dep_code="GHOST-999")
        result = self.td.execute_node(down_id)
        assert result["ok"] is False
        assert "依赖冲突" in result["message"]
        assert result["rolled_back"] is True
        assert store.table_rows("down_table") == []
        assert store.get_latest_output("down_table") is None

    def test_dependency_not_effective_blocks_execution(self):
        """依赖尚未生效（草稿）：阻断，下游不会收到残缺数据。"""
        self._upstream(activate=False)
        down_id = self._downstream()
        result = self.td.execute_node(down_id)
        assert result["ok"] is False
        assert "尚未生效" in result["message"]
        assert store.get_latest_output("down_table") is None

    def test_dependency_without_complete_output_blocks_execution(self):
        """依赖已生效但从未产出完整结果：阻断。"""
        self._upstream()
        down_id = self._downstream()
        result = self.td.execute_node(down_id)
        assert result["ok"] is False
        assert "完整" in result["message"]
        assert store.table_rows("down_table") == []

    def test_output_only_uses_complete_dependency_data(self):
        """依赖完整时下游输出只来自依赖节点的完整输出数据集。"""
        up_id = self._upstream()
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        up_result = self.td.execute_node(up_id)
        assert up_result["ok"] is True
        down_id = self._downstream()
        result = self.td.execute_node(down_id)
        assert result["ok"] is True, result["message"]
        output = store.get_latest_output("down_table")
        assert output is not None and output["complete"] is True
        assert output["dependencies"][0]["依赖节点"] == "DEP-001"
        assert output["dependencies"][0]["引用版本"] == "v1"
        assert len(output["rows"]) <= len(store.get_latest_output("mid_table")["rows"])

    def test_transaction_rolls_back_when_schema_registration_fails(self):
        """输出已写但结构登记失败时整体回滚：表数据、血缘、输出数据集都不留痕。"""
        up_id = self._upstream(output_table="mid_x")
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        self.td.execute_node(up_id)
        down_id = self._downstream(output_table="down_x", input_table="mid_x")
        lineage_before = len(store.get_lineage_edges())
        schema_before = len(store.rows("schema_registry"))
        original = SchemaRegistryService.register_transform_output

        def boom(*args, **kwargs):
            raise StoreTransactionError("模拟结构登记失败")

        SchemaRegistryService.register_transform_output = boom
        try:
            result = self.td.execute_node(down_id)
        finally:
            SchemaRegistryService.register_transform_output = original
        assert result["ok"] is False
        assert result["rolled_back"] is True
        assert store.table_rows("down_x") == []
        assert store.get_latest_output("down_x") is None
        assert len(store.get_lineage_edges()) == lineage_before
        assert len(store.rows("schema_registry")) == schema_before


class TestBatchAndVersioning:
    def setup_method(self):
        _reset()
        self.td = TransformDagService()

    def _node(self, output_table: str = "solo_table") -> int:
        return _make(self.td, {
            "节点编号": "SOLO-001", "输入表名": "dwd_order_detail",
            "输出表名": output_table, "转换脚本": GROUP_SCRIPT,
            "执行顺序": "1", "依赖节点": "",
        })

    def test_same_batch_keeps_single_complete_version(self):
        node_id = self._node()
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        first = self.td.execute_node(node_id)
        assert first["ok"]
        # 同批次再执行（数据变化），仍只保留该批次一个完整版本
        store.set_table_rows("dwd_order_detail", DWD_ROWS + [
            {"order_id": "O004", "order_amount": "80", "order_status": "待支付", "dt": "2026-11-02"},
        ])
        second = self.td.execute_node(node_id)
        assert second["ok"]
        assert second["batch_id"] == first["batch_id"]
        versions = store.get_output_versions("solo_table")
        assert len(versions) == 1
        assert versions[0]["batch_id"] == first["batch_id"]

    def test_new_batch_appends_version_without_losing_history(self):
        node_id = self._node()
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        first = self.td.execute_node(node_id)
        self.td.execute_node(node_id)  # 同批次重复
        # 显式开启新批次，并换一批输入
        store.set_table_rows("dwd_order_detail", DWD_ROWS + [
            {"order_id": "O009", "order_amount": "70", "order_status": "待支付", "dt": "2026-11-02"},
        ])
        store.set_current_batch("SOLO-001", store.next_batch_id("SOLO-001"))
        second_batch = self.td.execute_node(node_id)
        assert second_batch["ok"]
        assert second_batch["batch_id"] != first["batch_id"]
        versions = store.get_output_versions("solo_table")
        assert [v["version"] for v in versions] == ["v1", "v2"]
        # 历史版本内容不丢，且与新版本可区分
        assert versions[0]["rows"] != versions[1]["rows"]
        # 列表页能看到依赖状态、输出批次和结构版本
        items = self.td.list_output_datasets()
        solo = next(i for i in items if i["dataset"] == "solo_table")
        assert solo["batch_id"]
        assert solo["schema_version"]
        assert solo["依赖状态摘要"]

    def test_schema_registry_syncs_output(self):
        node_id = self._node()
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        result = self.td.execute_node(node_id)
        synced = SchemaRegistryService().list_transform_sync()
        item = next(s for s in synced if s["输出数据集"] == "solo_table")
        assert item["结构版本"] == result["schema_version"]
        assert item["输出批次"] == result["batch_id"]
        assert "order_status" in item["已确认字段"]  # 来自 dwd 已登记字段
        # 同批次重复执行不会产生第二个数据集版本
        self.td.execute_node(node_id)
        assert len(store.get_output_versions("solo_table")) == 1

    def test_schema_only_reads_registered_upstream_fields(self):
        """上游未登记的新字段不会被当成已确认来源，只能进待补登记清单。"""
        node_id = self._node(output_table="passthrough_table")
        # 无 GROUP BY 走透传，ghost_field 才会保留到输出
        entry = self.td.get_entry(node_id)
        entry["转换脚本"] = "SELECT * FROM dwd_order_detail"
        rows = [dict(r) for r in DWD_ROWS]
        rows[0]["ghost_field"] = "x"
        store.set_table_rows("dwd_order_detail", rows)
        result = self.td.execute_node(node_id)
        assert result["ok"], result["message"]
        schema = store.rows("schema_registry")[-1]
        assert "ghost_field" in schema["待补登记字段"]
        assert "ghost_field" not in schema["已确认字段"]
        assert "order_status" in schema["已确认字段"]


class TestDeactivationBoundary:
    def setup_method(self):
        _reset()
        self.td = TransformDagService()
        self.up = _make(self.td, {
            "节点编号": "DEP-100", "输入表名": "dwd_order_detail",
            "输出表名": "mid_100", "转换脚本": GROUP_SCRIPT,
            "执行顺序": "1", "依赖节点": "",
        })
        self.down = _make(self.td, {
            "节点编号": "DOWN-100", "输入表名": "mid_100",
            "输出表名": "down_100",
            "转换脚本": ("SELECT order_status,COUNT(order_amount_agg) "
                      "FROM mid_100 GROUP BY order_status"),
            "执行顺序": "2", "依赖节点": "DEP-100",
        })
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        assert self.td.execute_node(self.up)["ok"]
        # 下游先成功执行一次，留下旧版本
        self.first = self.td.execute_node(self.down)
        assert self.first["ok"]

    def test_wide_boundary_emits_new_version_with_deactivation_time(self):
        _, msg = self.td.run_action(self.up, "停用节点")
        assert "停用" in msg
        dep_entry = self.td.get_entry(self.up)
        assert dep_entry.get("停用时间")
        result = self.td.execute_node(self.down, boundary="wide")
        assert result["ok"] is True, result["message"]
        assert result["deactivated_dependencies"]
        assert result["deactivated_dependencies"][0]["停用时间"] == dep_entry["停用时间"]
        output = store.get_latest_output("down_100")
        assert output["deactivated_dependencies"][0]["依赖节点"] == "DEP-100"
        assert output["boundary"] == "wide"

    def test_narrow_boundary_keeps_old_version_and_lists_deactivated(self):
        self.td.run_action(self.up, "停用节点")
        result = self.td.execute_node(self.down, boundary="narrow")
        assert result["ok"] is True
        assert result["retained"] is True
        assert result["version"] == self.first["version"]
        output = store.get_latest_output("down_100")
        assert output["retained"] is True
        assert output["deactivated_dependencies"]
        # 停用后不会产生新的半成品输出：版本链仍是一个版本
        assert len(store.get_output_versions("down_100")) == 1

    def test_deactivated_without_complete_output_is_conflict(self):
        # 另一个全新下游，依赖一个停用且从未产出的节点
        other = _make(self.td, {
            "节点编号": "DEP-200", "输入表名": "dwd_order_detail",
            "输出表名": "mid_200", "转换脚本": "SELECT 1",
            "执行顺序": "1", "依赖节点": "",
        })
        self.td.run_action(other, "停用节点")
        down = _make(self.td, {
            "节点编号": "DOWN-200", "输入表名": "mid_200",
            "输出表名": "down_200", "转换脚本": "SELECT 1",
            "执行顺序": "2", "依赖节点": "DEP-200",
        })
        result = self.td.execute_node(down)
        assert result["ok"] is False
        assert "冲突" in result["message"]


class TestVersionSwitchAndStale:
    def setup_method(self):
        _reset()
        self.td = TransformDagService()
        self.up = _make(self.td, {
            "节点编号": "UP-300", "输入表名": "dwd_order_detail",
            "输出表名": "mid_300", "转换脚本": GROUP_SCRIPT,
            "执行顺序": "1", "依赖节点": "",
        })
        self.down = _make(self.td, {
            "节点编号": "DOWN-300", "输入表名": "mid_300",
            "输出表名": "down_300",
            "转换脚本": ("SELECT order_status,COUNT(order_amount_agg) "
                      "FROM mid_300 GROUP BY order_status"),
            "执行顺序": "2", "依赖节点": "UP-300",
        })
        store.set_table_rows("dwd_order_detail", DWD_ROWS)
        assert self.td.execute_node(self.up)["ok"]
        assert self.td.execute_node(self.down)["ok"]

    def test_switch_propagates_along_call_chain(self):
        # 上游再跑一个新批次，产生 v2
        store.set_current_batch("UP-300", store.next_batch_id("UP-300"))
        up_v2 = self.td.execute_node(self.up)
        assert up_v2["version"] == "v2"
        # 下游仍绑定 v1 —— 残留可被检测到
        stale = self.td.detect_stale()
        assert any(s["输出数据集"] == "down_300" for s in stale)
        # 切换上游到 v1，沿调用链修正下游引用
        result = self.td.switch_output_version(self.up, "v1")
        assert result["ok"], result["message"]
        assert result["propagated"] >= 1
        latest_down = store.get_latest_output("down_300")
        dep = next(d for d in latest_down["dependencies"] if d["依赖节点"] == "UP-300")
        assert dep["引用版本"] == "v1"

    def test_stale_detects_table_residue(self):
        # 直接改实时表制造残留，模拟旧值/下游残留根因
        store.set_table_rows("down_300", [{"order_status": "脏数据"}])
        stale = self.td.detect_stale()
        item = next(s for s in stale if s["输出数据集"] == "down_300")
        assert any("不一致" in reason for reason in item["残留原因"])

    def test_stale_detects_missing_schema_registration(self):
        # 删掉输出表结构注册，模拟「输出已写而结构未登记」
        table = "down_300"
        store.get_latest_output(table)
        synced_ids = store.get_schema_sync_index(table)
        store.rows("schema_registry")[:] = [
            r for r in store.rows("schema_registry") if r["id"] not in synced_ids]
        stale = self.td.detect_stale()
        item = next(s for s in stale if s["输出数据集"] == table)
        assert any("结构注册" in reason for reason in item["残留原因"])
        # 结构注册侧同步视图同样能复现
        sync = SchemaRegistryService().list_transform_sync()
        sync_item = next(s for s in sync if s["输出数据集"] == table)
        assert sync_item["结构状态"] == "未登记"

    def test_stopped_node_cannot_start_new_batch(self):
        self.td.run_action(self.down, "停用节点")
        result = self.td.execute_node(self.down)
        assert result["ok"] is False
        assert "停用" in result["message"]
        assert store.get_latest_output("down_300") is not None  # 历史输出不丢
