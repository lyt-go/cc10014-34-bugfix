"""转换编排 DAG 图服务。

修复后的执行契约：
- 带依赖节点的转换任务执行前先校验依赖：依赖缺失、尚未生效且无完整结果、或冲突无法
  裁定优先级时直接阻断，不再产生残缺数据；
- 依赖状态校验、输出批次写入、结构注册在 ``store.transaction()`` 同一事务内完成，
  任一步失败整体回滚，杜绝「输出已写入而结构未登记」的半成品；
- 输出只来自状态完整的依赖节点数据结果，并以版本化「转换节点输出数据集」保存，
  同一批次重复执行只保留一个完整版本，历史批次版本不丢失；
- 依赖停用与当前批次冲突时区分边界：宽边界基于历史完整结果出新版本并记录停用时间，
  缩窄边界保持旧版本并展示停用依赖清单；
- 版本切换沿调用链修正下游绑定的依赖结果；残留/旧值可在编排清单与结构注册两侧复现。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store, StoreTransactionError

MODULE = "transform_dag"
REQUIRED_FIELDS = ["节点编号", "输入表名", "输出表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用节点": "已停用"}
NEGATIVE_ACTIONS = ["停用节点"]
# 依赖节点处于这些状态才允许向其下游提供完整数据结果
READY_STATUS = "已生效"


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class TransformDagService:
    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            node = {"id": int(row.get("id", i + 1)),
                    "name": str(row.get("节点编号", "")),
                    "status": str(row.get("status", STATUS_ORDER[0])),
                    **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")}}
            latest = store.get_latest_output(str(row.get("输出表名", "")))
            if latest:
                node["输出批次"] = latest["batch_id"]
                node["输出版本"] = latest["version"]
                node["结构版本"] = latest.get("schema_version")
            nodes.append(node)
        # 基于依赖节点构建真实边
        name_to_id = {n["name"]: n["id"] for n in nodes}
        for node in nodes:
            for dep in self._dep_names(node):
                if dep in name_to_id:
                    edges.append({"from": name_to_id[dep], "to": node["id"], "label": "依赖"})
        # 补充基于输入输出表的血缘边
        for i, a in enumerate(nodes):
            for b in nodes[i + 1:]:
                if a.get("输出表名") and a["输出表名"] == b.get("输入表名"):
                    edges.append({"from": a["id"], "to": b["id"], "label": "表依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        for k in ["转换脚本", "执行顺序", "依赖节点", "失败策略", "调度周期"]:
            if k in values:
                entry[k] = values[k]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"转换节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于转换编排可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action == "停用节点":
            # 记录依赖停用时间，供批次冲突时写入输出数据集
            entry["停用时间"] = datetime.now().isoformat(timespec="seconds")
        store.save_version(MODULE, entry_id, entry)
        return entry, f"转换节点已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ---- 依赖校验 ----

    @staticmethod
    def _dep_names(entry: dict[str, Any]) -> list[str]:
        raw = str(entry.get("依赖节点", "") or "")
        return [part.strip() for part in raw.replace("，", ",").split(",") if part.strip()]

    def _find_node_by_code(self, node_code: str) -> dict[str, Any] | None:
        return store.find_by_field(MODULE, "节点编号", node_code)

    def _validate_dependencies(self, entry: dict[str, Any]) -> dict[str, Any]:
        """汇总依赖节点状态。conflicts 非空表示优先级无法裁定，必须 fail-closed。"""
        report: dict[str, Any] = {
            "dependencies": [],
            "deactivated": [],
            "conflicts": [],
        }
        for name in self._dep_names(entry):
            dep_node = self._find_node_by_code(name)
            if dep_node is None:
                report["conflicts"].append(
                    {"依赖节点": name, "问题": "依赖节点不存在", "类型": "缺失"})
                continue
            latest = store.get_latest_output(str(dep_node.get("输出表名", "")))
            complete = bool(latest and latest.get("complete"))
            status = str(dep_node.get("status", ""))
            dep_info = {
                "依赖节点": name,
                "依赖节点id": dep_node.get("id"),
                "依赖状态": status,
                "输出表名": dep_node.get("输出表名", ""),
                "完整输出": complete,
                "引用版本": latest["version"] if complete else None,
                "引用批次": latest["batch_id"] if complete else None,
                "停用时间": dep_node.get("停用时间"),
            }
            report["dependencies"].append(dep_info)
            if status == STATUS_ORDER[-1]:
                report["deactivated"].append(dep_info)
                if not complete:
                    report["conflicts"].append({
                        "依赖节点": name,
                        "问题": "依赖节点已停用且无完整历史输出，无法裁定数据来源",
                        "类型": "停用无完整输出",
                        "停用时间": dep_node.get("停用时间"),
                    })
                continue
            if status != READY_STATUS:
                report["conflicts"].append({
                    "依赖节点": name,
                    "问题": f"依赖节点尚未生效（当前状态：{status or '未知'}）",
                    "类型": "未生效",
                })
                continue
            if not complete:
                report["conflicts"].append({
                    "依赖节点": name,
                    "问题": "依赖节点缺少状态完整的数据结果",
                    "类型": "缺少完整输出",
                })
        return report

    def _collect_inputs(
        self, entry: dict[str, Any], report: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """只从状态完整的依赖节点输出数据集取数；无依赖时退回输入表数据。"""
        input_table = str(entry.get("输入表名", ""))
        dep_names = self._dep_names(entry)
        if not dep_names:
            data = store.table_rows(input_table)
            if not data:
                # 兼容旧链路：尝试由 DWD 服务构建明细数据
                from app.services.dwd_layer import DwdLayerService
                dwd_entry = store.find_by_field("dwd_layer", "表名称", input_table)
                if dwd_entry:
                    DwdLayerService().build_dwd_from_ods(dwd_entry["id"])
                    data = store.table_rows(input_table)
            return [dict(r) for r in data], []
        rows: list[dict[str, Any]] = []
        sources: list[dict[str, Any]] = []
        for dep_info in report["dependencies"]:
            if not dep_info["完整输出"]:
                continue
            latest = store.get_latest_output(dep_info["输出表名"])
            if latest is None:
                continue
            if dep_info["输出表名"] != input_table:
                report["conflicts"].append({
                    "依赖节点": dep_info["依赖节点"],
                    "问题": (f"依赖输出表 {dep_info['输出表名']} 与本节点输入表 "
                            f"{input_table} 不一致，优先级无法裁定"),
                    "类型": "输入不匹配",
                })
                continue
            rows.extend(dict(r) for r in latest["rows"])
            sources.append({
                "依赖节点": dep_info["依赖节点"],
                "依赖状态": dep_info["依赖状态"],
                "引用版本": latest["version"],
                "引用批次": latest["batch_id"],
                "停用时间": dep_info.get("停用时间"),
            })
        return rows, sources

    # ---- 执行入口 ----

    def execute_node(self, entry_id: int, *, boundary: str = "wide") -> dict[str, Any]:
        """执行转换节点：校验依赖 → 取完整上游数据 → 写输出数据集 → 血缘 → 结构注册。

        全部步骤处于同一事务；失败时返回 ``ok=False`` 且不会留下任何半成品输出。
        ``boundary``：``wide`` 宽边界（默认，停用依赖基于历史完整结果出新版本）；
        ``narrow`` 缩窄边界（保持旧版本，仅登记停用依赖清单）。
        """
        entry = self.get_entry(entry_id)
        if not entry:
            return {"ok": False, "message": "转换节点不存在"}
        if boundary not in ("wide", "narrow"):
            return {"ok": False, "message": f"未知边界策略：{boundary}"}
        node_code = str(entry.get("节点编号", ""))
        output_table = str(entry.get("输出表名", ""))
        try:
            with store.transaction():
                if str(entry.get("status", "")) == STATUS_ORDER[-1]:
                    raise StoreTransactionError(
                        f"转换节点 {node_code} 已停用，不能发起新的转换批次")
                report = self._validate_dependencies(entry)
                # 冲突未决：fail-closed，不猜哪个依赖优先
                if report["conflicts"]:
                    raise StoreTransactionError(
                        "依赖冲突未裁定，转换已阻断："
                        + "；".join(f"{c['依赖节点']}({c['问题']})" for c in report["conflicts"]))

                deactivated = report["deactivated"]
                if deactivated and boundary == "narrow":
                    return self._handle_narrow_boundary(entry, deactivated)

                input_data, sources = self._collect_inputs(entry, report)
                if report["conflicts"]:
                    raise StoreTransactionError(
                        "依赖冲突未裁定，转换已阻断："
                        + "；".join(f"{c['依赖节点']}({c['问题']})" for c in report["conflicts"]))
                if not input_data:
                    raise StoreTransactionError(
                        f"输入表 {entry.get('输入表名', '')} 无状态完整的依赖数据，转换已阻断")

                output_data = self._execute_script(input_data, entry.get("转换脚本", ""))
                if not output_data:
                    raise StoreTransactionError("转换结果为空，拒绝写入半成品输出")

                batch_id = store.get_current_batch(node_code) or store.next_batch_id(node_code)
                store.set_current_batch(node_code, batch_id)
                store.set_table_rows(output_table, output_data)
                store.add_lineage(
                    entry.get("输入表名", ""), output_table,
                    field_map=node_code,
                    transform_logic=entry.get("转换脚本", ""),
                    batch_id=batch_id,
                )
                from app.services.schema_registry import SchemaRegistryService
                schema = SchemaRegistryService().register_transform_output(
                    output_table=output_table,
                    batch_id=batch_id,
                    output_rows=output_data,
                    upstream_tables=[entry.get("输入表名", "")],
                    node_code=node_code,
                )
                deactivated_meta = [
                    {
                        "依赖节点": d["依赖节点"],
                        "停用时间": d.get("停用时间"),
                        "复用历史版本": d.get("引用版本"),
                        "复用批次": d.get("引用批次"),
                    }
                    for d in deactivated
                ]
                record = store.save_transform_output(
                    output_table=output_table,
                    node_code=node_code,
                    node_id=entry_id,
                    batch_id=batch_id,
                    rows=output_data,
                    dependencies=sources or [{
                        "依赖节点": "",
                        "依赖状态": "无依赖",
                        "引用版本": None,
                        "引用批次": None,
                        "输入表": entry.get("输入表名", ""),
                    }],
                    boundary=boundary,
                    schema_version=schema.get("定义版本"),
                    deactivated_dependencies=deactivated_meta,
                )
            return {
                "ok": True,
                "message": (f"节点执行完成，批次 {batch_id} 输出 {len(output_data)} 行到 "
                            f"{output_table}，结构版本 {schema.get('定义版本')}"),
                "batch_id": batch_id,
                "version": record["version"],
                "schema_version": record["schema_version"],
                "rows": output_data,
                "deactivated_dependencies": deactivated_meta,
                "boundary": boundary,
            }
        except StoreTransactionError as exc:
            return {"ok": False, "message": str(exc), "rolled_back": True}
        except Exception as exc:  # 防御：任何意外都回滚，避免半成品
            return {"ok": False, "message": f"转换失败并已回滚：{exc}", "rolled_back": True}

    def _handle_narrow_boundary(
        self, entry: dict[str, Any], deactivated: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """缩窄边界：不产生新半成品输出，保持旧版本并记录停用依赖清单。"""
        node_code = str(entry.get("节点编号", ""))
        output_table = str(entry.get("输出表名", ""))
        batch_id = store.get_current_batch(node_code) or store.next_batch_id(node_code)
        store.set_current_batch(node_code, batch_id)
        deactivated_meta = [
            {
                "依赖节点": d["依赖节点"],
                "停用时间": d.get("停用时间"),
                "复用历史版本": d.get("引用版本"),
                "复用批次": d.get("引用批次"),
            }
            for d in deactivated
        ]
        record = store.mark_output_retained(output_table, batch_id, deactivated_meta)
        if record is None:
            raise StoreTransactionError(
                "缩窄边界要求已有历史输出版本，当前无旧版本可保持，转换已阻断")
        return {
            "ok": True,
            "retained": True,
            "message": (f"缩窄边界：保持 {output_table} 旧版本 {record['version']}，"
                        f"未生成新输出，停用依赖 {len(deactivated_meta)} 个已记录"),
            "batch_id": batch_id,
            "version": record["version"],
            "schema_version": record.get("schema_version"),
            "deactivated_dependencies": deactivated_meta,
            "boundary": "narrow",
            "rows": record["rows"],
        }

    # ---- 输出数据集查询与残留检测 ----

    def list_output_datasets(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for output in store.list_transform_outputs():
            output["stale"] = self._stale_reasons(output)
            dep_summary = [
                f"{d.get('依赖节点') or '无依赖'}:{d.get('依赖状态', '—')}"
                f"@{d.get('引用版本') or '—'}"
                for d in output.get("dependencies", [])
            ]
            output["依赖状态摘要"] = "；".join(dep_summary) or "无依赖"
            result.append(output)
        return result

    def get_output_versions(self, output_table: str) -> list[dict[str, Any]]:
        return store.get_output_versions(output_table)

    def _stale_reasons(self, output: dict[str, Any]) -> list[str]:
        """复现「显示旧值、下游残留」根因：实时表、结构注册、依赖引用三处对账。"""
        reasons: list[str] = []
        table = output["dataset"]
        live_rows = store.table_rows(table)
        if live_rows != output.get("rows", []):
            reasons.append("实时输出表与最新输出数据集内容不一致（旧值/残留）")
        from app.services.schema_registry import SchemaRegistryService
        schemas = SchemaRegistryService().get_schema_for_table(table)
        if output.get("schema_version"):
            registered_versions = {str(s.get("定义版本")) for s in schemas}
            if output["schema_version"] not in registered_versions:
                reasons.append("结构注册缺少输出数据集绑定的结构版本")
        elif not schemas:
            reasons.append("输出已写入但结构注册未登记任何字段，下游无法确认字段来源")
        for dep in output.get("dependencies", []):
            dep_node = dep.get("依赖节点")
            if not dep_node:
                continue
            dep_entry = self._find_node_by_code(dep_node)
            if not dep_entry:
                reasons.append(f"依赖节点 {dep_node} 已不存在")
                continue
            upstream_latest = store.get_latest_output(str(dep_entry.get("输出表名", "")))
            if upstream_latest and dep.get("引用版本") \
                    and upstream_latest["version"] != dep["引用版本"]:
                reasons.append(
                    f"下游仍绑定依赖 {dep_node} 旧版本 {dep['引用版本']}，"
                    f"上游最新为 {upstream_latest['version']}")
        return reasons

    def detect_stale(self) -> list[dict[str, Any]]:
        return [
            {
                "输出数据集": item["dataset"],
                "来源转换节点": item["node"],
                "输出批次": item["batch_id"],
                "输出版本": item["version"],
                "结构版本": item.get("schema_version"),
                "残留原因": item["stale"],
            }
            for item in self.list_output_datasets()
            if item["stale"]
        ]

    # ---- 版本切换沿调用链修正 ----

    def switch_output_version(self, entry_id: int, version: str) -> dict[str, Any]:
        entry = self.get_entry(entry_id)
        if not entry:
            return {"ok": False, "message": "转换节点不存在"}
        node_code = str(entry.get("节点编号", ""))
        output_table = str(entry.get("输出表名", ""))
        target = store.find_output_version(output_table, version)
        if target is None or not target.get("complete"):
            return {"ok": False, "message": f"输出表 {output_table} 不存在完整版本 {version}"}
        try:
            with store.transaction():
                store.bind_output_version(
                    output_table, node_code, target["version"], target["batch_id"])
                propagated = self._propagate_switch(node_code, target, set())
            return {
                "ok": True,
                "message": f"{output_table} 已切换到版本 {version}，沿调用链修正 {propagated} 个下游绑定",
                "version": version,
                "batch_id": target["batch_id"],
                "propagated": propagated,
            }
        except StoreTransactionError as exc:
            return {"ok": False, "message": str(exc), "rolled_back": True}

    def _propagate_switch(
        self, upstream_code: str, upstream_output: dict[str, Any], visited: set[str],
    ) -> int:
        """递归修正直接/间接下游输出数据集里绑定的依赖节点结果引用。"""
        if upstream_code in visited:
            return 0
        visited.add(upstream_code)
        count = 0
        for row in store.rows(MODULE):
            if upstream_code not in self._dep_names(row):
                continue
            downstream_table = str(row.get("输出表名", ""))
            latest = store.get_latest_output(downstream_table)
            if latest is None:
                continue
            store.bind_dependency_version(
                downstream_table, upstream_code,
                upstream_output["version"], upstream_output["batch_id"])
            count += 1
            count += self._propagate_switch(str(row.get("节点编号", "")), latest, visited)
        return count

    def _execute_script(self, data: list[dict[str, Any]], script: str) -> list[dict[str, Any]]:
        if "GROUP BY" in script.upper():
            import re
            # 找聚合字段：SUM/COUNT/AVG/MAX/MIN(...)
            agg_m = re.search(r"(SUM|COUNT|AVG|MAX|MIN)\s*\(([^)]+)\)", script, re.I)
            measure = agg_m.group(2).strip() if agg_m else ""
            agg = (agg_m.group(1).upper() if agg_m else "SUM")
            # 提取 GROUP BY 后面的维度字段（排除聚合函数部分），保留原始大小写
            after_gb = script.split("GROUP BY", 1)[1].strip() if "GROUP BY" in script.upper() else script
            # 去掉末尾的聚合函数部分
            agg_keywords = [" SUM(", " COUNT(", " AVG(", " MAX(", " MIN(",
                          " sum(", " count(", " avg(", " max(", " min("]
            for kw in agg_keywords:
                if kw in after_gb.upper():
                    idx = after_gb.upper().index(kw)
                    after_gb = after_gb[:idx]
                    break
            dims_raw = [d.strip() for d in after_gb.split(",")]
            dims = [d for d in dims_raw if d and not re.search(r'^(SUM|COUNT|AVG|MAX|MIN)\s*\(', d, re.I)]
            groups: dict[str, dict[str, Any]] = {}
            for row in data:
                key = "|".join(str(row.get(d, "")) for d in dims) if dims else "*"
                if key not in groups:
                    groups[key] = {d: row.get(d) for d in dims} if dims else {}
                    groups[key]["_values"] = []
                try:
                    groups[key]["_values"].append(float(row.get(measure, 0) or 0))
                except Exception:
                    groups[key]["_values"].append(0.0)
            result = []
            for g in groups.values():
                out = {d: g[d] for d in dims} if dims else {}
                vals = g["_values"]
                if agg == "SUM":
                    out[f"{measure}_agg"] = sum(vals)
                elif agg == "COUNT":
                    out[f"{measure}_agg"] = len(vals)
                elif agg == "AVG":
                    out[f"{measure}_agg"] = sum(vals) / len(vals) if vals else 0
                elif agg == "MAX":
                    out[f"{measure}_agg"] = max(vals) if vals else 0
                elif agg == "MIN":
                    out[f"{measure}_agg"] = min(vals) if vals else 0
                else:
                    out[f"{measure}_agg"] = sum(vals)
                result.append(out)
            return result
        # 默认透传
        return [dict(r) for r in data]
