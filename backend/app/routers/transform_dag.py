"""转换编排 DAG 拓扑接口：返回节点、连线图结构，并承载转换执行与输出数据集管理。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.transform_dag import TransformDagService
from app.store import store

router = APIRouter(prefix="/api/transform_dag", tags=["转换编排"])
service = TransformDagService()
LIST_FIELDS = ["节点编号", "输入表名", "输出表名", "转换脚本", "执行顺序", "依赖节点", "失败策略", "调度周期"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


class ExecutePayload(BaseModel):
    """执行参数：边界策略与是否开启新批次。"""

    boundary: str = "wide"
    new_batch: bool = False


class SwitchVersionPayload(BaseModel):
    version: str


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/outputs")
def list_outputs() -> dict[str, Any]:
    """转换节点输出数据集列表：依赖状态、输出批次、结构版本一页可见。"""
    items = service.list_output_datasets()
    return {"items": items, "total": len(items)}


@router.get("/outputs/{output_table}/versions")
def list_output_versions(output_table: str) -> dict[str, Any]:
    """单个输出数据集的完整版本链，历史批次版本不丢失。"""
    versions = service.get_output_versions(output_table)
    if not versions:
        raise HTTPException(status_code=404, detail=f"输出数据集 {output_table} 暂无版本")
    return {"items": versions, "total": len(versions)}


@router.get("/stale")
def detect_stale() -> dict[str, Any]:
    """残留/旧值对账：转换编排清单与结构注册详情均可据此复现根因。"""
    items = service.detect_stale()
    return {"items": items, "total": len(items)}


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=transform_dag.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"转换节点 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="转换节点已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/execute")
def execute_node(entry_id: int, payload: ExecutePayload | None = None) -> dict[str, Any]:
    """执行转换节点：依赖校验、输出批次、结构注册同一事务完成。"""
    payload = payload or ExecutePayload()
    entry = service.get_entry(entry_id)
    if entry is None:
        return {"ok": False, "message": f"转换节点 {entry_id} 不存在"}
    if payload.new_batch:
        store.set_current_batch(str(entry.get("节点编号", "")),
                                store.next_batch_id(str(entry.get("节点编号", ""))))
    return service.execute_node(entry_id, boundary=payload.boundary)


@router.post("/{entry_id}/versions/switch")
def switch_version(entry_id: int, payload: SwitchVersionPayload) -> dict[str, Any]:
    """版本切换：沿调用链修正依赖节点结果引用。"""
    return service.switch_output_version(entry_id, payload.version)
