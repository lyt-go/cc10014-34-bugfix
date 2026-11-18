# 数仓分层治理平台

面向数据入库、清洗转换、分层建模、质量校验与血缘追踪的分层数仓治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 原始入库 | `raw_ingest` | 入库批次 | 批次编号、来源系统、文件格式 |
| 字段映射 | `column_mapping` | 映射规则 | 规则编号、源字段名、目标字段名 |
| 清洗策略 | `cleanse_rule` | 清洗策略 | 策略编号、目标字段、空值处理 |
| 去重配置 | `dedup_config` | 去重配置 | 配置编号、业务键列、排序字段 |
| 贴源层管理 | `ods_layer` | 贴源表 | 表编号、来源系统、表名称 |
| 明细层管理 | `dwd_layer` | 明细表 | 表编号、父层来源、表名称 |
| 汇总层管理 | `dws_layer` | 汇总表 | 表编号、统计粒度、汇总维度 |
| 维度建模 | `dim_model` | 维度模型 | 模型编号、维度名称、自然键列 |
| 快照管理 | `snapshot_mgr` | 数据快照 | 快照编号、目标数据集、快照时间 |
| 分区治理 | `partition_mgr` | 分区策略 | 策略编号、目标表名、分区字段 |
| 质量稽核 | `quality_check` | 稽核规则 | 规则编号、校验表名、校验字段 |
| 血缘追踪 | `lineage_track` | 血缘链路 | 链路编号、上游表名、下游表名 |
| 补数调度 | `backfill_job` | 补数任务 | 任务编号、目标表名、起始日期 |
| 表级比对 | `table_compare` | 比对任务 | 任务编号、左表名称、右表名称 |
| 结构注册 | `schema_registry` | 结构定义 | 定义编号、表名称、字段名称 |
| 数据发布 | `data_publish` | 发布版本 | 版本编号、数据集名称、发布范围 |
| 转换编排 | `transform_dag` | 转换节点 | 节点编号、输入表名、输出表名 |
| 冷热归档 | `cold_archive` | 归档策略 | 策略编号、目标表名、冷数据阈值 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 转换节点依赖校验与输出数据集（缺陷修复）

转换编排模块对带依赖节点的转换任务执行以下一致性约束：

- **依赖 fail-closed 校验**：依赖节点缺失、尚未生效、无完整输出结果，或冲突无法裁定
  优先级时，转换直接阻断并回滚，下游不会收到缺少上游的残缺数据。输出只来自状态完整
  （`已生效` 且有完整输出版本）的依赖节点数据结果。
- **同一事务**：依赖状态校验、输出批次写入、血缘生成、结构注册通过 `store.transaction()`
  在同一事务内完成；任一步失败整体回滚，杜绝「输出已写入而结构未登记」。
- **版本化输出数据集**：按输出表保存完整版本链，同一转换批次重复执行只保留该批次一个
  完整版本，历史批次版本不丢失。
- **依赖停用冲突的两种边界**：
  - 宽边界（默认）：基于依赖的历史完整结果生成当前批次新版本，并记录依赖停用时间；
  - 缩窄边界：不产生新输出，保持旧版本并在输出数据集上展示停用依赖清单。
- **版本切换沿调用链修正**：切换某节点输出版本后，递归修正直接/间接下游绑定的依赖引用。
- **残留对账**：`/api/transform_dag/stale` 与结构注册的
  `/api/schema_registry/transform-sync` 都能复现「显示旧值、下游残留、结构未登记」根因。
- **结构注册只读已登记字段**：上游未登记的字段不会冒充已确认来源，而是进入「待补登记
  字段」清单；依赖中断恢复后也只读取已经登记的字段。

相关接口：

| 接口 | 说明 |
| --- | --- |
| `POST /api/transform_dag/{id}/execute` | 执行转换，入参 `{boundary: wide\|narrow, new_batch: bool}` |
| `GET /api/transform_dag/outputs` | 输出数据集列表（依赖状态、输出批次、结构版本、残留检测） |
| `GET /api/transform_dag/outputs/{table}/versions` | 单数据集完整版本链 |
| `POST /api/transform_dag/{id}/versions/switch` | 版本切换并沿调用链修正下游 |
| `GET /api/transform_dag/stale` | 旧值/残留/结构未登记对账 |
| `GET /api/schema_registry/transform-sync` | 结构注册侧跨模块同步视图 |

### 迁移与部署

- 内存库的迁移在 `Store.migrate()` 中幂等完成：补齐输出数据集、批次序列、结构同步索引
  容器，并把历史已写入的转换结果回填为 `v1` 输出数据集。
- 本地开发（`backend/run.sh`）、容器部署（`backend/Dockerfile`、`docker-compose.yml`）
  与上线启动都经由 `app.main` 初始化 `store` 时执行同一条迁移路径，重复启动安全。

领域：数据工程
子方向：数仓与数据湖
