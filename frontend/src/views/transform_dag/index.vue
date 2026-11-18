<template>
  <section class="page" data-module="transform_dag">
    <header class="page-head">
      <div><h2>转换编排</h2><p class="page-desc">维护转换节点的输入、执行约束与结果记录；依赖状态、输出批次与结构注册同一事务完成，输出只来自状态完整的依赖节点。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记转换节点</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <dl v-if="selectedNode.输出批次"><dt>输出批次</dt><dd>{{ selectedNode.输出批次 }}</dd></dl>
      <dl v-if="selectedNode.输出版本"><dt>输出版本</dt><dd>{{ selectedNode.输出版本 }}</dd></dl>
      <dl v-if="selectedNode.结构版本"><dt>结构版本</dt><dd>{{ selectedNode.结构版本 }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px; flex-wrap:wrap;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
      <div style="grid-column:1/-1; display:flex; gap:6px; flex-wrap:wrap; align-items:center;">
        <label class="filter-item" style="display:flex;align-items:center;gap:4px;">
          <span>边界</span>
          <select v-model="boundary" data-testid="boundary-select">
            <option value="wide">宽边界（停用依赖基于历史完整结果出新版本）</option>
            <option value="narrow">缩窄边界（保持旧版本并记录停用依赖）</option>
          </select>
        </label>
        <button class="btn primary" type="button" data-testid="execute-node" @click="executeSelected(false)">执行转换</button>
        <button class="btn" type="button" data-testid="execute-new-batch" @click="executeSelected(true)">新批次执行</button>
      </div>
      <div v-if="executeMessage" style="grid-column:1/-1;" :class="executeOk ? 'ok-text' : 'error-text'" data-testid="execute-message">{{ executeMessage }}</div>
    </div>

    <h3 style="margin:16px 0 8px;">转换节点输出数据集</h3>
    <table class="data-table" data-testid="output-table">
      <thead>
        <tr>
          <th>输出数据集</th><th>来源节点</th><th>依赖状态</th><th>输出批次</th>
          <th>输出版本</th><th>结构版本</th><th>行数</th><th>停用依赖</th><th>残留检测</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in outputs" :key="item.dataset" :data-testid="`output-row-${item.dataset}`" :data-stale="item.stale?.length ? 'yes' : 'no'">
          <td>{{ item.dataset }}</td>
          <td>{{ item.node }}</td>
          <td>{{ item.依赖状态摘要 }}</td>
          <td>{{ item.batch_id }}</td>
          <td>{{ item.version }}</td>
          <td>{{ item.schema_version ?? '未登记' }}</td>
          <td>{{ item.row_count }}</td>
          <td>
            <span v-if="item.deactivated_dependencies?.length">
              {{ item.deactivated_dependencies.map((d: any) => `${d.依赖节点}@${d.停用时间 ?? '—'}`).join('；') }}
            </span>
            <span v-else>—</span>
          </td>
          <td>
            <span v-if="item.stale?.length" class="error-text" :data-testid="`stale-${item.dataset}`">{{ item.stale.join('；') }}</span>
            <span v-else class="ok-text">一致</span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`versions-${item.dataset}`" @click="openVersions(item)">版本链</button>
          </td>
        </tr>
        <tr v-if="!outputs.length"><td colspan="10" class="empty-state">暂无转换节点输出数据集，执行转换节点后生成</td></tr>
      </tbody>
    </table>

    <div v-if="versionDrawer" class="modal-backdrop" data-testid="version-drawer" @click.self="versionDrawer=null">
      <div class="modal" style="max-width:720px;">
        <h3>{{ versionDrawer.dataset }} 版本链（历史不丢失）</h3>
        <table class="data-table">
          <thead><tr><th>版本</th><th>批次</th><th>结构版本</th><th>边界</th><th>停用依赖</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="v in versionDrawer.versions" :key="v.version">
              <td>{{ v.version }}</td><td>{{ v.batch_id }}</td><td>{{ v.schema_version ?? '未登记' }}</td>
              <td>{{ v.boundary }}{{ v.retained ? '（保持旧版本）' : '' }}</td>
              <td>{{ (v.deactivated_dependencies || []).map((d: any) => d.依赖节点).join('，') || '—' }}</td>
              <td class="row-actions">
                <button class="link" type="button" :data-testid="`switch-${v.version}`" @click="switchVersion(v)">切换并修正下游</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions"><button class="btn" type="button" @click="versionDrawer=null">关闭</button></div>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记转换节点</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点 · {{ outputs.length }} 个输出数据集</span><span v-if="errorMessage" class="error-text">{{ errorMessage }}</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/transform_dag'
const columns = ["节点编号", "输入表名", "输出表名", "转换脚本", "执行顺序", "依赖节点", "失败策略", "调度周期"]
const actions = ["提交校验", "标记生效", "停用节点"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已生效':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const outputs = ref<any[]>([])
const boundary = ref<'wide'|'narrow'>('wide')
const executeMessage = ref('')
const executeOk = ref(false)
const versionDrawer = ref<{dataset: string; versions: any[]} | null>(null)
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) {
  selectedNode.value = selectedNode.value?.id === node.id ? null : node
  executeMessage.value = ''
}
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try {
    const res = await request(ENDPOINT); const data = await res.json()
    graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes)
    const outRes = await request(`${ENDPOINT}/outputs`); const outData = await outRes.json()
    outputs.value = outData.items || []
  } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try {
    const res = await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
    const payload = await res.json()
    if (!payload.ok) { errorMessage.value = payload.message; return }
    await reload()
  } catch(e) { errorMessage.value = '操作失败' }
}
async function executeSelected(newBatch: boolean) {
  if (!selectedNode.value) return
  executeMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${selectedNode.value.id}/execute`, {
      method: 'POST',
      body: JSON.stringify({ boundary: boundary.value, new_batch: newBatch }),
    })
    const payload = await res.json()
    executeOk.value = !!payload.ok
    executeMessage.value = payload.ok
      ? `${payload.message}${payload.retained ? '（保持旧版本，无新半成品输出）' : ''}`
      : payload.message
    await reload()
    const fresh = graph.nodes.find((n: any) => n.id === selectedNode.value?.id)
    if (fresh) selectedNode.value = fresh
  } catch(e: any) {
    executeOk.value = false
    executeMessage.value = e.message
  }
}
async function openVersions(item: any) {
  try {
    const res = await request(`${ENDPOINT}/outputs/${encodeURIComponent(item.dataset)}/versions`)
    const data = await res.json()
    versionDrawer.value = { dataset: item.dataset, versions: data.items || [] }
  } catch(e) { errorMessage.value = '版本链读取失败' }
}
async function switchVersion(v: any) {
  const node = graph.nodes.find((n: any) => n.输出表名 === versionDrawer.value?.dataset)
  if (!node) { errorMessage.value = '未找到输出版本所属转换节点'; return }
  const res = await request(`${ENDPOINT}/${node.id}/versions/switch`, {
    method: 'POST', body: JSON.stringify({ version: v.version }),
  })
  const payload = await res.json()
  executeOk.value = !!payload.ok
  executeMessage.value = payload.message
  if (payload.ok) versionDrawer.value = null
  await reload()
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
