<template>
  <section class="page" data-module="schema_registry">
    <header class="page-head">
      <div>
        <h2>结构注册管理</h2>
        <p class="page-desc">维护结构定义的输入、执行约束与结果记录，为结构注册提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记结构定义</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出结构注册清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无结构注册数据，可先登记结构定义</td>
        </tr>
      </tbody>
    </table>

    <h3 style="margin:18px 0 8px;">转换节点输出数据集结构同步</h3>
    <table class="data-table" data-testid="transform-sync-table">
      <thead>
        <tr>
          <th>输出数据集</th><th>来源转换节点</th><th>输出批次</th><th>输出版本</th>
          <th>结构版本</th><th>结构状态</th><th>已确认字段</th><th>待补登记字段</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in transformSync" :key="item.输出数据集" :data-testid="`sync-row-${item.输出数据集}`" :data-status="String(item.结构状态)">
          <td>{{ item.输出数据集 }}</td>
          <td>{{ item.来源转换节点 }}</td>
          <td>{{ item.输出批次 }}</td>
          <td>{{ item.输出版本 }}</td>
          <td>{{ item.结构版本 ?? '未登记' }}</td>
          <td>
            <span v-if="item.结构状态 === '未登记'" class="error-text">未登记（下游无法确认字段来源）</span>
            <span v-else>{{ item.结构状态 }}</span>
          </td>
          <td>{{ item.已确认字段 || '—' }}</td>
          <td>{{ item.待补登记字段 || '—' }}</td>
        </tr>
        <tr v-if="!transformSync.length">
          <td colspan="8" class="empty-state">暂无转换节点输出数据集，执行转换节点后在此核对结构注册</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记结构定义</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条结构注册记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/schema_registry'
const columns = ["定义编号", "表名称", "字段名称", "字段类型", "可空标识", "默认值", "定义版本", "生效批次"]
const actions = ["提交校验", "标记生效", "标记废弃"]
const statuses = ["草稿", "待校验", "已生效", "已废弃"]
const metricNames = ["结构定义总数", "待校验定义", "已生效定义"]

const rows = ref<Row[]>([])
const total = ref(0)
const transformSync = ref<Record<string, any>[]>([])
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'schema_registry.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('结构注册动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结构注册操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('结构定义列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    const syncResponse = await request(`${ENDPOINT}/transform-sync`)
    const syncPayload = await syncResponse.json()
    transformSync.value = syncPayload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结构注册列表读取失败'
  }
}

onMounted(reload)
</script>
