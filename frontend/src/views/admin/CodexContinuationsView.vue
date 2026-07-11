<template>
  <AppLayout>
    <div class="space-y-6">
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-semibold text-gray-900 dark:text-white">Codex 续写日志</h1>
        <button type="button" class="btn btn-secondary px-2" title="刷新" :disabled="loading" @click="load">
          <Icon name="refresh" size="sm" :class="loading && 'animate-spin'" />
        </button>
      </div>

      <div class="card p-4">
        <div class="flex flex-wrap items-center gap-3">
          <DateRangePicker v-model:start-date="startDate" v-model:end-date="endDate" @change="search" />
          <div class="w-36"><Select v-model="status" :options="statusOptions" @change="search" /></div>
          <input v-model.trim="requestId" class="input w-64" placeholder="请求 ID" @keyup.enter="search" />
          <button type="button" class="btn btn-primary" @click="search">查询</button>
        </div>
      </div>

      <div class="card overflow-hidden">
        <div v-if="error" class="border-b border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-900/70 dark:bg-red-950/30 dark:text-red-300">{{ error }}</div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 text-sm dark:divide-dark-700">
            <thead class="bg-gray-50 text-left text-xs font-medium uppercase text-gray-500 dark:bg-dark-800 dark:text-gray-400"><tr><th class="px-4 py-3">状态</th><th class="px-4 py-3">请求 ID</th><th class="px-4 py-3">用户</th><th class="px-4 py-3">账号</th><th class="px-4 py-3">模型</th><th class="px-4 py-3">轮次</th><th class="px-4 py-3">原因</th><th class="px-4 py-3">时间</th></tr></thead>
            <tbody class="divide-y divide-gray-100 dark:divide-dark-700">
              <tr v-if="loading"><td colspan="8" class="px-4 py-10 text-center text-gray-500">加载中...</td></tr>
              <tr v-else-if="items.length === 0"><td colspan="8" class="px-4 py-10 text-center text-gray-500">暂无续写审计记录</td></tr>
              <tr v-for="item in items" :key="item.id" class="cursor-pointer hover:bg-gray-50 dark:hover:bg-dark-800/70" @click="selected = item"><td class="px-4 py-3"><span :class="statusClass(item.status)">{{ statusText(item.status) }}</span></td><td class="px-4 py-3 font-mono text-xs">{{ item.request_id }}</td><td class="px-4 py-3">{{ item.user_id }}</td><td class="px-4 py-3">{{ item.account_id }}</td><td class="px-4 py-3">{{ item.model }}</td><td class="px-4 py-3">{{ item.rounds.length }}</td><td class="px-4 py-3 text-gray-500 dark:text-gray-400">{{ item.reason || '-' }}</td><td class="px-4 py-3 whitespace-nowrap text-gray-500 dark:text-gray-400">{{ formatTime(item.created_at) }}</td></tr>
            </tbody>
          </table>
        </div>
        <Pagination v-if="total > 0" :page="page" :total="total" :page-size="pageSize" @update:page="goPage" @update:pageSize="updatePageSize" />
      </div>
    </div>
  </AppLayout>

  <div v-if="selected" class="fixed inset-0 z-50 grid place-items-center bg-black/40 p-4" @click.self="selected = null">
    <section class="w-full max-w-lg rounded-lg bg-white p-6 shadow-xl dark:bg-dark-800">
      <div class="flex items-start justify-between gap-4"><div><h2 class="text-lg font-semibold text-gray-900 dark:text-white">续写明细</h2><p class="mt-1 break-all font-mono text-xs text-gray-500">{{ selected.request_id }}</p></div><button type="button" class="btn btn-ghost px-2" title="关闭" @click="selected = null"><Icon name="x" size="sm" /></button></div>
      <ol class="mt-5 space-y-3"><li v-for="round in selected.rounds" :key="round.round" class="flex items-center justify-between border-b border-gray-100 pb-3 text-sm last:border-0 dark:border-dark-700"><span>第 {{ round.round }} 轮<span v-if="round.kind" class="ml-2 text-xs text-gray-400">({{ roundKindText(round.kind) }})</span><span v-if="round.winner" class="ml-2 rounded bg-amber-100 px-1.5 py-0.5 text-xs text-amber-800 dark:bg-amber-900/40 dark:text-amber-300">采用</span></span><span class="font-mono text-gray-600 dark:text-gray-300">{{ round.reasoning_tokens }} tokens</span><span class="text-gray-500">层级 {{ round.tier || '-' }}</span></li><li v-if="selected.rounds.length === 0" class="text-sm text-gray-500">未获得轮次数据</li></ol>
    </section>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import DateRangePicker from '@/components/common/DateRangePicker.vue'
import Pagination from '@/components/common/Pagination.vue'
import Select from '@/components/common/Select.vue'
import Icon from '@/components/icons/Icon.vue'
import { adminUsageAPI, type CodexContinuationLog } from '@/api/admin/usage'

const items = ref<CodexContinuationLog[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const loading = ref(false)
const error = ref('')
const status = ref<CodexContinuationLog['status'] | ''>('')
const requestId = ref('')
const startDate = ref('')
const endDate = ref('')
const selected = ref<CodexContinuationLog | null>(null)
const statusOptions = [
  { label: '全部状态', value: '' },
  { label: '续写成功', value: 'continued' },
  { label: '无需续写', value: 'not_needed' },
  { label: '续写失败', value: 'failed' }
]

async function load() {
  loading.value = true
  error.value = ''
  try {
    const data = await adminUsageAPI.listCodexContinuations({ page: page.value, page_size: pageSize.value, status: status.value || undefined, request_id: requestId.value || undefined, start_date: startDate.value || undefined, end_date: endDate.value || undefined, timezone: Intl.DateTimeFormat().resolvedOptions().timeZone })
    items.value = data.items
    total.value = data.total
  } catch {
    error.value = '续写日志加载失败'
  } finally {
    loading.value = false
  }
}

function search() { page.value = 1; void load() }
function goPage(value: number) { page.value = value; void load() }
function updatePageSize(value: number) { pageSize.value = value; page.value = 1; void load() }
function statusText(value: CodexContinuationLog['status']) { return ({ continued: '续写成功', not_needed: '无需续写', failed: '续写失败' })[value] }
function roundKindText(value: NonNullable<CodexContinuationLog['rounds'][number]['kind']>) { return ({ truncation_continue: '截断续写', low_reasoning_retry: '低推理重试' })[value] }
function statusClass(value: CodexContinuationLog['status']) { return ['inline-flex rounded-md px-2 py-1 text-xs font-medium', value === 'continued' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : value === 'not_needed' ? 'bg-sky-100 text-sky-800 dark:bg-sky-900/40 dark:text-sky-300' : 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'] }
function formatTime(value: string) { return value ? new Date(value).toLocaleString() : '-' }

onMounted(() => { void load() })
</script>
