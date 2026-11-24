<template>
  <section class="panel" data-testid="import-replay-panel">
    <header class="panel-head">
      <div>
        <h3>备份包导入与导入重放</h3>
        <p class="panel-desc">
          上传外部归档包后先暂存为待核验来源；按批次申请重放，确认时才逐分片取源并写入交付记录。
        </p>
      </div>
      <button class="btn ghost" type="button" data-testid="panel-refresh" @click="refresh">刷新来源</button>
    </header>

    <div class="panel-grid">
      <form class="panel-block" @submit.prevent="submitUpload">
        <h4>1. 上传离线备份包</h4>
        <label class="field">
          <span>压缩包文件</span>
          <input type="file" accept=".zip" data-testid="package-file" @change="onFileChange" />
        </label>
        <label class="field">
          <span>材料标签（可选）</span>
          <input v-model="uploadLabel" data-testid="package-label" placeholder="例如：2026-11 离线补包" />
        </label>
        <button class="btn primary" type="submit" data-testid="upload-package" :disabled="busy || !selectedFile">
          {{ busy ? '处理中…' : '上传并检查包内批次与分片' }}
        </button>
        <p v-if="uploadResult" class="hint" :class="{ 'error-text': !uploadResult.ok }" data-testid="upload-result">
          {{ uploadSummary }}
        </p>
      </form>

      <form class="panel-block" @submit.prevent="submitLocalVolume">
        <h4>2. 登记本地归档卷</h4>
        <label class="field">
          <span>归档编号</span>
          <input v-model="archiveCode" data-testid="local-volume-code" placeholder="例如：HIST-1780" />
        </label>
        <button class="btn" type="submit" data-testid="register-local-volume" :disabled="busy || !archiveCode">
          登记为可读来源
        </button>
        <p v-if="localResult" class="hint" :class="{ 'error-text': !localResult.ok }" data-testid="local-result">
          {{ localSummary }}
        </p>
      </form>
    </div>

    <form class="panel-block" @submit.prevent="loadSources">
      <h4>3. 核对批次来源</h4>
      <div class="inline-form">
        <label class="field">
          <span>批次编号</span>
          <input v-model="batchCode" data-testid="batch-code" placeholder="例如：BATCH-A1" />
        </label>
        <button class="btn" type="submit" data-testid="load-sources" :disabled="busy || !batchCode">
          查看候选来源
        </button>
        <button class="btn primary" type="button" data-testid="request-replay" :disabled="busy || !sources" @click="submitRequest">
          申请历史重放
        </button>
      </div>
      <p v-if="panelError" class="hint error-text" data-testid="panel-error">{{ panelError }}</p>
    </form>

    <div v-if="sources" class="panel-block" data-testid="sources-block">
      <h4>
        候选来源
        <span v-if="sources.has_conflict" class="tag warn" data-testid="conflict-tag">来源冲突</span>
        <span v-else class="tag ok">版本一致</span>
      </h4>
      <table class="data-table">
        <thead>
          <tr><th>材料编号</th><th>来源类型</th><th>核验状态</th><th>分片版本</th><th>标签</th></tr>
        </thead>
        <tbody>
          <tr v-for="material in sources.materials" :key="material.material_id">
            <td>{{ material.material_id }}</td>
            <td>{{ material.kind }}</td>
            <td>{{ material.status }}</td>
            <td>{{ (material.versions || []).join('、') || '未声明' }}</td>
            <td>{{ material.label || '—' }}</td>
          </tr>
          <tr v-if="!sources.materials.length">
            <td colspan="5" class="empty-state">该批次还没有可用来源</td>
          </tr>
        </tbody>
      </table>

      <table v-if="sources.preview_shards.length" class="data-table">
        <thead>
          <tr><th>分片</th><th>若确认将取自</th><th>版本</th><th>校验值</th><th>条数</th><th>是否存在冲突</th></tr>
        </thead>
        <tbody>
          <tr v-for="shard in sources.preview_shards" :key="shard.shard_index">
            <td>{{ shard.shard_index }}</td>
            <td>{{ shard.material_id }}</td>
            <td>{{ shard.version }}</td>
            <td class="mono">{{ shortChecksum(shard.checksum) }}</td>
            <td>{{ shard.record_count }}</td>
            <td>{{ shard.conflict ? '是' : '否' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="request" class="panel-block" data-testid="request-block">
      <h4>重放申请</h4>
      <p class="hint">
        重放编号 <strong>{{ request.replay_code }}</strong>，状态 {{ request.status }}。
        申请阶段不会读取或交付任何数据，暂存的离线包仍只是候选来源。
      </p>
      <button class="btn primary" type="button" data-testid="confirm-replay" :disabled="busy || !request.replay_id" @click="submitConfirm">
        确认重放（逐片取源并写交付记录）
      </button>
      <p v-if="confirmError" class="hint error-text" data-testid="confirm-error">{{ confirmError }}</p>
    </div>

    <div v-if="confirmation" class="panel-block" data-testid="confirmation-block">
      <h4>已确认的重放</h4>
      <p class="hint" data-testid="confirmation-summary">
        共重放 {{ confirmation.records_replayed }} 条记录，交付记录 {{ confirmation.delivery_code }}，
        血缘链路 {{ confirmation.lineage_code }}。
      </p>
      <table class="data-table">
        <thead>
          <tr><th>分片</th><th>实际来源</th><th>来源类型</th><th>版本</th><th>条数</th><th>校验值</th></tr>
        </thead>
        <tbody>
          <tr v-for="shard in confirmation.shard_provenance" :key="shard.shard_index" :data-testid="`provenance-${shard.shard_index}`">
            <td>{{ shard.shard_index }}</td>
            <td>{{ shard.material_id }}</td>
            <td>{{ shard.material_kind }}</td>
            <td>{{ shard.version }}</td>
            <td>{{ shard.record_count }}</td>
            <td class="mono">{{ shortChecksum(shard.checksum) }}</td>
          </tr>
        </tbody>
      </table>
      <h4>留待复核的差异</h4>
      <ul class="review-list" data-testid="pending-review">
        <li v-for="(item, index) in confirmation.pending_review" :key="index">
          <strong>{{ item.kind }}</strong>（分片 {{ item.shard_index }}）：{{ item.reason }}
        </li>
        <li v-if="!confirmation.pending_review?.length" class="hint">没有待复核差异。</li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  confirmBatchReplay,
  loadBatchSources,
  registerLocalVolume,
  requestBatchReplay,
  uploadPackage,
} from '@/api/backend-service'
import type {
  BatchSources,
  ImportResult,
  PendingReview,
  ReplayConfirmation,
  ReplayRequest,
} from '@/data/types'

const busy = ref(false)
const panelError = ref('')
const selectedFile = ref<File | null>(null)
const uploadLabel = ref('')
const archiveCode = ref('')
const batchCode = ref('')
const uploadResult = ref<ImportResult | null>(null)
const localResult = ref<ImportResult | null>(null)
const sources = ref<BatchSources | null>(null)
const request = ref<ReplayRequest | null>(null)
const confirmation = ref<ReplayConfirmation | null>(null)
const confirmError = ref('')

const uploadSummary = computed(() => {
  const result = uploadResult.value
  if (!result) return ''
  if (!result.ok) {
    return `已拒收：${result.error || '备份包不可用'}。${(result.errors || []).join('；')}`
  }
  if (result.duplicate) return `重复上传，复用原材料 ${result.material_id}`
  const batches = Object.keys(result.batches || {})
  return `已暂存材料 ${result.material_id}（待核验），识别批次：${batches.join('、') || '无'}`
})

const localSummary = computed(() => {
  const result = localResult.value
  if (!result) return ''
  if (!result.ok) return `登记失败：${result.error || '本地归档卷不可读'}`
  return `本地归档卷已登记为 ${result.material_id}，批次：${Object.keys(result.batches || {}).join('、')}`
})

function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  selectedFile.value = input.files?.[0] ?? null
}

function shortChecksum(value: string) {
  if (!value) return '—'
  const hex = value.includes(':') ? value.split(':')[1] : value
  return hex.slice(0, 12) + '…'
}

function pendingKinds(items?: PendingReview[]) {
  return (items || []).map((item) => item.kind)
}

async function submitUpload() {
  if (!selectedFile.value) return
  busy.value = true
  panelError.value = ''
  uploadResult.value = null
  try {
    uploadResult.value = await uploadPackage(selectedFile.value, { label: uploadLabel.value })
    if (batchCode.value) await loadSources()
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '上传失败'
  } finally {
    busy.value = false
  }
}

async function submitLocalVolume() {
  busy.value = true
  panelError.value = ''
  localResult.value = null
  try {
    localResult.value = await registerLocalVolume(archiveCode.value)
    if (batchCode.value) await loadSources()
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '本地归档卷登记失败'
  } finally {
    busy.value = false
  }
}

async function loadSources() {
  if (!batchCode.value) return
  busy.value = true
  panelError.value = ''
  request.value = null
  confirmation.value = null
  confirmError.value = ''
  try {
    sources.value = await loadBatchSources(batchCode.value)
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '来源读取失败'
  } finally {
    busy.value = false
  }
}

async function submitRequest() {
  busy.value = true
  panelError.value = ''
  confirmError.value = ''
  try {
    request.value = await requestBatchReplay(batchCode.value)
    if (!request.value.ok) panelError.value = request.value.error || '重放申请失败'
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '重放申请失败'
  } finally {
    busy.value = false
  }
}

async function submitConfirm() {
  if (!request.value?.replay_id) return
  busy.value = true
  confirmError.value = ''
  try {
    const result = await confirmBatchReplay(request.value.replay_id)
    if (!result.ok) {
      confirmError.value = result.error || '重放确认失败'
      return
    }
    confirmation.value = result
    await loadSources()
  } catch (error) {
    confirmError.value = error instanceof Error ? error.message : '重放确认失败'
  } finally {
    busy.value = false
  }
}

async function refresh() {
  if (batchCode.value) await loadSources()
}

onMounted(() => {
  // 面板默认留空，由使用人先上传或登记来源后再核对批次。
})
</script>

<style scoped>
.panel { margin-top: 20px; border-top: 1px solid var(--border); padding-top: 16px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; }
.panel-head h3 { margin: 0 0 4px; }
.panel-desc { color: var(--muted); font-size: 13px; margin: 0 0 12px; }
.panel-grid { display: flex; gap: 16px; flex-wrap: wrap; }
.panel-block { flex: 1; min-width: 280px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-bottom: 12px; }
.panel-block h4 { margin: 0 0 8px; font-size: 14px; }
.field { display: block; margin-bottom: 8px; }
.field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.field input { width: 100%; padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; }
.inline-form { display: flex; gap: 10px; align-items: flex-end; flex-wrap: wrap; }
.inline-form .field { margin-bottom: 0; min-width: 220px; }
.hint { font-size: 12px; color: var(--muted); margin: 8px 0 0; }
.mono { font-family: monospace; font-size: 11px; }
.tag { margin-left: 8px; padding: 1px 8px; border-radius: 10px; font-size: 11px; }
.tag.warn { background: #fef3c7; color: #92400e; }
.tag.ok { background: #dcfce7; color: #166534; }
.review-list { margin: 6px 0 0; padding-left: 18px; font-size: 12px; }
</style>
