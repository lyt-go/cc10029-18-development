import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  BatchSources,
  EntryRow,
  ImportResult,
  OverviewResult,
  PageResult,
  ReplayConfirmation,
  ReplayDelivery,
  ReplayRequest,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 上传备份包：直接把压缩包字节作为请求体，文件名走查询参数。 */
export async function uploadPackage(
  file: File,
  options: { label?: string; requestedBy?: string } = {},
): Promise<ImportResult> {
  const query = new URLSearchParams({ filename: file.name })
  if (options.label) query.set('label', options.label)
  if (options.requestedBy) query.set('requested_by', options.requestedBy)
  const response = await request(`/api/import-replay/packages?${query.toString()}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream' },
    body: await file.arrayBuffer(),
  })
  if (!response.ok) throw new Error('备份包上传失败')
  return (await response.json()) as ImportResult
}

/** 登记恢复可读的本地归档卷。 */
export async function registerLocalVolume(archiveCode: string): Promise<ImportResult> {
  const response = await request('/api/import-replay/local-volumes', {
    method: 'POST',
    body: JSON.stringify({ archive_code: archiveCode }),
  })
  if (!response.ok) throw new Error('本地归档卷登记失败')
  return (await response.json()) as ImportResult
}

/** 查看批次的候选来源、版本冲突与来源预览。 */
export async function loadBatchSources(batchCode: string): Promise<BatchSources> {
  const response = await request(`/api/import-replay/batches/${encodeURIComponent(batchCode)}/sources`)
  if (!response.ok) throw new Error('批次来源读取失败')
  return (await response.json()) as BatchSources
}

/** 按批次申请历史重放（只登记待重放任务，不对外交付）。 */
export async function requestBatchReplay(
  batchCode: string,
  requestedBy = '数据工程组',
): Promise<ReplayRequest> {
  const response = await request('/api/import-replay/replays', {
    method: 'POST',
    body: JSON.stringify({ batch_code: batchCode, requested_by: requestedBy }),
  })
  if (!response.ok) throw new Error('重放申请失败')
  return (await response.json()) as ReplayRequest
}

/** 确认重放：逐分片取源并写入交付记录。 */
export async function confirmBatchReplay(replayId: number): Promise<ReplayConfirmation> {
  const response = await request(`/api/import-replay/replays/${replayId}/confirm`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  if (!response.ok) throw new Error('重放确认失败')
  return (await response.json()) as ReplayConfirmation
}

/** 查看一次重放实际采用的来源与待复核差异。 */
export async function loadReplayDelivery(replayId: number): Promise<ReplayDelivery> {
  const response = await request(`/api/import-replay/replays/${replayId}/delivery`)
  if (!response.ok) throw new Error('重放交付明细读取失败')
  return (await response.json()) as ReplayDelivery
}
