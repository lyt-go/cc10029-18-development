<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">跨系统数据交换与批量传输平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向异构系统间批量数据交换的字段契约、增量窗口抽取、暂存落地、校验和对账、断点续传与到达核对平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { MODULES } from '@/data/modules'

const navItems = [
  { label: '运营概览', path: '/' },
  ...MODULES.map((item) => ({ label: item.name, path: `/${item.key}` })),
]
</script>
