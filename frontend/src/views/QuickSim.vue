<template>
  <div class="qs-container">
    <!-- Top Navigation Bar: matches Home -->
    <nav class="navbar">
      <router-link to="/" class="nav-brand">MIROFISH</router-link>
      <div class="nav-links">
        <router-link to="/" class="github-link">
          ← Home
        </router-link>
      </div>
    </nav>

    <div class="main-content">
      <!-- Hero -->
      <section class="hero-section">
        <div class="hero-left">
          <div class="tag-row">
            <span class="orange-tag">QuickSim</span>
            <span class="version-text">/ one question → full simulation</span>
          </div>

          <h1 class="main-title" v-if="phase === 'idle'">
            Ask the future<br>
            <span class="gradient-text">a question.</span>
          </h1>
          <h1 class="main-title" v-else-if="phase === 'done'">
            <span class="gradient-text">Ready.</span>
          </h1>
          <h1 class="main-title" v-else-if="phase === 'failed'">
            <span class="gradient-text">Something broke.</span>
          </h1>
          <h1 class="main-title" v-else>
            <span class="gradient-text">Running…</span>
          </h1>

          <div class="hero-desc">
            <p v-if="phase === 'idle'">
              We'll generate the <span class="highlight-bold">seed context</span>,
              the <span class="highlight-bold">simulation prompt</span>,
              build the <span class="highlight-bold">knowledge graph</span>,
              spawn <span class="highlight-orange">agents</span>, and prep a
              simulation — <span class="highlight-code">no forms, no uploads</span>.
            </p>
            <p v-else-if="phase === 'running'">
              Simulation pipeline is streaming.
              Progress updates every 2s — if the tick counter grows, it's still
              working<span class="blinking-cursor">_</span>
            </p>
            <p v-else-if="phase === 'done'">
              Seed generated, graph built, agents spawned, simulation prepared.
              Jump to the simulation view to run it.
            </p>
            <p v-else>
              The pipeline failed mid-run. Error below — retry or tweak the
              question.
            </p>
          </div>

          <div class="decoration-square"></div>
        </div>
      </section>

      <!-- Console (single column; the right panel of Home becomes the center here) -->
      <section class="console-wrap">
        <div class="console-box">
          <!-- IDLE: input form -->
          <template v-if="phase === 'idle'">
            <div class="console-section">
              <div class="console-header">
                <span class="console-label">>_ 01 / Your Question</span>
                <span class="console-meta">plain english, 10-4000 chars</span>
              </div>
              <div class="input-wrapper">
                <textarea
                  v-model="question"
                  class="code-input"
                  rows="8"
                  placeholder="e.g. In 3-5 years, which AI-driven products will 15M Indian lifestyle MSME owners actually adopt, and what should Shiprocket triple down on to win this market?"
                  :disabled="loading"
                ></textarea>
                <div class="model-badge">Engine: MiroFish-V1.0</div>
              </div>
            </div>

            <div class="console-divider"><span>Parameters</span></div>

            <div class="console-section">
              <div class="console-header">
                <span class="console-label">02 / Project Name (optional)</span>
              </div>
              <div class="input-wrapper input-wrapper-slim">
                <input
                  v-model="projectName"
                  class="code-input code-input-slim"
                  placeholder="QuickSim"
                  :disabled="loading"
                />
              </div>
            </div>

            <div class="console-section btn-section">
              <button
                class="start-engine-btn"
                :disabled="!canSubmit || loading"
                @click="launch"
              >
                <span>{{ loading ? 'Launching…' : 'Launch QuickSim' }}</span>
                <span class="btn-arrow">→</span>
              </button>
              <div v-if="error" class="error-banner">{{ error }}</div>
            </div>
          </template>

          <!-- RUNNING / DONE / FAILED: progress + result panel -->
          <template v-else>
            <div class="console-section">
              <div class="console-header">
                <span class="console-label">>_ {{ phaseLabel }}</span>
                <span class="console-meta">elapsed {{ elapsedSec }}s</span>
              </div>

              <div class="progress-bar">
                <div class="progress-fill" :style="{ width: progressPct + '%' }"></div>
              </div>
              <div class="progress-row">
                <span class="progress-pct">{{ progressPct }}%</span>
                <span class="progress-msg">{{ message || '—' }}</span>
              </div>
              <div v-if="phase === 'running' && secondsSinceTick > 5" class="heartbeat">
                still working — last tick {{ secondsSinceTick }}s ago
              </div>
            </div>

            <div class="console-divider"><span>Output</span></div>

            <div class="console-section" v-if="phase === 'done' && result">
              <div class="result-row">
                <span class="result-label">simulation_id</span>
                <code>{{ result.simulation_id }}</code>
              </div>
              <div class="result-row">
                <span class="result-label">project_id</span>
                <code>{{ result.project_id }}</code>
              </div>
              <div class="result-row" v-if="result.graph_id">
                <span class="result-label">graph_id</span>
                <code>{{ result.graph_id }}</code>
              </div>
            </div>

            <div class="console-section" v-if="phase === 'failed'">
              <div class="error-banner">{{ error || 'Unknown failure' }}</div>
            </div>

            <div class="console-section btn-section">
              <router-link
                v-if="phase === 'done' && result"
                class="start-engine-btn"
                :to="`/simulation/${result.simulation_id}`"
              >
                <span>Open Simulation</span>
                <span class="btn-arrow">→</span>
              </router-link>
              <button
                v-else-if="phase === 'failed'"
                class="start-engine-btn"
                @click="reset"
              >
                <span>Try Again</span>
                <span class="btn-arrow">↻</span>
              </button>
              <button
                v-else
                class="start-engine-btn"
                :disabled="true"
              >
                <span>Running — please wait</span>
                <span class="btn-arrow">…</span>
              </button>
            </div>
          </template>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onBeforeUnmount, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { startQuickSim, getTaskStatus } from '../api/graph'

const route = useRoute()
const question = ref('')
const projectName = ref('')

onMounted(() => {
  // Home's inline CTA passes the question via ?q= to pre-fill here.
  const q = route.query?.q
  if (typeof q === 'string' && q.trim()) {
    question.value = q.trim()
  }
})
const loading = ref(false)
const error = ref('')

const phase = ref('idle') // idle | running | done | failed
const progressPct = ref(0)
const message = ref('')
const result = ref(null)

const startedAt = ref(0)
const elapsedSec = ref(0)
const lastTickAt = ref(0)
const secondsSinceTick = ref(0)
let pollTimer = null
let tickTimer = null

const canSubmit = computed(() => question.value.trim().length >= 10)

const phaseLabel = computed(() => {
  if (phase.value === 'done') return '✓ QuickSim ready'
  if (phase.value === 'failed') return '✗ QuickSim failed'
  return '⟳ QuickSim running'
})

async function launch() {
  if (!canSubmit.value || loading.value) return
  error.value = ''
  loading.value = true
  phase.value = 'running'
  progressPct.value = 1
  message.value = 'Submitting…'
  startedAt.value = Date.now()
  lastTickAt.value = Date.now()

  tickTimer = setInterval(() => {
    const now = Date.now()
    elapsedSec.value = Math.floor((now - startedAt.value) / 1000)
    secondsSinceTick.value = Math.floor((now - lastTickAt.value) / 1000)
  }, 500)

  try {
    const resp = await startQuickSim({
      question: question.value.trim(),
      project_name: projectName.value.trim() || undefined,
    })
    const data = resp?.data?.data || resp?.data || {}
    const taskId = data.task_id
    if (!taskId) throw new Error('Backend did not return a task_id')
    pollTask(taskId)
  } catch (e) {
    error.value = e?.response?.data?.error || e?.message || 'Request failed'
    phase.value = 'failed'
    loading.value = false
    stopTimers()
  }
}

function pollTask(taskId) {
  const tick = async () => {
    try {
      const resp = await getTaskStatus(taskId)
      const d = resp?.data?.data || {}
      if (typeof d.progress === 'number') progressPct.value = d.progress
      if (d.message) message.value = d.message
      lastTickAt.value = Date.now()

      const status = (d.status || '').toLowerCase()
      if (status === 'completed') {
        progressPct.value = 100
        phase.value = 'done'
        result.value = d.result || null
        loading.value = false
        stopTimers()
        return
      }
      if (status === 'failed') {
        phase.value = 'failed'
        error.value = d.message || d.error || 'QuickSim task failed'
        loading.value = false
        stopTimers()
        return
      }
    } catch (e) {
      console.warn('QuickSim poll failed:', e)
    }
  }
  pollTimer = setInterval(tick, 2000)
  tick()
}

function stopTimers() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
  if (tickTimer) { clearInterval(tickTimer); tickTimer = null }
}

function reset() {
  stopTimers()
  phase.value = 'idle'
  progressPct.value = 0
  message.value = ''
  result.value = null
  error.value = ''
  loading.value = false
  secondsSinceTick.value = 0
}

onBeforeUnmount(stopTimers)
</script>

<style scoped>
/* Match Home.vue palette exactly */
:root {
  --black: #000000;
  --white: #FFFFFF;
  --orange: #FF4500;
  --gray-text: #666666;
  --font-mono: 'JetBrains Mono', monospace;
  --font-sans: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
}

.qs-container {
  min-height: 100vh;
  background: #FFFFFF;
  font-family: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
  color: #000;
}

/* Navbar — same as Home */
.navbar {
  height: 60px;
  background: #000;
  color: #FFF;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 40px;
}
.nav-brand {
  font-family: 'JetBrains Mono', monospace;
  font-weight: 800;
  letter-spacing: 1px;
  font-size: 1.2rem;
  color: #FFF;
  text-decoration: none;
}
.nav-links { display: flex; align-items: center; }
.github-link {
  color: #FFF;
  text-decoration: none;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.9rem;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 8px;
  transition: opacity 0.2s;
}
.github-link:hover { opacity: 0.8; }

/* Main */
.main-content {
  max-width: 960px;
  margin: 0 auto;
  padding: 60px 40px;
}

/* Hero */
.hero-section {
  display: flex;
  margin-bottom: 40px;
}
.hero-left {
  flex: 1;
  padding-right: 40px;
}
.tag-row {
  display: flex;
  align-items: center;
  gap: 15px;
  margin-bottom: 25px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8rem;
}
.orange-tag {
  background: #FF4500;
  color: #FFF;
  padding: 4px 10px;
  font-weight: 700;
  letter-spacing: 1px;
  font-size: 0.75rem;
}
.version-text { color: #999; font-weight: 500; letter-spacing: 0.5px; }

.main-title {
  font-size: 4rem;
  line-height: 1.15;
  font-weight: 500;
  margin: 0 0 32px 0;
  letter-spacing: -2px;
  color: #000;
}
.gradient-text {
  background: linear-gradient(90deg, #000 0%, #444 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  display: inline-block;
}

.hero-desc {
  font-size: 1.05rem;
  line-height: 1.75;
  color: #666;
  max-width: 640px;
  font-weight: 400;
  margin-bottom: 24px;
}
.highlight-bold { color: #000; font-weight: 700; }
.highlight-orange { color: #FF4500; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
.highlight-code {
  background: rgba(0, 0, 0, 0.05);
  padding: 2px 6px;
  border-radius: 2px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.9em;
  color: #000;
  font-weight: 600;
}
.blinking-cursor {
  color: #FF4500;
  animation: blink 1s step-end infinite;
  font-weight: 700;
}
@keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
.decoration-square { width: 16px; height: 16px; background: #FF4500; }

/* Console box — matches Home */
.console-wrap { margin-top: 20px; }
.console-box {
  border: 1px solid #CCC;
  padding: 8px;
}
.console-section { padding: 20px; }
.console-section.btn-section { padding-top: 0; }
.console-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 15px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  color: #666;
}
.console-label { font-weight: 500; }
.console-meta { color: #999; }

.input-wrapper {
  position: relative;
  border: 1px solid #DDD;
  background: #FAFAFA;
}
.input-wrapper-slim { min-height: 54px; }
.code-input {
  width: 100%;
  box-sizing: border-box;
  border: none;
  background: transparent;
  padding: 20px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.9rem;
  line-height: 1.6;
  resize: vertical;
  outline: none;
  min-height: 150px;
  color: #000;
}
.code-input-slim { min-height: 44px; padding: 16px 20px; }
.model-badge {
  position: absolute;
  bottom: 10px;
  right: 15px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.7rem;
  color: #AAA;
}

.console-divider {
  display: flex;
  align-items: center;
  margin: 10px 0;
}
.console-divider::before,
.console-divider::after {
  content: '';
  flex: 1;
  height: 1px;
  background: #EEE;
}
.console-divider span {
  padding: 0 15px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.7rem;
  color: #BBB;
  letter-spacing: 1px;
}

/* Launch button — matches Home's start-engine-btn */
.start-engine-btn {
  width: 100%;
  box-sizing: border-box;
  background: #000;
  color: #FFF;
  border: 1px solid #000;
  padding: 20px;
  font-family: 'JetBrains Mono', monospace;
  font-weight: 700;
  font-size: 1.1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  cursor: pointer;
  transition: all 0.3s ease;
  letter-spacing: 1px;
  text-decoration: none;
}
.start-engine-btn:not(:disabled) {
  animation: pulse-border 2s infinite;
}
.start-engine-btn:hover:not(:disabled) {
  background: #FF4500;
  border-color: #FF4500;
  transform: translateY(-2px);
}
.start-engine-btn:active:not(:disabled) { transform: translateY(0); }
.start-engine-btn:disabled {
  background: #E5E5E5;
  color: #999;
  cursor: not-allowed;
  transform: none;
  border-color: #E5E5E5;
  animation: none;
}
@keyframes pulse-border {
  0%, 100% { box-shadow: 0 0 0 0 rgba(255, 69, 0, 0.25); }
  50% { box-shadow: 0 0 0 6px rgba(255, 69, 0, 0); }
}

/* Progress + result */
.progress-bar {
  height: 6px;
  background: #F0F0F0;
  overflow: hidden;
  margin-top: 10px;
}
.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, #000 0%, #FF4500 100%);
  transition: width 0.3s ease;
}
.progress-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 12px;
  gap: 16px;
  font-size: 0.9rem;
}
.progress-pct {
  font-family: 'JetBrains Mono', monospace;
  color: #FF4500;
  font-weight: 700;
}
.progress-msg { flex: 1; text-align: right; color: #666; }
.heartbeat {
  margin-top: 10px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  color: #999;
}

.result-row {
  display: flex;
  gap: 14px;
  align-items: center;
  margin: 8px 0;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.85rem;
}
.result-label { color: #999; min-width: 120px; }
.result-row code { color: #000; background: #F5F5F5; padding: 2px 6px; }

.error-banner {
  margin-top: 12px;
  padding: 12px 16px;
  background: #FFF5F2;
  border-left: 3px solid #FF4500;
  color: #CC2200;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8rem;
}
</style>
