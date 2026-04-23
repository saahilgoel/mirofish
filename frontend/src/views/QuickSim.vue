<template>
  <div class="quicksim-page">
    <nav class="qs-nav">
      <router-link to="/" class="qs-brand">MIROFISH</router-link>
      <span class="qs-tagline">QuickSim / one question → full simulation</span>
    </nav>

    <main class="qs-main">
      <section class="qs-card">
        <header class="qs-header">
          <span class="qs-kicker">>_ QuickSim</span>
          <h1 class="qs-title">Ask the future a question.</h1>
          <p class="qs-sub">
            We'll generate the seed context, the simulation prompt, build the
            knowledge graph, spawn agents, and prep a simulation — no forms.
          </p>
        </header>

        <div v-if="phase === 'idle'" class="qs-input-block">
          <label class="qs-label" for="qs-question">Your question</label>
          <textarea
            id="qs-question"
            v-model="question"
            class="qs-textarea"
            rows="8"
            placeholder="e.g. In 3-5 years, which AI-driven products will 15M Indian lifestyle MSME owners actually adopt, and what should Shiprocket triple down on to win this market?"
            :disabled="loading"
          ></textarea>

          <label class="qs-label qs-label-small" for="qs-projname">Project name (optional)</label>
          <input
            id="qs-projname"
            v-model="projectName"
            class="qs-input"
            placeholder="QuickSim"
            :disabled="loading"
          />

          <div class="qs-actions">
            <button
              class="qs-submit"
              :disabled="!canSubmit || loading"
              @click="launch"
            >
              <span v-if="!loading">Launch QuickSim</span>
              <span v-else>Launching...</span>
              <span class="qs-arrow">→</span>
            </button>
            <div v-if="error" class="qs-error">{{ error }}</div>
          </div>
        </div>

        <div v-else class="qs-progress-block">
          <div class="qs-progress-head">
            <span class="qs-phase-label">{{ phaseLabel }}</span>
            <span class="qs-elapsed">elapsed {{ elapsedSec }}s</span>
          </div>
          <div class="qs-progress-bar">
            <div class="qs-progress-fill" :style="{ width: progressPct + '%' }"></div>
          </div>
          <div class="qs-progress-row">
            <span class="qs-progress-pct">{{ progressPct }}%</span>
            <span class="qs-progress-msg">{{ message }}</span>
          </div>
          <div v-if="secondsSinceTick > 5" class="qs-heartbeat">
            still working — last tick {{ secondsSinceTick }}s ago
          </div>
          <div v-if="phase === 'failed'" class="qs-error">
            {{ error || 'QuickSim failed. Check backend logs.' }}
            <button class="qs-retry" @click="reset">Try again</button>
          </div>
          <div v-if="phase === 'done' && result" class="qs-result">
            <div class="qs-result-row">
              <span class="qs-result-label">simulation</span>
              <code>{{ result.simulation_id }}</code>
            </div>
            <div class="qs-result-row">
              <span class="qs-result-label">project</span>
              <code>{{ result.project_id }}</code>
            </div>
            <div class="qs-result-actions">
              <router-link class="qs-submit" :to="`/simulation/${result.simulation_id}`">
                Open Simulation <span class="qs-arrow">→</span>
              </router-link>
            </div>
          </div>
        </div>
      </section>
    </main>
  </div>
</template>

<script setup>
import { ref, computed, onBeforeUnmount } from 'vue'
import { startQuickSim, getTaskStatus } from '../api/graph'

const question = ref('')
const projectName = ref('')
const loading = ref(false)
const error = ref('')

// phase: idle | running | done | failed
const phase = ref('idle')
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
  message.value = 'Submitting...'
  startedAt.value = Date.now()
  lastTickAt.value = Date.now()

  // Start the elapsed/heartbeat ticker.
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
    clearInterval(tickTimer)
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
      // Network hiccup — keep polling; surface if it persists.
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
.quicksim-page {
  min-height: 100vh;
  background: #0b0b0e;
  color: #eaeaea;
  font-family: 'Inter', 'Space Grotesk', system-ui, sans-serif;
  display: flex;
  flex-direction: column;
}

.qs-nav {
  display: flex;
  align-items: baseline;
  gap: 16px;
  padding: 20px 32px;
  border-bottom: 1px solid #1c1c22;
}
.qs-brand {
  font-family: 'JetBrains Mono', monospace;
  letter-spacing: 0.15em;
  font-weight: 700;
  color: #f5f5f5;
  text-decoration: none;
}
.qs-tagline {
  font-family: 'JetBrains Mono', monospace;
  color: #6e6e78;
  font-size: 13px;
}

.qs-main {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
}

.qs-card {
  width: min(720px, 100%);
  background: #111116;
  border: 1px solid #1f1f27;
  border-radius: 14px;
  padding: 40px 40px 32px;
  box-shadow: 0 10px 40px rgba(0, 0, 0, 0.4);
}

.qs-kicker {
  display: inline-block;
  font-family: 'JetBrains Mono', monospace;
  color: #ff8a3d;
  font-size: 12px;
  letter-spacing: 0.15em;
  margin-bottom: 12px;
}

.qs-title {
  font-size: 34px;
  font-weight: 600;
  letter-spacing: -0.01em;
  margin: 0 0 12px;
}

.qs-sub {
  color: #9a9aa5;
  font-size: 15px;
  line-height: 1.55;
  margin: 0 0 28px;
  max-width: 560px;
}

.qs-label {
  display: block;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: #7a7a85;
  letter-spacing: 0.08em;
  margin: 20px 0 8px;
  text-transform: uppercase;
}
.qs-label-small { font-size: 11px; }

.qs-textarea, .qs-input {
  width: 100%;
  background: #0b0b0e;
  color: #f0f0f2;
  border: 1px solid #23232c;
  border-radius: 8px;
  padding: 14px 16px;
  font-size: 15px;
  line-height: 1.5;
  font-family: inherit;
  outline: none;
  resize: vertical;
  transition: border-color 0.15s;
  box-sizing: border-box;
}
.qs-textarea:focus, .qs-input:focus { border-color: #ff8a3d; }
.qs-textarea:disabled, .qs-input:disabled { opacity: 0.6; cursor: not-allowed; }
.qs-input { min-height: 44px; }

.qs-actions {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-top: 28px;
}

.qs-submit {
  background: #ff8a3d;
  color: #0b0b0e;
  border: none;
  padding: 14px 22px;
  font-weight: 600;
  font-size: 14px;
  border-radius: 8px;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 10px;
  text-decoration: none;
  transition: transform 0.1s, background 0.15s;
}
.qs-submit:hover:not(:disabled) { background: #ff9a55; transform: translateY(-1px); }
.qs-submit:disabled { opacity: 0.45; cursor: not-allowed; }

.qs-arrow { font-family: 'JetBrains Mono', monospace; }

.qs-error {
  color: #ff6b6b;
  font-size: 13px;
  display: flex;
  align-items: center;
  gap: 10px;
}

.qs-retry {
  background: transparent;
  color: #ff8a3d;
  border: 1px solid #ff8a3d;
  border-radius: 6px;
  padding: 4px 10px;
  cursor: pointer;
  font-size: 12px;
}

.qs-progress-block { padding-top: 12px; }
.qs-progress-head {
  display: flex;
  justify-content: space-between;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: #9a9aa5;
  margin-bottom: 10px;
}
.qs-elapsed { color: #6e6e78; }

.qs-progress-bar {
  height: 6px;
  background: #1a1a22;
  border-radius: 4px;
  overflow: hidden;
}
.qs-progress-fill {
  height: 100%;
  background: linear-gradient(90deg, #ff8a3d, #ff6b6b);
  transition: width 0.3s ease;
}

.qs-progress-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-top: 12px;
  font-size: 13px;
  color: #c7c7cf;
  gap: 16px;
}
.qs-progress-pct {
  font-family: 'JetBrains Mono', monospace;
  color: #ff8a3d;
  font-weight: 600;
}
.qs-progress-msg { flex: 1; text-align: right; color: #9a9aa5; }

.qs-heartbeat {
  margin-top: 10px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: #6e6e78;
}

.qs-result {
  margin-top: 24px;
  padding-top: 20px;
  border-top: 1px solid #1f1f27;
}
.qs-result-row {
  display: flex;
  gap: 12px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 13px;
  margin: 6px 0;
}
.qs-result-label { color: #6e6e78; min-width: 110px; }
.qs-result-row code { color: #eaeaea; }
.qs-result-actions { margin-top: 20px; }
</style>
