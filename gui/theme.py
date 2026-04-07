"""
PIK EVA GUI — Liquid Glass Theme.

Dual-mode (dark/light) CSS с настоящим glassmorphism, анимациями,
полноценной светлой темой и Apple-style liquid glass эффектами.
"""
from nicegui import ui

GLASS_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ══════════════════════════════════════════════
   DARK THEME
   ══════════════════════════════════════════════ */
body.body--dark {
  --bg-deep:        #0B1120;
  --bg-gradient:    linear-gradient(135deg, #0B1120 0%, #162036 40%, #1a1a3e 70%, #0B1120 100%);
  --glass-surface:  rgba(30, 41, 59, 0.55);
  --glass-border:   rgba(148, 163, 184, 0.12);
  --glass-shine:    rgba(255, 255, 255, 0.04);
  --glass-blur:     16px;
  --primary:        #60A5FA;
  --primary-muted:  rgba(96, 165, 250, 0.15);
  --success:        #34D399;
  --error:          #F87171;
  --warning:        #FBBF24;
  --text-primary:   #F1F5F9;
  --text-secondary: #94A3B8;
  --text-muted:     #64748B;
  --radius:         16px;
  --radius-sm:      10px;
  --shadow:         0 8px 32px rgba(0, 0, 0, 0.35);
  --shadow-hover:   0 12px 40px rgba(96, 165, 250, 0.12);
  --transition:     all 0.3s cubic-bezier(0.4, 0, 0.2, 1);

  --sidebar-bg:     rgba(11, 17, 32, 0.85);
  --sidebar-border: rgba(100, 116, 139, 0.15);
  --input-bg:       rgba(30, 41, 59, 0.6);
  --separator:      rgba(148, 163, 184, 0.1);

  --q-dark-page: #0B1120 !important;
}

/* ══════════════════════════════════════════════
   LIGHT THEME
   ══════════════════════════════════════════════ */
body.body--light {
  --bg-deep:        #F8FAFC;
  --bg-gradient:    linear-gradient(135deg, #F8FAFC 0%, #EFF6FF 40%, #F0F4FF 70%, #F8FAFC 100%);
  --glass-surface:  rgba(255, 255, 255, 0.65);
  --glass-border:   rgba(148, 163, 184, 0.22);
  --glass-shine:    rgba(255, 255, 255, 0.45);
  --glass-blur:     16px;
  --primary:        #3B82F6;
  --primary-muted:  rgba(59, 130, 246, 0.10);
  --success:        #16A34A;
  --error:          #DC2626;
  --warning:        #CA8A04;
  --text-primary:   #0F172A;
  --text-secondary: #475569;
  --text-muted:     #94A3B8;
  --radius:         16px;
  --radius-sm:      10px;
  --shadow:         0 2px 12px rgba(0, 0, 0, 0.04);
  --shadow-hover:   0 6px 24px rgba(59, 130, 246, 0.08);
  --transition:     all 0.3s cubic-bezier(0.4, 0, 0.2, 1);

  --sidebar-bg:     rgba(255, 255, 255, 0.8);
  --sidebar-border: rgba(148, 163, 184, 0.18);
  --input-bg:       rgba(255, 255, 255, 0.8);
  --separator:      rgba(148, 163, 184, 0.15);
}

/* Spinner for loading state */
@keyframes spin {
  to { transform: rotate(360deg); }
}
.loading-spinner {
  width: 20px; height: 20px;
  border: 2px solid var(--glass-border);
  border-top-color: var(--primary);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  display: inline-block;
}

/* ══════════════════════════════════════════════
   BASE STYLES
   ══════════════════════════════════════════════ */
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
  -webkit-font-smoothing: antialiased;
  background: var(--bg-gradient) !important;
  background-attachment: fixed !important;
  color: var(--text-primary);
}

/* ══════════════════════════════════════════════
   GLASS CARD — Apple-style liquid glass
   ══════════════════════════════════════════════ */
.glass-card {
  position: relative;
  background: var(--glass-surface) !important;
  backdrop-filter: blur(var(--glass-blur)) saturate(180%);
  -webkit-backdrop-filter: blur(var(--glass-blur)) saturate(180%);
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius) !important;
  box-shadow: var(--shadow) !important;
  transition: var(--transition) !important;
  overflow: hidden;
}
/* Внутренний shine слой */
.glass-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 50%;
  background: linear-gradient(
    180deg,
    var(--glass-shine) 0%,
    transparent 100%
  );
  border-radius: var(--radius) var(--radius) 0 0;
  pointer-events: none;
  z-index: 0;
}
.glass-card > * {
  position: relative;
  z-index: 1;
}
.glass-card:hover {
  border-color: rgba(96, 165, 250, 0.3) !important;
  box-shadow: var(--shadow-hover) !important;
  transform: translateY(-1px);
}

/* ══════════════════════════════════════════════
   TYPOGRAPHY
   ══════════════════════════════════════════════ */
.text-heading {
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: var(--text-primary);
}
.text-body {
  font-size: 14px;
  line-height: 1.6;
  color: var(--text-primary);
}
.text-caption {
  font-size: 12px;
  color: var(--text-secondary);
}
.font-mono {
  font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
}

/* ══════════════════════════════════════════════
   ANIMATIONS
   ══════════════════════════════════════════════ */
@keyframes fadeSlideUp {
  from { opacity: 0; transform: translateY(12px); }
  to   { opacity: 1; transform: translateY(0); }
}
.animate-in {
  animation: fadeSlideUp 0.4s ease-out both;
}
.animate-in-1 { animation-delay: 50ms; }
.animate-in-2 { animation-delay: 100ms; }
.animate-in-3 { animation-delay: 150ms; }
.animate-in-4 { animation-delay: 200ms; }
.animate-in-5 { animation-delay: 250ms; }

@keyframes pulse-glow {
  0%, 100% { box-shadow: 0 0 0 0 rgba(96, 165, 250, 0.4); }
  50%      { box-shadow: 0 0 24px 6px rgba(96, 165, 250, 0.2); }
}
.running {
  animation: pulse-glow 2s ease-in-out infinite;
}

.card-disabled {
  opacity: 0.35;
  pointer-events: none;
}

/* ══════════════════════════════════════════════
   SIDEBAR
   ══════════════════════════════════════════════ */
.q-drawer {
  background: var(--sidebar-bg) !important;
  backdrop-filter: blur(20px) saturate(180%) !important;
  -webkit-backdrop-filter: blur(20px) saturate(180%) !important;
  border-right: 1px solid var(--sidebar-border) !important;
}

.sidebar-btn {
  width: 100% !important;
  justify-content: flex-start !important;
  text-align: left !important;
  color: var(--text-secondary) !important;
  border-radius: var(--radius-sm) !important;
  padding: 10px 16px !important;
  font-size: 14px !important;
  font-weight: 500 !important;
  text-transform: none !important;
  transition: var(--transition) !important;
  min-height: 42px !important;
}
.sidebar-btn:hover {
  background: var(--primary-muted) !important;
  color: var(--primary) !important;
}
.sidebar-btn-active {
  background: var(--primary-muted) !important;
  color: var(--primary) !important;
  font-weight: 600 !important;
}

/* ══════════════════════════════════════════════
   QUASAR OVERRIDES — Expansion panels (glass)
   ══════════════════════════════════════════════ */
.q-expansion-item {
  border-radius: var(--radius) !important;
  margin-bottom: 8px;
  overflow: hidden !important;
}

.q-expansion-item > .q-expansion-item__container > .q-item {
  background: var(--glass-surface) !important;
  backdrop-filter: blur(12px) saturate(180%);
  -webkit-backdrop-filter: blur(12px) saturate(180%);
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius) !important;
  color: var(--text-primary) !important;
  transition: var(--transition);
  min-height: 48px;
}
.q-expansion-item > .q-expansion-item__container > .q-item:hover {
  border-color: rgba(96, 165, 250, 0.25) !important;
}

/* Когда раскрыт — убрать нижние скругления у заголовка */
.q-expansion-item--expanded > .q-expansion-item__container > .q-item {
  border-bottom-left-radius: 0 !important;
  border-bottom-right-radius: 0 !important;
  border-bottom-color: transparent !important;
}

/* Контент expansion — тоже glass */
.q-expansion-item__content {
  background: var(--glass-surface) !important;
  backdrop-filter: blur(12px) saturate(180%);
  -webkit-backdrop-filter: blur(12px) saturate(180%);
  border: 1px solid var(--glass-border) !important;
  border-top: none !important;
  border-bottom-left-radius: var(--radius) !important;
  border-bottom-right-radius: var(--radius) !important;
  color: var(--text-primary) !important;
  padding: 16px !important;
}

.q-expansion-item .q-icon {
  color: var(--text-secondary) !important;
}

/* ══════════════════════════════════════════════
   QUASAR OVERRIDES — Inputs, tabs, etc.
   ══════════════════════════════════════════════ */
.q-page-container {
  background: transparent !important;
}
.q-field__control {
  background: var(--input-bg) !important;
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius-sm) !important;
  color: var(--text-primary) !important;
}
.q-field__native, .q-field__input {
  color: var(--text-primary) !important;
}
.q-field__label {
  color: var(--text-muted) !important;
}
.q-checkbox__inner {
  color: var(--text-secondary) !important;
}
.q-checkbox__inner--truthy {
  color: var(--primary) !important;
}
.q-toggle__inner {
  color: var(--text-secondary) !important;
}
.q-toggle__inner--truthy {
  color: var(--primary) !important;
}
.q-tabs__content {
  border-bottom: 1px solid var(--separator) !important;
}
.q-tab {
  color: var(--text-secondary) !important;
  text-transform: none !important;
}
.q-tab--active {
  color: var(--primary) !important;
}
.q-tab-panels {
  background: transparent !important;
}
.q-tab-panel {
  padding: 16px 0 !important;
}
.q-separator {
  background: var(--separator) !important;
}
.q-linear-progress {
  border-radius: 4px !important;
  overflow: hidden;
}
.q-linear-progress__track {
  background: var(--glass-border) !important;
}
.q-select__dropdown-icon {
  color: var(--text-muted) !important;
}
.q-menu {
  background: var(--glass-surface) !important;
  backdrop-filter: blur(16px) saturate(180%) !important;
  -webkit-backdrop-filter: blur(16px) saturate(180%) !important;
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius-sm) !important;
}
.q-item--active {
  color: var(--primary) !important;
  background: var(--primary-muted) !important;
}
.q-notification {
  background: var(--glass-surface) !important;
  backdrop-filter: blur(16px) saturate(180%) !important;
  color: var(--text-primary) !important;
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius-sm) !important;
}

/* Quasar card defaults → transparent */
.q-card {
  background: transparent !important;
  box-shadow: none !important;
}

/* Table */
.q-table {
  background: transparent !important;
  color: var(--text-primary) !important;
}
.q-table__container {
  border-radius: var(--radius) !important;
  overflow: hidden;
  border: 1px solid var(--glass-border) !important;
  background: var(--glass-surface) !important;
  backdrop-filter: blur(12px) saturate(180%);
  -webkit-backdrop-filter: blur(12px) saturate(180%);
}
.q-table thead th {
  color: var(--text-secondary) !important;
  border-bottom: 1px solid var(--separator) !important;
  font-weight: 600;
  background: transparent !important;
}
.q-table tbody td {
  color: var(--text-primary) !important;
  border-bottom: 1px solid var(--separator) !important;
}
.q-table tbody tr {
  background: transparent !important;
}
.q-table tbody tr:hover td {
  background: var(--primary-muted) !important;
}
.q-table--dark .q-table__bottom,
.q-table .q-table__bottom {
  border-top: 1px solid var(--separator) !important;
}

/* Log */
.nicegui-log {
  background: var(--glass-surface) !important;
  backdrop-filter: blur(12px) saturate(180%) !important;
  -webkit-backdrop-filter: blur(12px) saturate(180%) !important;
  color: var(--text-secondary) !important;
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius) !important;
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 13px !important;
}

/* Code blocks */
.nicegui-code, .q-code, pre, code {
  background: var(--glass-surface) !important;
  border: 1px solid var(--glass-border) !important;
  border-radius: var(--radius-sm) !important;
  color: var(--text-primary) !important;
}

/* Freshness indicators */
.fresh-green { color: var(--success); }
.fresh-yellow { color: var(--warning); }
.fresh-red { color: var(--error); }

/* ══════════════════════════════════════════════
   SCROLLBAR
   ══════════════════════════════════════════════ */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--glass-border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
"""


def inject_theme():
    """Inject CSS, fonts, and color palette."""
    ui.add_head_html(f'<style>{GLASS_CSS}</style>')
    ui.colors(
        primary='#60A5FA',
        secondary='#818CF8',
        accent='#34D399',
        positive='#34D399',
        negative='#F87171',
        warning='#FBBF24',
        info='#60A5FA',
    )
