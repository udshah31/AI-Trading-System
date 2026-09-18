# AI-Trading-System Dashboard — Design Plan

## Design Brief Summary
**Product:** AI-Trading-System — Hybrid LLM + Quant trading system with paper/live trading  
**Audience:** Quantitative traders, portfolio managers, risk analysts  
**Primary Job:** Real-time monitoring of multi-strategy trading system, risk oversight, trade execution oversight  
**Differentiator:** Hybrid LLM+Quant fusion, multi-strategy (BTC Funding, Sniper, RL, Quant), multi-exchange, mathematical risk guarantees

---

## Color Tokens (4 core + semantic)

| Name | Hex | Role |
|------|-----|------|
| `ink` | `#0A0E14` | Deep charcoal base — not pure black, has blue depth |
| `paper` | `#E8ECEF` | Off-white with cool tint — readable, not sterile |
| `muted` | `#6B7A8D` | Secondary text, borders, inactive states |
| `signal` | `#00D4AA` | **Single accent** — precise teal for live/positive/active (P&L green, connected status, buy signals) |
| `alert` | `#FF4D6A` | Danger/loss/sell/stop — only for losses, stops, errors |
| `warning` | `#F5A623` | Caution/warning — funding rate thresholds, margin warnings |

**No other colors.** No gradient washes. No soft shadows. No gradient text.

**Usage rules:**
- `signal` = only for live/positive/buy/connected
- `alert` = only for losses/stops/sells/errors
- `warning` = only for thresholds approaching limits
- `muted` = labels, inactive, secondary
- `ink`/`paper` = primary text/background

---

## Typography

**Single family: `IBM Plex Mono` + `IBM Plex Sans`** (same family, two cuts)

| Role | Font | Size/Weight | Line Height |
|------|------|-------------|-------------|
| Display / Hero metric | `IBM Plex Sans` | 72px / 600 | 1.05 |
| Headline / Section | `IBM Plex Sans` | 24px / 500 | 1.2 |
| Body / UI | `IBM Plex Sans` | 14px / 400 | 1.5 |
| Data / Numbers / Tables / Code | `IBM Plex Mono` | 13px / 400 | 1.6 |
| Labels / Meta | `IBM Plex Sans` | 11px / 500 | 1.4 (uppercase, 0.05em tracking) |

**No other weights.** No italics. No all-caps for headlines. Tracking only on labels (0.05em).

**Line length:** Max 75ch for body. Tables use `IBM Plex Mono` at 13px for alignment.

---

## Layout Concept

**Concept: "Trading Terminal Clarity" — Left-aligned, density with breathing room**

```
┌─────────────────────────────────────────────────────────────────────┐
│  HERO: Live P&L / Equity Curve (full width, 280px tall)             │
│  ┌──────────────────┬──────────────────┬──────────────────┐        │
│  │  EQUITY  $124,847 │ DAILY P&L +2,341 │  OPEN POS  3      │  <- 3 KPIs   │
│  │  +1.9%            │  +1.9%           │  $12,400 exp      │     (dense)  │
│  └──────────────────┴──────────────────┴──────────────────┘        │
├─────────────────────────────────────────────────────────────────────┤
│  EQUITY CURVE (full width, 200px) — live streaming, no legend      │
├─────────────────────┬──────────────────────────────────────────────┤
│  STRATEGIES         │  POSITIONS TABLE (dense, mono)               │
│  (vertical list)    │  ┌────┬──────┬─────┬──────┬──────┬──────┐  │
│  ● BTC Funding      │  │SYM │ SIDE │ QTY │ ENTRY│ MARK │ P&L  │  │
│    ACTIVE  |  +42bps│  │BTC │ LONG │ 0.5 │ 67,200│68,100│+450  │  │
│    10bps/8h | APR45%│  │ETH │ SHORT│ 2.0 │ 3,420 │3,380 │ +80  │  │
│    Basis 6.3bps     │  │SOL │ LONG │ 50  │  142  │ 145  │+150  │  │
│                     │  └────┴──────┴─────┴──────┴──────┴──────┘  │
│  ● Sniper Bot       │  RECENT TRADES (last 10, mono)            │
│    STANDBY          │  (same table density)                      │
│                     │                                            │
│  ● RL Agent         │  AGENT STATUS (compact list)              │
│    TRAINING         │  ● openagent      CONNECTED  2m ago       │
│                     │  ● contextscout   CONNECTED  1m ago       │
│                     │  ● risk_agent     CONNECTED  30s ago      │
└─────────────────────┴──────────────────────────────────────────────┘
```

**Alignment:** Left-aligned throughout. No centered content except hero metric.  
**Density:** Tables use 8px row padding, 12px column gaps. No card padding waste.  
**Scrolling:** Only the positions/trades/agents panes scroll. Hero + equity curve fixed.  
**Responsive:** < 900px → stack strategy list above tables, keep hero full width.

---

## Principles (What Makes This Unique)

1. **Data First, Chrome Last** — No decorative cards, no borders for decoration, no shadow decoration. Tables are the UI. The grid *is* the design.

2. **One Accent, Three States** — `signal` (teal) = live/positive/buy, `alert` (red) = loss/stop/sell, `warning` (amber) = threshold. No other colors carry meaning.

3. **Terminal Density, Modern Clarity** — Monospace tables with precise alignment (like Bloomberg/Reuters terminals), but clean `IBM Plex` type, generous line-height, no visual noise.

4. **Live First, Static Never** — Hero metric streams. Equity curve streams. Tables update in place. No "last updated" timestamps — if it's stale, it shows stale.

4. **Risk Visible, Not Hidden** — Drawdown, margin, position risk always visible in hero KPIs. No drilling down to find risk.

5. **No Decoration** — No rounded cards, no soft shadows, no gradient washes, no decorative borders. The data *is* the visual language.

6. **Left-Aligned, Mono-Spaced Data** — Numbers align on decimal. Columns align. Scanning is instant.

---

## Self-Critique Checklist (Pre-Build)

- [ ] No warm cream / terracotta default palette
- [ ] No near-black + acid green default
- [ ] No broadsheet newspaper columns
- [ ] No SaaS card kit (rounded cards, soft shadows, gradient washes)
- [ ] No ALL-CAPS eyebrow labels above headings
- [ ] No `→` appended to buttons/links
- [ ] No single-word accent in headlines
- [ ] No tinted near-black (#0B0B0B) standing in for black
- [ ] No monospace for small labels only — mono for *all* data
- [ ] No numbered markers (01/02/03) unless actual sequence
- [ ] No fade/slide-up on every section
- [ ] No hover transitions on every card
- [ ] Hero is live P&L/equity, not a big number with small label

---

## Build Notes (Technical)

- **CSS:** Custom properties for tokens. No framework utility classes.
- **Charts:** Chart.js but styled to match tokens — no default colors, no legends, minimal gridlines.
- **Tables:** `<table>` with `IBM Plex Mono`, `table-layout: fixed`, decimal alignment via `text-align: right` + `tabular-nums`.
- **WebSocket:** Hero + tables update in place via `data-*` attributes, no re-render flash.
- **Responsive:** CSS Grid for layout, media query at 900px stacks strategy column.
- **Accessibility:** `prefers-reduced-motion` respected, focus-visible outlines in `signal`, WCAG AA contrast.

---

## Copy Guidelines

- **No selling language.** "Live P&L" not "Your Profit Dashboard"
- **Active verbs.** "Close Position" not "Submit Close"
- **Plain terms.** "Open Positions" not "Current Exposure"
- **Errors direct.** "Order rejected: insufficient margin" not "Something went wrong"
- **Empty states directive.** "No open positions. Deploy a strategy to begin." not "No data"