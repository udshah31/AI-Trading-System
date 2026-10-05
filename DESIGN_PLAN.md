# Dashboard design: "The Auditor's Ledger"

**Who uses it:** one person running a personal crypto bot on a free VM, checking it over an SSH tunnel, sometimes late at night, sometimes on a phone.
**Its job:** answer at a glance whether it's trading real money or simulating, what it just decided and why, and whether anything is near a safety limit. Then: the ledger of positions and trades, and auditing how the LLM reports were read.

The bot keeps the books; you audit them. Visual choices come from bookkeeping, not from trading-terminal clichés.

## Colour (meaning only, never decoration)

| Token | Light | Dark | Used for |
|---|---|---|---|
| paper | `#EDF0E4` ledger green | `#161B17` | background |
| rule | `#BCC7AE` | `#33402F` | table column rules, totals rules |
| ink | `#262B28` graphite | `#DCE3D3` | text, profit ("in the black"), the buy side |
| ink-2 | `#60685C` | `#8E9A88` | secondary text, including shaded table rows |
| red ink | `#A8261B` | `#F07365` | losses (in parentheses), sells, breached limits, live-money stamp |
| buy signal | `#166046` | `#96D5B5` | buy badge and upward arrow |
| wait signal | `#765113` | `#E3C472` | wait badge and pause mark |
| blue pencil | `#2250B8` | `#8DA9FF` | navigation, details, audit controls, and keyboard focus |
| highlighter | `#F4DF5A` | `#5E5215` | only "look at this": a limit near its cap, unlabelled work, neutral fallbacks |

## Type

Atkinson Hyperlegible Next for everything (400/500/700): its glyphs keep 0/O and 1/l/I apart, which matters when 0.05 vs 0.06 BTC is real money. Atkinson Hyperlegible Mono only for keyboard hints and IDs. Scale: 12 / 14 / 16 / 21 / 28. Sentence case throughout; no all-caps labels, no eyebrows.

## Layout

```
Trading ledger                                   Live feed connected
Paper trading · no real money                 [Paper mode]
Position snapshot receipt time
Overview  |  Stock research  |  Advanced tools  |  Activity

Overview
  Crypto decisions: Coin | Decision | Why
  ▸ Advanced details: scores and chart
  Account balance + sparkline           │ Safety limits
  What you own                          │
  Recent trades                         │

Stock research
  Browse a category
  Stock / company | Decision | Last close | Research date | Details
  ▸ Advanced details: scores, ranking and sources

Advanced tools
  Agents | Strategies | New-token watch
  Audit the LLM readings and label accuracy
  Indicator weights and proposal evidence

Activity
  Activity log
```

Left-aligned text; numbers right-aligned in tabular figures. Simple tables are the default;
charts, scores and provenance use native expandable details. On phones the four navigation
buttons form a visible two-by-two grid. Wide tables scroll inside their own focusable box.
The optional score chart redraws when opened so it is measured at its visible width.

## Principles

1. **Decisions first, details on request**: show Buy signal, Sell signal, Wait, or Waiting for data in compact tables. Use the reported analysis action, not a new decision calculated from today's thresholds. Unknown actions stay visibly unknown.
2. **Bookkeeping conventions carry meaning**: red ink and parentheses for losses; a single rule above a total and a double rule under it.
3. **Real money is loud, simulation is quiet**: a red "Live: real money" stamp is the only saturated block on the page.
4. **Recognizable states**: text, arrow/pause shapes and color work together. Green means a buy signal, red a sell signal, amber wait, and a neutral badge missing/unknown data. Signals are analysis; filled orders appear in Recent trades and stocks remain research-only.
5. **Motion only answers change**: a coin's mark slides when its score updates; nothing animates on load. Reduced motion is respected.
6. **Untrusted text is escaped**: LLM reports and API strings are always HTML-escaped.
7. **Honest freshness and mode**: start with mode unknown, distinguish received snapshots from source update times, and preserve refresh-failure warnings until a successful fetch.
