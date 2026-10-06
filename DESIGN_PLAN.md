# Dashboard design: "Trading control room"

**Who uses it:** one person running a personal crypto bot on a free VM, checking it over an SSH tunnel, sometimes late at night, sometimes on a phone.
**Its job:** answer at a glance whether it's trading real money or simulating, what it just decided, what is at risk, and whether anything needs attention. Then: positions and trades, stock research, operations, and auditing how the LLM reports were read.

The bot keeps the books; you operate and audit them. Visual choices come from a quiet control room: dark surfaces for long sessions, a thin signal rail for state, and strong typography for decisions rather than trading-terminal decoration.

## Colour (meaning only, never decoration)

| Token | Light | Dark | Used for |
|---|---|---|---|
| paper | `#F3F6F3` | `#10191D` | primary surface |
| paper-2 | `#E7EFEC` | `#17262A` | panels, hover, and secondary surface |
| rule | `#C8D8D3` | `#2C4244` | table rules and panel edges |
| ink | `#152327` | `#EAF4EF` | primary text and verified values |
| ink-2 | `#5C716D` | `#94AAA4` | secondary text and timestamps |
| red | `#C84C4B` | `#FF8178` | losses, sells, breached limits, live-money state |
| buy signal | `#147A5B` | `#7EE0C1` | buy badge and healthy state |
| wait signal | `#936812` | `#F0C56A` | wait, stale, and attention state |
| blue | `#147C7C` | `#7DD7D2` | navigation, details, and keyboard focus |

## Type

Atkinson Hyperlegible Next for everything (400/500/700): its glyphs keep 0/O and 1/l/I apart, which matters when 0.05 vs 0.06 BTC is real money. Atkinson Hyperlegible Mono only for keyboard hints and IDs. Scale: 12 / 14 / 16 / 21 / 28. Sentence case throughout; no all-caps labels, no eyebrows.

## Layout

```
Trading control room                           Live feed connected
Paper trading · no real money                 [Paper mode]
Position snapshot receipt time · No limits breached
Overview  |  Stock research  |  Audit & learning  |  Operations

Overview
  Account at a glance: Equity | Today's P&L | Exposure | Drawdown
  What the system decided                  │ Safety limits
    ▸ View evidence: score and reported rationale
  ▸ Advanced details: scores and chart
  What you own                          │
  Recent trades                         │

Stock research
  Find a company or browse a category
  Stock / company | Decision | Last close | Research date | Details
  ▸ Advanced details: scores, ranking and sources

Audit & learning
  Agents | Strategies | New-token watch
  Audit the LLM readings and label accuracy
  Indicator weights and proposal evidence

Operations
  Activity log
```

Left-aligned text; numbers right-aligned in tabular figures. Summary panels answer normal-user questions first;
simple tables remain the default for detail;
charts, scores and provenance use native expandable details. On phones the four navigation
buttons form a visible two-by-two grid. Wide tables scroll inside their own focusable box.
The optional score chart redraws when opened so it is measured at its visible width.

## Principles

1. **Trust before detail**: mode, feed state, freshness, and risk verification appear before decisions and tables.
2. **Decisions first, details on request**: show Buy signal, Sell signal, Wait, or Waiting for data in compact tables. Use the reported analysis action, not a new decision calculated from today's thresholds. Unknown actions stay visibly unknown.
3. **Missing data is not zero**: risk failures show unavailable and preserve the fail-closed state; no safety number is invented for presentation.
4. **Real money is loud, simulation is quiet**: a red "Live: real money" state is the only saturated block on the page.
5. **Recognizable states**: text, arrow/pause shapes and color work together. Green means a buy signal, red a sell signal, amber wait, and a neutral badge missing/unknown data. Signals are analysis; filled orders appear in Recent trades and stocks remain research-only.
6. **Progressive disclosure**: scores, reports, methodology, and learning evidence stay available without competing with the normal monitoring path.
7. **Motion only answers change**: a coin's mark slides when its score updates; nothing animates on load. Reduced motion is respected.
8. **Untrusted text is escaped**: LLM reports and API strings are always HTML-escaped.
9. **Honest freshness and mode**: start with mode unknown, distinguish received snapshots from source update times, and preserve refresh-failure warnings until a successful fetch.
