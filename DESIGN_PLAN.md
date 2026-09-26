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
| ink-2 | `#636B5F` | `#8E9A88` | secondary text |
| red ink | `#A8261B` | `#F07365` | losses (in parentheses), sells, breached limits, live-money stamp |
| blue pencil | `#2250B8` | `#8DA9FF` | only your audit labels and marks, and keyboard focus |
| highlighter | `#F4DF5A` | `#5E5215` | only "look at this": a limit near its cap, unlabelled work, neutral fallbacks |

## Type

Atkinson Hyperlegible Next for everything (400/500/700): its glyphs keep 0/O and 1/l/I apart, which matters when 0.05 vs 0.06 BTC is real money. Atkinson Hyperlegible Mono only for keyboard hints and IDs. Scale: 12 / 14 / 16 / 21 / 28. Sentence case throughout; no all-caps labels, no eyebrows.

## Layout

```
Trading ledger                                   Live feed connected
══════════════════════════════════════════════════════════════════
Holding. SOL is closest, 0.10 from the buy line.   [Simulated orders]
Next check in about 3 minutes.
            SOL
       ETH   │
  BTC   │    │
▨▨▨▨▨▨▨▨│    │    │    │            ▧▧▧▧▧▧▧▧▧▧▧   Sell ≤ .35   Buy ≥ .65
────────┼────●────●────●────────────┼─────────────
Balance + sparkline                      │ Safety limits (meters)
Open positions   (single rule, total,    │ Agents
                  double rule)           │ Strategies
Recent trades                            │ New-token watch
══════════════════════════════════════════════════════════════════
Audit the LLM readings: runs │ readings with weight, score, source,
                             │ your label (blue pencil)  ✓ / ✗
Accuracy by method  │  TypeSafe accuracy by confidence
▸ Activity log (collapsed)
```

Left-aligned text; numbers right-aligned in tabular figures. Under 600px the band keeps its pins and moves coin names into a list below it; tables scroll inside their own box.

## Principles

1. **The decision band is the one showpiece**: every coin's latest score between the sell and buy lines, with a sentence saying what that means. Everything else is quiet bookkeeping.
2. **Bookkeeping conventions carry meaning**: red ink and parentheses for losses; a single rule above a total and a double rule under it.
3. **Real money is loud, simulation is quiet**: a red "Live: real money" stamp is the only saturated block on the page.
4. **Plain sentences over labels**: "Holding. SOL is closest…", "Trading halted: the drawdown limit was hit."
5. **Motion only answers change**: a coin's mark slides when its score updates; nothing animates on load. Reduced motion is respected.
6. **Untrusted text is escaped**: LLM reports and API strings are always HTML-escaped.
