You are a Principal Fixed Income Trading Systems Architect specializing in Bloomberg TOMS, Optitrade, FIX post-trade workflows, and Java/Python/React engineering.

I am extending my existing Bloomberg TOMS + Optitrade replacement PoC (which already implements `AUDX`, `NTN`, `TBLT`, `TH`, and `TSIP`) to build a high-impact Operations PoC. Our primary stakeholders are Middle Office / Post-Trade Operations.

I need to implement two new 4-letter commands—strictly separating manual allocations from FIX-based STP allocations—while keeping 90% of the core deterministic and using AI only where unstructured inputs or anomaly scoring genuinely add value:

### 1. `MABL` (Manual Allocation Blotter)
- **Scope:** Strictly manual allocations (excludes FIX/STP auto-allocations).
- **Filtering:** Deterministic filter over `TBLT` for open, unallocated or partially allocated blocks, sorted by T+1 settlement cutoff urgency.
- **Clustered Split Suggestions:** Analyze the counterparty's last 15–20 `TH` allocations, cluster them by fund-list + weight signature into 2–3 distinct "Split Profiles" (with usage counts and field-level diff highlights), and let Ops toggle between them in one click.
- **Unstructured Intake (Optional AI):** Allow Ops to paste an email, IB chat, or CSV/Excel split to pre-populate the allocation grid.
- **Deterministic Bond Math Guardrails:** Enforce minimum-piece/increment rules, calculate accrued interest, and auto-balance sub-penny/odd-lot residuals to the largest account.
- **One-Click Commit:** Book allocations, write `AUDX` audit records, push to `TSIP`/Optitrade, and clear the block from `MABL`.

### 2. `FXAL` (FIX Allocation Blotter)
- **Scope:** Strictly FIX-based STP allocations (`35=J`) coming from venues like MarketAxess, Tradeweb, and BlackRock via a new FIX connector, where parent blocks are booked in TOMS by a separate STP process.
- **Out-of-Order Holding Buffer:** Park unmatched `35=J` messages for a configurable window (e.g., 60–120s) while polling TOMS `TBLT`/`TH` to eliminate race-condition "Block Not Found" false positives.
- **Near-Match Diagnostics:** When exact matching fails, score candidate open blocks, highlight the exact 1–2 mismatched fields (e.g., accrued interest off by $0.03, ISIN/CUSIP or account alias mismatch), and offer a one-click `[Force Match & Book]` override.
- **Atomic "Book Allocs → Cancel Block" State Machine:** Ensure idempotent execution so that if allocations book successfully but parent block cancellation fails, `FXAL` immediately raises a P1 Position/Double-Risk alert with a one-click `[Retry Block Cancel]` action.
- **Venue/Counterparty Anomaly Grouping:** Detect spikes in `35=J` rejects by venue/counterparty and group them by common root cause (e.g., unmapped fund account).

### What I need from you:
Help me decipher exactly what to build by producing a concrete, phased implementation blueprint covering:
1. **System Architecture & Component Boundaries** (Java deterministic core, FIX connector, state store, optional Python/AI service, React/WebSocket UI).
2. **Core Data Models & JSON Schemas** for `MABL` rows, clustered split profiles, `FXAL` exception records, and the `AUDX` audit trail.
3. **State Machine Diagrams** for both `MABL` manual commit and `FXAL` (`Receive 35=J → Buffer → Match → Book Allocs → Cancel Block → Reconcile`).
4. **Deterministic Algorithms** for clustering the last 15–20 splits, bond-math/penny auto-balancing, and near-match scoring.
5. **Phased 2-Week PoC Backlog** (MVP tasks ordered so I can demo `MABL` and `FXAL` end-to-end with synthetic data).

Keep the design practical, testable, and free of unnecessary AI bloat. Ask me any clarifying questions about my current tech stack or data schemas before generating code.
