
## In-house Chronicle FIX engine across four venues (Tradeweb, Bloomberg FIT/TSOX, MarketAxess/Trax, and Trumid) for the full RFQ → Quote → Post-trade lifecycle on portfolio trades. 

Here is a structured breakdown of all estimation tasks.

***

## Discovery \& Current State Audit

This is the foundation and is often underestimated. You need a full inventory of everything that someone currently handles on your behalf.

- Audit all active FIX sessions per venue: SenderCompID/TargetCompID pairs, FIX version, connection type (TCP/TLS)
- Document all managed custom tag mappings and proprietary field translations per venue
- Capture all sequence number management, gap-fill, and session recovery behaviors currently handled by ION
- Map all message flows per venue: RFQ lifecycle, quote submission/response, portfolio trade list workflows, post-trade TradeCaptureReport flows
- Identify all middleware rules, enrichment logic, and drop-copy configurations that would need to be re-implemented

***

## Venue FIX Specification Acquisition

Each of the four venues has its own FIX spec that must be independently analyzed. The effort is non-trivial because each venue has bespoke extensions.

- **Tradeweb**: Obtain FIX API spec for RFQ list trading (up to 300 line items), portfolio trade workflows, and FIX 35=J allocation messages; request access to their sandbox/cert environment
- **Bloomberg FIT/TSOX**: Obtain Direct Order Routing (DOR) FIX spec for automated RFQ and Order routing; TSOX uses custom tag extensions across its RFQ and Order protocols
- **MarketAxess (Trax)**: Separate specs for trade matching (TradeCaptureReport AE/AR) and APA quote reporting (MassQuote); note SenderSubID/TargetSubID routing for service disambiguation
- **Trumid**: Request FIX connectivity spec; Trumid competes heavily in credit e-trading and will have its own RFQ/quoting workflow extensions

***

## Chronicle FIX Engine Setup

Chronicle FIX is a high-performance Java engine with microsecond latency designed for exactly this use case.

- License procurement and commercial setup with Chronicle Software
- Infrastructure provisioning: dedicated servers with Chronicle Queue-compatible storage
- Configure FIX sessions per venue: logon/logout parameters, heartbeat intervals, sequence reset policies
- Set up the **FIX Router** for routing rules between your internal services and each venue endpoint
- Configure **FIX Version Translator** for venues running different FIX versions (e.g., 4.2 vs 4.4 vs 5.0)
- HA/DR setup: primary/secondary failover, sequence number persistence via Chronicle Queue, automated reconnect
- TLS/SSL certificate exchange with each venue; IP whitelisting and firewall rules

***

## FIX Message Development Per Venue

This is the largest development workstream. Each venue needs fully implemented message handlers across three workflows:

### RFQ Workflow (per venue)

- Outbound `QuoteRequest (35=R)` construction with all venue-required custom tags
- Inbound `Quote (35=S)` parsing and routing to internal decision engine
- `QuoteResponse / QuoteStatusReport` for accept/reject/counter
- Order execution via `NewOrderSingle (35=D)` and `ExecutionReport (35=8)` handling
- Portfolio trade list via `NewOrderList (35=E)`, `ListStatus (35=N)`, `ListExecute (35=L)`


### Quoting Workflow

- `MassQuote (35=i)` and `Quote (35=S)` inbound parsing if acting as liquidity provider
- `QuoteAcknowledgement (35=b)` generation


### Post-Trade Workflow

- `TradeCaptureReport (35=AE)` submission with all required fields (FirmTradeID, TradeReportID, lifecycle events)
- `TradeCaptureReportAck (35=AR)` parsing and error handling
- `Allocation (35=J)` and `AllocationAck (35=P)` for post-trade allocations
- Amendment, cancel, and lifecycle event flows including `TradeReportRefID (572)` race condition handling

***

## Internal System Integration

The Chronicle FIX layer needs to wire into your existing OMS/EMS and back-office stack.

- Internal message bus integration (Chronicle Queue is the natural choice for durable, low-latency pub/sub between FIX gateway and your services)
- OMS integration: order staging, execution report callbacks, list order status updates
- Pre-trade risk check hooks before sending RFQ/orders to venues
- Reference data integration: ISIN/CUSIP resolution, security master lookups for FIX instrument blocks
- Allocation service integration for post-trade account-level splits
- Settlement/affirmation downstream: DTCC, DTC, or internal matching system feeds

***

## Session Management \& Operational Logic

- FIX session state machine: `Logon (35=A)`, `Logout (35=5)`, `Heartbeat (35=0)`, `TestRequest (35=1)`, `ResendRequest (35=2)`, `SequenceReset (35=4)`
- Sequence number gap detection and fill request logic (critical for post-trade reconciliation)
- Scheduled session reset windows aligned with each venue's maintenance windows
- Reconnect backoff strategy and session recovery from Chronicle Queue replay

***

## Certification Per Venue

Each venue runs a mandatory certification process before you can go live. This is a calendar-driven workstream that can be a critical path item.


| Venue | Certification Type | Key Steps |
| :-- | :-- | :-- |
| **Tradeweb** | API Certification | Test RFQ list, portfolio trade, allocation flows in sandbox |
| **Bloomberg TSOX** | DOR Certification | Automated RFQ, Order, drop-copy; TSOX-specific tag validation  |
| **MarketAxess** | FIX Gateway Cert | Trax trade matching + APA quote reporting separately  |
| **Trumid** | FIX Connectivity Cert | RFQ/quoting workflow validation  |


***

## Monitoring, Alerting \& Ops Tooling

- Real-time FIX session health dashboard (session up/down, heartbeat lag, reject rates)
- Message latency tracking end-to-end (send timestamp → ExecutionReport ack)
- Drop-copy configuration for compliance and surveillance using Chronicle FIX's rule-based drop-copy
- Operational runbook per venue: session restart, sequence reset, escalation contacts
- Alert thresholds for reject storms, session drops, and stale quotes

***

## Testing Strategy

- **Unit tests**: FIX message codec correctness for each venue's custom tags
- **Integration tests**: Full round-trip flows against venue sandbox/certification environments
- **Regression tests**: Parity validation against existing behavior for every message type
- **Portfolio trade E2E tests**: Multi-line list RFQ → execution → allocation → TradeCaptureReport chain
- **HA/DR failover tests**: Simulate session drop, primary failure, Chronicle Queue replay
- **Performance tests**: Throughput and latency benchmarks, particularly for portfolio trade list processing

***

## Regulatory \& Compliance Tasks

- Drop copy configuration for trade surveillance
- FINRA TRACE post-trade reporting hook (for US corporate bond trades on MarketAxess/Trumid)
- Best execution audit trail: timestamps at each RFQ lifecycle stage
- MiFID II quote reporting via MarketAxess Trax APA if in scope

***

## Migration \& Cutover Planning

- Define parallel-run period where both existing and Chronicle FIX sessions are live and outputs are reconciled
- Venue-by-venue cutover sequence (lower-volume venues first to de-risk)
- Rollback procedure: session switchback to existing system within an agreed SLA window

***

## Estimation Summary by Workstream

| Workstream | Complexity | Notes |
| :-- | :-- | :-- |
| Discovery \& Audit | Medium | Blocked on existing platform documentation availability |
| Venue FIX Spec Analysis (×4) | High | Each venue is independent effort |
| Chronicle FIX Engine Setup | Medium | Well-documented but HA/DR needs care |
| FIX Message Dev (×4 venues × 3 flows) | Very High | Largest dev effort; ~12 independent work packages |
| Internal System Integration | High | Depends on existing OMS/EMS architecture |
| Session Management Logic | Medium | Reusable across venues |
| Venue Certification (×4) | High | Calendar-driven; venues control timelines |
| Monitoring \& Ops Tooling | Medium | Can parallelize with dev |
| Testing | High | Portfolio trade E2E especially complex |
| Compliance/Regulatory | Medium | Depends on jurisdictions in scope |
| Migration \& Cutover | Medium | Risk-heavy; needs dedicated PM |

The **critical path** is typically: Venue FIX spec acquisition → Chronicle FIX dev → Venue certification → Parallel run → Cutover. Bloomberg TSOX and Tradeweb certifications tend to be the longest lead-time items and should be initiated earliest.


---

# Estimations

These estimates assume **1 man-month = 1 senior developer working full-time for 1 calendar month**, with a baseline team of 2 senior Java/FIX engineers + 1 QA. The lower bound of each range is achievable with a senior dev but the realistic column accounts for venue-driven unknowns.

***

## Assumptions

- Estimates cover **design, implementation, unit/integration testing, and documentation** per workstream — certification and QA testing are broken out separately
- FIX message dev estimates include all 3 workflows: RFQ lifecycle, quoting, and post-trade per venue
- "Realistic" column absorbs typical unknowns: undocumented existing system behaviours, venue spec ambiguities, network/firewall delays, and cert round-trips
- Chronicle FIX's Java-native design means faster ramp-up vs. C++ engines; one tier-1 bank deployed a full EFX platform in ~6 months with it

***

## Effort Estimates by Workstream

| Workstream | Optimistic (mm) | Realistic (mm) | Key Risk / Driver |
| :-- | :-- | :-- | :-- |
| **Discovery \& Current State Audit** | 1.0 | 2.0 | existing system config documentation completeness |
| **Venue FIX Spec Analysis (×4)** | 1.5 | 2.5 | Spec accessibility; Bloomberg TSOX is notoriously dense |
| **Chronicle FIX Engine Setup** (sessions, HA/DR, TLS, FIX Router) | 1.5 | 2.5 | HA/DR design and cert exchange with 4 venues  |
| **FIX Message Dev — Tradeweb** | 4.0 | 6.0 | Portfolio trade list (300-line items), NewOrderList/ListStatus complexity |
| **FIX Message Dev — Bloomberg TSOX** | 3.5 | 5.0 | Bespoke tag extensions across DOR/RFQ/Order; tightest cert process |
| **FIX Message Dev — MarketAxess** | 3.0 | 4.5 | Two independent specs: Trax matching + APA quote reporting |
| **FIX Message Dev — Trumid** | 2.0 | 3.0 | Younger venue; simpler protocol but less public documentation  |
| **Internal System Integration** (OMS, Chronicle Queue bus, risk, ref data, allocation) | 3.0 | 5.0 | Largest unknown — depends on existing OMS/EMS architecture depth |
| **Session Management \& Operational Logic** (state machine, gap-fill, reconnect, resets) | 0.5 | 1.0 | Largely reusable across venues once built |
| **Venue Certification (×4)** | 3.0 | 6.0 | Calendar-driven; venues control pace; Bloomberg and Tradeweb slowest  |
| **Monitoring, Alerting \& Ops Tooling** (dashboard, drop-copy, runbooks) | 1.5 | 2.5 | Drop-copy config is a standalone stream per venue |
| **Testing Strategy** (unit, integration, E2E portfolio trade, HA/DR failover, perf) | 3.5 | 5.0 | E2E portfolio trade chain test harness is the biggest investment |
| **Regulatory \& Compliance** (FINRA TRACE, MiFID II/Trax APA, audit trail) | 1.0 | 2.0 | Scope depends on jurisdictions; Trax APA can be a parallel track |
| **Migration \& Cutover** (parallel run, venue-by-venue switchover, rollback) | 2.0 | 3.0 | Reconciliation tooling vs existing system output adds effort  |
| **TOTAL** | **31.0** | **50.5** |  |


***

## Elapsed Time Translation

| Team Size | Optimistic | Realistic |
| :-- | :-- | :-- |
| 1 senior developer (solo) | ~31 months | ~50 months |
| 2 senior devs + 1 QA | ~12–13 months | ~18–20 months |
| 3 senior devs + 1 QA + 0.5 PM | ~9–10 months | ~14–16 months |


***

## Biggest Risk Items

- **FIX Message Dev is ~40% of total effort** and the only item that doesn't parallelize well within a venue — each workflow in a venue has data dependencies on the prior one
- **Venue certification is the critical path bottleneck** — Bloomberg TSOX and Tradeweb typically require 6–12 weeks of elapsed time regardless of your code readiness; start cert environment access requests on day one
- **Internal OMS/EMS integration** has the widest estimate range (3–5 mm) because it's entirely dependent on how clean your internal interfaces are — if you have a well-defined Chronicle Queue event model already, you'll be at the low end
- **Trumid documentation is thin publicly** — budget 0.5–1.0 mm for spec gap analysis and back-and-forth with their connectivity team, absorbed within the Trumid dev estimate
- A reasonable **contingency buffer of 15–20%** on top of realistic estimates is standard for this class of migration project, putting the full realistic range at **~58–60 man-months** with contingency



---

# Further breakdown for "FIX Message Dev"

This is a deep technical breakdown. Organized by the natural layers of implementation: shared infrastructure first, then per-venue message development, with effort in **man-days** (20 days = 1 man-month) for granularity.

***

## Shared FIX Message Infrastructure

These are built once and reused across all four venues. Skimping here costs you dearly later.


| Task | Sub-tasks | Man-days |
| :-- | :-- | :-- |
| **Message Codec Framework** | Chronicle FIX encoder/decoder factory; tag registry per venue; field validation rules; custom UDF tag (5000–9999 range) registry| 5 |
| **FIX Session State Machine Base** | Logon/Logout/Heartbeat/TestRequest/ResendRequest/SequenceReset handlers; sequence number persistence via Chronicle Queue; gap-fill logic  | 5 |
| **Repeating Group Handler** | Generic repeating group builder/parser for `NoOrders`, `NoLegs`, `NoAllocs`, `NoPartyIDs` — critical for portfolio trade list  | 4 |
| **Reject \& Error Framework** | `BusinessMessageReject (35=j)`, `Reject (35=3)` parser; error code enum per venue; dead-letter queue; retry policy | 3 |
| **Internal Chronicle Queue Bus** | Outbound staging queue (OMS → FIX gateway); Inbound dispatch queue (venue → OMS/post-trade); durable replay design | 4 |
| **FIX Message State Correlation** | ClOrdID / OrigClOrdID / ListID / TradeReportID correlation store; in-flight order tracker; idempotency guard | 4 |
| **Subtotal** |  | **25 days (1.25 mm)** |


***

## Tradeweb — 4.0–6.0 mm

Tradeweb is the most complex venue due to portfolio trade list (up to 300 line items), multi-leg allocation, and multi-workflow FIX API.

### RFQ \& Order Workflow

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| QuoteRequest builder (single security) | `35=R`: ISIN/CUSIP instrument block, `OrdType`, `Side`, `OrderQty`, `Currency`, venue-specific custom tags | 3 |
| QuoteRequest builder — portfolio list mode | `35=R` with `NoRelatedSym` repeating group for multi-line basket; `ListID` linkage; `BidType (394)` for disclosed/non-disclosed convention  | 5 |
| Quote parser and routing | `35=S`: `QuoteID`, `BidPx/OfferPx`, `BidSize/OfferSize`, expiry; route to pricing engine | 2 |
| QuoteResponse / counter-quote | `35=AJ` or venue equivalent; accept/reject/counter logic | 2 |
| NewOrderSingle execution (single-line) | `35=D`: full field set, `TimeInForce`, `HandlInst`, `OrdType`; ClOrdID generation | 2 |
| NewOrderList construction (portfolio trade) | `35=E`: `ListID`, `BidType`, `TotNoOrders (68)`, `LastFragment (893)` for fragmentation; `ListOrdGrp` repeating group per leg | 6 |
| ListExecute trigger | `35=L`: timing logic after staging/review | 1 |
| ListStatus parsing | `35=N`: per-leg `OrdStatus`, partial fills, ListOrderStatus | 3 |
| ListCancelRequest | `35=K`: cancel in-flight list; state reconciliation | 2 |
| ExecutionReport handler (per leg) | `35=8`: `OrdStatus`, `ExecType`, `LastPx`, `CumQty`, `LeavesQty`; multi-fill aggregation | 3 |

### Post-Trade Workflow

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| TradeCaptureReport — New | `35=AE`: `TradeReportTransType (487)=0`, `FirmTradeID (1041)`, `TradeReportID`, full instrument block, `Parties` component | 4 |
| TradeCaptureReport — Amend/Cancel | `35=AE`: `TradeReportTransType=1/2`, `TradeReportRefID (572)` linkage; amendment chain state machine | 3 |
| TradeCaptureReportAck parser | `35=AR`: `TrdRptStatus (939)`, error parsing; resubmit flow on rejection | 2 |
| Allocation Instruction | `35=J`: `NoAllocs` repeating group; per-account qty/price splits; `AllocType`, `AllocTransType` | 4 |
| AllocationAck / AllocationInstructionAck | `35=P` / `35=AS`: acceptance/rejection handler; downstream settlement notification | 2 |
| TradeCaptureReportRequest (reconciliation) | `35=AD`: subscription or snapshot request; reconcile intraday vs internal blotter | 2 |

**Tradeweb Subtotal: ~46 man-days (2.3 mm optimistic) → 60 days (3.0 mm) realistic**

***

## Bloomberg TSOX/DOR — 3.5–5.0 mm

Bloomberg FIT's Direct Order Routing (DOR) replicates both RFQ and Order protocols through FIX with extensive proprietary tag extensions. This is the densest spec of the four.

### RFQ via DOR

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| DOR session handshake \& logon params | Proprietary Bloomberg logon tags; `SenderSubID`/`TargetSubID` routing for RFQ vs Order channel | 2 |
| QuoteRequest builder (Bloomberg DOR) | `35=R`: Bloomberg custom instrument tags (ISIN, Ticker, Yellow Key); `SettlType`, `SettlDate`; `NoPartyIDs` for dealer targeting | 4 |
| Multi-dealer RFQ fan-out logic | Dealer ID list population in `NoPartyIDs`; response correlation by `QuoteReqID` across multiple dealer responses | 3 |
| Quote parser (Bloomberg extensions) | `35=S`: Bloomberg custom pricing fields; quote validity window; dealer identity resolution | 2 |
| QuoteStatusReport | `35=AI`: Bloomberg-specific status codes for expired/pulled quotes | 1 |
| Order execution on RFQ hit | `35=D` with Bloomberg DOR routing tags; `ExDestination` for FIT; quote linkage via `QuoteID` | 3 |
| Order Cancel Request | `35=F`: Bloomberg cancel acknowledgement pattern; timing constraints | 2 |
| Order Cancel/Replace (amendment) | `35=G` → `35=8`: `OrigClOrdID` chain; Bloomberg reject codes | 2 |
| ExecutionReport handler | `35=8`: Bloomberg `ExecType` enum mapping to internal state; partial fill handling | 3 |

### Post-Trade (Bloomberg FIT)

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| TradeCaptureReport — New | `35=AE`: Bloomberg settlement instruction tags; `ClearingBusinessDate`; counterparty BIC/LEI in `Parties` block | 4 |
| TradeCaptureReport — Amend/Cancel | `35=AE`: Bloomberg-specific amendment tags; `TradeReportRefID` chain management | 3 |
| TradeCaptureReportAck parser | `35=AR`: Bloomberg-specific rejection reasons; resubmit / escalation flow | 2 |
| AllocationInstruction | `35=J`: Bloomberg fund/account codes in `NoAllocs`; `AllocSettlDate` per leg | 3 |
| AllocationAck handler | `35=P` / `35=AS`: downstream settlement confirmation trigger | 2 |

**Bloomberg Subtotal: ~36 man-days (1.8 mm optimistic) → 50 days (2.5 mm) realistic with spec ambiguity**

***

## MarketAxess — 3.0–4.5 mm

MarketAxess has **two completely separate FIX specs** that must be implemented independently: the trade matching gateway and the APA quote reporting gateway.

### RFQ \& Order Workflow (MarketAxess Trading)

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| Session routing — SenderSubID/TargetSubID | Separate SubID values for RFQ channel vs post-trade channel; connection manager | 2 |
| QuoteRequest builder | `35=R`: MarketAxess CUSIP-based instrument block; `SettlType`; dealer `NoPartyIDs` | 3 |
| Quote parser and response | `35=S` / `35=AJ`: quote price/size parsing; TRFQ (Targeted RFQ) dealer recommendation handling  | 2 |
| NewOrderSingle execution | `35=D`: MarketAxess-specific `OrdType`, `TimeInForce`, counterparty fields | 2 |
| ExecutionReport handler | `35=8`: fill confirmation, reject handling | 2 |

### Post-Trade — Trax Trade Matching

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| TradeCaptureReport — New | `35=AE`: `FirmTradeID (1041)`, `TradeReportTransType (487)=0`; full instrument + parties block  | 3 |
| TradeCaptureReport — Amend | `35=AE`: `TradeReportTransType=1`; `TradeReportRefID (572)` amendment chain; idempotency via FirmTradeID | 3 |
| TradeCaptureReport — Cancel | `35=AE`: `TradeReportTransType=2`; cancel confirmation workflow | 2 |
| TradeCaptureReportAck parser | `35=AR`: `TrdRptStatus (939)=0 (Accepted)` / `3 (Accepted with Errors)`; `TradeID (1003)` extraction; error resubmit flow  | 2 |

### Post-Trade — Trax APA Quote Reporting

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| APA TradeCaptureReport — New | `35=AE`: Trax-specific tags `TraxNotionalAmount (22600)`, `TraxNotionalCurrency (22601)`, `TraxRefPerStart (22602)`, `TraxRefPerEnd (22603)`; `PriceType (4230)`, `QtyType (854)=0 (Units)`  | 4 |
| APA TradeCaptureReportAck parser | `35=AR`: separate APA acceptance/rejection logic; regulatory reference extraction | 2 |
| APA session management | Separate FIX session for APA; timing alignment with reporting deadlines (T+0 for MiFID II) | 2 |

**MarketAxess Subtotal: ~27 man-days (1.35 mm optimistic) → 40 days (2.0 mm) realistic**

***

## Trumid — 2.0–3.0 mm

Trumid supports disclosed, fully/partially anonymous, and "Grey" RFQ types — all with their own FIX message variants. Spec documentation is thinner than the other three venues.

### RFQ Workflow

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| Spec gap analysis \& reverse engineering | Trumid FIX spec acquisition; undocumented custom tags identification via certification sandbox | 4 |
| QuoteRequest builder — disclosed RFQ | `35=R`: counterparty-disclosed mode; standard instrument/side/qty block | 2 |
| QuoteRequest builder — anonymous RFQ | `35=R`: anonymous/Grey mode; Trumid custom anonymity tags | 3 |
| Quote parser and response | `35=S`: price/size parsing; anonymous dealer ID resolution; expiry handling | 2 |
| Order execution | `35=D` → `35=8`: execution confirmation; Trumid custom `ExecType` enum | 2 |
| Order Cancel / Replace | `35=F` / `35=G`: Trumid cancel ack patterns | 1 |

### Post-Trade Workflow

| Task | FIX Messages | Man-days |
| :-- | :-- | :-- |
| TradeCaptureReport — New | `35=AE`: `FirmTradeID (1041)`, instrument block, Trumid custom settlement tags | 3 |
| TradeCaptureReport — Amend/Cancel | `35=AE`: `TradeReportTransType` lifecycle; amendment chain | 2 |
| TradeCaptureReportAck parser | `35=AR`: Trumid rejection codes; error handling / escalation | 1 |

**Trumid Subtotal: ~20 man-days (1.0 mm optimistic) → 30 days (1.5 mm) realistic**

***

## Full Effort Rollup

| Workstream | Man-days (Optimistic) | Man-days (Realistic) | Man-months |
| :-- | :-- | :-- | :-- |
| Shared Infrastructure | 25 | 25 | 1.25 mm |
| Tradeweb FIX Message Dev | 46 | 60 | 2.3–3.0 mm |
| Bloomberg TSOX FIX Message Dev | 36 | 50 | 1.8–2.5 mm |
| MarketAxess FIX Message Dev | 27 | 40 | 1.35–2.0 mm |
| Trumid FIX Message Dev | 20 | 30 | 1.0–1.5 mm |
| **Total FIX Message Dev** | **154 days** | **205 days** | **7.7–10.25 mm** |


***

## Sequencing Recommendation

The tasks have natural ordering dependencies within each venue:

1. **Shared infrastructure first** — without the codec framework and Chronicle Queue bus, nothing else builds cleanly
2. **Session management** → **RFQ workflow** → **Order execution** → **Post-trade** — each layer depends on the prior
3. **Trumid first for parallelization** — it's the simplest and lets a second developer get up to speed on the pattern before tackling Bloomberg TSOX
4. **Bloomberg TSOX and Tradeweb in parallel** if you have two developers — they are independent and these are the two longest certification queues, so starting them simultaneously minimizes elapsed time on your critical path



---

# How about TransFICC



Choosing TransFICC is a fundamentally different integration model. Instead of building raw FIX message handling per venue, integrate once against TransFICC's normalized One API — a hosted client library that translates all venue-specific protocols into a single uniform model. Here is the full breakdown.

***

## What TransFICC Actually Covers

All four of your target venues are live on TransFICC's One API, but with one important gap you need to flag immediately:


| Venue | RFQ | Post-Trade | Portfolio / Non-Contingent List | Status |
| :-- | :-- | :-- | :-- | :-- |
| **Tradeweb CORI** | RFQ ✅ | POST TRADE ✅ | PORTFOLIO ✅ | LIVE |
| **Bloomberg Bonds** | RFQ ✅ | POST TRADE ✅ | PORTFOLIO ⚠️ | **PLANNING** |
| **Bloomberg DOR** | RFQ BUYSIDE ✅ | POST TRADE ✅ | — | LIVE |
| **MarketAxess US** | RFQ ✅ | POST TRADE ✅ | NON CONTINGENT LIST ✅ | LIVE |
| **Trumid** | RFQ ✅ | POST TRADE ✅ | NON CONTINGENT LIST ✅ | LIVE |

**Bloomberg portfolio trade is still in PLANNING status** — this is a hard gap to wait on TransFICC's roadmap and negotiate a custom delivery schedule with them.

***

## Task Breakdown with Effort Estimates


### TransFICC API Setup — 0.5–1.0 mm

- Integrate TransFICC's One API client library into your Java stack
- Authentication and session setup — single connection point replacing 4 venue FIX sessions
- HA/DR connectivity: TransFICC manages hardware and venue connectivity failover on their side; you still need primary/secondary failover to TransFICC's endpoints
- Sandbox environment access and initial smoke tests


### Normalized Internal Event Model — 1.0–1.5 mm

This is the most architecturally important task and shouldn't be rushed. TransFICC normalizes venue APIs into their format, but you must design the translation layer between their model and your OMS/EMS.

- Design internal domain events: `RFQReceived`, `QuoteSubmitted`, `ExecutionConfirmed`, `TradeCaptureSubmitted` etc.
- Map TransFICC's normalized fields to your internal model (instrument, party, settlement, allocation)
- Chronicle Queue event bus integration for durable handoff between TransFICC gateway and internal services


### Portfolio Trade / Non-Contingent List — 1.0–2.0 mm

This is simpler than the raw FIX `NewOrderList (35=E)` 300-leg construction, but requires careful workflow design:


| Task | Man-days |
| :-- | :-- |
| Basket/list composition for Tradeweb CORI Portfolio workflow | 4 |
| Non-Contingent List workflow for MarketAxess and Trumid | 3 |
| Per-leg execution status aggregation and blotter update | 3 |
| Bloomberg Portfolio workflow (⚠️ PLANNING — timeline TBD with TransFICC) | 4 (risk item) |
| **Subtotal** | **~14 days (0.7 mm optimistic / 1.5 mm realistic)** |

### Post-Trade Integration — 1.5–2.5 mm

TransFICC normalizes TradeCaptureReport flows across venues, but you still need to implement the full lifecycle:


| Task | Man-days |
| :-- | :-- |
| TradeCaptureReport submission (New) via TransFICC normalized API | 4 |
| Amendment and cancel chain logic | 3 |
| TradeCaptureReportAck handler and resubmit logic | 3 |
| Allocation instruction and ack handler | 3 |
| Downstream settlement / reconciliation feed | 3 |
| **Subtotal** | **~16 days (0.8 mm optimistic / 1.5 mm realistic)** |

### Testing \& Sandbox E2E — 1.5–2.5 mm

Less heavy than in-house because there is no FIX codec testing, but you still own E2E workflow correctness:

- Unit tests on your normalized internal event model
- Integration tests against TransFICC sandbox per venue
- E2E portfolio trade chain: basket composition → execution → post-trade round-trip
- Regression: behaviour parity vs current system output
- HA/DR failover to TransFICC backup endpoints


### Monitoring \& Ops — 0.5–1.0 mm

TransFICC manages venue session health and hardware on their side, which eliminates the biggest ops burden:

- Internal health check: TransFICC API connectivity, latency metrics, reject rate alerting
- Drop-copy and compliance audit trail from TransFICC normalized events
- Runbooks for escalation to TransFICC support vs venue support


### Migration \& Cutover — 1.0–1.5 mm

- Parallel run period: both existing and TransFICC paths live, reconcile trade outputs
- Venue-by-venue cutover (Trumid → MarketAxess → Bloomberg → Tradeweb suggested order)

***

## Total Effort Comparison

| Workstream | TransFICC Optimistic | TransFICC Realistic | In-House Chronicle FIX |
| :-- | :-- | :-- | :-- |
| Commercial/Procurement | 0.25 mm | 0.5 mm | — |
| API Setup \& Infrastructure | 0.5 mm | 1.0 mm | 1.5–2.5 mm |
| Normalized Event Model | 1.0 mm | 1.5 mm | (embedded in FIX msg dev) |
| RFQ Workflow Integration | 1.0 mm | 1.5 mm | 12.5–17.0 mm |
| Portfolio Trade Integration | 0.75 mm | 1.5 mm | ↑ included above |
| Post-Trade Integration | 0.8 mm | 1.5 mm | ↑ included above |
| Testing | 1.5 mm | 2.5 mm | 3.5–5.0 mm |
| Monitoring \& Ops | 0.5 mm | 1.0 mm | 1.5–2.5 mm |
| Migration \& Cutover | 1.0 mm | 1.5 mm | 2.0–3.0 mm |
| **Total** | **~7.3 mm** | **~12.5 mm** | **31–50 mm** |


***

## Key Trade-offs to Factor into Your Decision

**TransFICC advantages:**

- Eliminates ~120–150 man-days of FIX message development and venue certification effort entirely
- Venue API upgrades are managed by TransFICC — no maintenance burden when venues change their specs


**TransFICC risks/downsides:**

- **Bloomberg Portfolio Trade is PLANNING** — hard dependency on their roadmap if this workflow is critical
- Ongoing subscription cost replaces one-time build cost; for high-volume shops, this can be significant over a 3–5 year horizon
- We are operationally dependent on TransFICC's uptime, release schedule, and support SLAs — introduce a third-party in your critical path
- Less control over message-level optimization; if we need sub-100 microsecond latency on RFQ response, the hosted model adds network hops vs co-located in-house FIX



