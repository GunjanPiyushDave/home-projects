You are acting as a principal software architect, senior full-stack engineer,
fixed-income trading systems specialist, and QA lead.

Build a runnable, local-first, command-driven institutional trading workstation
that reproduces the Bloomberg TOMS workflows I specify.

This is an independent implementation for simulation, training, workflow
prototyping, and software development. It must not claim to be Bloomberg,
an authorized Bloomberg product, or connected to Bloomberg services.

Use an original application name: “Trade Operations Workstation”.

Do not give me only an architecture document, screenshots, pseudocode, or a
static UI. Implement a working application with persistent data, functional
commands, navigable screens, real backend queries, workflow actions, and tests.

======================================================================
1. PRODUCT OBJECTIVE
======================================================================

The primary interaction is a terminal-style command bar.

A user enters a command and identifiers; the application resolves the command,
applies user permissions and regional context, retrieves the appropriate data,
and opens the corresponding functional screen.

Two mandatory workflows:

A. Instrument identification

Input:
    ID <ISIN>

Behavior:
- Resolve the identifier against the instrument master.
- Display instrument details for the resolved security.
- If no instrument exists, show an explicit no-result state.
- If source records conflict or the identifier resolves ambiguously, present
  a selection screen rather than silently choosing.
- Support navigation from instrument details to related trades and positions.

B. Trade audit/search and drill-down

Input:
    AUDX <TRADE_ID>

Behavior:
- Search within the currently selected region and the user's entitlements.
- Display a trade-results grid first, including when there is one match.
- Clicking or keyboard-selecting a row opens that specific trade's detail screen.
- The detail screen contains the tabs defined in the screen specification.
- Back navigation returns to the previous results with filters, sort order,
  selected row, and scroll position preserved.

These are user-supplied target behaviors. Do not claim that their exact syntax,
semantics, tabs, or layouts have been independently verified against Bloomberg.

The final product should support adding more command-driven workflows through
a command registry rather than hardcoded conditional logic throughout the UI.

======================================================================
2. EVIDENCE, EXACTNESS, AND NON-INVENTION RULES
======================================================================

I want high behavioral and visual fidelity to reference material I provide.

However:
- Do not invent Bloomberg commands and label them authentic.
- Do not invent exact Bloomberg field names, tab names, layouts, keyboard
  shortcuts, status codes, or business rules.
- Do not claim full TOMS parity without an enumerated, verified scope.
- Do not claim that all TOMS functions have been implemented.
- Do not obtain proprietary code, bypass access controls, scrape authenticated
  services, or use credentials embedded in screenshots.
- Do not reproduce Bloomberg logos or imply affiliation.
- Use only reference materials and data I am authorized to provide.

Classify every command and screen requirement as:
1. USER_SPECIFIED
2. REFERENCE_VERIFIED
3. SIMULATOR_ASSUMPTION
4. NOT_IMPLEMENTED

Maintain:
- docs/command-catalogue.md
- docs/screen-catalogue.md
- docs/assumptions.md
- docs/parity-matrix.md

For each command, record:
- Syntax and examples.
- Evidence source and confidence.
- Arguments and validation.
- Context requirements.
- Entitlements.
- Backend handler.
- Result screen.
- Failure states.
- Test coverage.
- Implementation status.

For each screen, record:
- Reference material, if any.
- Field labels, ordering, data types, and formatting.
- Tabs and sections.
- Actions and keyboard behavior.
- Known deviations and unresolved questions.

If I have not supplied screenshots or field definitions:
- Build a clearly documented provisional screen.
- Label its specification as SIMULATOR_ASSUMPTION.
- Centralize its configuration so it can later be replaced with the verified
  layout without rewriting the domain services.
- Do not block the initial vertical slice waiting for perfect references.

When I later supply reference screenshots, inspect them systematically:
- Identify layout regions, fields, labels, tabs, actions, and navigation.
- Record what is visible and what remains unknown.
- Do not infer hidden-tab contents from a screenshot of another tab.
- Use sanitized reference fixtures for visual regression tests.
- Never commit sensitive customer, counterparty, account, or trade data.

======================================================================
3. INITIAL SCOPE AND NON-GOALS
======================================================================

Initial scope:
- Fixed-income cash instruments.
- Instrument master and identifier resolution.
- Trade search and detail drill-down.
- Regional context.
- Trade capture and permitted lifecycle changes.
- Audit history.
- Simulated allocations, settlement, messages, and exceptions.
- Basic positions and explicitly scoped valuation outputs.
- User permissions.
- Persistent, deterministic demo data.

Not required in the first release:
- Complete cross-asset TOMS parity.
- Production exchange connectivity.
- Live Bloomberg data.
- Real regulatory submissions.
- Real settlement instructions sent externally.
- Production-grade risk analytics.
- Brokerage or execution of real trades.

Any integration implemented initially must use a simulated adapter unless
I explicitly authorize a real integration separately.

======================================================================
4. TECHNOLOGY AND REPOSITORY
======================================================================

Default stack:
- Frontend: React, TypeScript, Vite.
- Styling: CSS variables and reusable workstation components.
- Grid: an open-source grid with suitable licensing, such as AG Grid Community;
  do not silently rely on enterprise-only features.
- Backend: Python, FastAPI, Pydantic.
- Persistence: PostgreSQL, SQLAlchemy, Alembic.
- Tests: pytest, frontend component tests, Playwright.
- Local orchestration: Docker Compose.
- API contract: OpenAPI plus typed frontend client.
- Financial calculations: explicit decimal arithmetic and decimal-safe API
  serialization; do not rely on JavaScript Number for authoritative amounts.

Prefer a modular monolith.
Do not introduce Kafka, Kubernetes, microservices, or distributed event sourcing
unless a demonstrated requirement justifies them.

Suggested structure:

frontend/
backend/
  app/
    api/
    commands/
    domain/
    services/
    repositories/
    adapters/
    schemas/
    security/
  tests/
migrations/
seed/
tests/e2e/
docs/
docker-compose.yml
.env.example
README.md

If starting in an existing repository:
- Inspect it first.
- Preserve established conventions where reasonable.
- Do not overwrite unrelated functionality.
- Explain material stack deviations before implementing them.

======================================================================
5. TERMINAL WORKSTATION EXPERIENCE
======================================================================

Build a dense, keyboard-friendly trading workstation.

Visual direction:
- Dark background.
- Compact spacing.
- High-information-density grids.
- Monospaced command and identifier text.
- Clear active-screen and active-tab indicators.
- Restrained color coding with text/icon equivalents.
- Original branding.
- Readable focus states and accessible contrast.

Core shell:
- Global command bar.
- Current region selector.
- Current user and permission context.
- Environment label: SIMULATION.
- Connection indicator.
- Workspace tabs or panes.
- Breadcrumbs and back navigation.
- Status/error area.
- Command history.
- Recent instruments and trades.

Keyboard behavior:
- Enter executes a command or activates the selected grid row.
- Up/down navigate command history when the command bar is focused.
- Escape dismisses overlays or returns focus appropriately.
- Provide a documented shortcut to focus the command bar.
- Do not interfere with ordinary text editing inside form fields.

Commands must be deterministic parser-driven operations.
Do not put an LLM in the execution path for command resolution or trade changes.

======================================================================
6. COMMAND ENGINE
======================================================================

Implement a typed command registry containing:
- Name and aliases.
- Argument grammar.
- Context dependencies.
- Permission requirements.
- Read versus mutation classification.
- Validation.
- Handler.
- Result-view type.
- Help text.
- Evidence classification.

Minimum supported commands:
- ID <ISIN>
- AUDX <TRADE_ID>
- HELP
- HELP <COMMAND>

Optional independent application commands:
- REGION <REGION_CODE>
- TRADES
- POSITIONS
- BOOK <BOOK_CODE>

Explicitly identify the optional commands as application-defined unless
reference evidence verifies otherwise.

Parser requirements:
- Case-insensitive command names.
- Whitespace normalization.
- Quoted arguments where supported.
- Preservation of identifier semantics.
- Clear syntax errors.
- Unknown-command suggestions.
- No dynamic eval or execution of user input.
- Server-side validation and authorization even when the frontend validates.

Return a structured command response:
- Command execution ID.
- Normalized command.
- Resolved context.
- Result type.
- Result payload or resource reference.
- Warnings.
- Structured errors.
- Correlation ID.

Define whether command history is local or persisted.
Do not store sensitive arguments in general-purpose logs unnecessarily.

======================================================================
7. REGIONAL CONTEXT AND TRADE IDENTITY
======================================================================

Regions:
- AMER
- EMEA
- APAC

These are simulator partitions unless I supply a different regional model.

AUDX behavior:
- Search only the active region by default.
- Do not silently fall back to another region.
- Include the active region in the screen header.
- Offer an explicit cross-region search only to entitled users.
- Never leak the existence of unauthorized trades or regions.

Model identity carefully:
- Immutable internal trade UUID.
- External/business trade ID.
- Region.
- Source system.
- Trade version.
- Original or parent trade reference where applicable.

Do not assume that a business trade ID is globally unique.
Use the appropriate composite identity for lookup, and the immutable internal
UUID for opening a specific record.

Define separately:
- Multiple trades sharing an external ID.
- Multiple versions of one trade.
- Source-system duplicates.
- Cancel/rebook relationships.

The results screen must make these distinctions visible.

======================================================================
8. INSTRUMENT MASTER AND ID SCREEN
======================================================================

Support fields appropriate to the instrument type, including:
- Internal instrument ID.
- ISIN.
- Other identifiers where populated.
- Description.
- Issuer.
- Asset class and product type.
- Currency.
- Coupon type and rate.
- Issue date and maturity date.
- Day-count convention.
- Coupon frequency.
- Business-day convention.
- Settlement convention.
- Minimum denomination and increment.
- Country.
- Source and last-updated timestamp.

Do not fabricate identifiers, ratings, prices, or issuer facts as real data.
Use clearly synthetic fixtures and provenance metadata.

Validate identifier format and checksum where applicable.
Distinguish invalid format from valid-but-not-found.

Provisional tabs:
- Overview.
- Identifiers.
- Cashflows.
- Related Trades.
- Positions.
- Source Data.

These tab names are SIMULATOR_ASSUMPTION until I provide references.

Any pricing, accrued interest, or cashflow functionality must document:
- Supported instrument types.
- Calendar assumptions.
- Day-count and settlement rules.
- Unsupported cases.
- Test cases.

Do not apply a simplistic fixed-coupon calculation to unsupported products.

======================================================================
9. AUDX SEARCH RESULTS
======================================================================

Minimum columns:
- Business trade ID.
- Region.
- Source system.
- Version.
- Status.
- Trade date and execution timestamp.
- Settlement date.
- Instrument description.
- ISIN.
- Buy/sell.
- Quantity or nominal.
- Price.
- Currency.
- Counterparty.
- Book.
- Trader.
- Last update.

Support:
- Server-side filtering and pagination.
- Stable sorting.
- Column resizing and visibility preferences.
- Keyboard row navigation.
- Explicit row activation.
- Loading, empty, forbidden, and error states.
- Stale-response protection when commands or regions change quickly.
- Preservation of search state on back navigation.

Search semantics:
- Exact business-ID matching by default.
- Partial matching only through an explicit, documented mode.
- Version-history inclusion as an explicit option.
- No fuzzy matching that silently opens the wrong trade.

======================================================================
10. TRADE DETAIL SCREEN
======================================================================

Persistent header:
- Business trade ID.
- Internal record identifier where useful.
- Region.
- Source.
- Version.
- Status.
- Instrument.
- Buy/sell.
- Nominal.
- Price.
- Counterparty.
- Book.
- Trader.

Provisional tabs:
1. Summary.
2. Economics.
3. Parties.
4. Dates & Settlement.
5. Allocations.
6. Lifecycle.
7. Audit.
8. Messages.
9. Exceptions.
10. Source Payload.

These are proposed simulator tabs, not asserted Bloomberg tab names.

Summary:
- Core trade terms and workflow status.

Economics:
- Nominal, price convention, clean/dirty price where supported,
  accrued interest, principal, settlement amount, fees, currency.
- Show units and conventions explicitly.
- Distinguish supplied values from calculated values.

Parties:
- Counterparty, account, legal entity, trader, sales, broker where applicable.

Dates & Settlement:
- Trade date, execution timestamp, settlement date, settlement status,
  and masked synthetic settlement details.

Allocations:
- Allocation rows, quantities, accounts, and validation totals.

Lifecycle:
- Ordered business-event history and linked cancel/rebook records.

Audit:
- Actor, timestamp, action, reason, before/after changes,
  correlation ID, source, and version.

Messages:
- Synthetic inbound/outbound FIX or other integration messages.
- Search, formatting, and correlation to trade events.
- Clearly label simulated messages.

Exceptions:
- Validation failures, workflow breaks, ownership, and resolution history.

Source Payload:
- Sanitized raw source payload, provenance, and schema version.

Each tab must query actual persisted data.
Do not populate tabs with disconnected hardcoded example content.

======================================================================
11. WORKFLOW ACTIONS AND CONSISTENCY
======================================================================

Implement:
- Create draft.
- Validate.
- Book.
- Amend.
- Cancel.
- Cancel and rebook.
- Allocate.
- Simulate settlement progression.
- Resolve an operational exception.

Define allowed transitions explicitly.

Do not collapse trade lifecycle, allocation, settlement, and exception status
into one overloaded status field. Model related state machines separately.

Every mutation requires:
- Backend permission checks.
- Input validation.
- Expected-version check.
- User-visible preview/confirmation.
- Reason where appropriate.
- Atomic database transaction.
- Append-only audit event.
- Idempotency protection where retries could duplicate the action.

The business update and corresponding audit write must commit together.

Concurrent amendment behavior:
- Reject stale updates with a conflict response.
- Show the user that the record changed.
- Offer refresh and comparison, not silent overwrite.

Never implement amendment by overwriting history.
Never implement cancellation by deleting the original trade.

Describe audit guarantees accurately:
append-only application behavior is not equivalent to cryptographic
tamper-proofing or protection from database administrators.

======================================================================
12. DOMAIN MODEL AND API
======================================================================

Core entities:
- User.
- Role and permission.
- Region entitlement.
- Legal entity.
- Book.
- Counterparty.
- Instrument.
- Instrument identifier.
- Trade.
- Trade version or revision.
- Allocation.
- Lifecycle event.
- Audit event.
- Integration message.
- Settlement instruction.
- Operational exception.
- Position or position projection.

Use:
- Foreign keys and appropriate uniqueness constraints.
- UTC timestamps plus clear display timezone handling.
- Date-only types for business dates.
- Decimal precision appropriate to each financial field.
- Explicit price and quantity conventions.
- Source/provenance fields.
- Indexed regional trade lookup.

Design APIs for:
- Command execution.
- Instrument lookup.
- Trade search.
- Trade detail and tabs.
- Workflow mutations.
- User context and entitlements.
- Reference data.

All views must share the same authoritative domain services.
The command path and form/button path must not implement different business rules.

======================================================================
13. SECURITY AND LOCAL DEMO DATA
======================================================================

Implement server-side role-based access and regional entitlements.

Demo roles:
- Trader.
- Sales.
- Operations.
- Read-only support.
- Administrator.

Provide a documented permissions matrix.

Local demo authentication must:
- Be explicitly development-only.
- Not require real credentials.
- Fail closed outside the intended demo configuration.
- Keep secrets out of source control.

Seed deterministic synthetic data:
- At least 100 instruments.
- At least 1,000 trades across the three regions.
- Several books and counterparties.
- Multiple trade and settlement states.
- Duplicate external IDs across regions or sources.
- Versioned amendments.
- Cancel/rebook chains.
- Allocations.
- Messages and operational exceptions.
- At least one restricted-region user.

Include fixtures for:
- Invalid ISIN.
- Valid-format unknown ISIN.
- Unknown trade.
- Multiple matches.
- Stale update.
- Unauthorized access.
- Failed workflow transition.

Publish exact working demo commands in the README.
Provide a deterministic reset/reseed command.

======================================================================
14. POSITIONS AND ANALYTICS BOUNDARIES
======================================================================

If implementing positions:
- Define booking-side and sign conventions.
- Explain which trade states contribute.
- Amendments must replace prior contributions, not double count.
- Cancellations must reverse contributions correctly.
- Distinguish trade-date and settled positions if both are supported.

For P&L, yield, duration, DV01, or valuation:
- Implement only explicitly scoped, tested calculations.
- Identify market-data assumptions and timestamps.
- Label illustrative outputs.
- Do not display invented risk numbers merely to fill a dashboard.
- Mark unsupported analytics as unavailable.

======================================================================
15. TESTING AND ACCEPTANCE
======================================================================

Unit tests:
- Parser and registry.
- Identifier validation.
- Region resolution.
- Permissions.
- Lifecycle transitions.
- Decimal handling.
- Version conflicts.
- Position adjustment rules, if implemented.

Integration tests:
- Persisted instrument lookup.
- Region-scoped trade search.
- Composite identity handling.
- Trade-detail tab queries.
- Atomic mutation plus audit persistence.
- Idempotent retries.
- Authorization on every relevant endpoint.

End-to-end scenarios:
1. ID <seeded ISIN> opens instrument details.
2. Unknown ISIN shows an explicit no-result screen.
3. AUDX <seeded trade ID> shows results in the active region.
4. Selecting a result opens the correct immutable trade record.
5. All implemented tabs show data from that record.
6. Back navigation preserves result state.
7. Changing region changes the authorized search scope.
8. Cross-region duplicate IDs do not produce incorrect drill-down.
9. Unauthorized users cannot retrieve restricted records directly by API.
10. Amendment creates a new version and audit event.
11. Cancellation preserves historical data.
12. A stale amendment is rejected.
13. Restarting the stack preserves persisted changes.
14. Keyboard-only command/search/drill-down workflow works.
15. Unsupported commands produce help rather than fabricated behavior.

Use deterministic fixtures.
Do not report tests as passed unless you actually ran them.
If execution is unavailable, state exactly what remains unverified.

======================================================================
16. DELIVERY PLAN
======================================================================

Phase 0: Inspect and specify
- Inspect the repository.
- State assumptions and evidence gaps.
- Produce a concise architecture and implementation plan.
- Define the initial command and screen contracts.

Phase 1: Complete the mandatory vertical slice
- Dockerized frontend, backend, and database.
- Migrations and deterministic seed data.
- Workstation shell.
- ID command with persisted lookup.
- AUDX regional search.
- Results-to-detail navigation.
- At least Summary, Economics, and Audit tabs backed by persisted data.
- Working automated tests.
- README with executable startup steps.

Phase 2: Complete trade operations
- Remaining detail tabs.
- Permissions.
- Capture, amendment, cancellation, and cancel/rebook.
- Allocations, simulated settlement, messages, and exceptions.
- Concurrency and idempotency safeguards.

Phase 3: Extend and refine
- Positions and carefully scoped analytics.
- Additional documented commands.
- Reference-driven visual refinement.
- Performance measurement.
- Expanded regression tests.

Implement Phase 1 before broadening the feature set.
Do not spend the entire session planning.
Do not substitute an attractive dashboard for the two mandatory workflows.

At each phase, report:
- Files changed.
- Working features.
- Commands I can run.
- Tests executed and outcomes.
- Known limitations.
- Assumptions requiring my verification.

If session limits approach:
- Leave the repository runnable.
- Update docs/implementation-status.md.
- Record the next concrete tasks.
- Do not describe incomplete work as complete.

======================================================================
17. REQUIRED DELIVERABLES
======================================================================

Deliver:
- Working source code.
- Docker Compose configuration.
- Database migrations.
- Deterministic seed/reset tooling.
- Automated tests.
- README.
- Architecture notes.
- Command catalogue.
- Screen catalogue.
- Assumptions register.
- Permissions matrix.
- Parity matrix.
- API documentation.
- Known-limitations document.
- Short demo walkthrough.

Definition of done for the initial build:

From a clean checkout, I can:
1. Follow the README to start the application.
2. Sign in as a demo user.
3. Select a region.
4. Enter a documented ID command and inspect instrument details.
5. Enter a documented AUDX command and see regional trade results.
6. Select a trade and inspect its persisted detail tabs.
7. Return to the same results without losing context.
8. Run the documented test suite.

Begin by inspecting the available project and stating your assumptions.
Then implement the Phase 1 vertical slice immediately.
