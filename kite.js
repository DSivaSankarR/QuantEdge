/**
 * QuantEdge Cloudflare Worker — kite.js v4.90
 *
 * Changelog v4.90 (11-Jul-2026): Portfolio Intelligence Copilot (backend) — Phase 10.0.
 *   NOT AN LLM, NOT FREE-FORM NLU: deterministic regex pattern-matching against the 11 mandated
 *   question shapes, each mapped to exactly one reused engine call. No language model, no fuzzy
 *   inference. Unmatched questions return UNRECOGNIZED_QUESTION with the list of supported
 *   patterns — never a guess at intent.
 *   PURE ORCHESTRATION, zero new investment intelligence: replayDecision (5.0),
 *   computePortfolioMemory (Portfolio Memory), generateDecisionNarrative (6.0),
 *   computeExecutiveBriefing (8.0), computePortfolioStory (7.0), and
 *   _latestDecisionsBySymbol (2.0) are called directly and their output returned verbatim,
 *   tagged with which engine/field answered the question. "Which holding worries me most" /
 *   "strengthened the most" / "weakened recently" / "highest conviction" / "requires attention"
 *   all resolve to an ALREADY-COMPUTED field on Executive Briefing or Portfolio Story
 *   (todays_weakest_holding, biggest_improvement, biggest_deterioration,
 *   todays_highest_conviction, opportunities_requiring_attention respectively) — the Copilot
 *   translates the question into which existing field answers it, never computes a new one.
 *   "What changed since yesterday" reuses what_changed/why_changed already stored per decision
 *   (via Portfolio Memory's decision_journey) — no new query.
 *   SYMBOL VALIDATION: every candidate token in a question is checked against the actual
 *   distinct symbol list in qe_decision_log — only an exact match against a REAL recorded
 *   symbol is accepted, never a fuzzy/partial match, to avoid ever answering about the wrong
 *   holding.
 *   VALIDATED against live D1 before delivery: all 11 example questions from the directive
 *   correctly classified to their intended intent, symbol extraction correctly identified ABDL/
 *   SUZLON/ATHERENERG from natural phrasing, an unrelated control question ("what's the weather
 *   today") correctly fell through to UNRECOGNIZED_QUESTION, and classification was confirmed
 *   deterministic (same question classified twice, identical result). Traced "Why is ABDL HOLD?"
 *   end-to-end to real decision_log_id=8 (ABDL's actual latest HOLD decision) — this exact
 *   narrative path was already validated for correctness and determinism in the v4.85 delivery.
 *   ROUTE: GET /portfolio/copilot?q=... — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; all eight
 *   Phase 1–4 modules, Portfolio Memory Engine, Decision Replay Engine, Decision Narrative
 *   Engine, Decision Evolution Analytics, Portfolio Story Engine, Executive Portfolio Briefing,
 *   and Executive Cockpit all diffed byte-identical against their correct respective baselines.
 *   Full-file duplicate-declaration scan clean. Route count 77→78.
 *   SCOPE NOTE: this delivery is the backend orchestration engine only. The directive's "UI
 *   Vision" section (a premium, WWDC/Google I/O-caliber Copilot interface in index.html) is a
 *   separate, substantial frontend design undertaking not addressed in this change — flagged to
 *   Siva rather than delivered as a rushed addition.
 *
 * Changelog v4.89 (11-Jul-2026): Executive Cockpit — Phase 9.0.
 *   PRESENTATION LAYER, ZERO NEW QUERIES beyond System Status metadata: 8 of 12 sections
 *   (executive_summary, portfolio_health, highest_conviction, weakest_holding,
 *   capital_allocation, portfolio_risks, concentration_status, opportunities, watch_tomorrow)
 *   are direct citations of Executive Briefing's already-computed output (Phase 8.0) — zero
 *   recomputation. "Portfolio Story" embeds Portfolio Story's full output directly (Phase 7.0).
 *   "Today's Actions" reuses Portfolio Story's existing decision_tier_tally rather than
 *   recomputing anything — a decision tally IS the set of currently-recommended actions in this
 *   system's vocabulary, so no new query or logic was needed for this section, unlike the
 *   original plan to fetch a separate recommended_action field (reconsidered specifically to
 *   avoid a near-duplicate of the existing latest-per-symbol join pattern already used by
 *   _latestDecisionsBySymbol).
 *   SYSTEM STATUS is the only genuinely new content, and it is purely operational metadata, not
 *   investment intelligence: a citation of all 15 engine version constants (verified each
 *   resolves exactly once before delivery), the outcome resolver's config (reused
 *   _outcomeResolverConfig(), Phase 1.0), calibration coverage (reused from the
 *   computeCalibrationRecommendations() call already made), and data freshness (days since the
 *   latest portfolio snapshot, derived from Portfolio Story's already-computed date field).
 *   ROUTE: GET /portfolio/cockpit — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; all eight
 *   Phase 1–4 modules, Portfolio Memory Engine, Decision Replay Engine, Decision Narrative
 *   Engine, Decision Evolution Analytics, Portfolio Story Engine, and Executive Portfolio
 *   Briefing all diffed byte-identical against their correct respective baselines. Full-file
 *   duplicate-declaration scan clean. Route count 76→77.
 *
 * Changelog v4.88 (11-Jul-2026): Executive Portfolio Briefing — Phase 8.0.
 *   PRESENTATION LAYER ONLY, thinnest module in this build: Portfolio Story (7.0) already
 *   computed portfolio_health_trend, weakest_investment_thesis, biggest_improvement/
 *   deterioration, concentration/risk/capital observations — this engine cites those directly
 *   rather than recomputing anything. Only 3 sections needed new assembly, and each is a filter
 *   over an already-existing engine's output, never a new judgment: "Today's Highest Conviction"
 *   filters Portfolio Memory's records by the existing conviction_trend="IMPROVING" field, then
 *   argmaxes over the existing health_score field — the identical pattern Portfolio Story already
 *   used for strongest/weakest thesis, just with an added categorical filter, not a new ranking
 *   dimension. "Opportunities Requiring Attention" is a filter over Profit Protection (2.0),
 *   Capital Rotation (3.0), and Portfolio Optimizer (4.0) outputs, excluding their respective
 *   "no action" states. "Watch Tomorrow" reuses reversal_conditions directly from Portfolio
 *   Memory's decision_journey (already fetched, zero new query).
 *   DELIBERATELY NOT CALLED: Decision Narrative and Decision Replay — both are per-single-
 *   decision tools; a portfolio-level 60-second briefing needs the portfolio-level aggregates
 *   Portfolio Story already assembled. Being a listed allowed input is a whitelist of what may be
 *   reused, not a mandate to invoke every engine regardless of whether a section needs it.
 *   VALIDATED against live D1 before delivery: ran the highest-conviction filter+argmax against
 *   real current holdings — correctly selected IDFCFIRSTB (health 89, conviction IMPROVING) over
 *   ATHERENERG (health 86, but conviction STABLE, correctly excluded by the categorical filter).
 *   ROUTE: GET /portfolio/briefing — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; all eight
 *   Phase 1–4 modules, Portfolio Memory Engine, Decision Replay Engine, Decision Narrative
 *   Engine, Decision Evolution Analytics, and Portfolio Story Engine all diffed byte-identical
 *   against their correct respective baselines. Full-file duplicate-declaration scan clean.
 *   Route count 75→76.
 *
 * Changelog v4.87 (11-Jul-2026): Portfolio Story Engine — Phase 7.0.
 *   PURE AGGREGATION, zero new scoring: every field is a direct citation, a simple count/tally,
 *   or a min/max/average over an already-computed per-symbol metric. "Strongest/weakest thesis"
 *   ranks by the single existing health_score field, not a new composite. "Biggest improvement/
 *   deterioration" is a plain argmax/argmin over Decision Evolution's already-computed
 *   health_trend.delta, not a new judgment.
 *   STRICT REUSE, ZERO DUPLICATION: calls computeDecisionEvolution() (all symbols),
 *   computePortfolioMemory() (all symbols), computePortfolioCapitalFoundation(),
 *   evaluateOptimizationConstraints(), computeProfitProtectionRecommendations(), and
 *   computePortfolioOptimization() directly — never reimplements any of their logic. The only
 *   new query is a qe_portfolio_snapshot TIME-SERIES read (all dates) for the portfolio-level
 *   health trend — every existing snapshot query fetches a single row (by id, or latest); this
 *   is the only place that reads the full series, a genuinely distinct purpose.
 *   PER-FIELD INSUFFICIENT-EVIDENCE HANDLING: current-snapshot facts (concentration,
 *   diversification, capital, risk, strongest/weakest thesis) are reported whenever available
 *   even with thin history, rather than suppressing them behind a single all-or-nothing gate.
 *   Trend-dependent fields (health trend, conviction/stability trend, momentum) require >=2 data
 *   points and report INSUFFICIENT_PORTFOLIO_HISTORY per-field with the actual symbol/data-point
 *   count disclosed, so partial coverage is transparent rather than silently averaged over.
 *   VALIDATED against live D1 before delivery: portfolio_health time series (33→85 since 07-06,
 *   delta +52, IMPROVING) computed correctly and deterministically (ran twice, byte-identical).
 *   Also surfaced a useful fact while validating: top3_pct was already anomalously high (142.05%)
 *   on the very FIRST recorded snapshot (07-06), meaning the concentration anomaly flagged in
 *   earlier phases has existed since day one, not something that emerged recently — worth
 *   knowing when that anomaly is eventually investigated at UAT.
 *   ROUTE: GET /portfolio/story — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; all eight
 *   Phase 1–4 modules, Portfolio Memory Engine, Decision Replay Engine, Decision Narrative
 *   Engine, and Decision Evolution Analytics all diffed byte-identical against their correct
 *   respective baselines. Full-file duplicate-declaration scan clean. Route count 74→75.
 *
 * Changelog v4.86 (10-Jul-2026): Decision Evolution Analytics — Phase 7.0.
 *   PURE STATISTICS LAYER over computePortfolioMemory()'s already-assembled output (journey,
 *   health_evolution, thesis_events via the reused _deriveThesisEvents) — zero new
 *   reconstruction, zero new scoring system. Every metric is a count, ratio, or delta: upgrade/
 *   downgrade frequency (counts of decision-rank-changing thesis events), decision_stability_ratio
 *   (unchanged transitions / total transitions), decision_volatility (rank-changes per day over
 *   the recorded span), health_trend (first-vs-last qe_holding_history value, reused not
 *   recomputed), conviction_distribution (frequency counts across the journey), recommendation_
 *   persistence_days (days since the most recent recorded decision change). The only new query
 *   is a minimal (id, ts) lookup to attach decision_log_id onto each timeline entry, purely so a
 *   caller can cross-reference into Replay/Narrative for any point — narrower purpose than
 *   Portfolio Memory's richer journey fetch, not a duplication of it.
 *   Metrics needing >=2 data points (frequencies, stability, volatility, health/conviction trend)
 *   explicitly report HISTORICAL_DATA_NOT_AVAILABLE below that threshold rather than computing a
 *   degenerate number — confirmed live: CHOICEIN (1 decision logged) correctly returns
 *   INSUFFICIENT_HISTORICAL_EVIDENCE across every 2+-point metric.
 *   VALIDATED against live D1 before delivery: ran the extracted logic against ABDL's real
 *   2-decision history. Result surfaced a genuine, honest tension rather than a tidy story:
 *   health_trend over the full recorded span (07-06→07-10) shows IMPROVING (35→83, +48), while
 *   the single most recent decision-to-decision transition (89→83) is what actually triggered the
 *   ACCUMULATE→HOLD downgrade — both measurements are correct and intentionally kept separate,
 *   since they measure different things (long-span trend vs. single-step transition) and
 *   conflating them would be exactly the kind of unsupported inference the guardrails prohibit.
 *   ROUTE: GET /portfolio/evolution (optional ?symbol=X) — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; all eight
 *   Phase 1–4 modules, Portfolio Memory Engine, Decision Replay Engine, and Decision Narrative
 *   Engine (checked against the correct v4.85 baseline after an initial baseline mix-up in this
 *   session's own verification script, caught before delivery) all diffed byte-identical.
 *   Full-file duplicate-declaration scan clean. Route count 73→74.
 *
 * Changelog v4.85 (10-Jul-2026): Decision Narrative Engine — Phase 6.0.
 *   NOT AN LLM / NOT FREE-FORM GENERATION: every sentence is a fixed template filled with fields
 *   already recorded on the Decision Replay Engine's output — zero new judgment, zero new
 *   scoring. STRICT LAYERING: consumes replayDecision() (Phase 5.0) and computePortfolioMemory()
 *   (Portfolio Memory Engine) via direct calls only — never re-queries qe_decision_log,
 *   qe_portfolio_snapshot, or qe_holding_history directly, never re-runs
 *   mapVerdictToDecision/computeRecommendationConfidence or any other decision logic. Reuses
 *   _deriveThesisEvents() and DECISION_RANK (Portfolio Memory Engine) and PIE_CONFIG.bands
 *   (read-only reference, never re-executed) — zero new thresholds introduced.
 *   Answers exactly the 5 mandated questions: (1) why this decision — cites recorded
 *   decision/verdict/health/confidence/strongest_pillar/evidence_summary; (2) why not stronger —
 *   cites recorded weakest_pillar and conviction_trend, or reports N/A if already STRONG_BUY (not
 *   HISTORICAL_DATA_NOT_AVAILABLE — a structural fact, not missing data); (3) why not weaker —
 *   cites recorded health_score against the existing band table plus recorded reversal_conditions,
 *   or N/A if already EXIT_IMMEDIATELY; (4) what changed — diffs the current decision against the
 *   previous journey entry (fetched from Portfolio Memory's decision_journey, not re-queried) via
 *   the reused thesis-event detector plus simple numeric deltas, or reports N/A if this is the
 *   symbol's first decision; (5) what would change — surfaces recorded reversal_conditions
 *   verbatim. Every branch that lacks the specific field(s) it depends on reports
 *   HISTORICAL_DATA_NOT_AVAILABLE rather than estimating.
 *   VALIDATED against live D1 before delivery: extracted the exact logic to Node, ran it twice
 *   against ABDL's real ACCUMULATE→HOLD downgrade (07-09→07-10, health 89→83, weakest_pillar
 *   edge→risk) — byte-identical output both runs, confirming determinism. Output correctly
 *   handled a genuinely tricky real case: conviction_trend stayed IMPROVING even though the
 *   decision downgraded, and the Q2 template accurately reflected that nuance ("which was
 *   IMPROVING; other recorded evidence nonetheless did not support...") rather than overclaiming
 *   a clean causal story the data didn't support.
 *   ROUTE: GET /portfolio/narrative?decision_log_id=N — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, 2.0, 2.5, 3.0, 3.5, and 4.0 modules diffed byte-identical; Portfolio Memory Engine
 *   and Decision Replay Engine (most recently built) also confirmed byte-identical. Full-file
 *   duplicate-declaration scan (functions + constants) clean. Route count 72→73.
 *
 * Changelog v4.84 (10-Jul-2026): Decision Replay Engine — Phase 5.0.
 *   PURE RECONSTRUCTION, verified by construction: never calls any live compute function
 *   (Profit Protection, Capital Rotation, Portfolio Capital Foundation, Optimization Framework,
 *   Portfolio Optimizer, Calibration, Decision Quality Analytics) — those engines were
 *   deliberately built with zero historical persistence (an approved design choice at each
 *   phase), so no historical trace of their past outputs exists anywhere to replay. Disclosed as
 *   an INFRASTRUCTURE LIMITATION, not worked around: every replay reports
 *   HISTORICAL_DATA_NOT_AVAILABLE for those six dimensions with an explicit, identical reason,
 *   rather than silently backfilling with live/current values (which would be "applying today's
 *   logic to yesterday's decision," explicitly forbidden).
 *   GENUINELY REPLAYABLE (append-only/immutable by construction): qe_decision_log itself (frozen
 *   at write — direct read, not reconstruction), qe_portfolio_snapshot joined via the decision's
 *   own snapshot_id column (NEVER today's latest snapshot), qe_holding_history via nearest
 *   snapshot_date ON OR BEFORE the decision date (NEVER qe_holdings, which is current-only),
 *   qe_decision_outcomes (kept in an explicitly separate, hindsight-labeled section — resolved
 *   outcomes are known-after-the-fact evidence, never merged into "what was known at decision
 *   time"), qe_trade_outcomes (genuinely dated records, reused from Portfolio Memory Engine's
 *   _realizedPerformance unchanged).
 *   MARKET REGIME: qe_decision_log has no regime column — the literal live value read at that
 *   moment was never persisted. What's reconstructed instead reuses Phase 1.0's exact method
 *   (_regimeAsOf — NIFTYBEES closes up to the decision date, unchanged computePipelineRegime()),
 *   clearly labeled "reconstructed" with an explicit caveat distinguishing it from the
 *   unrecoverable literal value.
 *   BUG FOUND AND FIXED BEFORE THIS PHASE BEGAN (documented in the v4.83 entry above): while
 *   reviewing existing infrastructure per standing instruction, found that the previous Portfolio
 *   Memory Engine delivery had named its handler/route identically to pre-existing, pre-roadmap
 *   code (handlePortfolioMemory / /portfolio/memory, present since v4.77) — the second
 *   declaration silently won at runtime, meaning the original route was actually serving this
 *   engine's output instead of its own. Classified as an implementation defect introduced by that
 *   change; fixed by renaming to handleInstitutionalMemory / /portfolio/institutional-memory,
 *   verified the original route line is now byte-identical to the pre-Memory-Engine baseline, and
 *   ran a full-file duplicate-declaration scan (functions AND constants) — zero remaining
 *   collisions anywhere in the file.
 *   VALIDATED against live D1 before delivery: ABDL's decision changed ACCUMULATE (07-09,
 *   snapshot_id=5, total_value=56649.99, top3_pct=84.2) → HOLD (07-10, snapshot_id=19,
 *   total_value=30207.27, top3_pct=161.49) — dramatically different real values. Confirmed the
 *   snapshot join uses the decision's own snapshot_id column, not a "latest" lookup, so replaying
 *   the older decision is structurally guaranteed to return the 07-09 numbers, never today's.
 *   ROUTE: GET /portfolio/replay?decision_log_id=N — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, 2.0, 2.5, 3.0, 3.5, and 4.0 modules all diffed byte-identical (0 lines changed
 *   each). Full-file duplicate-declaration scan clean. Route count 70→72.
 *
 * Changelog v4.83 (10-Jul-2026): Portfolio Memory Engine — Institutional Memory foundation.
 *   CORRECTION (same day, before Phase 5.0 began): the original delivery named this engine's
 *   handler `handlePortfolioMemory` and routed it at `/portfolio/memory` — both already existed
 *   as pre-existing, pre-roadmap code (a simple qe_holding_history + pieConvictionTrend endpoint,
 *   present since v4.77, unrelated to this roadmap). The second declaration silently won at
 *   runtime (JS function-declaration redeclaration), meaning the pre-existing route was actually
 *   invoking THIS engine's output instead of its original behavior — a real, if accidental,
 *   regression, caught before Phase 5.0 began by grepping for duplicate declarations across the
 *   whole file, not just the phase-boundary diffs my regression checks had been using (a real
 *   gap in that methodology, now noted for future phases). FIXED: renamed to
 *   handleInstitutionalMemory / GET /portfolio/institutional-memory. Verified the original
 *   /portfolio/memory route line is now byte-identical to the pre-Memory-Engine baseline (restored
 *   to its original, untouched behavior), and confirmed zero duplicate function declarations
 *   remain anywhere in the file via a full-file scan. Classified as IMPLEMENTATION DEFECT
 *   introduced by this change, not a pre-existing issue — reopened and fixed per standing
 *   instruction to fix genuine implementation bugs even in an otherwise-frozen phase.
 *   SEQUENCING: built before Decision Replay per Siva's approval — this layer's own spec
 *   requires Replay/Copilot/Calibration to consume it rather than reconstruct history
 *   independently, a dependency pointing backwards from the original roadmap's listed order.
 *   Product roadmap unchanged; only implementation order.
 *   PURE MEMORY LAYER, verified by construction: no scoring, ranking, recommending, or
 *   explaining anywhere in this module. Every field is a direct read, a chronological list, or
 *   a mechanical transition detection between two adjacent already-stored values (e.g.
 *   "conviction changed STABLE→IMPROVING on date X" is a logged fact, not a judgment).
 *   current_thesis_snapshot is explicitly a factual bundle of latest already-computed signals,
 *   not a new synthesized validity verdict — documented as such in the field itself.
 *   NO NEW TABLE: every source is already append-only/immutable. Checked schema before writing
 *   any code — qe_holding_history (29 rows/8 symbols) gives DAILY-granularity health/pillar
 *   evolution, a materially better resolution than qe_decision_log (one row per pipeline run) —
 *   used in preference for health_evolution. Confirmed on real data: ABDL's health_score jumped
 *   35→86 between 2026-07-06 and 07-07, entirely invisible to decision_log alone (which only has
 *   entries from 07-09) — concrete evidence this reuse choice surfaces real signal decision_log
 *   would have missed. qe_trade_outcomes (pre-roadmap manual Trade Tracker, 8 CLOSED rows) reused
 *   for realized_performance, explicitly disclosed as a different mechanism than Phase 1.0's
 *   automated resolver — not conflated with it.
 *   COMPOSES, VIA DIRECT CALLS, ZERO REIMPLEMENTATION: computePortfolioCapitalFoundation (2.5)
 *   for unrealized_performance, computeProfitProtectionRecommendations (2.0) and
 *   computeCapitalRotationRecommendations (3.0) for current_thesis_snapshot fields.
 *   ROUTE: GET /portfolio/memory (optional ?symbol=X) — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.82 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, 2.0, 2.5, 3.0, 3.5, and 4.0 modules all diffed byte-identical (0 lines changed
 *   each). Route count 70→71.
 *
 * Changelog v4.82 (10-Jul-2026): Portfolio Optimizer — Phase 4.0.
 *   ARCHITECTURAL DISTINCTION FROM PHASE 2.0/3.0: those engines loop per-holding, one
 *   recommendation per symbol. This engine evaluates the PORTFOLIO AS A WHOLE — a small set of
 *   portfolio-scoped recommendation types (REDUCE_CONCENTRATION, IMPROVE_DIVERSIFICATION,
 *   HOLD_CASH/DEPLOY_CASH, INCREASE_POSITION, MAINTAIN_POSITION, NO_OPTIMIZATION_REQUIRED), each
 *   triggered by portfolio-level constraint/objective status (Phase 3.5), not independent
 *   per-holding quality judgment. Specific holdings may be NAMED as evidence within a
 *   recommendation but never independently scored here.
 *   NON-OVERRIDE GUARDRAIL, structural not just promised: every recommendation touching a
 *   specific symbol reads that symbol's already-computed Capital Rotation (3.0) and Profit
 *   Protection (2.0) verdicts directly and either aligns with them or reports disagreement as an
 *   explicit fact for human judgment — never re-derives or overrides either. A holding at
 *   Profit Protection=LET_PROFITS_RUN can never be named as a reduction target, structurally
 *   enforced in the REDUCE_CONCENTRATION logic (filtered out before any target is named).
 *   Reuses Phase 3.5's evidence aggregator and constraint evaluator directly, Phase 3.0's
 *   rotation output, Phase 2.0's protection output — zero recomputation. No new table, zero
 *   writes, computed on-demand.
 *   DATA QUALITY FINDING (disclosed, not fixed — outside this phase's scope, not caused by this
 *   phase's code): live qe_holdings currently shows NEGATIVE quantities for 4 of 7 holdings
 *   (IDFCFIRSTB=-2, LLOYDSME=-5, SONACOMS=-14, SUZLON=-1) — unusual for a cash-equity swing
 *   system. Confirmed this does not crash the Optimizer (tested the exact real values in Node)
 *   but does produce negative portfolio_weight_pct for those symbols, which is meaningless for
 *   concentration analysis. This is a genuine upstream data-integrity question (Kite sync / GTT
 *   partial-fill accounting), not a bug in this phase — flagged for Siva's attention rather than
 *   silently producing confident-looking output built on unreliable per-symbol weights.
 *   ROUTE: GET /portfolio/optimize — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.81 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, 2.0, 2.5, 3.0, and 3.5 modules all diffed byte-identical (0 lines changed each).
 *   Route count 69→70.
 *
 * Changelog v4.81 (10-Jul-2026): Portfolio Optimization Framework — Phase 3.5.
 *   INFRASTRUCTURE ONLY, verified by construction: no function in this module computes an
 *   optimal allocation, proposes a trade, or generates a hypothetical — the one "what-if"
 *   interface (evaluateOptimizationFeasibility) requires the caller to fully specify the
 *   proposed action; it only reports feasibility against existing constraints. Introduces ZERO
 *   new numeric thresholds — every constraint value is reused directly from Phase 2.5's
 *   already-classified constraints, a deliberate signal this phase stayed inside its
 *   infrastructure boundary rather than drifting into optimization.
 *   1. OBJECTIVES: OPTIMIZATION_OBJECTIVE_REGISTRY — 7 objectives (capital_preservation,
 *   risk_adjusted_return, portfolio_quality, concentration_reduction, capital_efficiency,
 *   diversification, cash_reserve_preservation), each a pointer to existing upstream evidence,
 *   zero scoring/weighting logic. Active/priority order KV-configurable
 *   (PORTFOLIO_OPTIMIZATION_ACTIVE_OBJECTIVES), default = all, registry order.
 *   2. CONSTRAINTS: evaluateOptimizationConstraints() — status check only (SATISFIED/VIOLATED/
 *   UNKNOWN/NOT_APPLICABLE) against Phase 2.5's 5 existing constraints, zero enforcement.
 *   Diversification/sector-exposure evaluated via SECTOR_MAP[symbol] (Phase 1.2 reuse), NOT
 *   qe_portfolio_snapshot.sector_json — confirmed still degenerate ({"UNKNOWN":100}) on live
 *   data, a known pre-existing gap in frozen code, not fixed here.
 *   3. EVIDENCE: buildOptimizationEvidence() — pure aggregation via direct calls to
 *   computePortfolioCapitalFoundation (2.5), computeProfitProtectionRecommendations (2.0),
 *   computeCapitalRotationRecommendations (3.0), computeCalibrationRecommendations (1.2), and
 *   _latestDecisionsBySymbol (2.0) — zero recomputation, merged by symbol only.
 *   4. INTERFACE: evaluateOptimizationFeasibility() — answers "is this caller-specified
 *   hypothetical feasible" against max_position_weight_pct and an estimated post-action
 *   top3_pct; never suggests what the hypothetical should be.
 *   DATA OBSERVATION (disclosed, not fixed — frozen code): live qe_portfolio_snapshot currently
 *   shows top3_pct=161.49%, mathematically odd for a percentage. Traced to the pre-existing
 *   top3_pct=top3_cap/capital*100 calculation in computePortfolioRisk (line ~7922, frozen,
 *   unmodified) — likely related to CHOICEIN having just entered holdings. Confirmed this
 *   module's constraint evaluation handles the value gracefully (correctly reports VIOLATED,
 *   no crash) rather than silently passing through an unexplained anomaly.
 *   ROUTES: GET /portfolio/optimization/objectives, /constraints, /evidence, /feasibility — all
 *   under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.80 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, 2.0, 2.5, and 3.0 modules all diffed byte-identical (0 lines changed each). Route
 *   count 65→69.
 *
 * Changelog v4.80 (10-Jul-2026): Capital Rotation Engine — Phase 3.0.
 *   Read-only, zero new tables, computed on-demand. Consumes Phase 1.2 (calibration), Phase 2.0
 *   (profit protection, reused via a direct call, not reimplemented), and Phase 2.5 (capital
 *   foundation, reused via a direct call) — modifies none of them.
 *   CANDIDATE SOURCE CORRECTION: Phase 2.5 assumed no persisted discovery-candidate source
 *   existed, so its Opportunity Interface required caller-supplied candidate data. Before writing
 *   any code for this phase, qe_forward_track was checked directly against live D1 and found to
 *   be exactly that — a real, structured, already-populated candidate source (score, base_score,
 *   edge_class from the existing Backtest Edge Confidence classifier). This engine reads it
 *   directly; Phase 2.5's buildOpportunityComparison() is untouched and remains available for
 *   callers with their own candidate source.
 *   METHODOLOGY DISCLOSURE: a discovery-scan score and a portfolio health_score are different
 *   methodologies (entry-setup technical quality vs. current portfolio-context health) — never
 *   treated as directly interchangeable. Candidate score/edge_class is used ONLY as a
 *   qualification gate (score>=75 AND edge_class=PROVEN_POSITIVE, named thresholds) — it does
 *   not drive the rotation tier, which is set entirely by signal counting on the CURRENT
 *   HOLDING's own evidence. This structurally enforces "never rotate purely on a higher score,"
 *   not just by comment.
 *   GUARDRAILS, each independently verified on real data before delivery: profit protection
 *   LET_PROFITS_RUN force-overrides to RETAIN_POSITION regardless of any candidate (confirmed);
 *   a holding at Decision Engine SELL/EXIT_IMMEDIATELY defers rather than getting a competing
 *   opinion (confirmed on live SUZLON); no qualifying candidate forces NO_ROTATION regardless of
 *   holding-side signals (confirmed); portfolio concentration, capital efficiency, and conviction
 *   are first-class signals, never single-metric decisions (confirmed: ABDL, with a qualifying
 *   score=100 candidate available, still correctly resolved to RETAIN_POSITION because its own
 *   conviction/efficiency/confidence signals outweighed the concentration+profit-protection
 *   signals — the guardrail holding in practice, not just in a code comment).
 *   ROUTE: GET /portfolio/capital-rotation — under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.79 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, 2.0, and 2.5 modules all diffed byte-identical (0 lines changed each). Route
 *   count 64→65.
 *
 * Changelog v4.79 (10-Jul-2026): Portfolio Capital Intelligence Foundation — Phase 2.5.
 *   PURE INFRASTRUCTURE — no buy/sell/rotation/reallocation logic anywhere in this module,
 *   confirmed by design: no function in this phase returns a recommendation, score, or ranking.
 *   Read-only, zero new tables, computed on-demand.
 *   REUSE OVER RECOMPUTATION: Total Portfolio Value / Invested Capital read from the existing
 *   qe_portfolio_snapshot (computePortfolioRisk(), already run daily) — not recalculated. Per-
 *   holding cost basis/current value/holding age/R-multiple/conviction read directly from
 *   qe_holdings; current decision from the latest qe_decision_log row per symbol (reuses
 *   _latestDecisionsBySymbol() from Phase 2.0 unchanged, zero duplicate query logic).
 *   [RETIRED 13-Jul-2026] Available Cash was originally a manually-maintained KV value
 *   (PORTFOLIO_AVAILABLE_CASH) with no live broker integration. This has been replaced
 *   entirely by a live Zerodha Kite Connect Funds/Margins integration — see
 *   _fetchZerodhaFunds() / _portfolioCapital() — per architectural decision: Zerodha is
 *   now the single source of truth for capital, no manual cash entry exists anywhere.
 *   Pending Capital Commitments reports 0 with an explicit reason rather than a fragile
 *   heuristic parse of qe_gtt_log (a mixed-purpose KV audit log not structured for this) — same
 *   honest-gap pattern Phase 1.2 used for CAPITAL_ALLOCATION_PREFERENCE.
 *   THRESHOLD CLASSIFICATION (per standing rule — every constant classified Derived/Portfolio
 *   management practice/Engineering assumption/Product assumption): max_portfolio_concentration_pct
 *   =80 is DERIVED, matching the existing hardcoded top3_pct>80 EXCESSIVE threshold already live
 *   in generateExecutiveDecisionReport (not rewired to this KV key — that would touch frozen
 *   code, out of scope for an infra-only phase; documented as a disclosed single-number
 *   duplication until a future phase connects them). max_position_weight_pct=25,
 *   max_sector_exposure_pct=30 (deliberately distinct from discovery-time PIPE_MAX_SECTOR_N=5,
 *   a different mechanism — candidate count at scan time, not held-position weight), and
 *   max_concurrent_new_positions=3 are PRODUCT_ASSUMPTION, no prior precedent. min_cash_reserve=0
 *   is ENGINEERING_ASSUMPTION. All five KV-overridable, none enforced anywhere in this phase.
 *   OPPORTUNITY INTERFACE: buildOpportunityComparison() returns a structural side-by-side
 *   (current holding's real data vs caller-supplied candidate fields) with no score/rank/verdict
 *   field at all, by design — satisfies "must not perform capital ranking."
 *   VALIDATED against live D1 before delivery: pulled real qe_portfolio_snapshot and qe_holdings
 *   data, ran the extracted arithmetic in Node — ATHERENERG's computed portfolio_weight_pct
 *   (32.26%) closely tracked the snapshot's independently-computed top_name_pct (33.12%), cross-
 *   confirming correctness. Missing days_held (4 of 6 holdings) correctly produces null
 *   capital_efficiency rather than a crash or fabricated number.
 *   ROUTES: GET /portfolio/capital, GET /portfolio/capital/compare — both under /portfolio/,
 *   existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.78 for all 8 protected functions; Phase 1.0,
 *   1.1, 1.2, and 2.0 modules all diffed byte-identical (0 lines changed each). Route count 62→64.
 *
 * Changelog v4.78 (10-Jul-2026): Profit Protection Engine — Phase 2.0 (institutional roadmap).
 *   Read-only, zero new tables, computed on-demand from qe_holdings (live portfolio snapshot),
 *   qe_decision_log (latest decision per symbol), qe_pipe_regime KV (live regime), and Phase
 *   1.1/1.2 outputs (enrichment only). Modifies none of them.
 *   SCOPE BOUNDARY: only evaluates holdings where the Decision Engine currently says CONTINUE
 *   (STRONG_BUY/BUY/ACCUMULATE/HOLD) AND r_multiple>0 in qe_holdings. Where the Decision Engine
 *   has already called REDUCE/SELL/EXIT_IMMEDIATELY, this engine returns DEFERRED_TO_DECISION_ENGINE
 *   rather than issuing a second, possibly conflicting opinion — never duplicates that
 *   responsibility.
 *   NOT A FIXED-PROFIT-TARGET ENGINE: r_multiple/pnl_pct are used only as the eligibility filter
 *   and as contextual evidence in the output, never as classification inputs. Classification is
 *   driven entirely by qe_holdings.evidence_json (pillars, conviction_trend, monitor_next
 *   distance-to-stop — all already computed live by the existing pipeline, not recomputed) plus
 *   portfolio_concentration_flag from the latest qe_decision_log row. Named, isolated thresholds
 *   (PP_RISK_PILLAR_CAUTION_BELOW=60, PP_TREND_PILLAR_CAUTION_BELOW=60, PP_NEAR_STOP_PCT=3) —
 *   zero hardcoded profit percentages anywhere in the classification path.
 *   REGIME- AND PORTFOLIO-AWARE: current regime read from the same qe_pipe_regime KV key the
 *   live pipeline itself reads; RISK-OFF tightens the signal threshold by one. Portfolio
 *   concentration is a first-class erosion signal, not ignored.
 *   FOUR OUTCOMES: LET_PROFITS_RUN, HOLD, PARTIAL_PROFIT_PROTECTION, FULL_PROFIT_PROTECTION
 *   (plus DEFERRED_TO_DECISION_ENGINE and NO_CHANGE for out-of-scope/insufficient-evidence
 *   cases) — every result carries confidence (reused from recommendation_confidence/
 *   data_confidence, never invented), supporting_evidence, risks, and reversal_conditions
 *   (reused verbatim from qe_decision_log, not regenerated).
 *   ENRICHMENT: Phase 1.2 calibration recommendations matched by r_multiple bucket
 *   (PROFIT_PROTECTION_SENSITIVITY) and decision×regime (SIGNAL_RELIABILITY_BY_REGIME), plus
 *   Phase 1.1 decision_effectiveness context — attached to supporting_evidence, never gates a
 *   classification. Confirmed the engine still functions correctly when both return
 *   NO_CALIBRATION/sparse data (today's actual state), rather than going silent.
 *   VALIDATED against live D1 before delivery: pulled real qe_holdings/qe_decision_log data for
 *   all 4 currently-profitable holdings, ran the extracted classification logic in Node. Result:
 *   ABDL (largest winner, 12.56R) correctly classified PARTIAL_PROFIT_PROTECTION — driven by
 *   2.75%-from-stop proximity and concentration flag, NOT by its profit size — concrete evidence
 *   the fixed-profit-target guardrail holds in practice, not just in the code comments.
 *   ROUTE: GET /portfolio/profit-protection — namespaced under /portfolio/, existing auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.77 for all 8 protected functions; Phase 1.0,
 *   1.1, and 1.2 modules all diffed byte-identical (0 lines changed each). Route count 61→62.
 *
 * Changelog v4.77 (10-Jul-2026): Decision Calibration Engine — Phase 1.2 (institutional roadmap).
 *   RECOMMENDATIONS ONLY — never writes to PIE_CONFIG, mapVerdictToDecision, or any production
 *   threshold. Every output defaults approval_status to PROPOSED (or NOT_APPLICABLE for
 *   NO_CALIBRATION) and requires a human-executed, versioned PIE_CONFIG bump before it can ever
 *   take effect — same governance posture as the existing computeCalibrationProposal() scaffold
 *   (line ~8192, confirmed byte-identical/untouched — that's a separate, pre-roadmap mechanism
 *   reading qe_trade_outcomes; this is the new automated-evidence-layer equivalent, deliberately
 *   NOT merged with it to avoid coupling two independent systems via a shared config key).
 *   Consumes ONLY qe_decision_log + qe_decision_outcomes, and reuses evaluateDecisionOutcome()
 *   (Phase 1.1a) UNCHANGED — this engine adds zero new interpretation of correctness, only asks
 *   whether evidence justifies a parameter change. Zero new tables; computed on-demand.
 *   GUARDRAILS (all 5 mandated categories, applied uniformly before any recommendation can be
 *   generated): (1) minimum evidence — sample size >= DECISION_CALIBRATION_MIN_SAMPLES (KV,
 *   default 30, sector held to 60 per the directive's own stricter caution); (2) stability over
 *   time — evidence must span >=3 distinct decision dates, directly relevant today since all 6
 *   current decisions are from one date; (3) overfitting protection — split-half accuracy must
 *   agree in direction (a genuine out-of-sample-style check; fixed a boundary bug during testing
 *   where exactly-50%-in-both-halves was misclassified as "unstable" instead of "no effect" —
 *   caught by unit-testing the guardrail against 5 constructed scenarios before delivery, not
 *   after); (4) cross-regime consistency — regime-sensitive parameters carry their supporting
 *   regime set; (5) statistical significance — two-proportion z-test, 95% CI, vs neutral
 *   baseline. Any guardrail failure returns NO_CALIBRATION with the specific reason — never a
 *   forced recommendation.
 *   11 PARAMETER CATEGORIES implemented via a shared generic framework (group evidence by the
 *   parameter's relevant dimension, run it through the same guardrail+z-test core) rather than
 *   11 bespoke analyzers — confidence thresholds, health-score bands (PIE_CONFIG.bands),
 *   indicator weightings (PIE_CONFIG.weights, by strongest_pillar), regime sensitivity, sector
 *   sensitivity (via existing SECTOR_MAP lookup, no new storage), holding-period guidance,
 *   profit-protection sensitivity (BEARISH decisions bucketed by r_multiple at decision time),
 *   risk weighting (via pillars_json.risk), and signal reliability by regime. TWO categories
 *   (decision score cutoffs, capital-allocation preferences) honestly return
 *   INSUFFICIENT_INFRASTRUCTURE — the evidence they'd need isn't in qe_decision_log/
 *   qe_decision_outcomes today and adding it is out of this phase's scope, not silently faked.
 *   VALIDATED before delivery: unit-tested all 5 guardrails against 5 constructed scenarios
 *   (including today's real 6-decision/1-date case, correctly returning INSUFFICIENT_EVIDENCE)
 *   and a passing 40-sample/5-date/75%-accuracy case, correctly returning PROPOSED with z=3.16.
 *   ROUTE: GET /portfolio/calibration/recommendations — namespaced under /portfolio/, existing
 *   auth gate.
 *   REGRESSION: MD5-verified byte-identical to v4.76 for all 8 protected functions; Phase 1.0
 *   and Phase 1.1 modules both diffed byte-identical (0 lines changed each); legacy
 *   computeCalibrationProposal/handleCalibration confirmed untouched. Route count 60→61.
 *
 * Changelog v4.76 (10-Jul-2026): Decision Quality Analytics — Phase 1.1 (institutional roadmap).
 *   Consumes Phase 1.0 (qe_decision_outcomes) exclusively, read-only — zero writes to
 *   qe_decision_log or qe_decision_outcomes, zero new tables, computed on-demand per request.
 *   ARCHITECTURE: two-layer split per approved design. (1) Decision Evaluation Policy — pure,
 *   deterministic functions (evaluateDecisionOutcome, DECISION_EXPECTATION map) that hold the
 *   ONLY interpretation logic in the system: BULLISH decisions (STRONG_BUY/BUY/ACCUMULATE)
 *   correct if price_change_pct>0; BEARISH (SELL/REDUCE/EXIT_IMMEDIATELY) correct if <0
 *   (confirms de-risking was justified); NEUTRAL_STABLE (HOLD) correct if mae_pct >= -10
 *   (named constant HOLD_ACCEPTABLE_DRAWDOWN_PCT, isolated in one place) — a HOLD that suffers
 *   a severe drawdown anyway classifies FALSE_NEGATIVE (missed de-risk signal). (2) Decision
 *   Quality Analytics — pure consumer of the Policy layer, does zero interpretation of its own;
 *   aggregates into per-decision-type/per-window accuracy, true/false positive/negative counts,
 *   avg upside captured, avg downside avoided, avg profit protected (only credited when
 *   r_multiple>0 at decision time AND price subsequently fell — sourced from qe_decision_log,
 *   no third evidence table), decision effectiveness (simple correct/total aggregate, no
 *   subjective weighting), confidence-calibration INPUTS only (10-wide confidence buckets vs
 *   empirical accuracy — grouped evidence for a future calibration engine to consume, does not
 *   itself recalibrate anything), and avg holding period (first→terminal SELL/EXIT_IMMEDIATELY
 *   timestamp span per symbol, derived purely from qe_decision_log — no qe_holdings dependency,
 *   per the mandated two-table evidence boundary).
 *   VALIDATED against live D1 before delivery: inserted 5 synthetic RESOLVED_NORMAL_WINDOW rows
 *   covering all three expectation classes (BULLISH correct/incorrect, BEARISH correct/incorrect,
 *   NEUTRAL_STABLE incorrect), ran the exact extracted policy logic in Node against real
 *   qe_decision_log context (real r_multiple/confidence values), confirmed all 5 classifications
 *   matched hand-derived expected results, then deleted the test rows — qe_decision_outcomes and
 *   qe_decision_log both confirmed unmodified by this validation.
 *   ROUTE: GET /portfolio/analytics/decision-quality (optional ?window=N filter) — namespaced
 *   under /portfolio/ so it inherits the existing pieAuthOk gate, zero new auth code.
 *   OUT OF SCOPE (explicitly not built here, per directive): Decision Calibration Engine (no
 *   threshold recalibration performed), Profit Protection Engine, Capital Rotation Engine.
 *   REGRESSION: MD5-verified byte-identical to v4.75 for all 8 protected functions; Phase 1.0
 *   module (persistDecisionLog through handleOutcomeResolverStatus) diffed byte-identical
 *   (0 lines changed) — Phase 1.0 confirmed untouched. Route count 59→60.
 *
 * Changelog v4.75 (10-Jul-2026): Decision Outcome Resolver — Phase 1.0 (institutional roadmap).
 *   PURPOSE: enabling infrastructure for Phase 1.1 (Decision Quality Analytics) — makes
 *   "evidence over opinion" concrete by automatically resolving what actually happened to
 *   price after every qe_decision_log recommendation, instead of relying on manually-logged
 *   Trade Tracker entries. EVIDENCE CHECKED BEFORE BUILDING: queried live D1 directly —
 *   ohlcv_daily already has 1.19M rows / 2,752 symbols, continuously appended by the existing
 *   Refresh stage, full coverage confirmed for all 6 current holdings. No new price feed was
 *   needed; this consumes infrastructure that already exists for scoring.
 *   NEW TABLE (D1, applied via connector before this deploy, PRAGMA-verified — 26 columns):
 *   qe_decision_outcomes. Never writes to qe_decision_log — read-only against it, immutability
 *   preserved by construction. Linked by decision_log_id.
 *   SCOPE BOUNDARY (approved design, mandatory): stores OBJECTIVE, UNINTERPRETED evidence only
 *   — reference/eval price, price_change_pct, MFE, MAE, annualized volatility, benchmark
 *   (NIFTYBEES proxy — D1 has no historical Nifty 50 index series, live regime pipeline only
 *   4h-caches it from Yahoo) return + relative return, regime_at_evaluation (reuses
 *   computePipelineRegime() UNCHANGED, fed retroactive NIFTYBEES closes), decision_engine_version
 *   + resolver_version (never mixed across engine versions), evaluation_method (v1:
 *   CLOSE_N_TRADING_DAYS; future methods add new rows, never mutate old), resolution_status
 *   (PENDING / RESOLVED_NORMAL_WINDOW / RESOLVED_WITH_DATA_GAP / DELISTED_HEURISTIC —
 *   staleness-heuristic, not an asserted fact / MISSING_DATA). Deliberately does NOT compute
 *   "was this decision correct," accuracy, or any judgment — that is Phase 1.1's job, not
 *   infrastructure's. CORPORATE_ACTION is a reserved enum value, NOT auto-detected in v1 —
 *   distinguishing a split from a genuine crash from price data alone is unreliable; a wrong
 *   auto-label would poison the evidence layer, so it's left for a future proper feed.
 *   HORIZON-AGNOSTIC: windows read from KV PIE_OUTCOME_WINDOWS (comma list, default "10,20")
 *   — adding a new horizon (5/40/60 etc.) requires a KV edit only, zero code change.
 *   PIPELINE: new stage 5.7 in runPortfolioPipeline(), between Decision Log (5.6) and Audit
 *   (6) — same try/catch non-blocking pattern as every stage since 5.5; a resolver failure
 *   cannot block scoring, alerts, digest, or the decision report, all already complete by then.
 *   Safe-by-default: no-ops unless KV PIE_OUTCOME_RESOLVER_ENABLED=1 (deploying this changes
 *   nothing until explicitly turned on).
 *   ROUTES: GET /portfolio/outcomes/run (manual trigger/backfill), GET /portfolio/outcomes/status
 *   (observability) — both namespaced under /portfolio/ so they inherit the existing pieAuthOk
 *   gate automatically, zero new auth code.
 *   REGRESSION: MD5-verified byte-identical to v4.74 for all 8 protected functions and QEGate;
 *   diffed the full file — only the stage-5.7 hook (9 lines) and the new module + 2 routes were
 *   added; nothing else changed. Route count 57→59.
 *
 * Changelog v4.74 (10-Jul-2026): Server-side fundamentals fetch — retires client CORS-proxy chain.
 *   GAP (independent audit, 10-Jul-2026): index.html's fetchFundamentals() routed every
 *   Screener.in fundamentals request through a 4-hop public CORS proxy chain (allorigins.win →
 *   corsproxy.io → codetabs.com → thingproxy.freeboard.io). Confirmed in code: the chain had
 *   already grown from 1 proxy to 4 (changelog line ~6059) after prior breakage — a symptom of
 *   an unreliable dependency being patched with more of the same dependency, not fixed. Public
 *   proxies can rate-limit, inject content, or go dark with zero signal, and a silent fundamentals
 *   gap is indistinguishable from "Screener changed their HTML" — violates the evidence-first
 *   principle already applied to OHLCV (never blame external services without proof).
 *   FIX: new GET /fundamentals?symbol=X route fetches Screener.in directly from the Worker.
 *   CORS is a browser-enforced restriction — it does not apply to Worker-to-Screener requests —
 *   so no proxy of any kind is needed. Parsing uses the Workers-native HTMLRewriter streaming
 *   parser (zero bundle cost; Workers have no DOMParser) with the *same* selectors and label
 *   regexes the retired browser-side _parseScreenerHTML() used (#top-ratios li .name/.number,
 *   section#profit-loss table, last-two-column growth calc) — field-extraction logic is
 *   unchanged, only the parsing engine moved from DOM to streaming-HTML.
 *   ADDED: 20h KV cache (fund_v1:<symbol>) — fundamentals move quarterly, not intraday; cuts
 *   Screener.in load and gives graceful degradation on a transient Screener outage. Cache miss
 *   falls through to live fetch; live-fetch failure returns null, never a stale/wrong guess.
 *   SCOPE: purely additive — one new route (56→57), 4 new standalone functions
 *   (_parseScreenerViaRewriter, fetchScreenerFundamentalsServer, fetchFundamentalsWithCache,
 *   handleFundamentals), inserted after computeCalibrationProposal/handleCalibration, before
 *   export default. Zero lines changed above the insertion point — MD5-verified byte-identical
 *   to v4.73 for everything through handleCalibration, so all 8 protected functions and the full
 *   alert path are untouched. index.html's fetchFundamentals() still needs updating to call this
 *   route instead of the proxy chain — tracked as the next step, not yet deployed.
 *
 * Changelog v4.73 (09-Jul-2026): Decision Explorer — backend support (searchable list + longer history window).
 *   GAP: at 100+ symbols over 30 days, the flat alphabetical /decision/symbols list becomes
 *   unusable — no way to distinguish currently-held from long-sold symbols, no recency signal.
 *   FIX: /decision/symbols now returns a `holdings` array (rich objects: symbol, last_ts,
 *   last_decision, last_health, held) alongside the original `symbols` array (kept for
 *   back-compat) — one query, LEFT JOIN against qe_holdings for held-status, ORDER BY held DESC,
 *   last_ts DESC so active holdings sort first and closed ones sort by recency. No new table —
 *   derived entirely from qe_decision_log + qe_holdings.
 *   /decision/history default limit raised 20→30 (max unchanged at 100) so the frontend's
 *   Decision Journey/momentum calculations have a full month of daily runs by default.
 *   Both routes remain exactly one hardcoded parameterized query each — no SQL surface added.
 *
 * Changelog v4.72 (09-Jul-2026): Decision Intelligence Log — read-only query surface for index.html.
 *   GAP: qe_decision_log (v4.71) was queryable only via the D1 dashboard/connector — no way for
 *   the frontend to show "what did QuantEdge tell me about this stock" without raw SQL access.
 *   DECISION: rejected a free-text SQL box in index.html (public GitHub Pages page) — any route
 *   accepting arbitrary SQL against a real-money production D1 database is a standing risk
 *   (accidental DELETE/UPDATE, injection surface) for a UX convenience that doesn't need it.
 *   FIX: two new read-only routes, each exactly one hardcoded, parameterized query — symbol is
 *   always bound as a parameter, never concatenated, so there is no SQL injection surface even
 *   in principle:
 *     GET /decision/symbols            → distinct symbols with decision history
 *     GET /decision/history?symbol=X   → up to `limit` (default 20, max 100) rows for that
 *                                         symbol, newest first, full reconstructable context
 *   Both gated by the same pieAuthOk() token check already used on /portfolio/* (F6 pattern).
 *   Zero writes possible through either route. Does not touch scoring, decision engine, report,
 *   or persistDecisionLog — pure read consumer of the v4.71 table.
 *
 * Changelog v4.71 (09-Jul-2026): Decision Intelligence Log — implementation gap closure (approved v4.68 architecture).
 *   GAP: generateExecutiveDecisionReport() computed decision, confidence, evidence, reversal
 *   conditions and recommended action per holding, but persisted none of it — Telegram was the
 *   only record. Approved v4.68 design required: Executive Decision Report → Decision
 *   Intelligence Log (D1) → Pipeline Audit. The log stage was never built.
 *   FIX: new table qe_decision_log (D1-first, PRAGMA-verified before this code deployed) —
 *   append-only, no UPDATE/overwrite path, one immutable row per holding per pipeline run.
 *   Captures: run_id, ts, symbol, snapshot_id (linked to same-day qe_portfolio_snapshot),
 *   decision, verdict, health_score, recommendation_confidence, data_confidence,
 *   conviction_trend, active_pillars, strongest/weakest pillar, full pillar snapshot,
 *   triggers, evidence_summary, what/why_changed, full (untruncated) reversal_conditions,
 *   recommended_action, portfolio_health, portfolio_top3_pct, concentration_flag,
 *   engine_version, ltp, r_multiple — sufficient to fully reconstruct any past recommendation
 *   without Telegram.
 *   ARCHITECTURE: new function persistDecisionLog(env, run_id, holdings_with_decisions, agg)
 *   is a pure CONSUMER of generateExecutiveDecisionReport()'s output — does not touch scoring,
 *   the decision engine (mapVerdictToDecision/computeRecommendationConfidence/etc.), or the
 *   report text itself. generateExecutiveDecisionReport() additively returns
 *   {holdings_with_decisions, agg} (previously discarded internal state) so the log has zero
 *   recomputation and zero drift risk from the report. New pipeline stage 5.6, inserted between
 *   the Decision Report (5.5) and Pipeline Audit (6), wrapped in try/catch — a logging failure
 *   cannot block scoring, persistence, digest, or the Telegram report (all already completed).
 *   Protected functions and alert path (sendTelegram, dispatchPortfolioAlert,
 *   dispatchPortfolioDigest) untouched — MD5-verified byte-identical.
 *
 * Changelog v4.70 (09-Jul-2026): Executive Decision Report — Telegram delivery bug fix.
 *   ROOT CAUSE (proven via manual /portfolio/run trigger + ntfy/Telegram delivery diff):
 *   generateExecutiveDecisionReport() built its Telegram message with parse_mode:"HTML"
 *   but two literals broke HTML entity parsing: (1) unescaped "&" in "Holdings & Decisions",
 *   (2) unescaped "<" in "reach <75%". Telegram's Bot API rejects the ENTIRE message on any
 *   entity parse error (HTTP 400). sendTelegram() fires sendNtfy() first, independently, so
 *   ntfy delivered the report fine while Telegram silently failed — confirmed by comparing
 *   against dispatchPortfolioDigest(), which has no such literals and delivered successfully
 *   via both channels the same run.
 *   COMPOUNDING BUG: generateExecutiveDecisionReport() never checked sendTelegram()'s return
 *   value — it hardcoded {sent:true,...} regardless of actual delivery, so even the pipeline
 *   audit log (qe_pie_pipeline_last_run) would have shown a false positive.
 *   FIX: escaped both literals (&amp;, &lt;); captured sendTelegram()'s real boolean into the
 *   returned {sent,...} so future failures are visible in the audit log, not masked.
 *   Scope: 3 lines changed inside generateExecutiveDecisionReport() only. No other Telegram
 *   senders audited/touched — flagged as a separate, unconfirmed risk (dynamic symbol/evidence
 *   text could theoretically contain HTML-special chars in future holdings; not an issue with
 *   current portfolio). Protected functions and alert path untouched.
 *
 * Changelog v4.69 (09-Jul-2026): Manual trigger for full portfolio pipeline (additive, ops-only).
 *   GAP: only /portfolio/refresh (P0 ingest) and the 16:15 IST cron could exercise
 *   runPortfolioPipeline() — the function that runs stage 5.5 (Executive Decision
 *   Report). No on-demand way existed to validate v4.68 without waiting for the cron.
 *   FIX: added POST /portfolio/run — calls the exact same runPortfolioPipeline(env)
 *   the cron calls (refresh→intel→digest→decision_report→audit). Already covered by
 *   the existing F6 pieAuthOk gate on all /portfolio/* routes. Zero new logic; one
 *   route line. runPortfolioCycle() and the cron branch are untouched.
 *
 * Changelog v4.68 (09-Jul-2026): Executive Decision Intelligence Report (additive, product-driven).
 *   PRODUCT GAP: verdicts (STRONG_HOLD, WATCH, REDUCE) are descriptive; decisions (HOLD, REDUCE,
 *   SELL) are prescriptive. Existing PIE emitted verdicts with no decision guidance or action path.
 *   ADDED: generateExecutiveDecisionReport(env) + decision mapping engine. For every holding:
 *   (1) maps verdict+health+pillars+triggers → decision (STRONG_BUY through EXIT_IMMEDIATELY),
 *   (2) computes recommendation_confidence (separate from data_confidence; 50–95%),
 *   (3) generates evidence summary (pillar narrative), reversal thresholds, risk narrative,
 *   (4) recommends actionable next step. Portfolio-level: concentration analysis, diversification
 *   assessment, capital deployment observations. Report answers ONE question per holding:
 *   «What should I do today?» Output: single per-day Telegram message (replaces digest; includes
 *   both state + decision). Enables portfolio review in <60 seconds with clear action list.
 *   No changes to PIE scoring, protected functions byte-identical (MD5-verified).
 *   Pipeline now: refresh → intelligence → persist → alerts → digest → decision → audit → exit.
 *   Ordered sequential await ensures decision always reads persisted verdicts after confidence
 *   gates, no race. Digest continues as shadow-state summary (informational);
 *   decision report is executive intelligence (actionable). Both independent consumers.
 *
 * Changelog v4.67 (07-Jul-2026): PIE daily pipeline orchestration (Path 1, additive).
 *   PRODUCT GAP: event-alerts (change-only) fired, but the complete end-of-day portfolio
 *   summary (dispatchPortfolioDigest) was built yet never wired — quiet days = near silence.
 *   ADDED: runPortfolioPipeline(env) — ordered single-responsibility stages: refresh →
 *   intelligence → persist → event-alerts (in-engine) → executive digest → audit → exit.
 *   Event-alert publisher and digest publisher are two INDEPENDENT consumers of the same
 *   persisted qe_holdings state. 16:15 cron repointed runPortfolioCycle → runPortfolioPipeline.
 *   Scoring engine + 8 protected functions byte-identical (MD5-verified). Alert path
 *   (runPortfolioIntelligenceShadow/piePersist/dispatchPortfolioAlert) byte-identical — event
 *   alerts behave exactly as before. runPortfolioCycle retained (independently callable).
 *   ROADMAP (Path 2, NOT this release): extract dispatchPortfolioAlert out of piePersist into a
 *   standalone Event Alert Publisher stage for full stage atomisation — deferred (refactor, no
 *   immediate customer value). The orchestrator is the seam that makes that extraction clean later.
 *
 * Changelog v4.66 (06-Jul-2026): PIE HOTFIX — indicator return-shape mismatch (production incident).
 *
 * Changelog v4.66 (06-Jul-2026): PIE HOTFIX — indicator return-shape mismatch (production incident).
 *   ROOT CAUSE: pieTechFromSeries consumed pipeEma/pipeRsi/pipeAdx/pipeAtr (scalars) and
 *   pipeSupertrend ({direction,value}) as if they were arrays (x&&x.length?x[x.length-1]:null).
 *   Scalars/objects have no .length -> every indicator coerced to null -> EMA/RSI/ADX/SUPERTREND
 *   UNCOMPUTED on EVERY holding regardless of valid OHLC -> trend pillar 0, confidence pinned at 45
 *   (price20+bars15+corp10, all technical adds skipped) -> spurious REDUCE/EXIT shadow verdicts.
 *   FIX: consume scalars directly (isFinite guard); read supertrend via .value/.direction. SCOPE:
 *   pieTechFromSeries ONLY. No Buy-Engine change, no scoring redesign, no confidence/gate/F1 change.
 *   8 protected functions byte-identical (MD5-verified). Gate/F1/ordering/qty items intentionally deferred.
 *
 * Changelog v4.65 (06-Jul-2026): RC1 PRE-RELEASE REMEDIATION (review findings F1-F4,F6,F7,F11).
 *   F1 health renormalises over pillars with live inputs (no silent neutral-pinning).
 *   F2 holding OHLCV live-fetch fallback (reuses pipeFetchNCandles) when scan-cache is thin.
 *   F3 R-multiple from actual cost basis; days_held from earliest signal. F4 conviction_trend persisted.
 *   F6 auth gate (PIE_API_TOKEN) on all /portfolio routes. F7 material failure paths now logged.
 *   Additive; buy engine + protected functions byte-identical.
 *
 * Changelog v4.64 (06-Jul-2026): PORTFOLIO INTELLIGENCE ENGINE — P5 (Calibration SCAFFOLD).
 *   computeCalibrationProposal replays outcomes → PROPOSES config (human-gated, never auto-applies).
 *   GET /portfolio/calibration. INSUFFICIENT_DATA until production history accrues. Protected fns identical.
 *
 * Changelog v4.63 (06-Jul-2026): PORTFOLIO INTELLIGENCE ENGINE — P4 (Claude Enrichment Routes).
 *   GET /portfolio/holding|memory|risk (read); POST /portfolio/claude-note writes ONLY claude_note/
 *   claude_flag (schema-enforced Claude/engine boundary). Additive, protected functions byte-identical.
 *
 * Changelog v4.62 (06-Jul-2026): PORTFOLIO INTELLIGENCE ENGINE — P3 (Portfolio Risk + Digest).
 *   computePortfolioRisk -> qe_portfolio_snapshot (concentration, sector exposure, weighted health).
 *   Daily digest reuses PIE_ALERTS_ENABLED. Additive, protected functions byte-identical.
 *
 * Changelog v4.61 (06-Jul-2026): PORTFOLIO INTELLIGENCE ENGINE — P2 (Explainability + Alerts).
 *   Evidence bundle + what-changed/why/monitor-next triad; verdict-change Telegram alerts with
 *   hysteresis; SEPARATE flag PIE_ALERTS_ENABLED (default OFF). Thresholds in PIE_CONFIG.alerts.
 *   Additive, flag-gated, protected functions byte-identical.
 *
 * Changelog v4.60 (06-Jul-2026): PORTFOLIO INTELLIGENCE ENGINE — P1 (Deterministic Scoring, SHADOW).
 *   5-pillar Health Score + Data Confidence GATE + hard triggers + INSUFFICIENT_DATA + Portfolio Memory.
 *   Params externalized to versioned PIE_CONFIG (PROVISIONAL). Shadow-only: no alerts. Claude-free score.
 *   Additive, flag-gated, protected functions byte-identical. New table: qe_holding_history.
 *
 * Changelog v4.59 (06-Jul-2026): PORTFOLIO INTELLIGENCE ENGINE — P0 (Holdings Ingest & Visibility).
 *   Additive, feature-flagged (PORTFOLIO_INTEL_ENABLED, default OFF), zero Buy-Engine impact.
 *   Adds: getPortfolio(holdings+positions) ingest → qe_holdings (D1) · GET /portfolio/status|refresh.
 *   Data-only: NO scoring/verdict/alerts (P1+). Protected functions byte-identical. See Build Manifest.
 *
 *
 * Changelog v4.58 (30-Jun-2026): DUAL BREAKOUT DETECTOR — Q1/Q2/Q3 follow-ups (operator-approved).
 *   Q1(B) forming-bar VOLUME-PACE projection, KV-gated FORMING_VOL_PACE (DEFAULT OFF -> live behavior is
 *   unchanged = option A). New projectFormingVolume() scales ONLY the forming run's last-bar volume up by
 *   the inverse elapsed-session fraction (NSE 09:15-15:30 IST) and feeds the result to QEGate.evaluate as
 *   INPUT — the protected function is byte-identical; only its candle input is adjusted, and only when the
 *   flag is on AND the run is forming AND the last bar is today. KV read happens once/run and only on
 *   forming runs. Display CMP/volume (from ohlcvMap) untouched. Flip FORMING_VOL_PACE='on' to activate.
 *   Q3 forming runs now skip dedup (skipDedup: isManual || isForming) so the 14:00 run re-scores the 11:15
 *   names on a more-formed bar; confirmed/discovery keep dedup ON. isForming via scanModeOf(label) (Step C
 *   single source of truth). Q2 (retire old 09:30/12:00/14:30 runs) is a deliberate ~1-week observation
 *   window — NO code change; both schedules run, separable via scan_mode='discovery'. All 8 protected
 *   functions + weekly buildUniverse trigger untouched.
 *
 * Changelog v4.57 (30-Jun-2026): DUAL BREAKOUT DETECTOR — STEP C (label unify + scan_mode column).
 *   UNIFY: scan-mode is now derived in ONE place — scanModeOf(label) returns "forming" | "confirmed" |
 *   "discovery" (legacy 09:30/12:00/14:30 + MANUAL + HTTP) — and scanModeTag(mode) maps it to the Telegram
 *   header tag. Both the consolidated signal tag AND the new D1 column draw from these helpers; the two
 *   scattered inline derivations added in v4.55/v4.56 are removed. runFullPipeline derives scanMode ONCE
 *   (right after label) and reuses it at the dispatch call site and the forward-track write. Telegram output
 *   is byte-identical for every run: scanModeTag("discovery") = "" (== the old null path); forming/confirmed
 *   tags unchanged.
 *   MEASUREMENT: qe_forward_track gains a scan_mode TEXT column (added to D1 out-of-band before this deploy),
 *   bound on every snapshot row (?21), so realized out-of-sample outcomes can be split by detector type
 *   (forming vs confirmed vs discovery). UPSERT also carries scan_mode=excluded.scan_mode. One extra bound
 *   value; no change to verdict/score/threshold/filter/ranking/dispatch. All 8 protected functions and the
 *   weekly buildUniverse trigger untouched. Steps A+B+C complete (forming-volume treatment Q1 still open).
 *
 * Changelog v4.56 (30-Jun-2026): DUAL BREAKOUT DETECTOR — STEP B (intraday forming scans, SCAFFOLD).
 *   Two new scheduled() branches — 11:15 IST (05:45 UTC) and 14:00 IST (08:30 UTC), Mon-Fri — route the
 *   SAME pipeline (runPipelineWithSummary -> runFullPipeline). Because the QE gate (Stage 9.5) already
 *   re-fetches LIVE candles via pipeFetch2yCandles and QEGate.evaluate scores them WITHOUT the forming-bar
 *   guard (that guard lives only in pipeComputeIndicatorsFromCandles, which the gate does not call), these
 *   runs ALREADY score on today's still-forming bar. Output tagged "🟡 INTRADAY BREAKOUT · forming" via a
 *   second scanMode value ("forming") on the SAME pipeDispatchTelegram hook added in v4.55. Purely additive:
 *   +2 cron branches, +1 modeTag branch, +1 scanMode branch. Both new labels are unique substrings, so every
 *   existing run (09:30/12:00/14:30 intraday, 16:15 confirmed, MANUAL, HTTP) is byte-identical. Cron matched
 *   in BOTH named (MON-FRI) and numeric (2-6) forms; 1-5 NOT matched (= Sun-Thu under Cloudflare 1=Sun..7=Sat).
 *   All 8 protected functions and the weekly buildUniverse trigger untouched.
 *   OPEN DECISION (NOT built — handed to operator): intraday-volume treatment. The forming bar's partial
 *   accumulated volume collapses the gate's volume sub-score (the Bug-A effect, now inside the gate), so the
 *   11:15 run under-fires on volume. Options: (A) leave as-is — forming = provisional, confirmed at 16:15;
 *   (B) time-of-day volume-pace projection; (C) relax the gate's volume component in forming mode only. Each
 *   is a behavior change on the live signal path with a different false-positive profile — left for approval.
 *   Step C (label unify + scan_mode column on qe_forward_track) still deferred.
 *
 * Changelog v4.55 (30-Jun-2026): DUAL BREAKOUT DETECTOR — STEP A (post-close confirmed scan).
 *   New scheduled() branch for 16:15 IST (10:45 UTC, Mon-Fri) routes the SAME SSOT/D1-first
 *   discovery + QE-gate pipeline used by the intraday runs (runPipelineWithSummary -> runFullPipeline
 *   -> QEGate.evaluate). Firing AFTER 15:45 IST means pipeComputeIndicatorsFromCandles includes
 *   TODAY's now-completed daily bar, so the gate scores the CONFIRMED close — identical logic to the
 *   manual /score gate. Dispatched signals are tagged "🟢 DAY-CLOSE BREAKOUT · confirmed" via a new
 *   optional scanMode arg on pipeDispatchTelegram (undefined for every existing caller -> byte-identical
 *   for the 09:30/12:00/14:30 intraday runs, MANUAL, and HTTP triggers). Cron string matched in BOTH the
 *   named (MON-FRI, as specified) and proven-working numeric (2-6) forms so the branch cannot silently
 *   no-op on either spelling; 1-5 deliberately NOT matched (= Sun-Thu under Cloudflare's 1=Sun..7=Sat).
 *   Purely additive: +1 cron branch, +1 optional param, +1 header tag, +1 scanMode derivation at the call
 *   site. The weekly buildUniverse trigger (0 3 * * 1) and all 8 protected functions are untouched.
 *   Steps B (intraday forming-bar runs at 11:15/14:00) and C (label unify + scan_mode on qe_forward_track)
 *   are deferred pending approval.
 *
 * Changelog v4.45 (23-Jun-2026): BACKFILL DATE-SHIFT FIX (one line, surgical). PROVEN
 *   root cause of the post-backfill corruption: d1WriteCandles derived bar_date via
 *   d.toISOString().slice(0,10). Kite's daily candle timestamp c[0] is an IST ISO string
 *   ("2026-06-19T00:00:00+0530"); toISOString() converts to UTC, so 00:00 IST became
 *   18:30 the PREVIOUS day and EVERY backfilled bar was written one calendar day early.
 *   Evidence: D1's "2026-06-18" RELIANCE bar held the real 19-Jun session (c/h/l exact
 *   match to NSE) — weekday sessions spilled onto weekends (phantom Sat/Sun bars), and
 *   real trading days (19-Jun) were left empty. The daily-append path was never affected
 *   (it uses today's date directly). Fix: derive bar_date straight from the IST string
 *   (numeric epoch shifted to IST first). NOTE: existing rows are already corrupted —
 *   wipe ohlcv_daily and re-run /d1/startbackfill on the fixed worker, then /d1/verify.
 *   No other code touched; pipeline/verdict/append/Fix-2 paths byte-identical.
 *
 * Changelog v4.44 (23-Jun-2026): TOKEN-FRESH REFRESH (Fix-2). Root cause of the silent
 *   16→22 Jun cache freeze: Zerodha flushes the Kite access token every morning AND the
 *   Kite app re-auth kills the Connect session mid-morning, so the fixed 16:00 daily D1
 *   append (handleD1Update) ran on a dead token, 403'd, wrote 0 rows — and because the
 *   bad-token quote path returns ok:true while the 16:00 cron DISCARDED the result, it
 *   rotted for days with no alert. Fix is additive plumbing only; NO verdict/score/
 *   ranking/Monte-Carlo/gate/detector/signal-dispatch change:
 *   1. runTokenFreshRefresh(): appends today's bar on the HOT token (just-minted at login)
 *      and writes ONE source-of-truth freshness stamp (KV qe_last_refresh: last_bar_date,
 *      refreshed_at IST, symbols).
 *   2. /callback runs the refresh right AFTER the token is stored (store-first, so login
 *      still succeeds if the refresh fails), shows the "data as of" stamp on the success
 *      page, and sends a checkmark Telegram confirmation.
 *   3. 16:00 cron: handleD1Update -> runTokenFreshRefresh + a GENTLE "tap to refresh" nudge
 *      (checkStaleAndAlert) when the cache isn't current. 20:00 IST (14:30 UTC) LOUD backstop
 *      gated inside the all-hours 10-minute cron. Mirrors the existing Sunday-rebuild alert pattern.
 *   4. NEW GET /refresh/status — exposes qe_last_refresh for the in-app "Data as of" line.
 *   All previously protected functions byte-identical; every existing route untouched.
 *
 * Changelog v4.43 (18-Jun-2026): FORWARD-TRACK SELF-HEAL (measurement-persistence fix;
 *   no verdict/score/threshold/filter/ranking/dispatch change; all named protected
 *   functions byte-identical).
 *   BUG: qe_forward_track used INSERT OR IGNORE on UNIQUE(snapshot_date,symbol), so the
 *   FIRST run of a day claimed each row. When an early run fell back to 1y data (stale
 *   token map) and a corrected 2y run followed the same day, the 2y verdict was silently
 *   discarded — the day's record froze at 1y and no amount of re-running could fix it.
 *   FIX: INSERT … ON CONFLICT(snapshot_date,symbol) DO UPDATE to the latest run's values,
 *   guarded by "WHERE NOT (existing=2y AND incoming=1y)" so a later stale run can never
 *   downgrade good 2y data. A re-run now upgrades 1y→2y automatically. QE_VERSION 4.42→4.43.
 *
 * Changelog v4.42 (18-Jun-2026): NTFY ACCOUNT AUTH (additive only; no verdict/score/
 *   threshold/filter change; all named protected functions byte-identical).
 *   ROOT CAUSE (proven via /ntfy/test): anonymous publishes to ntfy.sh are rate-limited
 *   per source IP; Cloudflare Workers egress from a SHARED IP pool, so that IP's free
 *   daily quota is exhausted by unrelated Workers traffic → HTTP 429 (code 42908). Fix:
 *   authenticate. sendNtfy + handleNtfyTest now read optional KV NTFY_TOKEN and, when
 *   present, send "Authorization: Bearer <token>" so ntfy counts the message against the
 *   account tier (own quota) instead of the shared IP. Token ABSENT → byte-for-byte the
 *   v4.41 anonymous behaviour (pure no-op until a token is set). /ntfy/test also reports
 *   ntfy_token_present + length (value never echoed). QE_VERSION 4.41→4.42.
 *
 * Changelog v4.41 (18-Jun-2026): NTFY DIAGNOSTIC + HARDENING (additive only; no verdict/
 *   score/threshold/filter change; all named protected functions byte-identical).
 *   • sendNtfy now trims/normalises NTFY_ENABLED ("true " / "TRUE" no longer disable it
 *     silently) and trims NTFY_TOPIC. Clean values behave exactly as before.
 *   • New read-only GET /ntfy/test (handleNtfyTest): reports NTFY_* presence + raw/trimmed
 *     lengths (exposes hidden whitespace), attempts a live ntfy.sh POST, returns the HTTP
 *     result, and sends a real test push. Diagnostic only — touches no pipeline logic.
 *   QE_VERSION 4.40→4.41.
 *
 * Changelog v4.40 (17-Jun-2026): RESILIENCE + VISIBILITY + MEASUREMENT (additive only; no
 *   verdict/score/threshold/filter change; all named protected functions byte-identical).
 *   • B (Manual=Kite/D1 + source transparency): NO CODE CHANGE — verified already implemented.
 *     Browser manual scan already routes through the worker ?symbol= NSE path (Kite-primary,
 *     Yahoo only on Kite failure), and the card already renders a KITE ✓ / YF source badge off
 *     meta.dataSource. Decision #1 was already satisfied; adding tags/labels would have been
 *     redundant dead code (and would have broken string-keyed 'kite'/'yf' logic), so reverted.
 *   • A (ntfy dual-send): new isolated sendNtfy(env,text), called at the TOP of sendTelegram,
 *     gated by KV NTFY_ENABLED='true' + NTFY_TOPIC. Independent channel — fires regardless of
 *     Telegram creds/outcome; can never throw/delay-fail/alter the Telegram path. Telegram stays
 *     primary and its return value is byte-identical.
 *   • C (S5 visibility): new read-only GET /pipe/rejects (handlePipeRejects) splitting S5 volume
 *     near-miss (reason "Volume ratio…") from other tech rejects; plus a near-miss line appended
 *     to the pipeline summary. Pure reads of the existing qe_pipe_survivorship log.
 *   • E1 (Commit E snapshot — MEASUREMENT ONLY): additive D1 table qe_forward_track + one snapshot
 *     row per candidate verdict written after the qe-gate audit. Captures edgeClass/freshBreakout/
 *     expSE additively onto cand.qe (read off the same r from QEGate.evaluate). No learning, no
 *     promotion, no tuning, no verdict/score/threshold change. INSERT OR IGNORE → idempotent/day.
 *   QE_VERSION synced 4.26→4.40 (was stale) so root route + Telegram footer prove the deploy.
 *
 * Changelog v4.39 (16-Jun-2026): LAYER DIFFERENTIAL ROUTE (Phase 1 — READ-ONLY, evidence only).
 *   New QEGate.evaluateDiff(rawCandles,regimeStr,rsScore) — reuses evaluate() UNCHANGED to get the
 *   production verdict, reconstructs the stock object from evaluate's OWN output (asserts the
 *   reconstructed verdict === production verdict as a fidelity self-check), then re-runs finalDecision
 *   on CLONES with ONE layer neutralized at a time: MC-veto (mc=null), Pro Filter (score=baseScore,
 *   isRejected=false), Elite (execution.action='ENTER'). The mode consts (_proFilterMode/_eliteMode)
 *   are NEVER reassigned. New read-only route GET /diff/layers?offset=0&limit=120&regime=neutral
 *   iterates the D1 universe and tallies how many verdicts each layer flips + direction
 *   (production→layer-off) + examples. NO writes, NO Telegram, NO scoring/ranking/verdict change.
 *   evaluate/finalDecision/scoring/computeExecutionDecision/applyProFilter all byte-identical to v4.38;
 *   IIFE now also exports evaluateDiff. Purely additive.
 *
 * Changelog v4.38 (15-Jun-2026): /backtest/windows RUNTIME FIX (scoping only — no calc/verdict change).
 *   handleBacktestWindows (top-level) called stockBacktest/tradeStats/edgeConfidence directly, but those
 *   are QEGate-IIFE-private → ReferenceError at runtime (route returned {ok:false}). Fix: export those
 *   three from the IIFE and qualify the three calls as QEGate.*. Identical functions, identical math;
 *   no scoring/ranking/verdict change. evaluate/breakoutDebug exports unchanged.
 *
 * Changelog v4.37 (15-Jun-2026): BREAKOUT DEBUG ROUTE (read-only, visibility only).
 *   New QEGate.breakoutDebug(C,H,L,V) — a verbose, non-short-circuiting mirror of detectFreshBreakout
 *   that emits every criterion value and asserts a `match` against the REAL detector. New read-only
 *   route GET /breakout/debug?symbols=A,B,C&max=5&back=N returns per-symbol criterion tables + the
 *   real gate verdict (QEGate.evaluate). detectFreshBreakout/finalDecision/scoring/verdict UNCHANGED;
 *   no KV/D1 writes, no Telegram. IIFE now also exports breakoutDebug (evaluate untouched).
 *
 * Changelog v4.36 (15-Jun-2026): TOKEN-MAP AGE GUARD (Commit S2 — operational, no verdict change).
 *   New tokenMapAgeGuard(env) reads qe_db_universe_ts (last successful-rebuild timestamp) and sends a
 *   🔧 OPS ⚠️ alert if the universe/token map is older than the 7-day refresh cycle (TTL 8d). Piggybacked
 *   on the all-day 10-minute cron, gated to run once/day at 12:00 UTC. buildUniverse is UNCHANGED (it already
 *   writes qe_db_universe_ts). Purely additive; finalDecision/scoring/ranking/signal-dispatch untouched.
 *
 * Changelog v4.35 (15-Jun-2026): SUNDAY REBUILD FAILURE ALERT (Commit S1 — operational, no verdict change).
 *   The weekly universe rebuild (cron 0 3 * * 1) was fire-and-forget; buildUniverse returns {ok:false}
 *   without throwing, so a silent Sunday failure showed "Success" while qe_db_token_map rotted (≤1-day
 *   margin → pipeline silently drops to 1y). Now the rebuild result is awaited: 🔧 OPS ⚠️ alert on
 *   failure (or thrown exception), 🔧 OPS ✅ success ping (muteable via KV qe_ops_ping=off). Only the
 *   scheduler's 0 3 * * 1 branch changed; finalDecision/scoring/ranking/RS/MC/signal-dispatch untouched.
 *
 * Changelog v4.34 (15-Jun-2026): FRESH-BREAKOUT ADMISSION (Commit D — mirror of index.html v41).
 *   New detectFreshBreakout(C,H,L,V) flags a stock only when ALL structural criteria hold (long
 *   tight base, volume-expansion breakout clearing the base high, near 250-bar high, EMA20>EMA50 &
 *   price>EMA200 & EMA20 rising). finalDecision Rule 1: proven-negative is now an absolute IGNORE
 *   checked first (invariant); an MC-vetoed fresh breakout that also clears score>=60 + reachable
 *   is routed to WATCH (WATCH_FRESH_BREAKOUT), never BUY. Gate stock/out carry freshBreakout.
 *   Non-fresh stocks behave identically to v4.33. A-priori thresholds, not tuned.
 *
 * Changelog v4.33 (15-Jun-2026): CONFIDENCE-AWARE EDGE GATE (Commit C — mirror of index.html v40).
 *   finalDecision Rule 1 splits the binary expectancy<0 gate by edgeClass: PROVEN_NEGATIVE (or
 *   unknown class) stays hard-IGNORE (safety invariant); INDETERMINATE negative routed to WATCH at
 *   new Rule 3c (only if it clears score+data). Positive-expectancy stocks untouched → no BUY lost.
 *   Gate stock.bt now carries edgeClass so finalDecision can read it. Only transition: IGNORE→WATCH.
 *
 * Changelog v4.32 (15-Jun-2026): WINDOW-COMPARISON CAPABILITY (Commit B — read-only, evidence only).
 *   Adds pipeFetchNCandles() (generalized N-day fetch; production pipeFetch2yCandles UNCHANGED) and
 *   a read-only route GET /backtest/windows?symbols=A,B,C&max=5 that runs the existing backtest
 *   chain (stockBacktest→tradeStats→edgeConfidence) on 2y/3y/5y slices and returns a comparison.
 *   Production window stays 2y. No verdict/score/selection/Telegram/scheduler change. No KV/D1 writes.
 *
 * Changelog v4.31 (15-Jun-2026): EDGE-CONFIDENCE INSTRUMENTATION (Commit A — report-only;
 *   pairs with index.html v39). Adds edgeConfidence() + two bt fields expSE (std of per-trade
 *   R / sqrt(n)) and edgeClass (PROVEN_POSITIVE/INDETERMINATE/PROVEN_NEGATIVE) surfaced in the
 *   gate `out` object. NO verdict / score / ranking / gate consumes these — measurement only,
 *   foundation for the confidence-aware gate (Commit C). finalDecision untouched. Diff 0/0/0.
 *
 * Changelog v4.30 (15-Jun-2026): PARITY + UNIFICATION (pairs with index.html v38).
 *   1. F2 symmetric MC veto in finalDecision — mirror of browser: any MC<30% vetoes unless
 *      expectancy>=0.2R (removes the mcZero-only small-sample rescue). NAM-INDIA-type BUYs gone.
 *   2. RS parity — applyRSAdjustment ported verbatim from index.html; evaluate(rawCandles,
 *      regimeStr, rsScore) now bumps the gate score by the same RS rule so the cron score
 *      matches the card (e.g. ANGELONE 79->87). finalDecision unchanged (SKIP status is
 *      independent of the RS bump).
 *   3. Unified alert gate — pipeDispatchTelegram + metricsLog dispatch BUY iff QE verdict BUY
 *      AND RS-adjusted score>=70, replacing the DS/ST/ADX integrity gate (which wrongly held
 *      ANGELONE — BUY, score 87, ADX 13 — as WATCH_ONLY). One rule on every path.
 *
 * Changelog v4.29 (15-Jun-2026): /gtt/create response now ECHOES the stored sl/t1 (sl_stored,
 *   t1_stored + in the message) so the UI can prove the stop was persisted, not just shown.
* Changelog v4.28 (15-Jun-2026): STOP-LOSS PLUMBING FIX. /gtt/create now REFUSES any entry
 *   with no valid sl (no stop -> no entry; a naked position is impossible). Also caps the
 *   "STOP NOT ARMED" alert to fire once then retry silently (was re-alerting every cron tick).
 *   Root cause of 15-Jun ABDL naked fill was client-side (gttConfirmPlace dropped sl/t1/t2);
 *   fixed in index.html v36. This is the server fail-safe + de-spam.
 * Changelog v4.27 (14-Jun-2026): CONFIRMED-EDGE WATCH TIER (server parity with index.html
 *   v33). _edgeWatchKind: a strong validated edge (expectancy>=0.2R, >=5 sig, not MC-vetoed)
 *   blocked ONLY by a timing overlay (EXTENDED_SEVERE / entryUnreachable) → finalDecision
 *   returns WAIT and computeExecutionDecision returns WAIT, instead of hard IGNORE/SKIP. So
 *   the cron + manual-pipeline gate verdict now matches the browser manual scan + load-and-
 *   analyse for stocks like SILVERTUC/APOLLO. STRUCT_FAIL/BREAKDOWN never qualify. SAFETY:
 *   gate pass keys on label==='BUY' (line out.pass), and WATCH!=='BUY', so ZERO BUY signals
 *   or Telegram dispatches change — only the recorded verdict (IGNORE→WATCH) for these names.
 *   Kill switch: QE_EDGE_WATCH=false.
 *
 * Changelog v4.22 (13-Jun-2026): MANUAL-trigger close fallback. When a manual run
 *   hits volume==0 (non-trading day/holiday), restore last session VOLUME from D1
 *   last bar so Stream A Fast replays it instead of rejecting on zero volume.
 *   Manual-only (gated by MANUAL label); crons untouched/live. Additive; widens only.
 *   Also carries v4.21 (segment filter drops INDICES; rebuild cron 0 3 * * 1).
 * Changelog v4.26 (14-Jun-2026): FIX — trigger deadlock after a dead run.
 *   The /pipe/trigger status-guard blocked any non-finished phase for 35 MINUTES
 *   with no phase awareness, so a run that died at STARTING (client disconnect)
 *   left the status frozen and deadlocked every future manual trigger for 35 min.
 *   Now phase-aware: STARTING stale after 3 min, other running phases after 10 min
 *   (real pipeline is ~60-90s). Self-healing; the 180s qe_pipe_lock still guards
 *   genuine concurrent runs. No change to pipeline logic.
 * Changelog v4.25 (14-Jun-2026): DIAGNOSTIC — gate 2y-fetch fallback reason.
 *   pipeFetch2yCandles now returns {candles, reason}; the QE-gate logs per symbol
 *   WHY it used 1y instead of 2y (NO_TOKEN / HTTP_<status> / FEW_<n> / EXC_<msg>) in
 *   the S9B_QEGATE audit line ("2yfetch:..."). Pinpoints the score-divergence cause
 *   (browser 2y vs server 1y) with evidence. No behaviour change to scoring.
 * Changelog v4.24 (14-Jun-2026): REGIME single-source-of-truth. Publishes the
 *   pipeline regime to KV (qe_regime) + new GET /pipe/regime route, so the browser
 *   manual scan scores under the SAME structural regime as cron/discovery/QE-gate/
 *   Telegram (verified: gate uses pipelineRegime at QEGate.evaluate). Kills the last
 *   cross-surface score divergence (manual scan was computing its own Yahoo regime
 *   with an intraday override). Additive; +1 KV write/run.
 * Changelog v4.23 (13-Jun-2026): Manual-trigger RUN-LOCK — short-TTL KV lock
 *   taken before the heartbeat so a retried/duplicate /pipe/trigger GET cannot
 *   start a second run or send duplicate signals; STARTING removed from the
 *   OK-to-start states; QE_VERSION 4.20->4.23 (footer was stale). Carries
 *   v4.22 (manual close fallback) + v4.21 (segment filter, rebuild cron).
 * Changelog v4.20 (12-Jun-2026) — QE gate 2-year parity (Option B) + learning-ready capture.
 *   ROOT CAUSE (proven): browser backtests on 2 YEARS (fetchOHLCV default 2y, ~500
 *   bars); the gate used the pipeline 1y _candles. stockBacktest walks the whole
 *   series, so 1y vs 2y yields a different trade set -> different expectancy/MC ->
 *   opposite verdict. Evidence: APARINDS browser IGNORE (-0.907R/MC0%) vs gate PASS.
 *   FIX: pipeFetch2yCandles() pulls a true 2y window (same Kite endpoint/auth as
 *   pipeFetchOhlcvSymbol; raw candles). Gate evaluates on 2y; on fetch failure
 *   falls back to 1y _candles (never drops a signal). Data basis (2y|1y) recorded
 *   on cand.qe, per-symbol + DONE audit lines, per-signal Telegram line, and the
 *   persisted rows. Budget +<=40 calls (~690/1000), ~1-2s CPU.
 *   LEARNING-READY: persisted QE-gate audit now carries schema_version:2, entry/
 *   SL/T1/T2/lastClose, and basis per row — the prediction record a future learning
 *   layer compares to realized outcomes. NO auto-tuning (locked: needs >=30 trades).
 *
 * Changelog v4.19 (12-Jun-2026) — QE SCORE GATE (PRO+ELITE) + named summary + audit.
 *   1. QE GATE (Stage 9.5): browser QuantEdge engine ported VERBATIM from the LIVE
 *      index.html (7587-line upload) and run with PRO FILTER ON + ELITE ON — the
 *      user's exact production screen. Ported: helpers, signalEngine/tradeEngine/
 *      stockBacktest (walk-forward, trailing+breakeven, rolling equity), tradeStats,
 *      real-trade monteCarlo, applyProFilter (5 layers), computeExecutionDecision
 *      (Elite ENTER/WAIT/SKIP), finalDecision Rules 1-5 INCLUDING 4a/4b/4c Elite-SKIP
 *      rescue and 4d/3 baseScore handling. Gate PASS = app verdict BUY. Telegram now
 *      carries only stocks passing BOTH Discovery AND QE(PRO+ELITE).
 *   2. NAMED SUMMARY: every run reports total scanned, per-stage funnel counts (now
 *      incl. "QE gate (P+E)"), a named list of stocks passing Discovery, and a named
 *      list passing BOTH Discovery + QE, plus a QE reject tally. Sent on zero-pass
 *      days too (full breakdown, no silent days).
 *   3. AUDIT HISTORY: complete per-candidate gate decision (both verdicts, scores,
 *      Elite action, WR/EV/MC/BT, pro-filter reasons) persisted UNTRUNCATED to KV
 *      key qe_pipe_qegate + a dated 14-day rolling key. New route GET /pipe/qegate
 *      (?date=&run= for history). Per-symbol S9B_QEGATE lines also flow to the
 *      pipeline audit log. Per-signal Telegram line shows QE/base/pro/Elite/WR/EV/MC/BT.
 *   4. 09:15 cron routed through the instrumented wrapper (was a bare call).
 *   Kill switch KV QE_SCORE_GATE="off" → dispatch as v4.18. +KV reads/writes only,
 *   ZERO new fetches. Entry path (placeGTT single-leg) byte-identical.
 *
 * Changelog v4.18 (12-Jun-2026) — NO-DEFERRAL SWEEP: full-coverage D1 + version unification.
 *   1. D1 FULL COVERAGE (functional gap found by audit): the S3B history budget
 *      (150 cron / 60 manual) never consulted the D1 flag, so flipping
 *      USE_D1_CACHE=true would still scan only 150 symbols. The budget existed
 *      solely for Kite's historical rate limit + live-fetch CPU — constraints D1
 *      reads don't have. Now: when D1 is ON, budget lifts to the ENTIRE ranked
 *      pool (audit-logged). Subrequest math: ~600 D1 reads worst case + ~50 other
 *      ≈ 650 of 1,000 paid cap. Flag OFF -> behavior byte-identical to v4.17.
 *   2. LIVE-FALLBACK GUARD: with the full pool queued, a degraded/empty D1 must
 *      not fire hundreds of slow Kite historical calls — live fallbacks capped at
 *      PIPE_MAX_OHLCV_CAP (150, the old budget). Soft counter; concurrent batch
 *      may overshoot by at most PIPE_BATCH_SIZE (10). Exhausted -> symbol marked
 *      failed with explicit reason in survivorship, run continues.
 *   3. OBSERVABILITY: S4 DONE audit line now reports D1 hits + live fallbacks.
 *   4. VERSION UNIFICATION: root route reported hardcoded "4.4" and the Telegram
 *      footer "Server Pipeline v4.1" — both stale. Single QE_VERSION constant now
 *      feeds both; root route is finally a valid deploy indicator.
 *
 * Changelog v4.17 (12-Jun-2026) — SELF-AUDIT FIX: time-aware forming-bar guard.
 *   Adversarial audit of v4.16 (11-scenario run-time matrix) caught a regression
 *   BEFORE deployment: the v4.16 guard dropped today's bar UNCONDITIONALLY when
 *   the date matched, which is correct intra-market but WRONG after close — it
 *   would have staled every post-close run to yesterday's data (last night's
 *   20:46 run found 3 candidates precisely BECAUSE it used today's completed,
 *   full-volume bar). v4.16 was never deployed.
 *   FIX: drop today's bar only while it is still forming — before 15:45 IST
 *   (15:30 close + closing-session buffer). From 15:45 the bar is complete and
 *   is KEPT. Verified across 11 scenarios incl. open/midday/pre-close scans
 *   (drop), 16:00 summary and evening/late-night runs (keep), weekend/holiday/
 *   Monday-morning stale-bar cases (keep), IST midnight boundary (keep), and a
 *   defensive bogus future-dated bar (safe drop).
 *   Known benign window: 15:45-16:00 IST live already holds today's completed
 *   bar while D1 gains it at the 16:00 daily update, so /d1/verify inside that
 *   15-minute window can say REVIEW; no scheduled scan runs there. Verify after
 *   16:05 for a clean read.
 *   Audit battery: D1 16:00 update wired and flag-independent; freshness 6d and
 *   400-bar headroom confirmed; single compute chokepoint (3 call sites); cross-
 *   run signal dedup intact; token-failure path logs and reports; ZERO new
 *   subrequests (diff vs pre-fix baseline shows no added fetches); entry path
 *   (placeGTT single-leg) byte-identical.
 *
 * Changelog v4.16 (12-Jun-2026) — ROOT-CAUSE FIX: forming-bar pollution.
 *   Diagnosed two real bugs from D1 parity evidence (RELIANCE verify):
 *   BUG A (forming bar): Kite's day-historical endpoint returns TODAY'S still-
 *     forming bar during market hours. At the 09:30 scan its volume is ~zero, so
 *     volRatio (= lastBarVol / 20d-avg) collapses to ~0.05 and the hard volume gate
 *     (volRatio < 0.8 -> reject) rejected nearly every stock -> 131 fetched, 1
 *     passed, 0 candidates. PROVEN: two verify reads 12 min apart showed live
 *     lastClose/volRatio/rsi moving with the open session while D1 stayed fixed.
 *     This silently crippled EVERY market-open scan (post-close runs worked because
 *     the bar was complete). FIX: pipeComputeIndicatorsFromCandles drops today's bar
 *     (IST-aware date check on candle[0]) before computing -> indicators use only
 *     COMPLETED bars. Shared by live + D1 paths, so it also fixes parity (D1 stores
 *     only completed bars; live now matches). The separate live-price breakout
 *     monitor still catches intraday breakouts; the scanner finds setups on closed
 *     bars (matches documented workflow).
 *   BUG B (window mismatch): D1 read cutoff used bare PIPE_OHLCV_RANGE while the live
 *     fetch uses (PIPE_OHLCV_RANGE + 10). D1 dropped ~10 of the oldest bars live
 *     keeps -> different bar set -> recursive EMA chain shifted (candleCount 246 vs
 *     256, EMA ~0.3%). FIX: D1 cutoff now uses the identical +10.
 *   Together these drive live-vs-D1 to parity AND restore market-open candidates.
 *   Entry path (placeGTT single-leg) byte-identical. No scope creep.
 *
 * Changelog v4.15 (11-Jun-2026) — Cron-driven backfill (replaces self-fetch).
 *   Cloudflare blocks a Worker from fetching its own URL, so the v4.13 self-chaining
 *   autobackfill stopped after one leg. Replaced with a CRON-DRIVEN backfill: each
 *   cron tick processes one chunk (D1_CRON_CHUNK=70, ~23s at Kite 3/sec, safe under
 *   30s CPU) and advances a KV cursor (qe_d1_bf_offset); disarms + sends COMPLETE
 *   when the cursor passes the universe end. Armed via POST /d1/startbackfill,
 *   stopped via POST /d1/stopbackfill. Tick is a NO-OP unless armed (qe_d1_bf_armed).
 *   Wired into the 5-min market-hours cron AND a new all-hours 10-min cron
 *   (add in dashboard during backfill, remove when done) so a full-universe load
 *   finishes overnight. Reports progress to Telegram each tick. Idempotent writes;
 *   bounded (cursor only advances, disarms at end). Removed the broken
 *   handleD1AutoBackfill + its self-fetch chain.
 *
 * Changelog v4.14 (11-Jun-2026) — FIX: D1 backfill "Invalid time value".
 *   Diagnostics (v4.13) revealed the 40/40 failure cause: d1WriteCandles did
 *   `new Date(c[0] * 1000)`, but Kite historical candle timestamp c[0] is an ISO
 *   STRING ("2026-06-10T00:00:00+0530"), not Unix seconds. string*1000 = NaN →
 *   new Date(NaN).toISOString() threw "Invalid time value" on EVERY symbol. Now
 *   parses robustly (string or number), rejects null/junk/pre-2000, skips bad bars
 *   without throwing. (The live pipeline never hit this because it reads c[1..5] and
 *   ignores the timestamp.) Backfill now writes bars correctly.
 *   NOTE: the universe contains index names ("NIFTY 50") that can't return tradeable
 *   candles; these now fail gracefully (caught/skipped), not throw. Universe cleanup
 *   deferred (cosmetic — wastes a few backfill slots, no corruption).
 *
 * Changelog v4.13 (11-Jun-2026) — D1 backfill: diagnostics + self-chaining auto-run.
 *   1. DIAGNOSTICS on /d1/backfill: the catch block used to swallow error reasons
 *      (catch(e){fail++}), leaving "40/40 failed" unexplained. Now the response
 *      reports tokens_missing_in_slice, token_map_size, error_breakdown (reason→
 *      count), and sample_errors (first 5 "symbol: reason"). One run now shows WHY
 *      symbols fail instead of guessing.
 *   2. NEW /d1/autobackfill (POST, self-chaining): trigger ONCE; processes batches
 *      back-to-back within an ~18s wall-time budget, then schedules the next leg via
 *      ctx.waitUntil + self-fetch — automatically working through the whole universe
 *      with no manual clicking. Reports progress to Telegram each leg. Bounded:
 *      offset always advances; stops at universe end (no runaway). Idempotent writes
 *      mean a retried/overlapping leg can't corrupt data. Start: POST /d1/autobackfill
 *      (optionally ?offset=N&batch=30).
 *   3. fetch handler signature gains ctx (additive — async fetch(request, env, ctx))
 *      to enable background chaining. Existing routes unaffected.
 *   UNTOUCHED: indicator math, scoring, filters, entry placement, A1 monitor.
 *
 * Changelog v4.12 (11-Jun-2026) — D1 history cache (Option 2): break the Kite
 *   historical rate limit so the full liquid universe is analysable per run.
 *   PROBLEM: Kite throttles historical data ~3 req/sec/key, so one run fetched only
 *     ~60–90 symbols' 365-bar history regardless of budget (proven: queue 60 →
 *     fetched 43). Coverage capped far below the ~620 liquid candidates.
 *   FIX: store daily OHLCV bars in Cloudflare D1; replace the per-run historical
 *     fetch with a cheap daily bulk-quote (NOT rate-limited). Indicators compute
 *     from stored bars. The rate-limited call disappears from the hot path.
 *   ARCHITECTURE (parity-by-construction):
 *     - Indicator math EXTRACTED verbatim into pipeComputeIndicatorsFromCandles().
 *       Live path and D1 path feed identical candles into this ONE function.
 *     - d1ReadCandles windows D1 bars by the SAME calendar-date cutoff the live
 *       path uses (PIPE_OHLCV_RANGE days) — NOT a fixed bar count. CRITICAL: a
 *       fixed-count trim left EMA200 diverging ~0.16–0.44% (EMA is recursive;
 *       different bar counts = different seed chain), enough to flip emaStackBull.
 *       Calendar-cutoff windowing drives divergence to 0.0000000000 (verified).
 *   SAFETY (nothing breaks unflipped):
 *     - Gated behind KV flag USE_D1_CACHE (must be exactly 'true'); default OFF.
 *     - Requires D1 binding env.QE_DB; absent → everything falls back to live fetch.
 *     - d1ReadCandles returns null (→ live fallback) on: too few bars, stale data
 *       (>D1_FRESH_DAYS old), or ANY error. Live-fetch path fully preserved.
 *     - Idempotent writes (ON CONFLICT). Backfill is chunked + rate-limited.
 *   ROUTES (admin): POST /d1/init, /d1/backfill?offset&limit, /d1/update;
 *     GET /d1/status, /d1/verify?symbol=X (Phase-B parity go/no-go).
 *   CRON: daily bar appended on the 16:00 IST cron (independent of the flag, so the
 *     cache stays current while you verify before flipping it on).
 *   UNTOUCHED: scoring/ranking/RS/Stream B/filters/thresholds; entry placement;
 *     A1 stop-loss/monitor. Indicator MATH unchanged (extracted, not rewritten).
 *   GO-LIVE: bind QE_DB → /d1/init → /d1/backfill (repeat) → /d1/verify (must PASS)
 *     → set KV USE_D1_CACHE='true'. Revert instantly by setting it 'false'.
 *
 * Changelog v4.11 (11-Jun-2026) — Recall improvements (zero precision cost):
 *   Two asymmetric fixes — more winning stocks identified, no relaxation of any
 *   quality filter (a stock must still clear every S5/S6 gate to signal):
 *     1. Sector cap PIPE_MAX_SECTOR_N 3 → 5. Momentum winners cluster by sector
 *        (sector rotation), so a cap of 3 silently dropped already-qualified winners
 *        during strong sector moves — the exact regime this strategy trades. Raising
 *        to 5 surfaces MORE stocks that already passed every filter; admits zero
 *        lower-quality stock. Pure recall gain.
 *     2. Dedup on PASS, not on ANALYSIS. Previously every symbol that entered OHLCV
 *        was marked "analysed today" and excluded from later same-day runs — so a
 *        stock that FAILED the morning run but broke out cleanly by afternoon was
 *        never re-examined (the freshest early-confirmed-breakout archetype). Now
 *        only PASSING symbols (candidates) are deduped; a failed symbol stays
 *        eligible for later runs and must still clear every filter to signal.
 *        Verified: candidates[].symbol === selected[].sym (both = the string passed
 *        to pipeFetchOhlcvSymbol, L1952 symbol:symbol), so the gate is exact.
 *   EXPLICITLY UNCHANGED (per plan, until P6 outcome data): ADX threshold, RSI
 *     threshold, S5 filter logic, S6 filter logic, scoring methodology. These are
 *     precision dials and must be tuned with realized T1/SL data, not intuition.
 *   UNTOUCHED: entry placement byte-for-byte unchanged.
 *
 * Changelog v4.10 (11-Jun-2026) — P5: Scan breadth increase (paid plan):
 *   CONTEXT: the 34-symbol-per-run deep-analysis budget was sized purely to fit the
 *     FREE-tier 50-subrequest cap. At ~108 symbols/day it scanned only ~3.7% of the
 *     liquid universe daily — most early breakouts were never seen in their first
 *     1–3 days, defeating the "catch movers early" objective.
 *   CHANGE (paid plan = 1,000 subrequests/invocation, 30s CPU):
 *     • PIPE_HISTORY_BUDGET 34 → 150 (subrequests now ~166/1000, 83% headroom).
 *     • PIPE_MAX_OHLCV_CAP 80 → 150 (must match budget, else silent re-cap).
 *     • PIPE_BATCH_SIZE 5 → 10, PIPE_BATCH_DELAY 300 → 200ms (fit 150 under 30s CPU;
 *       worst-case ≈18s).
 *     • Manual-run budget 30 → 60.
 *   RESULT: daily coverage ~3.7% → ~30% of the liquid universe, momentum-ranked so
 *     it's the right 30%. NO change to scoring, ranking, selection, sizing, entry/
 *     SL/T1/T2, or Telegram output — only the COUNT of symbols flowing through.
 *   NEW BINDING LIMITS (were not binding before): 30s CPU wall-time and Kite's
 *     historical rate limit. Both have graceful handling (CPU margin + one-retry-on-
 *     429 + survivorship log). Watch pipeline timing post-deploy (caveat C-P5a);
 *     if runs near 30s or Kite 429s rise, tune BATCH_SIZE/DELAY or add runs/day.
 *   UNTOUCHED: entry placement byte-for-byte unchanged.
 *
 * Changelog v4.9 (11-Jun-2026) — A4a: API auth scaffolding (safe-by-default):
 *   CONTEXT (audit Phase 6): /signal/store, /tg/register, /pipe/deep-result accept
 *     unauthenticated POSTs; CORS is "*". Anyone with the Worker URL could inject a
 *     signal into the approval queue or overwrite Telegram routing. (HMAC on the
 *     callback is what still prevents a forged signal from placing a GTT.)
 *   A4a (this release) — code/scaffolding that needs NO deployment input:
 *     1. requireApiAuth(request, env): a gate the three open endpoints now call.
 *        SAFE-BY-DEFAULT — if KV key QE_API_SECRET is unset (today), it is a NO-OP
 *        that ALLOWS the request (behaviour identical to before; deploying this
 *        cannot break anything). When QE_API_SECRET is set (A4b) AND the browser
 *        sends "X-QE-Auth: <secret>", it enforces. Constant-time comparison.
 *     2. corsHeadersFor(request, env): origin-allowlist scaffolding. If KV key
 *        QE_ALLOWED_ORIGIN is set (A4b), locks CORS to that origin; else "*".
 *     3. All three endpoints gated with the no-op guard.
 *   A4b (DEFERRED — needs your confirmation, NOT in this release):
 *     • Set QE_API_SECRET in KV + have the browser send X-QE-Auth.
 *     • Set QE_ALLOWED_ORIGIN to the exact Pages origin.
 *     • Remove the hardcoded HMAC_SECRET fallback ("QE_DB_v2_SIGNAL_SECRET") —
 *       RETAINED for now because deleting it before confirming the KV key is set
 *       would break signal verification and halt Telegram BUYs (execution risk).
 *   UNTOUCHED: entry placement (placeGTT, /gtt/create) byte-for-byte unchanged;
 *     no scoring/ranking/RS/discovery changes.
 *
 * Changelog v4.8 (11-Jun-2026) — A1: Exchange-resting stop loss (auto-armed OCO):
 *   PROBLEM (audit CRITICAL-1): every BUY path placed an entry-only single-leg
 *     GTT. After the entry filled, the position sat with NO stop and NO target
 *     resting on the exchange. SL/T1/T2 existed only in qe_gtt_log + the browser
 *     UI — the log claimed protection that did not exist on Kite. One gap-down
 *     could produce a loss many multiples of the modelled 1R.
 *   ROOT CAUSE: placeGTT and the /gtt/create handler both hardcoded
 *     type:"single" with a lone BUY leg; no code ever issued a SELL exit. Kite's
 *     two-leg OCO brackets a POSITION, so the exit can only be placed AFTER the
 *     entry fills — which nothing did.
 *   FIX (broker-validated vs Kite Connect v3 GTT docs):
 *     1. armExitBracket(): on a newly-filled entry, place a two-leg OCO
 *        (stop SELL + target SELL, CNC, LIMIT — GTT legs are LIMIT-only) to
 *        bracket the position. trigger_values ascending [stop, target]. Falls
 *        back to a single stop SELL if the OCO is rejected.
 *     2. Stop leg LIMIT price buffered 0.3% BELOW the trigger (A1_STOP_LIMIT_BUFFER)
 *        to widen fill probability on fast moves; the stop TRIGGER stays at the
 *        user's computed sl, so risk math is unchanged.
 *     3. monitorPositions wired to arm on status=="triggered", idempotent via an
 *        exitArmed flag, with retry of a previously-failed arm (armPending).
 *     4. Loud Telegram alerts: "STOP ARMED" on success, "STOP NOT ARMED — place
 *        manually" on any failure (a fill that fails to arm is a naked position).
 *     5. _retainActive(): armed/pending positions are never dropped by the 200-cap.
 *     6. Monitor cron tightened (30-min -> 5-min cadence, paid plan) to cut the
 *        fill->arm window to ~5 min. NOTE: wrangler.toml [triggers] crons must match.
 *   UNTOUCHED: entry placement (placeGTT, /gtt/create) byte-for-byte unchanged;
 *     no scoring/ranking/RS/discovery changes; no index.html changes.
 *   NOT COMPLETE UNTIL: a test fill produces an OCO visible in Kite's GTT book.
 *
 * Changelog v4.7 (10-Jun-2026) — Signal TTL extension + chatId auth gate:
 *   PROBLEM: Signals sent at 09:30 expired after 15 minutes. GTTs placed via
 *     QuantEdge Telegram buttons in the evening showed "Signal expired" and
 *     were rejected — gtt_log never written — daily summary showed 0 GTTs.
 *   ROOT CAUSE: SIGNAL_TTL_MS = 15 min + no chatId authentication on the
 *     callback handler. The short TTL was compensating for the missing auth.
 *   FIX (Option 4 — chatId gate first, then safe TTL extension):
 *     1. chatId gate added to handleTelegramCallback: reads tg_chat_id from
 *        KV and rejects any callback not from your registered chat. Closes the
 *        real security gap that existed regardless of TTL length.
 *     2. SIGNAL_TTL_MS: 15 min → 8 hours. Signals sent at 09:30 remain
 *        actionable until 17:30. Safe because chatId gate now prevents replay.
 *     3. KV expirationTtl for qe_signal_*: 1800s → 32400s (9 hours, 1hr
 *        buffer beyond the 8-hour action window so key never expires early).
 *
 * Changelog v4.6 (10-Jun-2026) — Subrequest budget fix (signals Telegram):
 *   ROOT CAUSE (proven from audit log run 20666b0615acea9a):
 *     Cloudflare Workers free tier: 50 subrequests/invocation hard cap.
 *     Exact sequence: 1 heartbeat + 12–13 bhav batches (13 when a 403 retry
 *     fires) + 1 Nifty + 36 OHLCV = 50–51. Signals Telegram sendTelegram()
 *     was the 51st call — Cloudflare blocked it → resp never obtained →
 *     catch(e) → return false → signalCount:0 every cron run after 09:30.
 *     Evidence: JUBLFOOD "Too many subrequests" at position 50 in S4_OHLCV,
 *     followed by signalCount:0 in pipe_status. Pattern consistent across
 *     12:00 and 14:30 cron runs on 10-Jun-2026.
 *   FIX: PIPE_HISTORY_BUDGET 36 → 34.
 *     Budget worst case: 1+13+1+34+1 = 50 (exactly within cap).
 *     Budget normal case: 1+12+1+34+1 = 49 (one spare).
 *     Daily coverage: 102 unique symbols/day (was 108). Difference: 6/day.
 *
 * Changelog v4.5 (09-Jun-2026) — KV write reduction + Telegram diagnostics
 *                               + Screener industry extraction:
 *   KV write reduction (free tier: 1000 writes/day):
 *     Removed 10 mid-run writePipeStatus calls (S1–S8, S10, S11).
 *     Removed per-symbol qe_pipe_ohlcv_* cache writes (27/run, never read).
 *     Combined: 36 puts/run → 88 puts/day normal (vs 236 before).
 *   Telegram error logging:
 *     sendTelegram() previously swallowed all errors: catch(_){return false}.
 *     Now logs exact HTTP status + Telegram error body to Cloudflare console.
 *     Enabled diagnosis of the subrequest cap bug above.
 *   Observability — rank snapshot:
 *     Writes qe_pipe_rank_<date>_<runId> after budget cut (post-decision).
 *     Stores {s,r,m,sel,px} for every Stream-A survivor. Zero subrequest cost
 *     (KV put). Enables single-key lookup to trace any symbol's exit stage.
 *   Screener industry extraction:
 *     parseScreenerFundamentals() now extracts industry label from Screener
 *     HTML and returns it in the fundamentals object. Used by index.html v22
 *     Opportunity Radar to replace hardcoded sector map progressively.
 *
 * Changelog v4.4 (05-Jun-2026) — Fundamentals: Screener.in server-side:
 *   ROOT CAUSE: Yahoo Finance v10/quoteSummary requires crumb authentication
 *   since late 2024. Both browser and Worker calls fail (crumb/IP blocked).
 *   FIX: Worker now fetches Screener.in directly (no CORS restriction server-side).
 *   parseScreenerFundamentals() parses PE, ROE, RevGr, ProfGr, D/E via regex
 *   (no DOMParser — not available in Cloudflare Workers).
 *   Returns { fundamentals: { pe, roe, revGr, profGr, de, mcap } } to browser.
 *   Browser _fetchFundamentalsYahoo() updated to read new response shape.
 *
 * Changelog v4.3 (05-Jun-2026) — Fundamental data bug fix:
 *   BUGFIX: type=fundamentals handler returned Kite quote data (last_price,
 *           volume, ohlc) — not the fundamental fields (pe, roe, revGr,
 *           profGr, de) the browser parser expected. Browser parsed
 *           json.quoteSummary which was undefined → null → silent fallback
 *           → all fundamental cards showed N/A on every scan.
 *   FIX:    Handler now proxies Yahoo Finance quoteSummary API
 *           (financialData + defaultKeyStatistics + summaryDetail modules).
 *           Returns { quoteSummary } matching exact shape _fetchFundamentalsYahoo()
 *           already parses — zero browser-side changes required.
 *           Tries query1.finance.yahoo.com then query2 as fallback.
 *           No Kite token required for this endpoint.
 *
 * Changelog v4.2 (04-Jun-2026) — Signal Integrity + Macro Snapshot:
 *   FIX2. Telegram signal integrity gate added to pipeDispatchTelegram():
 *         Gate criteria: DS >= 60, Supertrend bullish, ADX >= 18.
 *         Failed candidates dispatched as WATCH_ONLY — never as BUY-eligible.
 *         watchOnly flag stored in KV signal for browser to read.
 *         Gate pass/fail logged to pipeline audit at S9_TELEGRAM stage.
 *   FIX3. Pipeline regime snapshot: computePipelineRegime() derives structural
 *         regime (bull/sideways/bear) from Nifty closes at pipeline run time.
 *         pipelineRegime embedded in every qe_pipe_signals KV entry.
 *         Browser runDeepOnCandidates() uses pipelineRegime instead of
 *         current browser _regime — deep analysis is now deterministic.
 *
 * Changelog v4.1 (02-Jun-2026) — Critical Fixes:
 *   CF1. OHLCV history range: 180 → 365 days (EMA200 now has reliable 260 trading days)
 *   CF2. OHLCV cap: max 80 symbols, sorted by volume desc before cap (highest liquidity first)
 *        Per-symbol fetch timeout: 12s AbortController on both quote + historical calls
 *        New KV stat: ohlcvQueue + ohlcvCapped in lastRun summary
 *   CF3. Sector map: expanded from ~120 → ~350 symbols across 22 sectors
 *        Reduces OTHER bucket, improves sector concentration control accuracy
 *
 * Changelog v4.0 (01-Jun-2026):
 *   All v3.1 routes preserved UNCHANGED.
 *   New additions — SERVER-SIDE DISCOVERY PIPELINE:
 *
 *   ARCHITECTURE:
 *     Universe (KV) → Bhav Copy ingest → OHLCV batch fetch + compute
 *     → Stream A filters → RS Engine → Sector Engine → Merge Engine
 *     → Survivorship Tracking → Audit System → KV signal store
 *     → Telegram Dispatch → Browser reads KV (Part 3)
 *
 *   NEW CRON SCHEDULES:
 *     04:00 UTC Mon–Fri (09:30 IST) — Bhav Copy ingest + pipeline trigger
 *     04:30 UTC Mon–Fri (10:00 IST) — Pipeline completion check + Telegram dispatch
 *
 *   NEW KV KEYS (qe_pipe_* namespace — zero collision with qe_db_*):
 *     qe_pipe_run_id          — current pipeline run UUID
 *     qe_pipe_status          — pipeline status JSON {phase, pct, startedAt, ...}
 *     qe_pipe_bhav_date       — last bhav copy date ingested (YYYY-MM-DD)
 *     qe_pipe_bhav_raw        — raw bhav copy symbol→close map JSON
 *     qe_pipe_ohlcv_{symbol}  — per-symbol computed OHLCV indicators JSON (TTL 24h)
 *     qe_pipe_stream_a        — Stream A filter output: symbols that passed (JSON array)
 *     qe_pipe_rs_ranked       — RS-ranked candidates after Stream A (JSON array)
 *     qe_pipe_sector_map      — sector assignment map JSON
 *     qe_pipe_candidates      — final merged candidates for deep analysis (JSON array)
 *     qe_pipe_signals         — today's completed signals for browser to read (JSON array)
 *     qe_pipe_survivorship    — survivorship log: all eliminated stocks with reason (JSON array)
 *     qe_pipe_audit           — pipeline audit log for current run (JSON array, max 500 entries)
 *     qe_pipe_last_run        — last successful run summary JSON
 *     qe_pipe_nifty_closes    — cached Nifty 50 daily closes for RS calc (JSON array)
 *     qe_pipe_nifty_ts        — nifty closes cache timestamp
 *
 *   NEW ROUTES:
 *     GET  /pipe/trigger      — manually trigger full pipeline run
 *     GET  /pipe/status       — current pipeline run status + progress
 *     GET  /pipe/signals      — read completed signals from KV (browser polls this)
 *     GET  /pipe/candidates   — read candidates list (pre-deep-analysis)
 *     GET  /pipe/audit        — pipeline audit log for last run
 *     GET  /pipe/survivorship — eliminated stocks with rejection reason
 *     POST /pipe/deep-result  — browser posts deep analysis result per symbol
 *
 *   KV KEYS (all — v3.1 existing + v4.0 new):
 *     kite_access_token       — Kite OAuth token (daily)
 *     kite_token_timestamp    — token refresh time
 *     api_secret              — Kite API secret
 *     tg_bot_token            — Telegram bot token
 *     tg_chat_id              — Telegram chat ID
 *     HMAC_SECRET             — signal signing secret
 *     qe_db_universe          — dynamic NSE universe (JSON array of symbols)
 *     qe_db_universe_ts       — universe build timestamp (ms)
 *     qe_db_universe_count    — universe stock count
 *     qe_signals              — active signals (legacy v2.0 — preserved)
 *     qe_gtt_log              — GTT placement audit log
 *     qe_watchlist            — watchlist
 *     qe_rejection_log        — rejection analytics
 *     [all qe_pipe_* keys listed above]
 */

const KITE_API_BASE  = "https://api.kite.trade";
const QE_VERSION     = "4.90";  // single source of truth for displayed version (root route + Telegram footer)
const API_KEY        = "x9atdliuwa1evccb";
const KV_TOKEN_KEY   = "kite_access_token";
const QE_URL         = "https://dsivasankarr.github.io/QuantEdge";
const WORKER_LOGIN_URL = "https://quantedge-kite.siva-d-sankar.workers.dev/login"; // v4.44: used by stale-data nudges
const SIGNAL_TTL_MS  = 8 * 60 * 60 * 1000; // 8 hours — safe: chatId gate added to callback handler

// ─── CORS ─────────────────────────────────────────────────────────────────────
const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function cors(body, status = 200, extra = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...CORS, ...extra },
  });
}
function corsErr(msg, status = 400) {
  return cors({ status: "error", message: msg }, status);
}

// ═══════════════════════════════════════════════════════════════════════════════
// A4a — API AUTH SCAFFOLDING (safe-by-default; enforcement is opt-in via KV)
//
// Provides a single gate the unauthenticated write endpoints (/signal/store,
// /tg/register, /pipe/deep-result) can call. Design goals:
//   • ZERO deployment dependency to ship: if KV key QE_API_SECRET is NOT set,
//     this is a NO-OP that ALLOWS the request — behaviour is identical to today,
//     so deploying this change cannot break anything (no capital/execution risk).
//   • Enforcement turns on the moment you set QE_API_SECRET in KV (that's the
//     A4b deployment step) AND have the browser send the matching header. Until
//     BOTH are true, requests pass — no lockout risk.
//   • Constant-time comparison to avoid timing leaks.
//
// Header expected once enabled: "X-QE-Auth: <secret>".
// Returns null when allowed, or a Response (401) when blocked.
// ═══════════════════════════════════════════════════════════════════════════════
function _timingSafeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

async function requireApiAuth(request, env) {
  // Opt-in: no secret configured → allow (preserves current open behaviour).
  let secret;
  try { secret = await env.KITE_STORE.get("QE_API_SECRET"); } catch (_) { secret = null; }
  if (!secret) return null; // A4a no-op until A4b sets the secret

  const provided = request.headers.get("X-QE-Auth") || "";
  if (_timingSafeEqual(provided, secret)) return null; // authorized
  return cors({ status: "error", message: "Unauthorized" }, 401);
}

// A4a — CORS origin allowlist scaffolding (safe-by-default).
// If KV key QE_ALLOWED_ORIGIN is set (A4b), echo it back ONLY for matching
// requests; otherwise fall back to "*" (current behaviour). Lets you lock the
// frontend origin without a code change, and without risking a self-lockout
// before you've confirmed the exact Pages origin.
async function corsHeadersFor(request, env) {
  let allowed;
  try { allowed = await env.KITE_STORE.get("QE_ALLOWED_ORIGIN"); } catch (_) { allowed = null; }
  if (!allowed) return CORS; // no-op default "*"
  const origin = request.headers.get("Origin") || "";
  const list = allowed.split(",").map(s => s.trim()).filter(Boolean);
  if (list.includes(origin)) {
    return { ...CORS, "Access-Control-Allow-Origin": origin };
  }
  // Origin not in allowlist: still return a valid CORS object (locked to the
  // first configured origin) so the browser blocks cross-origin reads.
  return { ...CORS, "Access-Control-Allow-Origin": list[0] };
}

// ─── Token helpers ────────────────────────────────────────────────────────────
async function getToken(env) {
  const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
  if (!token) throw new Error("Access token not found. Please login at /login");
  return token;
}
function kiteAuthHeader(token) {
  return `token ${API_KEY}:${token}`;
}

// ─── Kite API proxy ───────────────────────────────────────────────────────────
async function kiteRequest(method, path, body, token) {
  const url     = `${KITE_API_BASE}${path}`;
  const headers = {
    "X-Kite-Version": "3",
    Authorization: kiteAuthHeader(token),
  };
  let fetchOptions = { method, headers };
  if (body && method !== "GET") {
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    fetchOptions.body = new URLSearchParams(body).toString();
  }
  const resp = await fetch(url, fetchOptions);
  const data = await resp.json();
  return { ok: resp.ok, status: resp.status, data };
}

// ─── Yahoo Finance proxy ──────────────────────────────────────────────────────
async function proxyYahooFinance(symbol, interval, range) {
  const iv  = interval || "1d";
  const rng = range    || "1y";
  const headers = { "User-Agent": "Mozilla/5.0", "Accept": "application/json" };
  const urls = [
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${iv}&range=${rng}&includePrePost=false`,
    `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${iv}&range=${rng}&includePrePost=false`,
  ];
  for (const yfUrl of urls) {
    try {
      const ctrl  = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 12000);
      const res   = await fetch(yfUrl, { signal: ctrl.signal, headers });
      clearTimeout(timer);
      if (!res.ok) continue;
      const data = await res.json();
      if (!data.chart || !data.chart.result || !data.chart.result[0]) continue;
      return new Response(JSON.stringify(data), {
        status: 200,
        headers: { "Content-Type": "application/json", ...CORS }
      });
    } catch (_) { continue; }
  }
  return new Response(JSON.stringify({
    chart: { result: null, error: "Yahoo Finance unavailable for " + symbol }
  }), { status: 200, headers: { "Content-Type": "application/json", ...CORS } });
}

// ═══════════════════════════════════════════════════════════════════════════════
// TELEGRAM HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

async function getTgCreds(env) {
  const token = await env.KITE_STORE.get("tg_bot_token");
  const chat  = await env.KITE_STORE.get("tg_chat_id");
  return { token, chat, ok: !!(token && chat) };
}

// A (17-Jun v4.40): ntfy dual-send — a fully independent resilience channel.
// Gated by KV NTFY_ENABLED='true' + NTFY_TOPIC. Wrapped so it can NEVER throw into,
// delay-fail, or alter the Telegram path or any caller. Telegram remains primary and
// is unaffected whether ntfy is enabled, disabled, or errors.
async function sendNtfy(env, text) {
  try {
    // v4.41: tolerate stray whitespace / casing entered via the dashboard
    // ("true " or "TRUE" would previously have disabled ntfy silently).
    const enabledRaw = await env.KITE_STORE.get("NTFY_ENABLED");
    if (!enabledRaw || enabledRaw.trim().toLowerCase() !== "true") return false;
    const topicRaw = await env.KITE_STORE.get("NTFY_TOPIC");
    const topic = topicRaw ? topicRaw.trim() : "";
    if (!topic) return false;
    // v4.42: optional account auth. Sending WITH a token makes ntfy.sh count the
    // message against the account's tier instead of the shared Cloudflare egress IP,
    // which is what causes anonymous 429 "daily quota" rejections. Absent -> behaves
    // exactly as v4.41 (anonymous publish).
    const ntfyTokenRaw = await env.KITE_STORE.get("NTFY_TOKEN");
    const ntfyToken = ntfyTokenRaw ? ntfyTokenRaw.trim() : "";
    // ntfy delivers plain text; strip HTML tags Telegram uses (parse_mode HTML).
    const plain = String(text || "").replace(/<[^>]+>/g, "").trim();
    if (!plain) return false;
    const ctrl  = new AbortController();
    const timer = setTimeout(function () { ctrl.abort(); }, 8000);
    try {
      const ntfyHeaders = { "Title": "QuantEdge", "Content-Type": "text/plain; charset=utf-8" };
      if (ntfyToken) ntfyHeaders["Authorization"] = "Bearer " + ntfyToken;
      const resp = await fetch("https://ntfy.sh/" + encodeURIComponent(topic), {
        method: "POST",
        headers: ntfyHeaders,
        body: plain.slice(0, 4000),
        signal: ctrl.signal,
      });
      if (!resp.ok) {
        const e = await resp.text().catch(function () { return "(unreadable)"; });
        console.error("[sendNtfy] FAIL HTTP " + resp.status + ": " + e.slice(0, 200));
      }
      return resp.ok;
    } finally { clearTimeout(timer); }
  } catch (e) {
    console.error("[sendNtfy] THROW: " + (e && e.message));
    return false; // never propagate — Telegram path is independent
  }
}

// v4.41: GET /ntfy/test — read-only diagnostic. Reports the NTFY_* config (lengths
// reveal hidden whitespace), attempts a live POST to ntfy.sh, and returns the exact
// HTTP result. Sends a real push so the phone confirms reachability. No verdict/score
// impact; touches nothing in the pipeline.
async function handleNtfyTest(env) {
  const enabledRaw = await env.KITE_STORE.get("NTFY_ENABLED");
  const topicRaw   = await env.KITE_STORE.get("NTFY_TOPIC");
  const tokenRaw   = await env.KITE_STORE.get("NTFY_TOKEN");
  const enabled    = !!enabledRaw && enabledRaw.trim().toLowerCase() === "true";
  const topic      = topicRaw ? topicRaw.trim() : "";
  const token      = tokenRaw ? tokenRaw.trim() : "";
  const out = {
    status:                  "success",
    ntfy_enabled_present:     enabledRaw != null,
    ntfy_enabled_raw_length:  enabledRaw ? enabledRaw.length : 0,   // >4 means stray space in "true"
    ntfy_enabled_ok:          enabled,
    ntfy_topic_present:       topicRaw != null,
    ntfy_topic_raw_length:    topicRaw ? topicRaw.length : 0,
    ntfy_topic_trimmed_length: topic.length,                        // differs from raw → stray space
    ntfy_token_present:       token.length > 0,                     // are we authenticating?
    ntfy_token_length:        token.length,                         // value itself never echoed
  };
  if (!enabled) return cors(Object.assign(out, { sent: false, reason: "NTFY_ENABLED is not 'true' (check for a stray space or capital letters)" }));
  if (!topic)   return cors(Object.assign(out, { sent: false, reason: "NTFY_TOPIC is empty" }));
  try {
    const headers = { "Title": "QuantEdge diagnostic", "Content-Type": "text/plain; charset=utf-8" };
    if (token) headers["Authorization"] = "Bearer " + token;
    const resp = await fetch("https://ntfy.sh/" + encodeURIComponent(topic), {
      method:  "POST",
      headers: headers,
      body:    "QuantEdge ntfy test — if this reached your phone, the worker can talk to ntfy. " + new Date().toISOString(),
    });
    const respText = await resp.text().catch(function () { return "(unreadable)"; });
    return cors(Object.assign(out, { sent: resp.ok, http_status: resp.status, ntfy_response: respText.slice(0, 300) }));
  } catch (e) {
    return cors(Object.assign(out, { sent: false, reason: "FETCH_THREW", error: (e && e.message) }));
  }
}

async function sendTelegram(env, text, replyMarkup) {
  // A (17-Jun v4.40): fire the independent ntfy channel first, isolated, so a Telegram
  // outage (bad creds / ban / rate-limit) never loses the signal. Its result is ignored
  // here on purpose — the Telegram return value below is byte-for-byte the prior logic.
  await sendNtfy(env, text);
  const { token, chat, ok } = await getTgCreds(env);
  if (!ok) return false;
  const body = { chat_id: chat, text, parse_mode: "HTML" };
  if (replyMarkup) body.reply_markup = replyMarkup;
  try {
    const resp = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(body),
    });
    if (!resp.ok) {
      // Log the exact Telegram error so silent failures become diagnosable.
      // Previously: catch (_) { return false; } — swallowed all rejection reasons.
      const errBody = await resp.text().catch(function() { return "(unreadable)"; });
      console.error("[sendTelegram] FAIL HTTP " + resp.status + ": " + errBody.slice(0, 200));
    }
    return resp.ok;
  } catch (e) {
    console.error("[sendTelegram] THROW: " + (e && e.message));
    return false;
  }
}

async function answerCallback(env, callbackQueryId, text) {
  const { token } = await getTgCreds(env);
  if (!token) return;
  await fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ callback_query_id: callbackQueryId, text, show_alert: false }),
  });
}

async function editTgMessage(env, chatId, messageId, text) {
  const { token } = await getTgCreds(env);
  if (!token) return;
  await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ chat_id: chatId, message_id: messageId,
                              text, parse_mode: "HTML" }),
  });
}

// ─── HMAC verification ────────────────────────────────────────────────────────
async function verifyHmac(env, signalId, symbol, entry, expiry, providedHmac) {
  try {
    const secret  = await env.KITE_STORE.get("HMAC_SECRET") || "QE_DB_v2_SIGNAL_SECRET";
    const enc     = new TextEncoder();
    const data    = `${signalId}|${symbol}|${entry}|${expiry}`;
    const key     = await crypto.subtle.importKey(
      "raw", enc.encode(secret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const sig     = await crypto.subtle.sign("HMAC", key, enc.encode(data));
    const computed = Array.from(new Uint8Array(sig))
      .map(b => ("00" + b.toString(16)).slice(-2)).join("").slice(0, 32);
    return computed === providedHmac;
  } catch (_) { return false; }
}

// ─── HMAC sign helper (pipeline uses this to sign outbound signals) ───────────
async function signPayload(env, signalId, symbol, entry, expiry) {
  try {
    const secret = await env.KITE_STORE.get("HMAC_SECRET") || "QE_DB_v2_SIGNAL_SECRET";
    const enc    = new TextEncoder();
    const data   = `${signalId}|${symbol}|${entry}|${expiry}`;
    const key    = await crypto.subtle.importKey(
      "raw", enc.encode(secret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const sig    = await crypto.subtle.sign("HMAC", key, enc.encode(data));
    return Array.from(new Uint8Array(sig))
      .map(b => ("00" + b.toString(16)).slice(-2)).join("").slice(0, 32);
  } catch (_) { return ""; }
}

// ─── GTT placement helper (shared by UI and Telegram callback) ────────────────
async function placeGTT(env, symbol, entry, sl, t1, t2, quantity, cmp) {
  const token      = await getToken(env);
  const entryF     = parseFloat(entry).toFixed(2);
  const cmpF       = parseFloat(cmp || entry).toFixed(2);

  const condition = JSON.stringify({
    exchange:       "NSE",
    tradingsymbol:  symbol.toUpperCase(),
    trigger_values: [parseFloat(entryF)],
    last_price:     parseFloat(cmpF),
  });

  const orders = JSON.stringify([{
    exchange:         "NSE",
    tradingsymbol:    symbol.toUpperCase(),
    transaction_type: "BUY",
    quantity:         parseInt(quantity, 10),
    order_type:       "LIMIT",
    product:          "CNC",
    price:            parseFloat(entryF),
  }]);

  const { ok, data } = await kiteRequest(
    "POST", "/gtt/triggers",
    { type: "single", condition, orders },
    token
  );

  if (!ok) throw new Error(data.message || "GTT creation failed");
  const triggerId = data.data.trigger_id;

  await appendGttLog(env, {
    timestamp:  new Date().toISOString(),
    symbol:     symbol.toUpperCase(),
    entry:      parseFloat(entryF),
    sl:         sl    ? parseFloat(sl)   : null,
    t1:         t1    ? parseFloat(t1)   : null,
    t2:         t2    ? parseFloat(t2)   : null,
    quantity:   parseInt(quantity, 10),
    trigger_id: triggerId,
    source:     "telegram_approval",
  });

  return triggerId;
}

async function appendGttLog(env, entry) {
  try {
    const raw = await env.KITE_STORE.get("qe_gtt_log");
    const log = raw ? JSON.parse(raw) : [];
    log.unshift(entry);
    await env.KITE_STORE.put("qe_gtt_log", JSON.stringify(log.slice(0, 200)));
  } catch (e) { console.warn("[appendGttLog] non-fatal:", e && e.message); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// A1 — EXIT BRACKET ARMING (auto-armed OCO after entry fill)
// Places a two-leg OCO (stop SELL + target SELL) to bracket a freshly-filled
// position, so the stop rests on the EXCHANGE instead of living only in a log.
// Falls back to a single stop SELL if the two-leg is rejected. Idempotency is
// enforced by the caller via the `exitArmed` flag on the qe_gtt_log record.
//
// Broker-validated against Kite Connect v3 GTT docs (kite.trade/docs/connect/v3/gtt):
//   - type "two-leg" = OCO; requires EXACTLY 2 ascending trigger_values [stop, target]
//   - GTT legs support order_type "LIMIT" ONLY (no SL / SL-M on a GTT) — so the stop
//     is a LIMIT triggered at the stop price. To widen fill probability on a fast
//     move, the stop leg's LIMIT price is buffered slightly BELOW the trigger, while
//     the trigger itself stays at the user's computed stop (risk math unchanged).
//   - all legs CNC, SELL.
// Returns { ok, exitTriggerId, mode } where mode is "oco" | "stop_only" | "fail".
// ═══════════════════════════════════════════════════════════════════════════════
const A1_STOP_LIMIT_BUFFER = 0.003; // 0.3% below trigger for the stop leg's LIMIT floor

async function armExitBracket(env, token, rec, lastPrice) {
  const symbol = String(rec.symbol).toUpperCase();
  const qty    = parseInt(rec.filledQty || rec.quantity, 10);
  const slTrig = rec.sl ? parseFloat(parseFloat(rec.sl).toFixed(2)) : null;   // stop TRIGGER (risk math)
  const t1F    = rec.t1 ? parseFloat(parseFloat(rec.t1).toFixed(2)) : null;   // target
  const ltpF   = parseFloat(parseFloat(lastPrice || rec.entry).toFixed(2));

  // A stop is mandatory. Without it, arm nothing — the caller will alert.
  if (!slTrig || slTrig <= 0 || !qty || qty <= 0) {
    return { ok: false, mode: "fail", reason: "missing stop or quantity" };
  }

  // Stop leg LIMIT floor: slightly below the trigger so a fast tick still crosses
  // it. Trigger fires at slTrig; the order is a LIMIT at slLimit. NSE tick = 0.05.
  const slLimit = parseFloat((Math.round((slTrig * (1 - A1_STOP_LIMIT_BUFFER)) / 0.05) * 0.05).toFixed(2));

  // ── Attempt 1: two-leg OCO (stop + target), trigger_values ASCENDING [stop, target]
  if (t1F && t1F > slTrig) {
    const condition = JSON.stringify({
      exchange: "NSE", tradingsymbol: symbol,
      trigger_values: [slTrig, t1F], last_price: ltpF,
    });
    const orders = JSON.stringify([
      { exchange:"NSE", tradingsymbol:symbol, transaction_type:"SELL",
        quantity:qty, order_type:"LIMIT", product:"CNC", price:slLimit },   // stop leg (index 0 ↔ trigger_values[0])
      { exchange:"NSE", tradingsymbol:symbol, transaction_type:"SELL",
        quantity:qty, order_type:"LIMIT", product:"CNC", price:t1F },        // target leg (index 1 ↔ trigger_values[1])
    ]);
    const { ok, data } = await kiteRequest(
      "POST", "/gtt/triggers", { type:"two-leg", condition, orders }, token
    );
    if (ok && data && data.data && data.data.trigger_id) {
      return { ok:true, exitTriggerId:data.data.trigger_id, mode:"oco" };
    }
    rec._ocoError = (data && data.message) || "two-leg rejected";
  }

  // ── Attempt 2 (fallback): single stop SELL — a resting stop alone still protects.
  const condition2 = JSON.stringify({
    exchange: "NSE", tradingsymbol: symbol,
    trigger_values: [slTrig], last_price: ltpF,
  });
  const orders2 = JSON.stringify([
    { exchange:"NSE", tradingsymbol:symbol, transaction_type:"SELL",
      quantity:qty, order_type:"LIMIT", product:"CNC", price:slLimit },
  ]);
  const r2 = await kiteRequest(
    "POST", "/gtt/triggers", { type:"single", condition:condition2, orders:orders2 }, token
  );
  if (r2.ok && r2.data && r2.data.data && r2.data.data.trigger_id) {
    return { ok:true, exitTriggerId:r2.data.data.trigger_id, mode:"stop_only" };
  }
  return { ok:false, mode:"fail",
           reason:(r2.data && r2.data.message) || rec._ocoError || "exit placement failed" };
}

// A1: keep the 200 most-recent records BUT never drop a position that filled and
// is still being protected (armed / arm-pending / exit not yet closed) — losing
// it would orphan a live stop from our tracking.
function _retainActive(log) {
  const recent = log.slice(0, 200);
  const seen   = new Set(recent.map(r => r.trigger_id));
  const active = log.filter(r =>
    (r.alerted && !r.exitArmed) || r.armPending || (r.exitArmed && !r.exitClosed)
  );
  for (const r of active) if (!seen.has(r.trigger_id)) recent.push(r);
  return recent;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 1 — TELEGRAM CALLBACK HANDLER
// ═══════════════════════════════════════════════════════════════════════════════
async function handleTelegramCallback(request, env) {
  let update;
  try { update = await request.json(); } catch (_) { return cors({ ok: true }); }

  const cq = update.callback_query;
  if (!cq) return cors({ ok: true });

  const callbackQueryId = cq.id;
  const messageId       = cq.message && cq.message.message_id;
  const chatId          = cq.message && cq.message.chat && cq.message.chat.id;

  // ── Authorisation gate ────────────────────────────────────────────────────
  // Only the registered Telegram chat (your account) may trigger BUY/WATCH.
  // Without this check, anyone who sees the Telegram message buttons could
  // place a GTT on your Zerodha account. chatId is read from KV tg_chat_id
  // (same key used by getTgCreds) — set once at /login.
  const authorisedChat = await env.KITE_STORE.get("tg_chat_id");
  if (authorisedChat && String(chatId) !== String(authorisedChat)) {
    await answerCallback(env, callbackQueryId, "❌ Unauthorised.");
    return cors({ ok: true });
  }
  // ── End authorisation gate ────────────────────────────────────────────────

  let slim;
  try { slim = JSON.parse(cq.data); } catch (_) {
    await answerCallback(env, callbackQueryId, "Invalid signal data.");
    return cors({ ok: true });
  }

  // Slim button payload: { a: action, s: signalId }. Full trade details are
  // read back from KV (qe_signal_<signalId>) — the button can't carry them
  // because Telegram limits callback_data to 64 bytes.
  const action   = slim.a;
  const signalId = slim.s;
  if (!signalId || !action) {
    await answerCallback(env, callbackQueryId, "Invalid signal reference.");
    return cors({ ok: true });
  }

  let sig;
  try {
    const sigRaw = await env.KITE_STORE.get("qe_signal_" + signalId);
    sig = sigRaw ? JSON.parse(sigRaw) : null;
  } catch (_) { sig = null; }
  if (!sig) {
    await answerCallback(env, callbackQueryId, "⏱ Signal expired or not found. Run a new scan.");
    if (chatId && messageId) {
      await editTgMessage(env, chatId, messageId,
        "⏱ <b>Signal Expired</b>\nRun a fresh Discovery scan for new signals.");
    }
    return cors({ ok: true });
  }

  const { symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac } = sig;

  if (!expiry || Date.now() > expiry) {
    await answerCallback(env, callbackQueryId, "⏱ Signal expired. Run a new scan.");
    await editTgMessage(env, chatId, messageId,
      `⏱ <b>Signal Expired — ${symbol}</b>\nRun a fresh Discovery scan for new signals.`);
    return cors({ ok: true });
  }

  const valid = await verifyHmac(env, signalId, symbol, entry, expiry, hmac);
  if (!valid) {
    await answerCallback(env, callbackQueryId, "❌ Invalid signal signature.");
    return cors({ ok: true });
  }

  if (action === "BUY") {
    try {
      const raw = await env.KITE_STORE.get("qe_gtt_log");
      const log = raw ? JSON.parse(raw) : [];
      const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
      const dup = log.some(function(g) {
        return g.symbol === symbol.toUpperCase() &&
               new Date(g.timestamp).getTime() > cutoff;
      });
      if (dup) {
        await answerCallback(env, callbackQueryId,
          `⚠️ Duplicate: GTT already placed for ${symbol} within 7 days.`);
        return cors({ ok: true });
      }

      const triggerId = await placeGTT(env, symbol, entry, sl, t1, t2, qty, cmp);

      await answerCallback(env, callbackQueryId, `✅ GTT placed for ${symbol}!`);
      await editTgMessage(env, chatId, messageId,
        `✅ <b>GTT Placed — ${symbol}</b>\n\n`
        + `Entry: ₹${entry} | SL: ₹${sl}\n`
        + `T1: ₹${t1} | Qty: ${qty}\n`
        + `Trigger ID: <code>${triggerId}</code>\n`
        + `<i>Source: Discovery Engine v3.0</i>`
      );

      await sendTelegram(env,
        `✅ <b>GTT Confirmed — ${symbol}</b>\n`
        + `Entry: ₹${entry} | SL: ₹${sl} | T1: ₹${t1}\n`
        + `Qty: ${qty} | Trigger: #${triggerId}`
      );

    } catch (e) {
      await answerCallback(env, callbackQueryId, `❌ GTT failed: ${e.message}`);
      await editTgMessage(env, chatId, messageId,
        `❌ <b>GTT Failed — ${symbol}</b>\n${e.message}`
      );
    }
  }

  else if (action === "WATCH") {
    try {
      const raw = await env.KITE_STORE.get("qe_watchlist") || "[]";
      const wl  = JSON.parse(raw);
      if (!wl.find(function(w) { return w.symbol === symbol; })) {
        wl.unshift({ symbol, entry, sl, t1, addedAt: new Date().toISOString(), signalId });
        await env.KITE_STORE.put("qe_watchlist", JSON.stringify(wl.slice(0, 50)));
      }
    } catch (e) { console.warn("[handleTelegramCallback] non-fatal:", e && e.message); }

    await answerCallback(env, callbackQueryId, `👀 ${symbol} added to watchlist`);
    await editTgMessage(env, chatId, messageId,
      `👀 <b>Watching — ${symbol}</b>\n`
      + `Entry: ₹${entry} | SL: ₹${sl}\n`
      + `<i>Will alert on breakout or score improvement</i>`
    );
  }

  else if (action === "REJECT") {
    try {
      const raw = await env.KITE_STORE.get("qe_rejection_log") || "[]";
      const rl  = JSON.parse(raw);
      rl.unshift({ symbol, signalId, rejectedAt: new Date().toISOString() });
      await env.KITE_STORE.put("qe_rejection_log", JSON.stringify(rl.slice(0, 200)));
    } catch (e) { console.warn("[handleTelegramCallback] non-fatal:", e && e.message); }

    await answerCallback(env, callbackQueryId, `❌ ${symbol} rejected`);
    await editTgMessage(env, chatId, messageId,
      `❌ <b>Rejected — ${symbol}</b>\n<i>Logged for analytics</i>`
    );
  }

  return cors({ ok: true });
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 2 — DAILY AUTH REMINDER (8:45am IST = 03:15 UTC)
// ═══════════════════════════════════════════════════════════════════════════════
async function sendAuthReminder(env) {
  const tokenTs = await env.KITE_STORE.get("kite_token_timestamp");
  if (tokenTs) {
    const tokenAge = Date.now() - parseInt(tokenTs);
    if (tokenAge < 3 * 60 * 60 * 1000) return;
  }

  const loginUrl = "https://quantedge-kite.siva-d-sankar.workers.dev/login";
  await sendTelegram(env,
    `🔑 <b>QuantEdge — Daily Kite Authorisation</b>\n\n`
    + `Markets open in ~30 minutes.\n`
    + `Tap below to connect Kite for today's session.\n\n`
    + `<a href="${loginUrl}">🔑 Authorise Kite Now</a>\n\n`
    + `<i>Required daily — Zerodha security policy</i>`
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 3 — SCHEDULED SCAN TRIGGER (9:15am IST = 03:45 UTC)
// Now triggers the server-side pipeline instead of just sending a deep-link.
// Bridge: if pipeline is not yet built (v4.0 deploy day), falls back to deep-link.
// ═══════════════════════════════════════════════════════════════════════════════
async function triggerDiscoveryScan(env) {
  const token = await env.KITE_STORE.get(KV_TOKEN_KEY);

  if (!token) {
    await sendTelegram(env,
      `⚠️ <b>QuantEdge Discovery — Blocked</b>\n\n`
      + `Kite not authorised for today.\n`
      + `<a href="https://quantedge-kite.siva-d-sankar.workers.dev/login">🔑 Login first</a>, `
      + `then run Discovery manually.`
    );
    return;
  }

  // v4.0: trigger server-side pipeline
  await sendTelegram(env,
    `🔭 <b>QuantEdge Discovery — Pipeline Starting</b>\n\n`
    + `Kite connected ✅\n`
    + `Server-side pipeline triggered at market open.\n`
    + `Phase 1: Bhav Copy ingest → Stream A filters → RS ranking\n\n`
    + `<i>Candidates will appear in QuantEdge Discovery panel (~10 min)</i>`
  );

  // Fire-and-forget: run pipeline in background
  // ctx.waitUntil not available here — use direct call
  // Pipeline will write results to KV; browser polls /pipe/signals
  // v4.19: route through the SAME instrumented wrapper as every other entry point
  // (heartbeat + funnel + named sections + error reporting). Was a legacy bare call.
  await runPipelineWithSummary(env, "09:15 IST open scan");
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 4 — POSITION MONITOR (every 30 min, market hours)
// ═══════════════════════════════════════════════════════════════════════════════
async function monitorPositions(env) {
  let token;
  try { token = await getToken(env); } catch (_) { return; }

  const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
  if (!ok) return;

  const allKiteGTTs = data.data || [];
  // R2 fix: build a map of trigger_id → actual Kite status. The previous logic
  // inferred "triggered" from ABSENCE in the active list — but a GTT also
  // disappears from active when it is cancelled, deleted, rejected, or expired,
  // producing a FALSE "GTT Triggered" alert. We now read the real status field
  // and only fire the trigger alert when status === "triggered".
  const statusById = {};
  for (let k = 0; k < allKiteGTTs.length; k++) {
    statusById[String(allKiteGTTs[k].id)] = allKiteGTTs[k].status;
  }
  // A1: best-effort reference LTP for exit-bracket last_price. armExitBracket
  // falls back to rec.entry if null. The stop TRIGGER comes from rec.sl, not
  // this — so a stale LTP never affects the protective price.
  statusById._ltp = (allKiteGTTs[0] && allKiteGTTs[0].condition && allKiteGTTs[0].condition.last_price) || null;

  const raw    = await env.KITE_STORE.get("qe_gtt_log");
  const ourLog = raw ? JSON.parse(raw) : [];

  const alerts  = [];
  const armExitAlerts = [];   // A1: exit-arming notifications (arm success / fail / retry)

  for (let i = 0; i < ourLog.length; i++) {
    const logged = ourLog[i];
    if (!logged.trigger_id) continue;
    const kiteStatus = statusById[String(logged.trigger_id)];
    // Genuine fill only — explicit "triggered" status. Absence (undefined),
    // "cancelled", "deleted", "rejected", "expired" are NOT trade fills.
    const triggered = kiteStatus === "triggered";
    if (triggered && !logged.alerted) {
      alerts.push(logged);
      logged.alerted   = true;
      logged.alertedAt = new Date().toISOString();
    }
    // ── A1: auto-arm exit bracket on a newly-filled entry ──────────────────────
    // Only our own BUY entries (SELL records are exit legs — never re-bracket),
    // only once (exitArmed flag), only when the entry actually triggered. This is
    // where the stop comes to rest on the exchange.
    if (triggered && !logged.exitArmed && logged.transaction !== "SELL") {
      logged.filledQty = logged.filledQty || logged.quantity;
      const armed = await armExitBracket(env, token, logged, statusById._ltp);
      if (armed.ok) {
        logged.exitArmed     = true;
        logged.exitTriggerId = armed.exitTriggerId;
        logged.exitMode      = armed.mode;
        logged.exitArmedAt   = new Date().toISOString();
        logged.armPending    = false;
        armExitAlerts.push({ rec: logged, mode: armed.mode });
      } else {
        logged.armPending    = true;   // retried on next monitor run
        if (!logged.armFailAlerted) {  // v4.28: alert ONCE on fail, then retry silently — no per-tick spam
          logged.armFailAlerted = true;
          armExitAlerts.push({ rec: logged, mode: "fail", reason: armed.reason });
        }
      }
    }
    // ── A1: retry a previously-failed arm (entry filled earlier, arm had failed) ─
    else if (logged.armPending && !logged.exitArmed && logged.transaction !== "SELL") {
      logged.filledQty = logged.filledQty || logged.quantity;
      const armed = await armExitBracket(env, token, logged, statusById._ltp);
      if (armed.ok) {
        logged.exitArmed     = true;
        logged.exitTriggerId = armed.exitTriggerId;
        logged.exitMode      = armed.mode;
        logged.exitArmedAt   = new Date().toISOString();
        logged.armPending    = false;
        armExitAlerts.push({ rec: logged, mode: armed.mode, retried: true });
      }
    }
    const age = (Date.now() - new Date(logged.timestamp).getTime()) / (1000 * 60 * 60 * 24);
    // Stale check only applies to GTTs that are STILL active (not filled/gone).
    if (kiteStatus === "active" && age > 25 && !logged.staleAlerted) {
      alerts.push(Object.assign({}, logged, { stale: true }));
      logged.staleAlerted = true;
    }
  }

  // A1: persist if EITHER fill/stale alerts OR exit-arming changed the log.
  // Without the armExitAlerts condition, exitArmed/armPending flags would be lost
  // when there are no other alerts, causing the position to be re-armed next run.
  // _retainActive replaces the bare slice(0,200) so an armed-but-unclosed trade is
  // never dropped (which would orphan a live exchange stop from our tracking).
  if (alerts.length || armExitAlerts.length) {
    await env.KITE_STORE.put("qe_gtt_log", JSON.stringify(_retainActive(ourLog)));
    for (let j = 0; j < alerts.length; j++) {
      const a = alerts[j];
      if (a.stale) {
        await sendTelegram(env,
          `⏰ <b>Stale Position — ${a.symbol}</b>\n\n`
          + `GTT open for >25 days.\n`
          + `Entry: ₹${a.entry} | SL: ₹${a.sl} | T1: ₹${a.t1}\n`
          + `Trigger ID: #${a.trigger_id}\n\n`
          + `Consider reviewing this position.`
        );
      } else {
        await sendTelegram(env,
          `🎯 <b>GTT Triggered — ${a.symbol}</b>\n\n`
          + `Your GTT order has been activated on Kite.\n`
          + `Entry: ₹${a.entry} | Qty: ${a.quantity}\n`
          + `Check Kite for execution status.\n`
          + `<a href="https://kite.zerodha.com/orders">View in Kite →</a>`
        );
      }
    }
    // ── A1: exit-arming notifications ────────────────────────────────────────
    // A detected fill that fails to arm = a NAKED position. It must scream.
    for (let m = 0; m < armExitAlerts.length; m++) {
      const e = armExitAlerts[m];
      if (e.mode === "fail") {
        await sendTelegram(env,
          `⚠️ <b>STOP NOT ARMED — ${e.rec.symbol}</b>\n\n`
          + `Entry filled but the exchange stop could NOT be placed.\n`
          + `Reason: ${e.reason || "unknown"}\n`
          + `Qty: ${e.rec.filledQty} | Intended SL: ₹${e.rec.sl}\n\n`
          + `❗ PLACE A STOP MANUALLY IN KITE NOW.\n`
          + `<a href="https://kite.zerodha.com/gtt">Open Kite GTT →</a>`
        );
      } else {
        await sendTelegram(env,
          `🛡️ <b>STOP ARMED — ${e.rec.symbol}</b>${e.retried ? " (retry)" : ""}\n\n`
          + `${e.mode === "oco" ? "OCO bracket" : "Stop"} now resting on the exchange.\n`
          + `SL: ₹${e.rec.sl}${e.mode === "oco" ? ` | T1: ₹${e.rec.t1}` : ""} | Qty: ${e.rec.filledQty}\n`
          + `Exit Trigger ID: <code>${e.rec.exitTriggerId}</code>\n`
          + `<a href="https://kite.zerodha.com/gtt">Verify in Kite →</a>`
        );
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 5 — DAILY SUMMARY (4:00pm IST = 10:30 UTC)
// ═══════════════════════════════════════════════════════════════════════════════
async function sendDailySummary(env) {
  let token;
  try { token = await getToken(env); } catch (_) {
    await sendTelegram(env, `📊 <b>QuantEdge Daily Summary</b>\n\n⚠️ Kite not connected today.`);
    return;
  }

  const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
  const activeGTTs   = ok ? (data.data || []).filter(function(g) { return g.status === "active"; }) : [];

  const raw    = await env.KITE_STORE.get("qe_gtt_log");
  const gttLog = raw ? JSON.parse(raw) : [];
  const today  = new Date().toISOString().slice(0, 10);
  const todayGTTs = gttLog.filter(function(g) {
    return g.timestamp && g.timestamp.startsWith(today);
  });

  const rjRaw  = await env.KITE_STORE.get("qe_rejection_log");
  const rejLog = rjRaw ? JSON.parse(rjRaw) : [];
  const todayRejections = rejLog.filter(function(r) {
    return r.rejectedAt && r.rejectedAt.startsWith(today);
  });

  // v4.0: include pipeline summary
  let pipelineSummary = "";
  try {
    const pipeRaw = await env.KITE_STORE.get("qe_pipe_last_run");
    if (pipeRaw) {
      const pr = JSON.parse(pipeRaw);
      if (pr.runDate === today) {
        pipelineSummary = `\n🔭 <b>Discovery Pipeline</b>\n`
          + `  Universe: ${pr.universeCount} → RS passed: ${pr.rsPassCount}\n`
          + `  Stream A passed: ${pr.streamACount} → Candidates: ${pr.candidateCount}\n`
          + `  Signals dispatched: ${pr.signalCount}\n`;
      }
    }
  } catch (e) { console.warn("[sendDailySummary] non-fatal:", e && e.message); }

  const capitalDeployed = todayGTTs.reduce(function(sum, g) {
    return sum + (g.entry * g.quantity);
  }, 0);

  const msg = `📊 <b>QuantEdge Daily Summary — ${today}</b>\n\n`
    + `🔭 GTTs placed today: <b>${todayGTTs.length}</b>\n`
    + `❌ Signals rejected: <b>${todayRejections.length}</b>\n`
    + `📋 Total active GTTs: <b>${activeGTTs.length}</b>\n`
    + `💰 Capital deployed today: <b>₹${capitalDeployed.toLocaleString("en-IN")}</b>\n`
    + pipelineSummary
    + "\n"
    + (todayGTTs.length
      ? todayGTTs.map(function(g) {
          return `  • ${g.symbol} @ ₹${g.entry} × ${g.quantity} = ₹${(g.entry * g.quantity).toLocaleString("en-IN")}`;
        }).join("\n") + "\n\n"
      : "  No new positions today.\n\n")
    + `<i>QuantEdge Discovery Engine v3.0</i>`;

  await sendTelegram(env, msg);
}

// ─── /kv/get  (read KV from frontend) ────────────────────────────────────────
async function handleKvGet(url, env) {
  const key = url.searchParams.get("key");
  if (!key) return corsErr("Missing key");
  // Extended allowed list — v4.0 pipe keys added
  const allowed = [
    "qe_db_universe", "qe_db_universe_ts", "qe_watchlist",
    "qe_pipe_signals", "qe_pipe_candidates", "qe_pipe_status",
    "qe_pipe_audit",   "qe_pipe_survivorship", "qe_pipe_last_run",
  ];
  // Rank-list snapshots are dynamically named (qe_pipe_rank_<date>_<runId>),
  // so they can't be enumerated in the static allowlist — permit by prefix.
  // Read-only observability data; same exposure class as qe_pipe_survivorship.
  const isRankKey = key.indexOf("qe_pipe_rank_") === 0;
  if (!allowed.includes(key) && !isRankKey) return corsErr("Key not readable", 403);
  try {
    const value = await env.KITE_STORE.get(key);
    return cors({ key, value: value || null });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ─── /tg/register  (store TG credentials from QuantEdge UI) ──────────────────
async function handleTgRegister(request, env) {
  const authErr = await requireApiAuth(request, env); if (authErr) return authErr; // A4a
  let body;
  try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON"); }
  const { bot_token, chat_id } = body;
  if (!bot_token || !chat_id) return corsErr("Required: bot_token, chat_id");
  await env.KITE_STORE.put("tg_bot_token", bot_token);
  await env.KITE_STORE.put("tg_chat_id",   String(chat_id));
  return cors({ status: "success", message: "Telegram credentials stored in KV" });
}

// ─── /signal/store  (store signal payload for callback verification) ─────────
async function handleSignalStore(request, env) {
  const authErr = await requireApiAuth(request, env); if (authErr) return authErr; // A4a
  let body;
  try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON"); }
  const { signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac } = body;
  if (!signalId || !symbol || !expiry) return corsErr("Required: signalId, symbol, expiry");
  if (Date.now() > expiry) return corsErr("Signal already expired", 400);
  await env.KITE_STORE.put(
    `qe_signal_${signalId}`,
    JSON.stringify({ signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac }),
    { expirationTtl: 32400 }
  );
  return cors({ status: "success", signalId });
}

// ═══════════════════════════════════════════════════════════════════════════════
// UNIVERSE MANAGER (v3.1 — preserved exactly)
// ═══════════════════════════════════════════════════════════════════════════════
async function buildUniverse(env) {
  let token;
  try {
    token = await getToken(env);
  } catch (e) {
    return { ok: false, error: "Kite token not available: " + e.message, count: 0 };
  }

  let csv;
  try {
    const resp = await fetch(`${KITE_API_BASE}/instruments/NSE`, {
      headers: {
        "X-Kite-Version": "3",
        "Authorization": kiteAuthHeader(token),
      },
    });
    if (!resp.ok) {
      return { ok: false, error: "Kite instruments fetch failed: HTTP " + resp.status, count: 0 };
    }
    csv = await resp.text();
  } catch (e) {
    return { ok: false, error: "Kite instruments fetch error: " + e.message, count: 0 };
  }

  const lines = csv.split("\n");
  if (lines.length < 2) {
    return { ok: false, error: "Empty instruments CSV returned", count: 0 };
  }

  const headers = lines[0].split(",").map(function(h) { return h.trim().replace(/"/g, ""); });
  const colTradingsymbol  = headers.indexOf("tradingsymbol");
  const colInstrumentType = headers.indexOf("instrument_type");
  const colLastPrice      = headers.indexOf("last_price");
  const colExchange       = headers.indexOf("exchange");
  const colInstrToken     = headers.indexOf("instrument_token"); // Commit 1: capture token
  const colSegment        = headers.indexOf("segment");           // v4.21: drop index segment

  if (colTradingsymbol < 0 || colInstrumentType < 0) {
    return { ok: false, error: "CSV missing required columns. Got: " + headers.join(","), count: 0 };
  }

  const symbols  = [];
  const tokenMap = {}; // Commit 1: symbol → instrument_token, eliminates per-symbol /quote in S4
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const cols = line.split(",");
    if (cols.length < headers.length) continue;

    const instrType = (cols[colInstrumentType] || "").trim().replace(/"/g, "");
    const exchange  = colExchange >= 0 ? (cols[colExchange] || "").trim().replace(/"/g, "") : "NSE";
    const symbol    = (cols[colTradingsymbol] || "").trim().replace(/"/g, "");
    const lastPrice = colLastPrice >= 0 ? parseFloat(cols[colLastPrice]) : 0;

    if (instrType !== "EQ") continue;
    if (exchange !== "NSE") continue;
    // v4.21: indices (NIFTY 50, INDIA VIX, NIFTY GS bonds) are EQ/NSE but segment=INDICES.
    const segment = colSegment >= 0 ? (cols[colSegment] || "").trim().replace(/"/g, "") : "NSE";
    if (segment !== "NSE") continue;
    if (!symbol) continue;
    if (lastPrice > 0 && lastPrice < 100) continue;
    if (/[-&]/.test(symbol) && symbol !== "BAJAJ-AUTO") continue;

    symbols.push(symbol);
    // Commit 1: capture instrument token for this symbol (used by S4 history fetch)
    if (colInstrToken >= 0) {
      const tok = parseInt((cols[colInstrToken] || "").trim().replace(/"/g, ""), 10);
      if (tok > 0) tokenMap[symbol] = tok;
    }
  }

  if (symbols.length < 50) {
    return { ok: false, error: "Too few symbols after filter: " + symbols.length, count: 0 };
  }

  const ts = Date.now();
  try {
    await env.KITE_STORE.put("qe_db_universe",       JSON.stringify(symbols));
    await env.KITE_STORE.put("qe_db_universe_ts",    String(ts));
    await env.KITE_STORE.put("qe_db_universe_count", String(symbols.length));
    // Commit 1: persist symbol→token map (TTL 8 days; rebuilt weekly with universe)
    await env.KITE_STORE.put("qe_db_token_map", JSON.stringify(tokenMap),
      { expirationTtl: 8 * 24 * 60 * 60 });
  } catch (e) {
    return { ok: false, error: "KV write failed: " + e.message, count: symbols.length };
  }

  return {
    ok:         true,
    count:      symbols.length,
    tokenCount: Object.keys(tokenMap).length,
    builtAt:    new Date(ts).toISOString(),
    sample:     symbols.slice(0, 10),
  };
}

/* ── Commit S2 (v4.36): TOKEN-MAP AGE GUARD — read-only daily health check, no verdict impact ──
   Reads qe_db_universe_ts (the last successful-rebuild timestamp, written atomically with the
   token map on every cron/manual rebuild) and alerts if the map is older than the 7-day refresh
   cycle. The token map has an 8-day TTL refreshed weekly (~1-day margin), so a silently-failed
   Sunday rebuild leaves a stale timestamp; this guard catches that before the day-8 expiry.
   Threshold 7d: normal max age ≈6.4d (guard runs 12:00 UTC, rebuild 03:00 UTC) → no false alarms. */
async function tokenMapAgeGuard(env) {
  try {
    const ts = await env.KITE_STORE.get("qe_db_universe_ts");
    if (!ts) return;   // never built / no timestamp yet — do not false-alarm
    const ageDays = (Date.now() - parseInt(ts, 10)) / 86400000;
    if (ageDays > 7) {
      await sendTelegram(env, "🔧 OPS ⚠️ Universe/token map is " + ageDays.toFixed(1) +
        " days old (TTL 8d, weekly rebuild) — the Sunday rebuild may have silently failed. " +
        "Log in to Kite and hit /universe/refresh before it expires.");
    }
  } catch (_) {}
}

async function handleUniverseRefresh(env) {
  const result = await buildUniverse(env);
  if (!result.ok) {
    return corsErr("Universe build failed: " + result.error, 500);
  }
  return cors({
    status:   "success",
    count:    result.count,
    built_at: result.builtAt,
    sample:   result.sample,
    message:  `Universe built: ${result.count} NSE EQ stocks (price > ₹100)`,
  });
}

async function handleUniverseStatus(env) {
  try {
    const ts    = await env.KITE_STORE.get("qe_db_universe_ts");
    const count = await env.KITE_STORE.get("qe_db_universe_count");
    const hasUniverse = !!(await env.KITE_STORE.get("qe_db_universe"));
    const ageMs   = ts ? Date.now() - parseInt(ts) : null;
    const ageDays = ageMs ? Math.floor(ageMs / (1000 * 60 * 60 * 24)) : null;
    return cors({
      status:       "success",
      has_universe: hasUniverse,
      count:        count ? parseInt(count) : 0,
      built_at:     ts ? new Date(parseInt(ts)).toISOString() : null,
      age_days:     ageDays,
      stale:        ageDays === null ? true : ageDays > 7,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
// SERVER-SIDE DISCOVERY PIPELINE — v4.0
// ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
//
// Pipeline stages (all server-side):
//   Stage 1  — Universe load          (KV read)
//   Stage 2  — NSE Bhav Copy ingest   (Kite /quote bulk — last_price, volume, change)
//   Stage 3  — Stream A Fast filters  (bhav-only: price, volume, circuit — no history)
//   Stage 4  — OHLCV fetch + compute  (Kite historical per symbol, batched)
//   Stage 5  — Stream A Technical      (EMA stack, RSI, ADX, Supertrend, ATR, volume)
//   Stage 5B — Stream B Discovery      (hidden-gem branch on same ohlcvMap)
//   Stage 6  — RS Engine              (percentile rank vs Nifty, 3-period weighted)
//   Stage 7  — Sector Engine          (sector concentration limit)
//   Stage 8  — Merge Engine           (Discovery Score, rank, top N)
//   Stage 9  — KV signal store         (candidates + signals JSON for browser)
//   Stage 10 — Survivorship write     (all eliminated symbols logged with reason)
//   Stage 11 — Telegram dispatch       (top signals with WATCH/SKIP buttons)
//
// Design constraints:
//   - Total pipeline CPU budget: Cloudflare Workers paid = 30s per invocation
//   - Universe is ~800–1400 symbols; OHLCV batch is ONLY run on Stream A candidates
//   - Stream A pre-filter uses Bhav Copy (last_price) only — zero historical API calls
//   - Historical fetch only on symbols that pass Stream A (typically 40–80 symbols)
//   - Batch size: 5 symbols per fetch batch, 300ms delay between batches
//   - Worker timeout defence: pipeline writes checkpoint to KV at each stage
//
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Pipeline constants ───────────────────────────────────────────────────────
const PIPE_BATCH_SIZE    = 10;    // symbols per OHLCV batch (P5: 5→10 so 150-symbol budget fits under 30s CPU)
const PIPE_BATCH_DELAY   = 200;   // ms between OHLCV batches (P5: 300→200; 15 batches × 200ms ≈ 3s delay)
const PIPE_MIN_CANDLES   = 220;   // min daily bars (EMA200 needs 200; +20 margin for reliable seed)
const PIPE_RS_THRESHOLD  = 55;    // RS percentile cutoff for Stream A pass
const PIPE_MAX_SECTOR_N  = 5;     // max candidates per sector in final output.
                                 // Recall fix (11-Jun): 3→5. Momentum winners cluster by sector
                                 // (sector rotation), so a cap of 3 silently dropped already-qualified
                                 // winners during strong sector moves — exactly the regime this strategy
                                 // trades. Raising to 5 admits MORE stocks that already passed every
                                 // quality filter; it does not admit any lower-quality stock (pure recall
                                 // gain, zero precision cost). S5/S6/scoring unchanged.
const PIPE_TOP_N         = 20;    // max candidates to write to KV for browser
const PIPE_SIGNAL_TOP    = 5;     // max signals dispatched via Telegram per run
// Critical Fix 1: 365 calendar days ≈ 260 trading days — sufficient for reliable EMA200
const PIPE_OHLCV_RANGE   = 760;   // ~2y read window (Option A): d1ReadCandles cutoff + gate live fallback. Browser (src=d1) and gate now read the SAME 2y D1 array -> score parity. Was 365 (1y), which flipped verdicts vs the 2y browser scan.

// ═══════════════════════════════════════════════════════════════════════════════
// D1 HISTORY CACHE (Option 2) — constants. See full design in QuantEdge_D1_Design.md
// Stores daily OHLCV bars in Cloudflare D1 so the rate-limited 365-bar historical
// fetch is replaced by a single cheap bulk-quote per day. Flag-gated; OFF until KV
// key USE_D1_CACHE === 'true'. With flag off / QE_DB unbound, the pipeline runs
// EXACTLY as before (live fetch). Nothing breaks unflipped.
// ═══════════════════════════════════════════════════════════════════════════════
const D1_BARS_STORED     = 560;   // bars stored per symbol (>=220 needed; 560 ~= 2.2y, headroom over the 760d read window). Was 400 (~1.8y). Re-run /d1/startbackfill after deploy to deepen existing rows.
const D1_BACKFILL_LIMIT  = 150;   // symbols per backfill chunk (rate-limited; click to continue)
const D1_FRESH_DAYS      = 6;     // D1 data must have a bar within N days, else fall back to live
const D1_BULK_QUOTE_SIZE = 200;   // symbols per bulk-quote call in daily update (Kite allows ~500)
const PIPE_NIFTY_TTL_MS  = 4 * 60 * 60 * 1000; // nifty cache valid 4h
// Critical Fix 2 (P5-revised): Cap OHLCV processing.
// Paid plan = 1,000 subrequests/invocation (not 50) and 30s CPU. Per-symbol cost
// is now 1 subrequest (token-map removed the per-symbol /quote). The binding
// limits at scale are CPU wall-time and Kite's historical rate limit, NOT
// subrequests. Cap raised 80→150 to match PIPE_HISTORY_BUDGET; if this were left
// below the history budget it would silently re-cap before the budget applied.
// Sorted by volume desc so highest-liquidity stocks process first. Symbols beyond
// cap are survivorship-logged as OHLCV_CAP_EXCEEDED.
const PIPE_MAX_OHLCV_CAP = 150;   // max symbols entering OHLCV fetch (P5: 80→150, matches history budget)
// Per-symbol fetch timeout — prevents a single slow Kite response stalling the batch
const PIPE_SYMBOL_TIMEOUT_MS = 12000; // 12s per symbol (quote + historical)

// ─── Sector map (NSE tradingsymbols → sector label) ──────────────────────────
// Covers ~350 liquid NSE stocks. Unlisted symbols default to "OTHER".
// Expanded in v4.0 (Critical Fix 3) to reduce OTHER bucket and improve
// sector concentration control accuracy.
const SECTOR_MAP = {
  // ── BANKING ────────────────────────────────────────────────────────────────
  HDFCBANK:"BANK", ICICIBANK:"BANK", SBIN:"BANK", KOTAKBANK:"BANK", AXISBANK:"BANK",
  INDUSINDBK:"BANK", BANKBARODA:"BANK", PNB:"BANK", CANBK:"BANK", UNIONBANK:"BANK",
  IDFCFIRSTB:"BANK", BANDHANBNK:"BANK", AUBANK:"BANK", FEDERALBNK:"BANK",
  YESBANK:"BANK", IDBI:"BANK", RBLBANK:"BANK", DCBBANK:"BANK",
  KARNATAKBANK:"BANK", CSBBANK:"BANK", SOUTHBANK:"BANK", KARURVYSYA:"BANK",
  TMVHL:"BANK", UJJIVANSFB:"BANK", ESAFSFB:"BANK", EQUITASBNK:"BANK",
  // ── NBFC / FINSERV ─────────────────────────────────────────────────────────
  BAJFINANCE:"NBFC", BAJAJFINSV:"NBFC", CHOLAFIN:"NBFC", RECLTD:"NBFC", PFC:"NBFC",
  IRFC:"NBFC", HUDCO:"NBFC", PNBHOUSING:"NBFC", IIFL:"NBFC", MUTHOOTFIN:"NBFC",
  MANAPPURAM:"NBFC", MAHINDCIE:"NBFC", M_MFIN:"NBFC", CREDITACC:"NBFC",
  SUNDARMFIN:"NBFC", LTFH:"NBFC", SHRIRAMFIN:"NBFC", MASFIN:"NBFC",
  HDFCAMC:"FINSERV", NIPPONLIFE:"FINSERV", ANGELONE:"FINSERV", CDSL:"FINSERV",
  BSE:"FINSERV", MCX:"FINSERV", CAMS:"FINSERV", MOTILALOS:"FINSERV",
  "360ONE":"FINSERV", NUVAMA:"FINSERV", KFINTECH:"FINSERV", UTIAMC:"FINSERV",
  HDFCLIFE:"INSURANCE", SBILIFE:"INSURANCE", ICICIPRULI:"INSURANCE",
  ICICIGI:"INSURANCE", STARHEALTH:"INSURANCE", GICRE:"INSURANCE",
  NIACL:"INSURANCE", LICI:"INSURANCE",
  // ── IT & TECH ──────────────────────────────────────────────────────────────
  TCS:"IT", INFY:"IT", HCLTECH:"IT", WIPRO:"IT", TECHM:"IT", LTIM:"IT",
  MPHASIS:"IT", COFORGE:"IT", PERSISTENT:"IT", KPITTECH:"IT", TATAELXSI:"IT",
  LTTS:"IT", CYIENT:"IT", MASTEK:"IT", INTELLECT:"IT", NEWGEN:"IT", AFFLE:"IT",
  TANLA:"IT", ROUTE:"IT", OFSS:"IT", HEXAWARE:"IT", BIRLASOFT:"IT",
  NIITTECH:"IT", ZENSAR:"IT", SONATSOFTW:"IT", RATEGAIN:"IT", NETWEB:"IT",
  TATACOMM:"IT", GTLINFRA:"IT",
  // ── PHARMA ─────────────────────────────────────────────────────────────────
  SUNPHARMA:"PHARMA", DRREDDY:"PHARMA", CIPLA:"PHARMA", DIVISLAB:"PHARMA",
  ZYDUSLIFE:"PHARMA", LUPIN:"PHARMA", ALKEM:"PHARMA", TORNTPHARM:"PHARMA",
  NATCOPHARM:"PHARMA", GRANULES:"PHARMA", AUROPHARMA:"PHARMA", IPCALAB:"PHARMA",
  GLENMARK:"PHARMA", ABBOTINDIA:"PHARMA", LAURUSLABS:"PHARMA", JBCHEPHARM:"PHARMA",
  SUVEN:"PHARMA", PFIZER:"PHARMA", SANOFI:"PHARMA", AJANTPHARM:"PHARMA",
  GLAXO:"PHARMA", JUBLPHARMA:"PHARMA", GLAND:"PHARMA", ERIS:"PHARMA",
  LAXMI_N_INC:"PHARMA", SEQUENT:"PHARMA", SOLARA:"PHARMA",
  // ── HEALTHCARE ─────────────────────────────────────────────────────────────
  APOLLOHOSP:"HEALTHCARE", FORTIS:"HEALTHCARE", NARAYANA:"HEALTHCARE",
  LALPATHLAB:"HEALTHCARE", METROPOLIS:"HEALTHCARE", MAXHEALTH:"HEALTHCARE",
  THYROCARE:"HEALTHCARE", KRSNAA:"HEALTHCARE", VIJAYA:"HEALTHCARE",
  MEDANTA:"HEALTHCARE", ASTER:"HEALTHCARE", RAINBOW:"HEALTHCARE",
  // ── AUTO & EV ──────────────────────────────────────────────────────────────
  MARUTI:"AUTO", TATAMOTORS:"AUTO", EICHERMOT:"AUTO", "BAJAJ-AUTO":"AUTO",
  HEROMOTOCO:"AUTO", ASHOKLEY:"AUTO", MOTHERSON:"AUTO", SUNDRMFAST:"AUTO",
  BHARATFORG:"AUTO", SANSERA:"AUTO", BALKRISIND:"AUTO", MRF:"AUTO",
  BOSCHLTD:"AUTO", TIINDIA:"AUTO", CRAFTSMAN:"AUTO", GABRIEL:"AUTO",
  SUPRAJIT:"AUTO", ENDURANCE:"AUTO", MNFL:"AUTO", SCHAEFFLER:"AUTO",
  EXIDEIND:"AUTO", AMARAJABAT:"AUTO", LUMAXTECH:"AUTO", SUBROS:"AUTO",
  SHREECEM:"AUTO", SPARKMINDA:"AUTO", VARROC:"AUTO",
  // ── CAPITAL GOODS ──────────────────────────────────────────────────────────
  LT:"CAPGOODS", SIEMENS:"CAPGOODS", ABB:"CAPGOODS",
  THERMAX:"CAPGOODS", CUMMINSIND:"CAPGOODS",
  ELECON:"CAPGOODS", RATNAMANI:"CAPGOODS", WELCORP:"CAPGOODS",
  AHLUCONT:"CAPGOODS", KSB:"CAPGOODS", GRINDWELL:"CAPGOODS",
  SKFINDIA:"CAPGOODS", TIMKEN:"CAPGOODS", VOLTAMP:"CAPGOODS",
  KECL:"CAPGOODS", KALPATPOWR:"CAPGOODS", BHEL:"CAPGOODS",
  RAILVIKAS:"CAPGOODS", RVNL:"CAPGOODS", IRCON:"CAPGOODS",
  NBCC:"CAPGOODS", WABCO:"CAPGOODS", ELGIEQUIP:"CAPGOODS",
  // ── DEFENCE ────────────────────────────────────────────────────────────────
  BEL:"DEFENCE", HAL:"DEFENCE", COCHINSHIP:"DEFENCE", MAZAGON:"DEFENCE",
  GRSE:"DEFENCE", SOLARINDS:"DEFENCE", MTAR:"DEFENCE", HBLENGINE:"DEFENCE",
  DATAPATTNS:"DEFENCE", PARAS:"DEFENCE", ZEN:"DEFENCE", IDEAFORGE:"DEFENCE",
  ROSSARI:"DEFENCE", MIDHANI:"DEFENCE",
  // ── ENERGY / OIL & GAS ─────────────────────────────────────────────────────
  ONGC:"ENERGY", BPCL:"ENERGY", IOC:"ENERGY", HINDPETRO:"ENERGY",
  GAIL:"ENERGY", IGL:"ENERGY", MGL:"ENERGY", PETRONET:"ENERGY",
  OIL:"ENERGY", MRPL:"ENERGY", CHENNPETRO:"ENERGY",
  // ── POWER ──────────────────────────────────────────────────────────────────
  POWERGRID:"POWER", NTPC:"POWER", TATAPOWER:"POWER", CESC:"POWER",
  TORNTPOWER:"POWER", JSWENERGY:"POWER", SUZLON:"POWER", INOXWIND:"POWER",
  SJVN:"POWER", NHPC:"POWER", RECLTD:"POWER", GREENPWR:"POWER",
  ADANIPOWER:"POWER", ADANIGREEN:"POWER",
  // ── METALS & MINING ────────────────────────────────────────────────────────
  TATASTEEL:"METALS", JSWSTEEL:"METALS", HINDALCO:"METALS",
  COALINDIA:"METALS", NMDC:"METALS", VEDL:"METALS",
  NALCO:"METALS", MOIL:"METALS", SAIL:"METALS", JINDALSTEL:"METALS",
  WELSPUNIND:"METALS", APLAPOLLO:"METALS", ASHAPURMIN:"METALS",
  RAMKRISHNA:"METALS", GPPL:"METALS", HINDCOPPER:"METALS",
  // ── FMCG / CONSUMER ────────────────────────────────────────────────────────
  HINDUNILVR:"FMCG", NESTLEIND:"FMCG", DABUR:"FMCG", MARICO:"FMCG",
  GODREJCP:"FMCG", EMAMILTD:"FMCG", BRITANNIA:"FMCG", TATACONSUM:"FMCG",
  COLPAL:"FMCG", VBL:"FMCG", RADICO:"FMCG", UBL:"FMCG",
  MCDOWELL_N:"FMCG", ITC:"FMCG", GODFRYPHLP:"FMCG", VSTIND:"FMCG",
  PATANJALI:"FMCG", BAJAJCON:"FMCG", JYOTHYLAB:"FMCG",
  // ── RETAIL ─────────────────────────────────────────────────────────────────
  TRENT:"RETAIL", DMART:"RETAIL", PAGEIND:"RETAIL", MANYAVAR:"RETAIL",
  METRO:"RETAIL", BATAINDIA:"RETAIL", SHOPERSTOP:"RETAIL",
  VMART:"RETAIL", ZUDIO:"RETAIL",
  // ── HOSPITALITY / TRAVEL ───────────────────────────────────────────────────
  INDHOTEL:"HOSPITALITY", LEMONTREE:"HOSPITALITY", CHALET:"HOSPITALITY",
  EIHOTEL:"HOSPITALITY", MAHINDHOLIDAY:"HOSPITALITY",
  IRCTC:"TRAVEL", THOMASCOOK:"TRAVEL", SPICEJET:"TRAVEL",
  INDIGO:"TRAVEL", GMRAIRPORT:"TRAVEL",
  // ── INTERNET / PLATFORM ────────────────────────────────────────────────────
  ZOMATO:"INTERNET", NYKAA:"INTERNET", POLICYBZR:"INTERNET",
  INDIAMART:"INTERNET", NAUKRI:"INTERNET", JUSTDIAL:"INTERNET",
  CARTRADE:"INTERNET", PAYTM:"INTERNET", DELHIVERY:"INTERNET",
  MAPMYINDIA:"INTERNET",
  // ── TELECOM ────────────────────────────────────────────────────────────────
  BHARTIARTL:"TELECOM", TATACOMM:"TELECOM", RAILTEL:"TELECOM",
  HFCL:"TELECOM", STLTECH:"TELECOM",
  // ── CEMENT ─────────────────────────────────────────────────────────────────
  ULTRACEMCO:"CEMENT", JKCEMENT:"CEMENT",
  RAMCOCEM:"CEMENT", DALMIA:"CEMENT", AMBUJACEMENT:"CEMENT",
  ACCLTD:"CEMENT", HEIDELBERG:"CEMENT", BIRLACORPN:"CEMENT",
  NCLIND:"CEMENT", JKLAKSHMI:"CEMENT",
  // ── REALTY ─────────────────────────────────────────────────────────────────
  OBEROIRLTY:"REALTY", GODREJPROP:"REALTY", DLF:"REALTY",
  PRESTIGE:"REALTY", BRIGADE:"REALTY", SOBHA:"REALTY",
  PHOENIXLTD:"REALTY", SUNTECK:"REALTY", MAHLIFE:"REALTY",
  KOLTEPATIL:"REALTY", LODHA:"REALTY", SIGNATURE:"REALTY",
  // ── CHEMICALS ──────────────────────────────────────────────────────────────
  PIIND:"CHEM", DEEPAKNTR:"CHEM", ALKYLAMINE:"CHEM",
  TATACHEM:"CHEM", NAVINFLUOR:"CHEM", UPL:"CHEM",
  AARTI:"CHEM", VINATI:"CHEM", FINEORG:"CHEM", GALAXYSURF:"CHEM",
  CLEAN_SCI:"CHEM", NOCIL:"CHEM", SUDARSCHEM:"CHEM",
  BALAJI_AM:"CHEM", THIRUMALCHM:"CHEM",
  // ── ELECTRONICS & MANUFACTURING ────────────────────────────────────────────
  DIXON:"ELEC", KAYNES:"ELEC", SYRMA:"ELEC", AMBER:"ELEC", PGEL:"ELEC",
  HAVELLS:"ELEC", POLYCAB:"ELEC", VOLTAS:"ELEC",
  BLUESTARCO:"ELEC", VGUARD:"ELEC",
  AVALON:"ELEC", ABSLAMC:"ELEC",
  // ── PAINTS / BUILDING MATERIALS ────────────────────────────────────────────
  ASIANPAINT:"PAINTS", PIDILITIND:"PAINTS", BERGER:"PAINTS",
  KANSAINER:"PAINTS", INDIGO_P:"PAINTS",
  SUPREMEIND:"BUILDMAT", ASTRAL:"BUILDMAT", PRINCEPIPE:"BUILDMAT",
  FINOLEX:"BUILDMAT", CENTURYPLY:"BUILDMAT", GREENPANEL:"BUILDMAT",
  KAJARIACER:"BUILDMAT", ORIENTBELL:"BUILDMAT",
  // ── AGRI / FERTILISERS ─────────────────────────────────────────────────────
  CHAMBLFERT:"AGRI", COROMANDEL:"AGRI", GNFC:"AGRI",
  GSFC:"AGRI", NFL:"AGRI", PARADEEP:"AGRI",
  KAVERI:"AGRI", AVANTIFEED:"AGRI",
  // ── LOGISTICS ──────────────────────────────────────────────────────────────
  CONCOR:"LOGISTICS", BLUEDART:"LOGISTICS", TCI:"LOGISTICS",
  VRL:"LOGISTICS", ALLCARGO:"LOGISTICS", MAHSCOOTER:"LOGISTICS",
  GATI:"LOGISTICS",
  // ── CONGLOMERATE ───────────────────────────────────────────────────────────
  RELIANCE:"CONGLOMERATE", ADANIENT:"CONGLOMERATE",
  // ── LUXURY / LIFESTYLE ─────────────────────────────────────────────────────
  TITAN:"LUXURY", CERA:"LUXURY", PCJEWELLER:"LUXURY",
  SENCO:"LUXURY", KALYAN:"LUXURY",
  // ── MEDIA ──────────────────────────────────────────────────────────────────
  ZEEL:"MEDIA", SUNTV:"MEDIA", PVR:"MEDIA", INOX:"MEDIA",
  SAREGAMA:"MEDIA", TIPS:"MEDIA",
  // ── SMALLCAP SPECIAL SITUATIONS (in universe, need sector for cap control) ─
  ACUTAAS:"SMALLCAP", AEROFLEX:"SMALLCAP", ATHERENERG:"SMALLCAP",
  SPANDANA:"SMALLCAP", APTUS:"SMALLCAP", CAMPUS:"SMALLCAP",
  AAVAS:"SMALLCAP", HOMEFIRST:"SMALLCAP", SPORTKING:"SMALLCAP",
};

// ─── Pipeline audit logger ────────────────────────────────────────────────────
// Appends to in-memory audit array during a pipeline run.
// Written to KV at end of pipeline.
function makePipeAudit() {
  const entries = [];
  function log(phase, symbol, action, detail) {
    entries.push({
      ts:     new Date().toISOString(),
      phase:  phase,
      symbol: symbol || "",
      action: action,
      detail: detail || "",
    });
  }
  function getAll() { return entries; }
  return { log: log, getAll: getAll };
}

// ─── Pipeline status writer ───────────────────────────────────────────────────
async function writePipeStatus(env, phase, pct, extra) {
  const status = Object.assign({
    phase:     phase,
    pct:       pct,
    updatedAt: new Date().toISOString(),
  }, extra || {});
  try {
    await env.KITE_STORE.put("qe_pipe_status", JSON.stringify(status));
  } catch (e) { console.warn("[writePipeStatus] non-fatal:", e && e.message); }
}

// ─── Survivorship logger ──────────────────────────────────────────────────────
// Records every eliminated symbol with the stage and reason it was removed.
// Written to qe_pipe_survivorship at end of pipeline.
function makeSurvivorshipLog() {
  const eliminated = [];
  function drop(symbol, stage, reason) {
    eliminated.push({
      symbol: symbol,
      stage:  stage,
      reason: reason,
      ts:     new Date().toISOString(),
    });
  }
  function getAll() { return eliminated; }
  return { drop: drop, getAll: getAll };
}

// ─── Unique run ID generator ──────────────────────────────────────────────────
function genRunId() {
  const arr = new Uint8Array(8);
  crypto.getRandomValues(arr);
  return Array.from(arr).map(function(b) {
    return ("00" + b.toString(16)).slice(-2);
  }).join("");
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 1 — UNIVERSE LOAD
// ═══════════════════════════════════════════════════════════════════════════════
async function pipeLoadUniverse(env, audit) {
  const raw = await env.KITE_STORE.get("qe_db_universe");
  if (!raw) {
    audit.log("S1_UNIVERSE", "", "ERROR", "qe_db_universe not found in KV — run universe refresh first");
    return null;
  }
  let symbols;
  try {
    symbols = JSON.parse(raw);
  } catch (_) {
    audit.log("S1_UNIVERSE", "", "ERROR", "Failed to parse qe_db_universe JSON");
    return null;
  }
  if (!Array.isArray(symbols) || symbols.length < 50) {
    audit.log("S1_UNIVERSE", "", "ERROR", "Universe too small: " + (symbols ? symbols.length : 0));
    return null;
  }
  audit.log("S1_UNIVERSE", "", "LOADED", "Universe: " + symbols.length + " symbols");
  return symbols;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 2 — NSE BHAV COPY INGEST
// Uses Kite bulk quote to get last_price, prev_close, volume for all universe symbols.
// Bhav Copy = today's close, volume, and price change — used for Stream A pre-filter.
// No historical data here — this is the "fast" pass to eliminate ~80% of universe.
//
// Kite /quote accepts up to 500 instruments per call in format "NSE:SYMBOL".
// We batch into chunks of 400 to stay well within limit.
// ═══════════════════════════════════════════════════════════════════════════════
const BHAV_BATCH_SIZE = 250;   // Kite full /quote hard cap = 250 instruments/call (was 400 → HTTP 403)

// Parse one /quote response into the bhav map. Returns count written.
function pipeWriteQuotesToBhav(quotes, batch, bhav) {
  let written = 0;
  for (let bi = 0; bi < batch.length; bi++) {
    const sym = batch[bi];
    const q   = quotes["NSE:" + sym];
    if (!q) continue;
    const last   = q.last_price || 0;
    const ohlc   = q.ohlc || {};
    const prev   = ohlc.close || last;
    const vol    = q.volume || 0;
    const chgPct = prev > 0 ? ((last - prev) / prev) * 100 : 0;
    bhav[sym] = {
      last_price: last, prev_close: prev, volume: vol, change_pct: chgPct,
      day_open: ohlc.open || 0, day_high: ohlc.high || 0, day_low: ohlc.low || 0,
    };
    written++;
  }
  return written;
}

// Fetch one batch of symbols via /quote. Evidence (audit 2026-06-09 run
// 811de142): a 250-sym batch returned HTTP 403, but the SAME symbols succeeded
// on smaller/slower calls with ZERO poison symbols found — i.e. the 403 is a
// transient Kite rate-limit, not a bad ticker. So we do ONE retry after a short
// delay rather than a recursive split (the split fan-out consumed ~10 extra
// subrequests and tripped the 50-cap, failing 6 OHLCV fetches + the completion
// message). One retry = at most 1 extra subrequest. Cost-safe.
async function pipeFetchQuoteBatch(env, token, syms, bhav, audit) {
  if (syms.length === 0) return;
  const istr = syms.map(function(s) { return "i=NSE:" + encodeURIComponent(s); }).join("&");

  for (let attempt = 1; attempt <= 2; attempt++) {
    let resp;
    try {
      resp = await fetch(`${KITE_API_BASE}/quote?${istr}`,
        { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) } });
    } catch (e) {
      audit.log("S2_BHAV", "", "WARN", "Quote batch fetch error: " + e.message);
      return;
    }
    if (resp.ok) {
      const data = await resp.json();
      pipeWriteQuotesToBhav((data && data.data) ? data.data : {}, syms, bhav);
      return;
    }
    // Transient rate-limit (403/429/5xx) → wait and retry ONCE.
    const retryable = resp.status === 403 || resp.status === 429 || resp.status >= 500;
    if (attempt === 1 && retryable) {
      audit.log("S2_BHAV", "", "RETRY",
        "Batch HTTP " + resp.status + " — waiting 800ms then retrying once (rate-limit)");
      await new Promise(function(r) { setTimeout(r, 800); });
      continue;
    }
    audit.log("S2_BHAV", "", "WARN",
      "Batch failed after " + attempt + " attempt(s): HTTP " + resp.status
      + " — " + syms.length + " symbols not fetched this run");
    return;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// v4.22: D1 last-session bar map (close + volume) for the MANUAL close fallback.
// One bulk read = ONE subrequest of every symbol's most-recent stored bar.
// Verified vs Cloudflare D1 limits: reads count toward the 1000/invocation budget
// (this is 1), no row-count cap for a ~2.9k-row/~90KB result, 30s max duration.
// Uses the same .all()->.results client shape proven by d1ReadCandles. Returns {}
// (caller keeps live values) if QE_DB unbound or the query errors — never throws.
async function pipeD1LastBarMap(env, audit) {
  if (!env.QE_DB) {
    audit.log("S2_BHAV", "", "FALLBACK", "D1 unbound — close fallback source unavailable");
    return {};
  }
  try {
    const rs = await env.QE_DB
      .prepare("SELECT symbol, c, v, bar_date FROM ohlcv_daily WHERE bar_date = (SELECT MAX(bar_date) FROM ohlcv_daily)")
      .all();
    const rows = (rs && rs.results) || [];
    const map = {};
    let barDate = null;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!r || !r.symbol) continue;
      map[r.symbol] = { c: Number(r.c) || 0, v: Number(r.v) || 0 };
      if (!barDate) barDate = r.bar_date;
    }
    audit.log("S2_BHAV", "", "FALLBACK",
      "D1 last-bar map: " + Object.keys(map).length + " symbols @ " + (barDate || "?"));
    return map;
  } catch (e) {
    audit.log("S2_BHAV", "", "FALLBACK", "D1 last-bar query failed: " + (e && e.message));
    return {};
  }
}

async function pipeBhavCopy(env, token, symbols, audit, survive, opts) {
  audit.log("S2_BHAV", "", "START", "Fetching bhav copy for " + symbols.length + " symbols");

  const bhav = {}; // symbol → { last_price, prev_close, volume, change_pct }

  for (let i = 0; i < symbols.length; i += BHAV_BATCH_SIZE) {
    const batch = symbols.slice(i, i + BHAV_BATCH_SIZE);

    // Healthy batch = 1 /quote call. On transient 403/429/5xx, one spaced
    // retry recovers it (rate-limit, not poison symbol — proven by audit).
    await pipeFetchQuoteBatch(env, token, batch, bhav, audit);

    // Small delay between bulk quote batches
    if (i + BHAV_BATCH_SIZE < symbols.length) {
      await new Promise(function(r) { setTimeout(r, 200); });
    }
  }

  const covered = Object.keys(bhav).length;
  audit.log("S2_BHAV", "", "DONE",
    "Bhav: " + covered + "/" + symbols.length + " symbols with live quote");

  // ── v4.22: MANUAL-ONLY close fallback (data-driven, volume==0) ───────────────
  // Verified (Kite forum/docs): on a non-trading day/holiday last_price IS the last
  // close and ohlc.close is the prior close (so change_pct already reflects the last
  // session's move) — only `volume` resets to 0, and that 0 is what kills every
  // symbol at Stream A Fast. On a MANUAL trigger (opts.closeFallback) restore the
  // last session's VOLUME from D1's most recent bar so Stream A Fast replays that
  // session instead of rejecting on zero volume. Crons never set this flag -> always
  // live. Purely ADDITIVE: can only RESTORE symbols volume==0 falsely dropped; never
  // removes any. Graceful: D1 unbound / no bar / bar.v<=0 -> symbol keeps live values.
  if (opts && opts.closeFallback) {
    const d1 = await pipeD1LastBarMap(env, audit);
    let restoredVol = 0, builtFromD1 = 0;
    for (let i = 0; i < symbols.length; i++) {
      const s2  = symbols[i];
      const bar = d1[s2];
      if (!bar || !(bar.v > 0)) continue;          // nothing usable in D1 -> leave as-is
      const e = bhav[s2];
      if (e) {
        if (e.volume === 0) {                       // live had no volume (non-trading day)
          e.volume = bar.v;                         // replay last session's traded volume
          if (!(e.last_price > 0) && bar.c > 0) {   // LTP also missing -> use D1 close
            e.last_price = bar.c;
            e.change_pct = 0;                       // neutral; real move re-checked at OHLCV stage
          }
          e.source = "d1_volume_fallback";
          restoredVol++;
        }
      } else if (bar.c > 0) {                        // no live quote at all -> build from D1
        bhav[s2] = {
          last_price: bar.c, prev_close: bar.c, volume: bar.v, change_pct: 0,
          day_open: 0, day_high: 0, day_low: 0, source: "d1_full_fallback",
        };
        builtFromD1++;
      }
    }
    audit.log("S2_BHAV", "", "FALLBACK",
      "Manual close fallback: volume restored for " + restoredVol
      + " symbols, " + builtFromD1 + " built from D1. Live volume>0 left untouched.");
  }

  // Symbols with no quote get eliminated here (unlisted, halted, etc.)
  const noQuote = symbols.filter(function(s) { return !bhav[s]; });
  for (let ni = 0; ni < noQuote.length; ni++) {
    survive.drop(noQuote[ni], "S2_BHAV", "No quote returned from Kite");
  }

  // Store bhav to KV for audit / debugging
  try {
    await env.KITE_STORE.put("qe_pipe_bhav_raw", JSON.stringify(bhav));
    await env.KITE_STORE.put("qe_pipe_bhav_date", new Date().toISOString().slice(0, 10));
  } catch (e) { console.warn("[pipeBhavCopy] non-fatal:", e && e.message); }

  return bhav;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 3 — STREAM A FILTERS (bhav-only pass — no historical API calls)
//
// Filters applied purely from Bhav Copy data.
// Purpose: eliminate ~80–90% of universe before expensive OHLCV fetch.
//
// Criteria (ALL must pass):
//   A1. Price ≥ ₹100                    (already enforced in universe, re-check)
//   A2. Volume > 200,000 shares/day      (minimum liquidity)
//   A3. |change_pct| <= 15%              (circuit filter — avoid halt/breakout anomaly)
//   A4. last_price > 0                   (valid trading price)
//
// Note: EMA/RSI/ADX checks require historical data — those are Stage 5 Stream A
// post-OHLCV filters. This stage is intentionally lightweight.
// ═══════════════════════════════════════════════════════════════════════════════
const STREAM_A_MIN_PRICE  = 100;
const STREAM_A_MIN_VOL    = 200000;
const STREAM_A_MAX_CHANGE = 15;   // %, absolute

function pipeStreamAFast(bhav, symbols, audit, survive) {
  audit.log("S3_STREAM_A_FAST", "", "START",
    "Applying bhav-only filters to " + symbols.length + " symbols");

  const passed  = [];
  let rejPrice  = 0;
  let rejVol    = 0;
  let rejChange = 0;

  for (let i = 0; i < symbols.length; i++) {
    const sym = symbols[i];
    const b   = bhav[sym];
    if (!b) continue; // already eliminated in S2

    if (b.last_price < STREAM_A_MIN_PRICE) {
      survive.drop(sym, "S3_STREAM_A_FAST", "Price < ₹" + STREAM_A_MIN_PRICE + " (₹" + b.last_price.toFixed(2) + ")");
      rejPrice++;
      continue;
    }

    if (b.volume < STREAM_A_MIN_VOL) {
      survive.drop(sym, "S3_STREAM_A_FAST", "Volume < " + STREAM_A_MIN_VOL + " (" + b.volume.toLocaleString("en-IN") + ")");
      rejVol++;
      continue;
    }

    if (Math.abs(b.change_pct) > STREAM_A_MAX_CHANGE) {
      survive.drop(sym, "S3_STREAM_A_FAST", "Change " + b.change_pct.toFixed(1) + "% exceeds ±" + STREAM_A_MAX_CHANGE + "% — circuit/anomaly");
      rejChange++;
      continue;
    }

    passed.push(sym);
  }

  audit.log("S3_STREAM_A_FAST", "", "DONE",
    "Passed: " + passed.length + " | Rej price: " + rejPrice
    + " | Rej vol: " + rejVol + " | Rej change: " + rejChange);

  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 4 — OHLCV FETCH + COMPUTE
//
// For each symbol that passed Stream A Fast:
//   1. Fetch Kite historical data (PIPE_OHLCV_RANGE days, daily candles)
//   2. Compute: EMA(20), EMA(50), EMA(200), RSI(14), ADX(14),
//               ATR(14), Supertrend(10,2), Volume SMA(20), last close,
//               52-week high proximity, percent above EMA20
//   3. Store computed result per symbol to KV (TTL: 24h)
//
// Batched PIPE_BATCH_SIZE=5 with PIPE_BATCH_DELAY=300ms between batches.
// Failed symbols → dropped with reason.
// ═══════════════════════════════════════════════════════════════════════════════

// Math helpers (self-contained — no dependency on index.html functions)
function pipeEma(values, period) {
  if (!values || values.length < period) return null;
  const k = 2 / (period + 1);
  let ema = values.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  for (let i = period; i < values.length; i++) {
    ema = values[i] * k + ema * (1 - k);
  }
  return ema;
}

function pipeSma(values, period) {
  if (!values || values.length < period) return null;
  const slice = values.slice(-period);
  return slice.reduce(function(a, b) { return a + b; }, 0) / slice.length;
}

function pipeRsi(closes, period) {
  period = period || 14;
  if (!closes || closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains  += diff;
    else          losses -= diff;
  }
  let avgGain = gains  / period;
  let avgLoss = losses / period;
  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (diff < 0 ? -diff : 0)) / period;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function pipeAtr(highs, lows, closes, period) {
  period = period || 14;
  if (!highs || highs.length < period + 1) return null;
  const trs = [];
  for (let i = 1; i < highs.length; i++) {
    const tr = Math.max(
      highs[i]  - lows[i],
      Math.abs(highs[i]  - closes[i - 1]),
      Math.abs(lows[i]   - closes[i - 1])
    );
    trs.push(tr);
  }
  if (trs.length < period) return null;
  // Wilder smoothing
  let atr = trs.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  for (let i = period; i < trs.length; i++) {
    atr = (atr * (period - 1) + trs[i]) / period;
  }
  return atr;
}

function pipeAdx(highs, lows, closes, period) {
  period = period || 14;
  if (!highs || highs.length < period * 2) return null;
  const len = highs.length;
  const dmPlus  = [];
  const dmMinus = [];
  const trs     = [];

  for (let i = 1; i < len; i++) {
    const upMove   = highs[i]  - highs[i - 1];
    const downMove = lows[i - 1] - lows[i];
    dmPlus.push((upMove > downMove && upMove > 0)   ? upMove   : 0);
    dmMinus.push((downMove > upMove && downMove > 0) ? downMove : 0);
    trs.push(Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i]  - closes[i - 1]),
      Math.abs(lows[i]   - closes[i - 1])
    ));
  }

  // Wilder smooth
  function wilderSmooth(arr, p) {
    let s = arr.slice(0, p).reduce(function(a, b) { return a + b; }, 0);
    const out = [s];
    for (let i = p; i < arr.length; i++) {
      s = s - (s / p) + arr[i];
      out.push(s);
    }
    return out;
  }

  const smTr     = wilderSmooth(trs,     period);
  const smDmPlus = wilderSmooth(dmPlus,  period);
  const smDmMinus= wilderSmooth(dmMinus, period);

  const dx = [];
  for (let i = 0; i < smTr.length; i++) {
    if (smTr[i] === 0) { dx.push(0); continue; }
    const diPlus  = (smDmPlus[i]  / smTr[i]) * 100;
    const diMinus = (smDmMinus[i] / smTr[i]) * 100;
    const sum = diPlus + diMinus;
    dx.push(sum === 0 ? 0 : Math.abs(diPlus - diMinus) / sum * 100);
  }

  if (dx.length < period) return null;
  let adx = dx.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  for (let i = period; i < dx.length; i++) {
    adx = (adx * (period - 1) + dx[i]) / period;
  }
  return adx;
}

function pipeSupertrend(highs, lows, closes, period, multiplier) {
  period     = period     || 10;
  multiplier = multiplier || 2;
  if (!highs || highs.length < period + 1) return null;

  const len = highs.length;
  const atrArr = [];

  // Compute per-bar ATR first
  for (let i = 1; i < len; i++) {
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i]  - closes[i - 1])
    );
    atrArr.push(tr);
  }

  // Wilder ATR
  let atr = atrArr.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  const atrSmooth = [atr];
  for (let i = period; i < atrArr.length; i++) {
    atr = (atr * (period - 1) + atrArr[i]) / period;
    atrSmooth.push(atr);
  }

  // Supertrend — start from index period (first valid ATR)
  let stDir   = 1; // 1 = up (bullish), -1 = down (bearish)
  let stFinal = (highs[period] + lows[period]) / 2 + multiplier * atrSmooth[0];

  for (let i = period + 1; i < len; i++) {
    const ai   = i - period; // atrSmooth index
    const hl2  = (highs[i] + lows[i]) / 2;
    const up   = hl2 - multiplier * atrSmooth[ai];
    const dn   = hl2 + multiplier * atrSmooth[ai];

    if (stDir === 1) {
      // Bullish: use lower band
      stFinal = Math.max(up, stFinal);
      if (closes[i] < stFinal) { stDir = -1; stFinal = dn; }
    } else {
      // Bearish: use upper band
      stFinal = Math.min(dn, stFinal);
      if (closes[i] > stFinal) { stDir = 1; stFinal = up; }
    }
  }

  return { direction: stDir, value: stFinal }; // 1=bullish, -1=bearish
}

// ─── Fetch Nifty 50 closes for RS calculation ─────────────────────────────────
async function pipeLoadNiftyCloses(env, token, audit) {
  // Check cache first
  try {
    const tsRaw = await env.KITE_STORE.get("qe_pipe_nifty_ts");
    if (tsRaw && Date.now() - parseInt(tsRaw) < PIPE_NIFTY_TTL_MS) {
      const cached = await env.KITE_STORE.get("qe_pipe_nifty_closes");
      if (cached) {
        const closes = JSON.parse(cached);
        audit.log("S4_NIFTY", "NIFTY", "CACHED", closes.length + " bars from KV cache");
        return closes;
      }
    }
  } catch (e) { console.warn("[pipeLoadNiftyCloses] non-fatal:", e && e.message); }

  // Fetch Nifty via Yahoo Finance (^NSEI) — no Kite instrument token needed
  try {
    const now      = new Date();
    const fromDate = new Date(now.getTime() - (PIPE_OHLCV_RANGE + 30) * 86400000);
    const from     = Math.floor(fromDate.getTime() / 1000);
    const to       = Math.floor(now.getTime() / 1000);

    const headers  = { "User-Agent": "Mozilla/5.0", "Accept": "application/json" };
    const yfUrl    = `https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI?interval=1d&period1=${from}&period2=${to}`;

    const ctrl  = new AbortController();
    const timer = setTimeout(function() { ctrl.abort(); }, 15000);
    const res   = await fetch(yfUrl, { signal: ctrl.signal, headers });
    clearTimeout(timer);

    if (!res.ok) throw new Error("Nifty YF HTTP " + res.status);

    const data = await res.json();
    const result = data && data.chart && data.chart.result && data.chart.result[0];
    if (!result) throw new Error("No Nifty chart result");

    const closes = (result.indicators && result.indicators.quote &&
                    result.indicators.quote[0] && result.indicators.quote[0].close) || [];
    const validCloses = closes.filter(function(c) { return c !== null && !isNaN(c); });

    if (validCloses.length < 50) throw new Error("Too few Nifty bars: " + validCloses.length);

    // Cache
    await env.KITE_STORE.put("qe_pipe_nifty_closes", JSON.stringify(validCloses));
    await env.KITE_STORE.put("qe_pipe_nifty_ts",     String(Date.now()));

    audit.log("S4_NIFTY", "NIFTY", "FETCHED", validCloses.length + " bars from Yahoo Finance");
    return validCloses;
  } catch (e) {
    audit.log("S4_NIFTY", "NIFTY", "ERROR", "Nifty fetch failed: " + e.message);
    return null;
  }
}

// ─── Fetch + compute OHLCV for a single symbol ───────────────────────────────
// Commit 1: instrToken now passed in from the cached token-map (qe_db_token_map),
// eliminating the per-symbol /quote call. This halves S4 subrequest cost from
// 2/symbol (quote+historical) to 1/symbol (historical only).
async function pipeFetchOhlcvSymbol(env, token, symbol, instrToken) {
  const now      = new Date();
  const fromDate = new Date(now.getTime() - (PIPE_OHLCV_RANGE + 10) * 86400000);
  const fromStr  = fromDate.toISOString().slice(0, 10);
  const toStr    = now.toISOString().slice(0, 10);

  // Step 1: instrument token comes from the cached map — NO /quote fetch.
  if (!instrToken || instrToken <= 0) {
    throw new Error("No cached token for " + symbol);
  }

  // Step 2: Fetch historical daily candles — with timeout guard
  const histCtrl  = new AbortController();
  const histTimer = setTimeout(function() { histCtrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
  let histRes;
  try {
    histRes = await fetch(
      `${KITE_API_BASE}/instruments/historical/${instrToken}/day?from=${fromStr}&to=${toStr}`,
      { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) },
        signal: histCtrl.signal }
    );
  } finally {
    clearTimeout(histTimer);
  }
  if (!histRes.ok) throw new Error("Historical HTTP " + histRes.status);
  const histData = await histRes.json();
  const candles  = (histData && histData.data && histData.data.candles) || [];

  if (candles.length < PIPE_MIN_CANDLES) {
    throw new Error("Insufficient bars: " + candles.length + " < " + PIPE_MIN_CANDLES);
  }

  // D1 refactor: indicator computation extracted into a shared function so the
  // live-fetch path and the D1-cache path feed IDENTICAL candles into IDENTICAL
  // math — guaranteeing byte-identical output by construction (Phase B verifies).
  return pipeComputeIndicatorsFromCandles(symbol, candles);
}

// ─── 2-YEAR RAW-CANDLE FETCH (QE gate parity, v4.20) ──────────────────────────
// The browser backtests on 2 YEARS (index.html fetchOHLCV default '2y'); the
// pipeline stores ~1y. stockBacktest walks the WHOLE series, so a 1y vs 2y window
// yields a different trade set -> different expectancy/MC -> OPPOSITE verdict
// (proven: APARINDS browser IGNORE @ -0.907R/MC0% vs gate PASS). To mirror the
// user's screen the gate MUST backtest on the SAME 2y window. This returns RAW
// candles (NOT indicators) because the gate's evaluate() needs the bar array.
// Same Kite endpoint/auth/forming-bar discipline as pipeFetchOhlcvSymbol — only
// the window length differs. Returns [] on any failure so the caller can fall
// back to the 1y _candles it already holds (logged), never dropping a signal.
const QE_GATE_HISTORY_DAYS = 760;  // ~2y of calendar days (matches Yahoo '2y' ≈ 500 trading bars)
async function pipeFetch2yCandles(env, token, symbol, instrToken) {
  try {
    if (!instrToken || instrToken <= 0) return { candles: [], reason: "NO_TOKEN" };
    const now     = new Date();
    const fromStr = new Date(now.getTime() - QE_GATE_HISTORY_DAYS * 86400000).toISOString().slice(0, 10);
    const toStr   = now.toISOString().slice(0, 10);
    const ctrl    = new AbortController();
    const timer   = setTimeout(function(){ ctrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(
        `${KITE_API_BASE}/instruments/historical/${instrToken}/day?from=${fromStr}&to=${toStr}`,
        { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) }, signal: ctrl.signal }
      );
    } finally { clearTimeout(timer); }
    if (!res.ok) return { candles: [], reason: "HTTP_" + res.status };
    const j = await res.json();
    const candles = (j && j.data && j.data.candles) || [];
    if (candles.length < PIPE_MIN_CANDLES) return { candles: [], reason: "FEW_" + candles.length };
    return { candles, reason: "OK_" + candles.length };
  } catch (e) { return { candles: [], reason: "EXC_" + (((e && e.message) || "?") + "").slice(0, 40) }; }
}

/* ── Commit B (v4.32): WINDOW-COMPARISON CAPABILITY — READ-ONLY, PRODUCTION UNCHANGED ──────
   Generalized N-day Kite candle fetch. pipeFetch2yCandles (production) is NOT modified;
   production still fetches QE_GATE_HISTORY_DAYS (2y). This is used ONLY by the comparison route. */
async function pipeFetchNCandles(env, token, symbol, instrToken, daysBack) {
  try {
    if (!instrToken || instrToken <= 0) return { candles: [], reason: "NO_TOKEN" };
    const now     = new Date();
    const fromStr = new Date(now.getTime() - daysBack * 86400000).toISOString().slice(0, 10);
    const toStr   = now.toISOString().slice(0, 10);
    const ctrl    = new AbortController();
    const timer   = setTimeout(function(){ ctrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(
        `${KITE_API_BASE}/instruments/historical/${instrToken}/day?from=${fromStr}&to=${toStr}`,
        { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) }, signal: ctrl.signal }
      );
    } finally { clearTimeout(timer); }
    if (!res.ok) return { candles: [], reason: "HTTP_" + res.status };
    const j = await res.json();
    const candles = (j && j.data && j.data.candles) || [];
    return { candles, reason: "OK_" + candles.length };
  } catch (e) { return { candles: [], reason: "EXC_" + (((e && e.message) || "?") + "").slice(0, 40) }; }
}

/* GET /backtest/windows?symbols=A,B,C&max=5 — READ-ONLY evidence tool for Commit B.
   For each symbol: fetch ~5y of Kite daily candles, then run the EXISTING production backtest
   chain (stockBacktest → tradeStats → edgeConfidence) on 2y / 3y / 5y bar-slices. Returns a
   per-symbol per-window comparison (bars, total, expectancy, expSE, edgeClass) plus edge-class
   stability counts. Does NOT change the production 2y window, does NOT write KV/D1, does NOT
   touch Telegram / scheduler / selection / any verdict. CPU note: signalEngine is O(n) per bar
   (backtest ≈ O(n²)); the 5y slice is heavy, so max is capped low — batch the audit set. */
async function handleBacktestWindows(request, env) {
  try {
    const url = new URL(request.url);
    const symsParam = (url.searchParams.get("symbols") || "").trim();
    if (!symsParam) return cors({ ok:false, error:"pass ?symbols=A,B,C (comma-separated), optional &max=5 (cap 10)" }, 400);
    const HARD_MAX = 10;
    let max = parseInt(url.searchParams.get("max") || "5", 10);
    if (!(max > 0)) max = 5;
    if (max > HARD_MAX) max = HARD_MAX;
    let syms = symsParam.split(",").map(function(s){ return s.trim().toUpperCase(); }).filter(Boolean);
    const requested = syms.length;
    syms = syms.slice(0, max);

    const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
    if (!token) return cors({ ok:false, error:"no kite token in KV — daily /login required" }, 503);
    let tokenMap = {};
    try { const t = await env.KITE_STORE.get("qe_db_token_map"); if (t) tokenMap = JSON.parse(t); } catch (_) {}

    // Map Kite [ts,o,h,l,c,v] -> {t,o,h,l,c,v} IDENTICALLY to evaluate(), then run the production
    // backtest chain on the slice. Backtest metrics only — no score / proFilter / verdict.
    function btSlice(rawSlice, sym) {
      const data = rawSlice.map(function(c){
        const t = (typeof c[0]==='string') ? Math.floor(Date.parse(c[0])/1000) : c[0];
        return { t:t, o:c[1], h:c[2], l:c[3], c:c[4], v:c[5] };
      });
      if (data.length < PIPE_MIN_CANDLES) return { bars:data.length, total:0, expectancy:null, expSE:null, edgeClass:"INSUFFICIENT_DATA" };
      const trades = QEGate.stockBacktest(data, sym);
      const st = QEGate.tradeStats(trades);
      const ec = QEGate.edgeConfidence(st.expectancy, trades.map(function(t){ return t.rMultiple; }));
      return { bars:data.length, total:st.total, expectancy:st.expectancy, expSE:ec.expSE, edgeClass:ec.edgeClass };
    }

    const WINDOWS = [ { name:"2y", bars:504 }, { name:"3y", bars:756 }, { name:"5y", bars:1260 } ];
    const results = [];
    let flips_2y_3y = 0, flips_3y_5y = 0, evaluated = 0;
    for (const sym of syms) {
      const instrToken = tokenMap[sym];
      if (!instrToken) { results.push({ symbol:sym, error:"NO_TOKEN (not in qe_db_token_map)" }); continue; }
      const fetched = await pipeFetchNCandles(env, token, sym, instrToken, 1825); // ~5y calendar days
      const all = fetched.candles || [];
      if (!all.length) { results.push({ symbol:sym, error:fetched.reason || "FETCH_FAIL" }); continue; }
      const row = { symbol:sym, fetched_bars:all.length, windows:{} };
      for (const w of WINDOWS) {
        const slice = all.slice(Math.max(0, all.length - w.bars));
        row.windows[w.name] = btSlice(slice, sym);
      }
      const c2 = row.windows["2y"].edgeClass, c3 = row.windows["3y"].edgeClass, c5 = row.windows["5y"].edgeClass;
      if (c2 !== c3) flips_2y_3y++;
      if (c3 !== c5) flips_3y_5y++;
      row.edgeClass_path = c2 + " -> " + c3 + " -> " + c5;
      results.push(row); evaluated++;
    }

    return cors({
      ok: true,
      note: "READ-ONLY comparison. Production window UNCHANGED (still 2y). Data source: Kite (the gate's source).",
      summary: {
        production_window: "2y",
        symbols_requested: requested,
        symbols_evaluated: evaluated,
        max_per_call: max,
        edgeClass_flips_2y_to_3y: flips_2y_3y,
        edgeClass_flips_3y_to_5y: flips_3y_5y
      },
      results
    }, 200);
  } catch (e) {
    return cors({ ok:false, error:(e && e.message) || String(e) }, 500);
  }
}

/* GET /diff/layers?offset=0&limit=120&regime=neutral — Phase 1 READ-ONLY layer differential (v4.39).
   Measures the PRODUCTION signal path on Kite/D1: for each D1 symbol, runs the verbatim gate
   (QEGate.evaluate) to get the real verdict, then re-evaluates finalDecision with ONE layer
   neutralized at a time (MC-veto / Pro Filter / Elite) and tallies which verdicts flip + direction
   (production -> layer-off). Strictly diagnostic — NO writes, NO Telegram, NO change to any
   score/verdict/ranking/selection. Paginate via offset; limit hard-capped to protect CPU budget. */
async function handleDiffLayers(request, env){
  try {
    if(!env.QE_DB) return cors({ ok:false, error:"QE_DB unbound — D1 universe unavailable" }, 503);
    const url = new URL(request.url);
    let offset = parseInt(url.searchParams.get("offset") || "0", 10);   if(!(offset >= 0)) offset = 0;
    let limit  = parseInt(url.searchParams.get("limit")  || "120", 10); if(!(limit  >  0)) limit  = 120;
    const HARD_MAX = 250; if(limit > HARD_MAX) limit = HARD_MAX;
    const regime = (url.searchParams.get("regime") || "neutral").trim();

    const totRs = await env.QE_DB.prepare("SELECT COUNT(DISTINCT symbol) AS n FROM ohlcv_daily").first();
    const universeTotal = (totRs && totRs.n) || 0;
    const symRs = await env.QE_DB
      .prepare("SELECT DISTINCT symbol FROM ohlcv_daily ORDER BY symbol LIMIT ?1 OFFSET ?2")
      .bind(limit, offset).all();
    const syms = ((symRs && symRs.results) || []).map(function(r){ return r.symbol; });

    const dist = { BUY:0, WAIT:0, IGNORE:0 };
    const layers = {
      mc_veto:    { flips:0, dir:{}, examples:[] },
      pro_filter: { flips:0, dir:{}, examples:[] },
      elite:      { flips:0, dir:{}, examples:[] }
    };
    let evaluated = 0, skipped = 0, reconMismatch = 0;
    const skipReasons = {};
    const EX_CAP = 6;

    function record(layer, base, toggleLabel, sym, r){
      if(base === toggleLabel) return;
      layer.flips++;
      const key = base + "->" + toggleLabel;
      layer.dir[key] = (layer.dir[key] || 0) + 1;
      if(layer.examples.length < EX_CAP){
        layer.examples.push({ symbol:sym, from:base, to:toggleLabel,
          ev:r.ev, mcProb:r.mcProb, edgeClass:r.edgeClass, score:r.score, baseScore:r.baseScore,
          isRejected:r.isRejected, freshBreakout:r.freshBreakout, elite:r.elite });
      }
    }

    for(const sym of syms){
      const candles = await d1ReadCandles(env, sym);
      if(!candles){ skipped++; skipReasons["no_candles_or_stale"] = (skipReasons["no_candles_or_stale"]||0) + 1; continue; }
      const r = QEGate.evaluateDiff(candles, regime, null);
      if(!r || !r.ok){ skipped++; const rk = (r && r.reason) || "NO_VERDICT"; skipReasons[rk] = (skipReasons[rk]||0) + 1; continue; }
      if(r.reconOK === false) reconMismatch++;
      evaluated++;
      if(dist[r.base] !== undefined) dist[r.base]++;
      record(layers.mc_veto,    r.base, r.noMC,    sym, r);
      record(layers.pro_filter, r.base, r.noPro,   sym, r);
      record(layers.elite,      r.base, r.noElite, sym, r);
    }

    return cors({
      ok:true,
      note:"READ-ONLY Phase-1 differential. NO writes, NO Telegram, NO verdict/score/ranking change. " +
           "Production engine (QEGate) on Kite/D1. Each layer neutralized one at a time; direction = production -> layer-off.",
      basis:{ data_source:"D1 (Kite cache)", regime:regime, rs:"neutralized (rsScore=null)",
              universe_total:universeTotal, offset:offset, limit:limit,
              symbols_in_slice:syms.length, evaluated:evaluated, skipped:skipped, skip_reasons:skipReasons },
      fidelity:{ recon_vs_production_mismatches:reconMismatch,
                 note:"reconstructed-stock verdict vs production verdict (0 = reconstruction faithful, toggles trustworthy)" },
      baseline_distribution:dist,
      layers:layers
    }, 200);
  } catch(e){
    return cors({ ok:false, error:(e && e.message) || String(e) }, 500);
  }
}

/* GET /breakout/debug?symbols=A,B,C&max=5&back=N — READ-ONLY criterion-level visibility for the
   fresh-breakout detector. Per symbol: fetch ~3y Kite candles (optional &back=N slices to a
   historical snapshot so a benchmark can be tested AT its breakout window), then run
   QEGate.breakoutDebug for every criterion value + the REAL detector result + a match check, plus
   the real gate verdict via QEGate.evaluate. Modifies NOTHING; no KV/D1 writes, no Telegram. */
async function handleBreakoutDebug(request, env){
  try {
    const url = new URL(request.url);
    const symsParam = (url.searchParams.get("symbols") || "").trim();
    if(!symsParam) return cors({ ok:false, error:"pass ?symbols=A,B,C (comma-separated), optional &max=5 &back=N" }, 400);
    const HARD_MAX = 10;
    let max = parseInt(url.searchParams.get("max") || "5", 10); if(!(max>0)) max=5; if(max>HARD_MAX) max=HARD_MAX;
    let back = parseInt(url.searchParams.get("back") || "0", 10); if(!(back>=0)) back=0;
    let syms = symsParam.split(",").map(function(s){ return s.trim().toUpperCase(); }).filter(Boolean);
    const requested = syms.length; syms = syms.slice(0, max);

    const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
    if(!token) return cors({ ok:false, error:"no kite token in KV — daily /login required" }, 503);
    let tokenMap = {}; try { const t = await env.KITE_STORE.get("qe_db_token_map"); if(t) tokenMap = JSON.parse(t); } catch(_){}
    let regimeStr = "NEUTRAL"; try { const rg = await env.KITE_STORE.get("qe_regime"); if(rg){ const ro = JSON.parse(rg); regimeStr = ro.regime || ro.label || regimeStr; } } catch(_){}

    const results = [];
    for(const sym of syms){
      const instrToken = tokenMap[sym];
      if(!instrToken){ results.push({ symbol:sym, error:"NO_TOKEN (not in qe_db_token_map)" }); continue; }
      const fetched = await pipeFetchNCandles(env, token, sym, instrToken, 1100); // ~3y so &back=N still leaves >=260
      const all = fetched.candles || [];
      if(!all.length){ results.push({ symbol:sym, error:fetched.reason || "FETCH_FAIL" }); continue; }
      const used = back > 0 ? all.slice(0, Math.max(0, all.length - back)) : all;
      const C = used.map(function(c){ return c[4]; }), H = used.map(function(c){ return c[2]; }),
            L = used.map(function(c){ return c[3]; }), V = used.map(function(c){ return c[5]; });
      const dbg = QEGate.breakoutDebug(C, H, L, V);
      let verdict = "?";
      try { const ev = QEGate.evaluate(used, regimeStr, 0); verdict = (ev && (ev.label || ev.reason)) || "?"; }
      catch(e){ verdict = "ERR:" + (((e && e.message) || "") + "").slice(0, 30); }
      results.push(Object.assign({ symbol:sym, back:back, fetched_bars:all.length, used_bars:used.length, final_verdict:verdict }, dbg));
    }
    return cors({ ok:true,
      note:"READ-ONLY. detectFreshBreakout & evaluate are UNCHANGED; this route only reads. final_verdict uses regime from KV + rsScore=0 (informational).",
      symbols_requested:requested, max_per_call:max, back:back, results:results }, 200);
  } catch(e){ return cors({ ok:false, error:(e && e.message) || String(e) }, 500); }
}

// ─── Shared indicator computation (used by BOTH live fetch and D1 cache) ───────
// candles: array of [timestamp, open, high, low, close, volume]. This is the
// SINGLE source of indicator math. Do not duplicate this logic anywhere.
function pipeComputeIndicatorsFromCandles(symbol, candles) {
  // ── FORMING-BAR GUARD (root-cause fix 12-Jun-2026; time-aware in v4.17) ──────
  // Kite's day-historical endpoint returns TODAY'S still-forming bar during market
  // hours. At the 09:30 scan its volume is ~zero, so volRatio (= lastBarVol /
  // 20d-avg) collapses to ~0.05 and the hard volume gate (volRatio < 0.8 -> reject)
  // rejected almost every stock -> 0 candidates at open. It also broke D1 parity
  // (live carried the forming bar; D1 stores only completed bars). PROVEN: two
  // verify reads 12 min apart showed live lastClose/volRatio/rsi tracking the open
  // session while D1 stayed fixed.
  // TIME-AWARE RULE (v4.17 — self-audit caught a v4.16 regression): drop today's
  // bar ONLY while it is still forming, i.e. before 15:45 IST (15:30 close + buffer
  // for the closing session). AFTER 15:45 IST today's bar is COMPLETE and must be
  // KEPT — evening runs (e.g. last night 20:46, which found 3 candidates) depend on
  // today's full-volume bar. Unconditional dropping would have silently staled
  // every post-close scan to yesterday's data.
  // Known benign window: 15:45–16:00 IST live keeps today's bar but D1 only gains
  // it at the 16:00 daily update -> /d1/verify in that 15-min window may say REVIEW.
  // No scheduled scan runs in that window; verify after 16:05 for a clean read.
  if (candles && candles.length > 1) {
    const lastTs = candles[candles.length - 1][0]; // ISO string e.g. "2026-06-12T00:00:00+0530"
    if (lastTs) {
      const barDate  = String(lastTs).slice(0, 10);                    // 'YYYY-MM-DD'
      // IST clock (UTC + 5h30m), independent of server TZ.
      const istNow   = new Date(Date.now() + (5 * 60 + 30) * 60000);
      const istToday = istNow.toISOString().slice(0, 10);
      const istMins  = istNow.getUTCHours() * 60 + istNow.getUTCMinutes();
      const BAR_COMPLETE_IST_MINS = 15 * 60 + 45;  // 15:45 IST
      if (barDate === istToday && istMins < BAR_COMPLETE_IST_MINS) {
        candles = candles.slice(0, -1); // drop the incomplete forming bar
      }
    }
  }

  const opens   = candles.map(function(c) { return c[1]; });
  const highs   = candles.map(function(c) { return c[2]; });
  const lows    = candles.map(function(c) { return c[3]; });
  const closes  = candles.map(function(c) { return c[4]; });
  const volumes = candles.map(function(c) { return c[5]; });

  const lastClose = closes[closes.length - 1];
  const lastVol   = volumes[volumes.length - 1];
  const lastHigh  = highs[highs.length - 1];

  // Compute indicators
  const ema20  = pipeEma(closes, 20);
  const ema50  = pipeEma(closes, 50);
  const ema200 = pipeEma(closes, 200);
  const rsi14  = pipeRsi(closes, 14);
  const atr14  = pipeAtr(highs, lows, closes, 14);
  const adx14  = pipeAdx(highs, lows, closes, 14);
  const st     = pipeSupertrend(highs, lows, closes, 10, 2);
  const volSma = pipeSma(volumes, 20);

  // Derived metrics
  const atrPct      = (atr14 !== null && lastClose > 0) ? (atr14 / lastClose) * 100 : null;
  const pctAboveE20 = (ema20 !== null && lastClose > 0) ? ((lastClose - ema20) / ema20) * 100 : null;
  // R3 fix: 365 calendar days ≈ 248 trading days fetched. Use the actual
  // available window (min of 248 and what we have) rather than 252, which
  // silently fell short and made "52w high" really a ~49-week high.
  const win52w      = Math.min(closes.length, 248);
  const hi52w       = Math.max.apply(null, highs.slice(-win52w));
  const prox52w     = hi52w > 0 ? ((lastClose / hi52w) * 100) : null; // % of 52w high
  const volRatio    = (volSma && volSma > 0) ? lastVol / volSma : null;

  // EMA stack (bullish = 20 > 50 > 200 AND price > 20)
  const emaStackBull = (ema20 !== null && ema50 !== null && ema200 !== null)
    ? (ema20 > ema50 && ema50 > ema200 && lastClose > ema20)
    : false;

  // Supertrend bullish
  const stBull = st ? st.direction === 1 : false;

  // ── Stream B field completion (additive — no existing field touched) ──────────
  // All derived from arrays/values already in scope. No new fetches.
  const sbVolSma3   = pipeSma(volumes.slice(-3), 3);
  const sbVolSma10  = pipeSma(volumes.slice(-10), 10);
  const sbVolAccel  = (sbVolSma3 && sbVolSma10) ? (sbVolSma3 > sbVolSma10) : false;
  const sbMtv       = (volSma && lastClose) ? (lastClose * volSma) / 100000 : 0;        // ₹ Lakh/day
  const sbPctBelow52w = (hi52w > 0) ? ((hi52w - lastClose) / hi52w) * 100 : 99;          // % BELOW high
  const sbH20       = Math.max.apply(null, highs.slice(-20));
  const sbL20       = Math.min.apply(null, lows.slice(-20));
  const sbRange20pct = (sbL20 > 0 && isFinite(sbL20)) ? ((sbH20 - sbL20) / sbL20) * 100 : 99;
  const sbRs1m      = (closes.length >= 22)
    ? ((lastClose - closes[closes.length - 22]) / closes[closes.length - 22])
    : 0;

  return {
    symbol:       symbol,
    lastClose:    lastClose,
    lastVol:      lastVol,
    ema20:        ema20,
    ema50:        ema50,
    ema200:       ema200,
    rsi14:        rsi14,
    atr14:        atr14,
    atrPct:       atrPct,
    adx14:        adx14,
    stBull:       stBull,
    stValue:      st ? st.value : null,
    volSma20:     volSma,
    volRatio:     volRatio,
    pctAboveE20:  pctAboveE20,
    hi52w:        hi52w,
    prox52w:      prox52w,
    emaStackBull: emaStackBull,
    closes:       closes.slice(-60),  // last 60 bars for RS calc — trimmed to save KV space
    candleCount:  candles.length,
    _candles:     candles,  // post-guard bars for QE gate (Stage 9.5); never persisted
    mtv:          sbMtv,
    volAccel:     sbVolAccel,
    pctBelow52w:  sbPctBelow52w,
    range20pct:   sbRange20pct,
    rs1m:         sbRs1m,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// D1 HISTORY CACHE LAYER (Option 2)
// All functions defensively no-op / fall back to live fetch if QE_DB is unbound.
// ═══════════════════════════════════════════════════════════════════════════════

// Is the D1 cache enabled? Requires BOTH the binding present AND the KV flag 'true'.
async function d1Enabled(env) {
  if (!env.QE_DB) return false;
  try {
    const flag = await env.KITE_STORE.get("USE_D1_CACHE");
    return flag === "true";
  } catch (_) { return false; }
}

// Read a symbol's candles from D1 in the [ts,o,h,l,c,v] shape the math expects.
// Returns null if absent, too few bars, or stale (latest bar older than D1_FRESH_DAYS).
async function d1ReadCandles(env, symbol) {
  if (!env.QE_DB) return null;
  try {
    const rs = await env.QE_DB
      .prepare("SELECT bar_date,o,h,l,c,v FROM ohlcv_daily WHERE symbol=?1 ORDER BY bar_date ASC")
      .bind(symbol.toUpperCase())
      .all();
    const rows = (rs && rs.results) || [];
    if (rows.length < PIPE_MIN_CANDLES) return null;

    // Freshness: latest stored bar must be recent, else the symbol fell out of
    // daily updates and we must not compute on stale data — fall back to live.
    const latest = rows[rows.length - 1].bar_date;             // 'YYYY-MM-DD'
    const latestMs = new Date(latest + "T00:00:00Z").getTime();
    if (Date.now() - latestMs > D1_FRESH_DAYS * 86400000) return null;

    // Build [timestamp, o, h, l, c, v]. Timestamp is synthetic (unused by the math).
    // PARITY-CRITICAL: the live path fetches by CALENDAR DATE (from = now −
    // PIPE_OHLCV_RANGE days). EMA is recursive, so to be byte-identical the D1 path
    // must compute on the SAME bar set — i.e. the same calendar-date cutoff, NOT a
    // fixed bar count (holiday counts vary). We store 400 bars for headroom but only
    // feed bars on/after the live cutoff into the math.
    // PARITY FIX (12-Jun): live fetches from (PIPE_OHLCV_RANGE + 10) days back
    // (pipeFetchOhlcvSymbol line ~1974), NOT the bare range. The D1 cutoff MUST use
    // the identical +10 or D1 drops ~10 of the oldest bars live keeps → different bar
    // set → recursive EMA chain shifts (proven: candleCount 246 vs 256, EMA ~0.3%).
    const cutoffMs  = Date.now() - (PIPE_OHLCV_RANGE + 10) * 86400000;
    const cutoffStr = new Date(cutoffMs).toISOString().slice(0, 10); // 'YYYY-MM-DD'
    const windowed  = rows.filter(function(r) { return r.bar_date >= cutoffStr; });
    // Guard: if the windowed set is too short (gappy history), use what we have but
    // never fewer than PIPE_MIN_CANDLES — fall back to live if even that fails.
    const use = (windowed.length >= PIPE_MIN_CANDLES) ? windowed : rows.slice(-PIPE_MIN_CANDLES);
    if (use.length < PIPE_MIN_CANDLES) return null;
    return use.map(function(r) {
      return [ new Date(r.bar_date + "T00:00:00Z").getTime() / 1000,
               r.o, r.h, r.l, r.c, r.v ];
    });
  } catch (e) {
    console.warn("[d1ReadCandles] " + symbol + ": " + (e && e.message));
    return null; // any D1 error → fall back to live
  }
}

// Fetch a symbol's full history from Kite (the rate-limited call) — used by backfill.
async function d1FetchHistory(env, token, symbol, instrToken) {
  if (!instrToken || instrToken <= 0) throw new Error("No token for " + symbol);
  const now      = new Date();
  const fromDate = new Date(now.getTime() - (D1_BARS_STORED + 30) * 86400000 * 1.5);
  const fromStr  = fromDate.toISOString().slice(0, 10);
  const toStr    = now.toISOString().slice(0, 10);
  const ctrl  = new AbortController();
  const timer = setTimeout(function() { ctrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
  let res;
  try {
    res = await fetch(
      `${KITE_API_BASE}/instruments/historical/${instrToken}/day?from=${fromStr}&to=${toStr}`,
      { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) }, signal: ctrl.signal }
    );
  } finally { clearTimeout(timer); }
  if (!res.ok) throw new Error("Historical HTTP " + res.status);
  const data = await res.json();
  return (data && data.data && data.data.candles) || [];
}

// Write candles for one symbol into D1 (idempotent via PK). Keeps last D1_BARS_STORED.
async function d1WriteCandles(env, symbol, candles) {
  if (!env.QE_DB || !candles || !candles.length) return 0;
  const sym  = symbol.toUpperCase();
  const keep = candles.slice(-D1_BARS_STORED);
  const stmts = [];
  for (let i = 0; i < keep.length; i++) {
    const c = keep[i];
    // Kite historical candle timestamp c[0] is an ISO STRING
    // ("2026-06-10T00:00:00+0530"), NOT Unix seconds. The earlier code did
    // `new Date(c[0] * 1000)` → string*1000 = NaN → new Date(NaN).toISOString()
    // threw "Invalid time value" on EVERY symbol. Parse robustly; skip junk.
    const d = (typeof c[0] === "number") ? new Date(c[0] * 1000) : new Date(c[0]);
    if (!c[0] || isNaN(d.getTime()) || d.getFullYear() < 2000) continue;  // reject null/junk/epoch
    // FIX v4.45 (DATE-SHIFT CORRUPTION): bar_date MUST be the IST calendar date.
    // Kite returns c[0] as an IST ISO string ("2026-06-19T00:00:00+0530"); the old
    // d.toISOString().slice(0,10) converted to UTC, turning 00:00 IST into 18:30 the
    // PREVIOUS day and shifting EVERY backfilled bar back one calendar date — real
    // sessions landed on weekends and real trading days (e.g. 19-Jun) were left empty.
    // Take the date straight from the IST string; for the rare numeric epoch, shift
    // into IST first. (Mirrors the correct pattern at line ~2762.)
    const barDate = (typeof c[0] === "string")
      ? c[0].slice(0, 10)
      : new Date(c[0] * 1000 + (5 * 60 + 30) * 60000).toISOString().slice(0, 10);
    stmts.push(env.QE_DB.prepare(
      "INSERT INTO ohlcv_daily (symbol,bar_date,o,h,l,c,v) VALUES (?1,?2,?3,?4,?5,?6,?7) " +
      "ON CONFLICT(symbol,bar_date) DO UPDATE SET o=?3,h=?4,l=?5,c=?6,v=?7"
    ).bind(sym, barDate, c[1], c[2], c[3], c[4], c[5]));
  }
  if (!stmts.length) return 0;
  await env.QE_DB.batch(stmts);
  return stmts.length;
}

// Route: POST /d1/init — create the table (run once).
async function handleD1Init(env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound. Bind a D1 database named quantedge_history.", 400);
  try {
    await env.QE_DB.exec(
      "CREATE TABLE IF NOT EXISTS ohlcv_daily (symbol TEXT NOT NULL, bar_date TEXT NOT NULL, o REAL, h REAL, l REAL, c REAL, v INTEGER, PRIMARY KEY (symbol, bar_date))"
    );
    await env.QE_DB.exec(
      "CREATE INDEX IF NOT EXISTS idx_symbol_date ON ohlcv_daily(symbol, bar_date)"
    );
    return cors({ status: "success", message: "D1 table ohlcv_daily ready" });
  } catch (e) { return corsErr("D1 init failed: " + (e && e.message), 500); }
}

// Route: GET /d1/status — coverage snapshot.
async function handleD1Status(env) {
  if (!env.QE_DB) return cors({ status: "success", d1_bound: false, enabled: false });
  try {
    const flag = await env.KITE_STORE.get("USE_D1_CACHE");
    const cnt  = await env.QE_DB.prepare("SELECT COUNT(*) AS n FROM ohlcv_daily").first();
    const syms = await env.QE_DB.prepare("SELECT COUNT(DISTINCT symbol) AS n FROM ohlcv_daily").first();
    const late = await env.QE_DB.prepare("SELECT MAX(bar_date) AS d FROM ohlcv_daily").first();
    return cors({
      status: "success", d1_bound: true, enabled: flag === "true",
      total_rows: (cnt && cnt.n) || 0,
      symbols: (syms && syms.n) || 0,
      latest_bar: (late && late.d) || null,
    });
  } catch (e) { return corsErr("D1 status failed: " + (e && e.message), 500); }
}

// Route: POST /d1/backfill?offset=N&limit=L — chunked history populate.
// Returns next_offset; click until done==true. Rate-limited (uses historical fetch).
async function handleD1Backfill(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  const token = await getToken(env);
  if (!token) return corsErr("No Kite token — log in first", 401);

  const url    = new URL(request.url);
  const offset = parseInt(url.searchParams.get("offset") || "0", 10);
  const limit  = parseInt(url.searchParams.get("limit") || String(D1_BACKFILL_LIMIT), 10);

  // Liquid universe + token map from KV (same source the pipeline uses).
  let universe = [], tokenMap = {};
  try {
    const u = await env.KITE_STORE.get("qe_db_universe");
    if (u) universe = JSON.parse(u);
    const t = await env.KITE_STORE.get("qe_db_token_map");
    if (t) tokenMap = JSON.parse(t);
  } catch (e) { return corsErr("Universe/token-map read failed: " + e.message, 500); }
  if (!universe.length) return corsErr("Empty universe — run /universe/refresh first", 400);

  const slice = universe.slice(offset, offset + limit);
  let ok = 0, fail = 0, bars = 0;
  const errors = {};          // reason → count
  const sampleErrors = [];    // first few "symbol: reason" for diagnosis
  let noTokenCount = 0;
  for (let i = 0; i < slice.length; i++) {
    const sym = slice[i];
    if (!tokenMap[sym]) noTokenCount++;   // diagnostic: is the token even present?
    try {
      const candles = await d1FetchHistory(env, token, sym, tokenMap[sym]);
      if (candles.length) { bars += await d1WriteCandles(env, sym, candles); ok++; }
      else {
        fail++;
        errors["empty_candles"] = (errors["empty_candles"] || 0) + 1;
        if (sampleErrors.length < 5) sampleErrors.push(sym + ": returned 0 candles");
      }
    } catch (e) {
      fail++;
      const msg = (e && e.message) || "unknown";
      // bucket the reason (strip symbol-specific text)
      const key = msg.replace(/for [A-Z0-9&-]+/i, "for <sym>");
      errors[key] = (errors[key] || 0) + 1;
      if (sampleErrors.length < 5) sampleErrors.push(sym + ": " + msg);
    }
  }
  const nextOffset = offset + limit;
  const done = nextOffset >= universe.length;
  return cors({
    status: "success",
    processed: slice.length, ok: ok, failed: fail, bars_written: bars,
    offset: offset, next_offset: done ? null : nextOffset,
    universe_size: universe.length, done: done,
    // ── DIAGNOSTICS (added to find why symbols fail) ──
    tokens_missing_in_slice: noTokenCount,
    token_map_size: Object.keys(tokenMap).length,
    error_breakdown: errors,
    sample_errors: sampleErrors,
    hint: done ? "Backfill complete. Run /d1/verify next." :
                 "Call again with offset=" + nextOffset,
  });
}

// ─── CRON-DRIVEN BACKFILL ─────────────────────────────────────────────────────
// One batch of backfill, returning structured progress.
async function d1BackfillBatch(env, token, universe, tokenMap, offset, limit) {
  const slice = universe.slice(offset, offset + limit);
  let ok = 0, fail = 0, bars = 0, noTok = 0;
  const sampleErrors = [];
  for (let i = 0; i < slice.length; i++) {
    const sym = slice[i];
    if (!tokenMap[sym]) noTok++;
    try {
      const candles = await d1FetchHistory(env, token, sym, tokenMap[sym]);
      if (candles.length) { bars += await d1WriteCandles(env, sym, candles); ok++; }
      else { fail++; if (sampleErrors.length < 3) sampleErrors.push(sym + ": 0 candles"); }
    } catch (e) {
      fail++;
      if (sampleErrors.length < 3) sampleErrors.push(sym + ": " + ((e && e.message) || "err"));
    }
  }
  return { ok, fail, bars, noTok, processed: slice.length, sampleErrors };
}

// Self-fetch chaining is blocked by Cloudflare (a Worker calling its own URL), so
// the backfill is driven by the cron instead: each tick processes ONE chunk and
// advances a KV cursor (qe_d1_bf_offset). When the cursor passes the universe end,
// it disarms and sends a COMPLETE message. Armed via POST /d1/startbackfill.
// KV keys: qe_d1_bf_armed ('true'|absent), qe_d1_bf_offset (number string).
const D1_CRON_CHUNK = 70;   // symbols per cron tick (~23s at 3/sec, safe under 30s CPU)

async function d1BackfillTick(env) {
  // Only run if armed.
  let armed = false;
  try { armed = (await env.KITE_STORE.get("qe_d1_bf_armed")) === "true"; } catch (_) {}
  if (!armed) return;

  const token = await getToken(env);
  if (!token) { console.warn("[d1BackfillTick] no token; skipping this tick"); return; }

  let universe = [], tokenMap = {}, offset = 0;
  try {
    const u = await env.KITE_STORE.get("qe_db_universe");
    if (u) universe = JSON.parse(u);
    const t = await env.KITE_STORE.get("qe_db_token_map");
    if (t) tokenMap = JSON.parse(t);
    const o = await env.KITE_STORE.get("qe_d1_bf_offset");
    offset = o ? parseInt(o, 10) : 0;
  } catch (e) { console.warn("[d1BackfillTick] read failed:", e && e.message); return; }
  if (!universe.length) return;

  const r = await d1BackfillBatch(env, token, universe, tokenMap, offset, D1_CRON_CHUNK);
  const nextOffset = offset + D1_CRON_CHUNK;
  const done = nextOffset >= universe.length;

  try {
    await env.KITE_STORE.put("qe_d1_bf_offset", String(done ? universe.length : nextOffset));
    if (done) await env.KITE_STORE.delete("qe_d1_bf_armed");  // disarm
  } catch (_) {}

  const pct = Math.min(100, Math.round((nextOffset / universe.length) * 100));
  const msg = done
    ? `✅ D1 backfill COMPLETE — full universe loaded (${universe.length} symbols processed).\nNext: GET /d1/verify?symbol=RELIANCE (must say PASS), then set USE_D1_CACHE='true'.`
    : `⏳ D1 backfill ${pct}% — processed ${nextOffset}/${universe.length} (this tick: ok+${r.ok} fail+${r.fail}, ${r.bars} bars). Auto-continuing each cron tick.`;
  try { await sendTelegram(env, msg); } catch (_) {}
}

// Route: POST /d1/startbackfill — ARM the cron-driven backfill (cursor=0).
async function handleD1StartBackfill(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  const url = new URL(request.url);
  const startAt = parseInt(url.searchParams.get("offset") || "0", 10);
  try {
    await env.KITE_STORE.put("qe_d1_bf_offset", String(startAt));
    await env.KITE_STORE.put("qe_d1_bf_armed", "true");
  } catch (e) { return corsErr("Arm failed: " + e.message, 500); }
  try { await sendTelegram(env, "🚀 D1 backfill ARMED (from offset " + startAt + "). It will fill automatically on each cron tick — watch for progress messages."); } catch (_) {}
  return cors({
    status: "success", armed: true, start_offset: startAt,
    note: "Backfill will advance ~" + D1_CRON_CHUNK + " symbols every cron tick (every 5 min during market hours). Watch Telegram + /d1/status. To stop early: POST /d1/stopbackfill.",
  });
}

// Route: POST /d1/stopbackfill — disarm.
async function handleD1StopBackfill(env) {
  try { await env.KITE_STORE.delete("qe_d1_bf_armed"); } catch (_) {}
  return cors({ status: "success", armed: false, note: "Backfill disarmed. Cursor preserved; re-arm to resume." });
}

// ═══════════════════════════════════════════════════════════════════════════════
// TRADE OUTCOME TRACKER (v4.51) — durable realized-trade audit log in D1.
// Table qe_trade_outcomes. The SERVER computes return_pct / return_r / hold_days
// from raw inputs, so the client never sends a derived number. Idempotent UPSERT
// on (symbol, buy_date). Read-only /list is open; /delete needs an explicit id.
// ═══════════════════════════════════════════════════════════════════════════════
function _otValidDate(s) {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s) && !isNaN(Date.parse(s));
}
function _otNum(x) {
  const n = (x === null || x === undefined || x === "") ? null : Number(x);
  return (n === null || isNaN(n)) ? null : n;
}
function _otRound(x, d) {
  if (x === null || x === undefined || isNaN(x)) return null;
  const f = Math.pow(10, d);
  return Math.round(x * f) / f;
}
// Derive return_pct / return_r / hold_days from raw inputs. return_r = profit per
// share / (actual entry − planned stop). Null when the inputs aren't available.
function _otDerive(buyPrice, sellPrice, signalSl, buyDate, sellDate) {
  const out = { return_pct: null, return_r: null, hold_days: null };
  if (sellPrice !== null && buyPrice) {
    out.return_pct = _otRound((sellPrice - buyPrice) / buyPrice * 100, 2);
    if (signalSl !== null && (buyPrice - signalSl) !== 0) {
      out.return_r = _otRound((sellPrice - buyPrice) / (buyPrice - signalSl), 3);
    }
  }
  if (_otValidDate(buyDate) && _otValidDate(sellDate)) {
    out.hold_days = Math.round((Date.parse(sellDate) - Date.parse(buyDate)) / 86400000);
  }
  return out;
}
// Fallback: pull signal context from qe_forward_track (nearest snapshot on/before buy_date).
async function _otSignalFallback(env, symbol, buyDate) {
  try {
    const row = await env.QE_DB.prepare(
      "SELECT snapshot_date, score, label, entry, sl, t1, t2 FROM qe_forward_track WHERE symbol = ?1 AND snapshot_date <= ?2 ORDER BY snapshot_date DESC LIMIT 1"
    ).bind(symbol, buyDate).first();
    if (!row) return null;
    return {
      signal_date: row.snapshot_date, signal_score: row.score, signal_label: row.label,
      signal_entry: row.entry, signal_sl: row.sl, signal_t1: row.t1, signal_t2: row.t2,
    };
  } catch (_) { return null; }
}

// Route: POST /outcome/add — log a trade (open, or full round-trip). Idempotent UPSERT on (symbol, buy_date).
async function handleOutcomeAdd(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  let b;
  try { b = await request.json(); } catch (_) { return corsErr("Invalid JSON body", 400); }
  const symbol = (b.symbol || "").toString().trim().toUpperCase();
  if (!symbol) return corsErr("symbol is required", 400);
  if (!_otValidDate(b.buy_date)) return corsErr("buy_date must be YYYY-MM-DD", 400);
  const buyPrice = _otNum(b.buy_price);
  if (buyPrice === null || buyPrice <= 0) return corsErr("buy_price must be a positive number", 400);

  // Signal context: prefer caller-supplied, else fall back to qe_forward_track.
  let sig = {
    signal_date: (_otValidDate(b.signal_date) ? b.signal_date : null),
    signal_score: _otNum(b.signal_score), signal_label: (b.signal_label || null),
    signal_entry: _otNum(b.signal_entry), signal_sl: _otNum(b.signal_sl),
    signal_t1: _otNum(b.signal_t1), signal_t2: _otNum(b.signal_t2),
  };
  if (sig.signal_sl === null && sig.signal_entry === null) {
    const fb = await _otSignalFallback(env, symbol, b.buy_date);
    if (fb) sig = fb;
  }

  const sellDate  = _otValidDate(b.sell_date) ? b.sell_date : null;
  const sellPriceIn = _otNum(b.sell_price);
  const isClosed  = sellDate !== null && sellPriceIn !== null && sellPriceIn > 0;
  const status    = isClosed ? "CLOSED" : "OPEN";
  const sellPrice = isClosed ? sellPriceIn : null;
  const d = _otDerive(buyPrice, sellPrice, sig.signal_sl, b.buy_date, sellDate);
  const qty   = _otNum(b.qty);
  const now   = new Date().toISOString();
  const notes = (b.notes || null);
  const src   = (b.source || "app");
  const exitR = isClosed ? (b.exit_reason || "MANUAL") : null;

  try {
    await env.QE_DB.prepare(
      "INSERT INTO qe_trade_outcomes (symbol,signal_date,signal_score,signal_label,signal_entry,signal_sl,signal_t1,signal_t2,buy_date,buy_price,qty,sell_date,sell_price,status,return_pct,return_r,hold_days,exit_reason,notes,source,created_ts,updated_ts) " +
      "VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?21) " +
      "ON CONFLICT(symbol,buy_date) DO UPDATE SET " +
      "signal_date=?2,signal_score=?3,signal_label=?4,signal_entry=?5,signal_sl=?6,signal_t1=?7,signal_t2=?8,buy_price=?10,qty=?11,sell_date=?12,sell_price=?13,status=?14,return_pct=?15,return_r=?16,hold_days=?17,exit_reason=?18,notes=?19,source=?20,updated_ts=?21"
    ).bind(
      symbol, sig.signal_date, sig.signal_score, sig.signal_label, sig.signal_entry, sig.signal_sl, sig.signal_t1, sig.signal_t2,
      b.buy_date, buyPrice, qty, sellDate, sellPrice, status,
      d.return_pct, d.return_r, d.hold_days, exitR, notes, src, now
    ).run();
  } catch (e) { return corsErr("DB write failed: " + (e && e.message), 500); }

  const saved = await env.QE_DB.prepare("SELECT * FROM qe_trade_outcomes WHERE symbol=?1 AND buy_date=?2").bind(symbol, b.buy_date).first();
  return cors({ status: "success", trade: saved });
}

// Route: POST /outcome/resolve — close an OPEN trade by id or (symbol, buy_date).
async function handleOutcomeResolve(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  let b;
  try { b = await request.json(); } catch (_) { return corsErr("Invalid JSON body", 400); }
  if (!_otValidDate(b.sell_date)) return corsErr("sell_date must be YYYY-MM-DD", 400);
  const sellPrice = _otNum(b.sell_price);
  if (sellPrice === null || sellPrice <= 0) return corsErr("sell_price must be a positive number", 400);

  let row;
  const id = _otNum(b.id);
  if (id !== null) {
    row = await env.QE_DB.prepare("SELECT * FROM qe_trade_outcomes WHERE id=?1").bind(id).first();
  } else if (b.symbol && _otValidDate(b.buy_date)) {
    row = await env.QE_DB.prepare("SELECT * FROM qe_trade_outcomes WHERE symbol=?1 AND buy_date=?2")
            .bind((b.symbol || "").toString().trim().toUpperCase(), b.buy_date).first();
  } else {
    return corsErr("Provide id, or symbol + buy_date", 400);
  }
  if (!row) return corsErr("Trade not found", 404);
  if (row.status === "CLOSED") return corsErr("Trade already CLOSED (id " + row.id + ")", 409);

  const d = _otDerive(row.buy_price, sellPrice, row.signal_sl, row.buy_date, b.sell_date);
  const now = new Date().toISOString();
  try {
    await env.QE_DB.prepare(
      "UPDATE qe_trade_outcomes SET sell_date=?1, sell_price=?2, status='CLOSED', return_pct=?3, return_r=?4, hold_days=?5, exit_reason=?6, updated_ts=?7 WHERE id=?8"
    ).bind(b.sell_date, sellPrice, d.return_pct, d.return_r, d.hold_days, (b.exit_reason || "MANUAL"), now, row.id).run();
  } catch (e) { return corsErr("DB update failed: " + (e && e.message), 500); }

  const saved = await env.QE_DB.prepare("SELECT * FROM qe_trade_outcomes WHERE id=?1").bind(row.id).first();
  return cors({ status: "success", trade: saved });
}

// Route: GET /outcome/list — all trades + portfolio summary stats.
async function handleOutcomeList(env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  let rows;
  try {
    const res = await env.QE_DB.prepare("SELECT * FROM qe_trade_outcomes ORDER BY buy_date ASC, id ASC").all();
    rows = (res && res.results) ? res.results : [];
  } catch (e) { return corsErr("DB read failed: " + (e && e.message), 500); }

  const closed = rows.filter(function (r) { return r.status === "CLOSED"; });
  const open   = rows.filter(function (r) { return r.status !== "CLOSED"; });
  const wins   = closed.filter(function (r) { return (r.return_pct || 0) > 0; });
  const withR  = closed.filter(function (r) { return r.return_r !== null && r.return_r !== undefined; });
  const sum = function (arr, key) { return arr.reduce(function (a, r) { return a + (r[key] || 0); }, 0); };
  const summary = {
    total: rows.length,
    closed: closed.length,
    open: open.length,
    wins: wins.length,
    win_rate: closed.length ? _otRound(wins.length / closed.length * 100, 1) : null,
    avg_return_pct: closed.length ? _otRound(sum(closed, "return_pct") / closed.length, 2) : null,
    total_return_pct: _otRound(sum(closed, "return_pct"), 2),
    avg_return_r: withR.length ? _otRound(sum(withR, "return_r") / withR.length, 3) : null,
    avg_hold_days: closed.length ? _otRound(sum(closed, "hold_days") / closed.length, 1) : null,
  };
  return cors({ status: "success", summary: summary, trades: rows });
}

// Route: POST /outcome/delete — remove ONE row by explicit numeric id (defensive: no bulk delete).
async function handleOutcomeDelete(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  let b;
  try { b = await request.json(); } catch (_) { return corsErr("Invalid JSON body", 400); }
  const id = _otNum(b.id);
  if (id === null || id <= 0 || Math.floor(id) !== id) return corsErr("A positive integer id is required", 400);
  let changes = 0;
  try {
    const res = await env.QE_DB.prepare("DELETE FROM qe_trade_outcomes WHERE id=?1").bind(id).run();
    changes = (res && res.meta && res.meta.changes) || 0;
  } catch (e) { return corsErr("DB delete failed: " + (e && e.message), 500); }
  return cors({ status: "success", deleted: changes, id: id });
}


// Route: POST /d1/update — daily incremental. Appends today's bar via bulk-quote.
// Wired to the 16:00 IST cron. Cheap: ~bulk calls, NOT the historical rate limit.
// ═══ CENTRAL CONFIGURATION — Bulk Pre-Scan gate (13-Jul-2026 consolidation) ═══
// ROOT CAUSE OF THE "Failed to fetch" INCIDENT ON /prescan: these 8 values used to be
// scattered individual consts that were dropped entirely during a past merge (see
// prescanGetCandles' own comment: "grafted back in v4.52, verbatim from v4.43
// source"). Every /prescan call hit an immediate uncaught ReferenceError — before the
// token check even ran — and with no global try/catch around fetch(), Cloudflare
// returned its own CORS-less error page instead of this Worker's response, which a
// cross-origin browser fetch() reports as "Failed to fetch". Consolidated here into
// one configuration object, not scattered consts. The 4 gate values are read
// directly from the Bulk Pre-Scan panel's own published UI copy (index.html:
// "Gate: price ≥ ₹100 · vol ≥ 200K · |chg| ≤ 15% · close ≥ 0.97×EMA200" and "paste
// up to 80") — matching what's already promised to the user, not newly invented.
// The 3 operational values (history window, minimum bars, fetch batch size) aren't
// published anywhere; set to reasonable values consistent with this file's existing
// conventions — HISTORY_DAYS mirrors the identical "365 calendar days ≈ 248 trading
// days" comment already used by pipeComputeIndicatorsFromCandles, which this gate
// calls. Flag to Siva: if the original v4.43 source had different values for these
// 3, override here.
// _validatePrescanConfig() is called at the top of handlePrescan on every request —
// if any key is ever missing or non-numeric (e.g. a future edit drops one, exactly
// this class of bug again), the request fails explicitly with CONFIGURATION_INCOMPLETE
// instead of silently proceeding with a partially-undefined config or a silently-chosen default.
const PRESCAN_CONFIG = {
  MAX_SYMBOLS:   80,      // UI: "paste up to 80"
  MIN_PRICE:     100,     // UI: "price ≥ ₹100"
  MIN_VOL:       200000,  // UI: "vol ≥ 200K"
  MAX_CHANGE:    15,      // UI: "|chg| ≤ 15%"
  EMA200_BUFFER: 0.97,    // UI: "close ≥ 0.97×EMA200"
  HISTORY_DAYS:  365,     // not published — mirrors pipeComputeIndicatorsFromCandles' own 365-day convention
  MIN_BARS:      200,     // not published — EMA200 needs at least 200 bars to be valid
  FETCH_BATCH:   10,      // not published — conservative parallel-fetch batch size to stay well under subrequest limits
};
function _validatePrescanConfig() {
  return Object.keys(PRESCAN_CONFIG).filter(k => PRESCAN_CONFIG[k] == null || typeof PRESCAN_CONFIG[k] !== "number" || isNaN(PRESCAN_CONFIG[k]));
}

// ── Design A (v4.46): BULK PRE-SCAN gate — grafted back in v4.52 (verbatim from v4.43 source).
// Cheap server-side floors (price/vol/chg via bulk quote) + close>=0.97xEMA200 via D1/live history.
// GATE ONLY — finalDecision remains the BUY/WAIT/IGNORE authority. Reuses getToken/d1ReadCandles/pipeFetchQuoteBatch.
async function prescanGetCandles(env, token, sym, instrToken) {
  const d1 = await d1ReadCandles(env, sym);
  if (d1 && d1.length) return { candles: d1, reason: "D1_" + d1.length };
  const res = await pipeFetchNCandles(env, token, sym, instrToken, PRESCAN_CONFIG.HISTORY_DAYS);
  return { candles: (res && res.candles) || [], reason: "live:" + (res ? res.reason : "no-data") };
}

// Route: POST /prescan  body {symbols:[...] | "A,B,C"}
// GET /forward-track/today?symbol=X — Priority 2 (13-Jul-2026): exposes today's
// CONFIRMED (16:15 post-close) forward_track verdict for a symbol, so the browser
// scan can defer to the server's authoritative daily read instead of independently
// recomputing on whatever candle is available at click-time. Deliberately excludes
// 'forming' scan_mode rows — a forming-only day should NOT silently satisfy this
// lookup, because that's exactly the mechanism that let a stale/unsettled verdict
// drive a real recommendation (13-Jul FUSION incident). available:false is the
// correct response when only forming data exists; the caller should fall back to
// live computation, not treat a forming row as good enough.
async function handleForwardTrackToday(url, env) {
  const symbol = (url.searchParams.get("symbol") || "").toUpperCase().trim();
  if (!symbol) return corsErr("symbol query param required", 400);
  const todayIst = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
  try {
    const row = await env.QE_DB.prepare(
      "SELECT symbol,label,reason,score,base_score,edge_class,ev,exp_se,mc_prob,fresh_breakout,cmp,entry,sl,t1,t2,data_basis,data_basis_reason,created_ts,scan_mode " +
      "FROM qe_forward_track WHERE snapshot_date=?1 AND symbol=?2 AND scan_mode='confirmed'"
    ).bind(todayIst, symbol).first();
    if (!row) return cors({ ok: true, available: false, reason: "No confirmed (16:15 post-close) scan for this symbol today yet — use live computation." });
    return cors({ ok: true, available: true, row });
  } catch (e) { return corsErr(e.message || "Forward-track lookup failed", 500); }
}

async function handlePrescan(request, env) {
  const missingConfig = _validatePrescanConfig();
  if (missingConfig.length) return cors({ ok: false, status: "CONFIGURATION_INCOMPLETE", missing_keys: missingConfig }, 500);

  const r2 = function (x) { return (x == null || isNaN(x)) ? null : Math.round(x * 100) / 100; };

  // 1) Parse + sanitise input (accept array OR delimited string)
  let body;
  try { body = await request.json(); }
  catch (_) { return corsErr('Invalid JSON body — expected {"symbols":[...]}', 400); }
  let raw = (body && body.symbols) != null ? body.symbols : [];
  if (typeof raw === "string") raw = raw.split(/[\s,]+/);
  if (!Array.isArray(raw)) return corsErr("symbols must be an array or a comma/space-separated string", 400);
  const pastedCount = raw.length;

  const seen = {};
  const symbols = [];
  for (let i = 0; i < raw.length; i++) {
    let s = String(raw[i] == null ? "" : raw[i]).trim().toUpperCase();
    s = s.replace(/^NSE:/, "").replace(/\.(NS|BO)$/, "");
    if (!s || !/^[A-Z0-9&\-]{2,20}$/.test(s)) continue;
    if (seen[s]) continue;
    seen[s] = true;
    symbols.push(s);
  }
  const dedupedCount = symbols.length;
  const wasCapped = dedupedCount > PRESCAN_CONFIG.MAX_SYMBOLS;
  const scanList = symbols.slice(0, PRESCAN_CONFIG.MAX_SYMBOLS);
  if (scanList.length === 0) return cors({ ok: false, error: "No valid NSE symbols after cleanup." }, 400);

  // 2) Live token required — NO silent fallback (data-integrity rule)
  const token = await getToken(env);
  if (!token) return cors({ ok: false, error: "NO_TOKEN — Kite session expired. Tap 'Login & Refresh Data' and retry." }, 401);

  // 3) Resolve instrument tokens from the universe token-map
  let tokenMap = {};
  try { const t = await env.KITE_STORE.get("qe_db_token_map"); if (t) tokenMap = JSON.parse(t); } catch (_) {}

  const rejects = [];
  const tier1in = [];
  for (let i = 0; i < scanList.length; i++) {
    const sym = scanList[i];
    if (!tokenMap[sym] || tokenMap[sym] <= 0) {
      rejects.push({ symbol: sym, stage: "RESOLVE", reason: "Not in NSE universe (no instrument token)" });
      continue;
    }
    tier1in.push(sym);
  }

  // 4) Tier 1 — bulk quote floors (one /quote call for ≤80; built-in retry)
  const audit = makePipeAudit();
  const bhav = {};
  if (tier1in.length) {
    try { await pipeFetchQuoteBatch(env, token, tier1in, bhav, audit); }
    catch (e) { return cors({ ok: false, error: "Quote fetch failed: " + ((e && e.message) || "unknown") }, 502); }
  }

  const tier2in = [];
  for (let i = 0; i < tier1in.length; i++) {
    const sym = tier1in[i];
    const b = bhav[sym];
    if (!b) { rejects.push({ symbol: sym, stage: "TIER1", reason: "No quote returned (illiquid/suspended?)" }); continue; }
    if (b.last_price < PRESCAN_CONFIG.MIN_PRICE) { rejects.push({ symbol: sym, stage: "TIER1", reason: "Price ₹" + b.last_price.toFixed(2) + " < ₹" + PRESCAN_CONFIG.MIN_PRICE }); continue; }
    if (b.volume < PRESCAN_CONFIG.MIN_VOL) { rejects.push({ symbol: sym, stage: "TIER1", reason: "Volume " + b.volume.toLocaleString("en-IN") + " < " + PRESCAN_CONFIG.MIN_VOL.toLocaleString("en-IN") }); continue; }
    if (Math.abs(b.change_pct) > PRESCAN_CONFIG.MAX_CHANGE) { rejects.push({ symbol: sym, stage: "TIER1", reason: "Change " + b.change_pct.toFixed(1) + "% exceeds ±" + PRESCAN_CONFIG.MAX_CHANGE + "% (circuit/anomaly)" }); continue; }
    tier2in.push(sym);
  }

  // 5) Tier 2 — live history (batched, parallel-limited) + trend floor close ≥ 0.97×EMA200
  const survivors = [];
  for (let i = 0; i < tier2in.length; i += PRESCAN_CONFIG.FETCH_BATCH) {
    const batch = tier2in.slice(i, i + PRESCAN_CONFIG.FETCH_BATCH);
    let fetched;
    try {
      fetched = await Promise.all(batch.map(function (sym) {
        return prescanGetCandles(env, token, sym, tokenMap[sym])
          .then(function (r) { return { sym: sym, candles: r.candles, reason: r.reason }; });
      }));
    } catch (e) {
      for (let k = 0; k < batch.length; k++) rejects.push({ symbol: batch[k], stage: "TIER2", reason: "History fetch error: " + ((e && e.message) || "unknown") });
      continue;
    }
    for (let k = 0; k < fetched.length; k++) {
      const sym = fetched[k].sym;
      const candles = fetched[k].candles || [];
      if (candles.length < PRESCAN_CONFIG.MIN_BARS) {
        rejects.push({ symbol: sym, stage: "TIER2", reason: "Insufficient history (" + candles.length + " bars; need ≥" + PRESCAN_CONFIG.MIN_BARS + ") — " + fetched[k].reason });
        continue;
      }
      const ind = pipeComputeIndicatorsFromCandles(sym, candles);
      if (ind.ema200 == null || !(ind.lastClose > 0)) {
        rejects.push({ symbol: sym, stage: "TIER2", reason: "EMA200 unavailable" });
        continue;
      }
      const floor = PRESCAN_CONFIG.EMA200_BUFFER * ind.ema200;
      if (ind.lastClose < floor) {
        rejects.push({ symbol: sym, stage: "TIER2", reason: "Below trend floor — close ₹" + ind.lastClose.toFixed(2) + " < 0.97×EMA200 (₹" + floor.toFixed(2) + ")" });
        continue;
      }
      survivors.push({
        symbol:       sym,
        cmp:          r2(ind.lastClose),
        ema20:        r2(ind.ema20),
        ema50:        r2(ind.ema50),
        ema200:       r2(ind.ema200),
        rsi14:        r2(ind.rsi14),
        adx14:        r2(ind.adx14),
        stBull:       !!ind.stBull,
        emaStackBull: !!ind.emaStackBull,
        prox52w:      r2(ind.prox52w),
        volRatio:     r2(ind.volRatio),
        changePct:    bhav[sym] ? r2(bhav[sym].change_pct) : null,
      });
    }
  }

  // 6) Response — survivors (for the browser auto-handoff) + full reject ledger. Writes nothing.
  survivors.sort(function (a, b) { return (b.prox52w || 0) - (a.prox52w || 0); });
  return cors({
    ok: true,
    counts: {
      pasted:    pastedCount,
      deduped:   dedupedCount,
      scanned:   scanList.length,
      capped:    wasCapped,
      resolved:  tier1in.length,
      tier1pass: tier2in.length,
      survivors: survivors.length,
      rejected:  rejects.length,
    },
    survivors: survivors,
    rejects:   rejects,
  });
}

// ── Phase 2 (v4.53): SINGLE SOURCE OF TRUTH — /score runs the SAME gate the cron runs.
// Per symbol: D1 completed bars (d1ReadCandles) + regime from KV → QEGate.evaluate (PROTECTED,
// the exact scorer the pipeline uses). The browser calls this and renders THIS verdict, so a
// manual scan can no longer disagree with the Telegram signal on the same completed bar.
// rsScore=null (neutralized) — identical pattern to /diff/layers & /breakout/debug. Read-only:
// no KV/D1 writes, no Telegram. Verdict + edge fields are authoritative; entry/SL/T1/T2 remain
// browser-derived from the same candles (deterministic, non-divergent).
async function handleScore(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  let body;
  try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON body", 400); }
  let syms = [];
  if (Array.isArray(body.symbols)) syms = body.symbols;
  else if (typeof body.symbols === "string") syms = body.symbols.split(/[\s,]+/);
  syms = syms.map(function (s) { return String(s || "").trim().toUpperCase().replace(/\.NS$|\.BO$/, ""); })
             .filter(function (s) { return s.length >= 2; });
  syms = Array.from(new Set(syms)).slice(0, 80);
  if (!syms.length) return corsErr("Provide symbols: [..] or comma string", 400);

  // Regime — SAME source the cron's gate reads (KV qe_pipe_regime).
  let regimeStr = "sideways";
  try {
    const rg = await env.KITE_STORE.get("qe_regime");
    if (rg) { const ro = JSON.parse(rg); regimeStr = ro.regime || ro.label || regimeStr; }
  } catch (_) {}

  // RS parity (v4.54) — feed the gate the cron's OWN per-symbol RS percentile
  // (KV qe_pipe_rs_ranked, rewritten every pipeline run) so the score digit matches
  // the cron/Telegram. The percentile is universe-relative and cannot be recomputed
  // standalone, so symbols absent from the cache (not in today's pipeline) pass
  // rsScore=null ⇒ no adjustment (honest, conservative; same as pre-v4.54).
  let _rsMap = {};
  try {
    const rr = await env.KITE_STORE.get("qe_pipe_rs_ranked");
    if (rr) { const arr = JSON.parse(rr); if (Array.isArray(arr)) arr.forEach(function (x) { if (x && x.sym != null) _rsMap[String(x.sym).toUpperCase()] = x.rsScore; }); }
  } catch (_) {}
  let _rsHits = 0;

  // Priority 2 fix (13-Jul-2026): the cron's confirmed run does NOT always use the
  // D1 1y cache — it attempts a LIVE 2-year fetch first (pipeFetch2yCandles, same
  // QE_GATE_HISTORY_DAYS window) and uses whichever candle set is longer. /score
  // previously had no equivalent step at all, meaning it was structurally capped at
  // 1y regardless of what the cron used for that symbol that day — a genuine
  // candle-basis mismatch, not a timing artifact. This mirrors the cron's own
  // selection rule exactly. Best-effort: any failure here (no token, no instrument
  // map, fetch error) falls back to the existing D1 read — zero regression for the
  // cases that already worked.
  let _scoreToken = null;
  try { _scoreToken = await getToken(env); } catch (_) {}
  let _gateTokenMap = {};
  try { const _tm = await env.KITE_STORE.get("qe_db_token_map"); if (_tm) _gateTokenMap = JSON.parse(_tm); } catch (_) {}

  // Root cause fix (16-Jul-2026): the per-symbol loop below was strictly sequential —
  // each symbol's live 2y fetch (added by the earlier candle-basis-parity fix) had to
  // finish before the next symbol started. For a realistic multi-symbol scan (e.g. 10
  // Discovery survivors), that meant 10 sequential Kite API round-trips, easily
  // exceeding the browser's 20-second client-side abort timeout. On abort, the whole
  // /score call fails SILENTLY (by design, to avoid breaking the scan on a down
  // server) — meaning EVERY card in the batch falls back to pure browser scoring,
  // which includes a macro/news adjustment (±8 pts) the server-side scorer never
  // applies. That is the actual, verified cause of the systematic score gap observed
  // across an entire scan, not a per-symbol data issue. Fix: bounded concurrency
  // (3 symbols in flight at once, matching Kite Connect's documented ~3 req/sec rate
  // limit) instead of one-at-a-time — cuts worst-case latency roughly 3x while
  // staying safely inside Kite's own rate limit. No change to QEGate.evaluate, the
  // 2y-vs-1y selection rule, or any scoring/decision logic — only how many symbols
  // are in flight at once.
  const _SCORE_CONCURRENCY = 3; // ENGINEERING_ASSUMPTION — matches Kite Connect's documented rate limit
  const results = [];
  async function _scoreOneSymbol(sym) {
    try {
      const d1Candles = await d1ReadCandles(env, sym);
      let candles = d1Candles, basis = "1y", basisReason = "NO_TOKEN";
      if (_scoreToken && _gateTokenMap[sym]) {
        try {
          const r2y = await pipeFetch2yCandles(env, _scoreToken, sym, _gateTokenMap[sym]);
          basisReason = (r2y && r2y.reason) || "NULL";
          if (r2y && r2y.candles && d1Candles && r2y.candles.length > d1Candles.length) {
            candles = r2y.candles.map(function(c){ return [c[0], c[1], c[2], c[3], c[4], c[5]]; });
            basis = "2y";
          } else if (r2y && r2y.candles && !d1Candles) {
            candles = r2y.candles; basis = "2y"; // D1 cache empty/stale but live 2y succeeded
          }
        } catch (e) { basisReason = "EXC_" + (((e && e.message) || "?") + "").slice(0, 40); }
      }
      if (!candles || !candles.length) return { symbol: sym, ok: false, error: "NO_D1_CANDLES" };
      const _rs = Object.prototype.hasOwnProperty.call(_rsMap, sym) ? _rsMap[sym] : null;
      if (_rs !== null) _rsHits++;
      const r = QEGate.evaluate(candles, regimeStr, _rs);   // PROTECTED scorer — identical to cron (RS = cron cached percentile)
      return {
        symbol: sym, ok: true,
        label: r.label, reason: r.reason || r.fdReason || r.label, pass: r.pass,
        score: r.score, baseScore: r.baseScore, proScore: r.proScore,
        wr: r.wr, ev: r.ev, mcProb: r.mcProb, btTotal: r.btTotal, expSE: r.expSE,
        edgeClass: r.edgeClass, freshBreakout: r.freshBreakout, elite: r.elite,
        isRejected: r.isRejected, proReason: r.proReason, bars: candles.length,
        entry: r.entry, sl: r.sl, t1: r.t1, t2: r.t2, cmp: r.cmp,
        rsScore: _rs, rsBasis: (_rs === null ? "none" : "cron_kv"),
        dataBasis: basis, dataBasisReason: basisReason,
        newest_bar: candles[candles.length - 1] ? (candles[candles.length - 1].date || candles[candles.length - 1].bar_date || null) : null
      };
    } catch (e) { return { symbol: sym, ok: false, error: (((e && e.message) || "GATE_ERR") + "").slice(0, 80) }; }
  }
  for (let i = 0; i < syms.length; i += _SCORE_CONCURRENCY) {
    const batch = syms.slice(i, i + _SCORE_CONCURRENCY);
    const batchResults = await Promise.all(batch.map(_scoreOneSymbol));
    results.push(...batchResults);
  }
  return cors({ ok: true, regime: regimeStr, rs: ("cron RS percentile from KV (" + _rsHits + "/" + results.length + " matched; absent⇒null)"), source: "D1 completed bars", count: results.length, results: results });
}

async function handleD1Update(env) {
  if (!env.QE_DB) return { ok: false, msg: "QE_DB not bound" };
  const token = await getToken(env);
  if (!token) return { ok: false, msg: "No token" };

  let universe = [];
  try {
    const u = await env.KITE_STORE.get("qe_db_universe");
    if (u) universe = JSON.parse(u);
  } catch (_) {}
  if (!universe.length) return { ok: false, msg: "Empty universe" };

  const today = new Date().toISOString().slice(0, 10);
  let written = 0, batches = 0;

  for (let i = 0; i < universe.length; i += D1_BULK_QUOTE_SIZE) {
    const batch = universe.slice(i, i + D1_BULK_QUOTE_SIZE);
    const istr  = batch.map(function(s) { return "i=NSE:" + encodeURIComponent(s); }).join("&");
    let qres;
    try {
      const r = await fetch(`${KITE_API_BASE}/quote?${istr}`,
        { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) } });
      if (!r.ok) continue;
      qres = await r.json();
    } catch (_) { continue; }
    batches++;

    const data = (qres && qres.data) || {};
    const stmts = [];
    for (const key in data) {
      const q = data[key];
      const sym = key.replace(/^NSE:/, "").toUpperCase();
      const o = q.ohlc && q.ohlc.open, h = q.ohlc && q.ohlc.high,
            l = q.ohlc && q.ohlc.low,  c = q.last_price, v = q.volume;
      if (o == null || h == null || l == null || c == null) continue;
      stmts.push(env.QE_DB.prepare(
        "INSERT INTO ohlcv_daily (symbol,bar_date,o,h,l,c,v) VALUES (?1,?2,?3,?4,?5,?6,?7) " +
        "ON CONFLICT(symbol,bar_date) DO UPDATE SET o=?3,h=?4,l=?5,c=?6,v=?7"
      ).bind(sym, today, o, h, l, c, v || 0));
    }
    if (stmts.length) { await env.QE_DB.batch(stmts); written += stmts.length; }
  }

  // Optional: trim each symbol to D1_BARS_STORED would require per-symbol deletes;
  // skipped here (storage is ample). A weekly re-backfill keeps data adjusted (C-D1b).
  return { ok: true, written: written, batches: batches, date: today };
}

// ── Fix-2 (v4.44): TOKEN-FRESH REFRESH ───────────────────────────────────────
// The Kite token is reliably alive only in the seconds right after a /login (the
// Kite app re-auth kills it mid-morning, and Zerodha flushes it each ~7:30 AM).
// So instead of trusting a fixed 16:00 clock, the refresh rides the login itself.
// Pure data-freshness plumbing — touches NO verdict/score/ranking/signal path.

// IST timestamp string, e.g. "23-Jun-2026 16:05 IST".
function istStamp(ms) {
  const d   = new Date((ms || Date.now()) + (5 * 60 + 30) * 60000);
  const mon = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][d.getUTCMonth()];
  const pad = function (n) { return (n < 10 ? "0" : "") + n; };
  return pad(d.getUTCDate()) + "-" + mon + "-" + d.getUTCFullYear() +
         " " + pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes()) + " IST";
}

// Newest bar_date currently in the cache (YYYY-MM-DD) or null.
async function d1NewestBar(env) {
  if (!env.QE_DB) return null;
  try {
    const r = await env.QE_DB.prepare("SELECT MAX(bar_date) AS d FROM ohlcv_daily").first();
    return (r && r.d) || null;
  } catch (_) { return null; }
}

// Most recent weekday (Mon–Fri) on/before now (IST), as YYYY-MM-DD. Used ONLY to
// decide whether the cache looks behind for the stale nudge. Before ~16:00 IST
// today's bar isn't expected yet, so we step back a day. (A market holiday may
// trigger one benign nudge — acceptable vs. another silent multi-day freeze.)
function lastExpectedTradingDate(ms) {
  let d = new Date((ms || Date.now()) + (5 * 60 + 30) * 60000);
  if (d.getUTCHours() < 15 || (d.getUTCHours() === 15 && d.getUTCMinutes() < 30)) {
    d = new Date(d.getTime() - 86400000); // before 15:30 IST close → today's bar not expected yet
  }
  let dow = d.getUTCDay(); // 0=Sun .. 6=Sat
  while (dow === 0 || dow === 6) { d = new Date(d.getTime() - 86400000); dow = d.getUTCDay(); }
  return d.toISOString().slice(0, 10);
}

// Single source-of-truth freshness stamp (read by /refresh/status and the in-app
// "Data as of" line, and echoed in every confirmation/nudge — so app + Telegram
// can never disagree about how current the data is).
async function writeLastRefresh(env, lastBar, written, symbols, source) {
  try {
    await env.KITE_STORE.put("qe_last_refresh", JSON.stringify({
      last_bar_date: lastBar || null,
      refreshed_at:  istStamp(Date.now()),
      refreshed_ms:  Date.now(),
      written:       (written == null ? null : written),
      symbols:       (symbols == null ? null : symbols),
      source:        source || "unknown",
    }));
  } catch (e) { console.warn("[writeLastRefresh] " + (e && e.message)); }
}

// Shared refresh engine: append today's bar on the hot token, then record the
// stamp. Token-store always happens BEFORE this is called, so a refresh failure
// can never block login. Returns a small summary for the confirmation message.
async function runTokenFreshRefresh(env, source) {
  // Only append a bar once the session has CLOSED (>= 15:30 IST). A pre-close quote
  // would write a PARTIAL bar for today; a morning login still mints the token and
  // refreshes the stamp (showing the last complete close) — it just won't write a
  // half-formed bar that the evening refresh would then have to correct.
  const istD   = new Date(Date.now() + (5 * 60 + 30) * 60000);
  const istMin = istD.getUTCHours() * 60 + istD.getUTCMinutes();
  const marketClosed = istMin >= (15 * 60 + 30);
  let res = { ok: true, written: 0 };
  if (marketClosed) {
    try { res = await handleD1Update(env); }
    catch (e) { res = { ok: false, written: 0, msg: (e && e.message) || "exception" }; }
  }
  const lastBar = await d1NewestBar(env);
  let symbols = null;
  try {
    const s = await env.QE_DB.prepare("SELECT COUNT(DISTINCT symbol) AS n FROM ohlcv_daily").first();
    symbols = (s && s.n) || null;
  } catch (_) {}
  await writeLastRefresh(env, lastBar, (res && res.written) || 0, symbols, source);
  return {
    ok:            !!(res && res.ok),
    written:       (res && res.written) || 0,
    last_bar_date: lastBar,
    symbols:       symbols,
    market_closed: marketClosed,
  };
}

// Stale check + nudge. level: "gentle" (4 PM reminder) or "loud" (8 PM backstop).
// Pings ONLY when the cache is behind the last expected trading date. Mirrors the
// Sunday-rebuild alert pattern; never throws into the cron.
async function checkStaleAndAlert(env, level) {
  try {
    const newest   = await d1NewestBar(env);
    const expected = lastExpectedTradingDate(Date.now());
    if (newest && newest >= expected) return; // current — stay silent
    const asOf = newest || "no data";
    const kb   = { inline_keyboard: [[{ text: "🔄 Login & Refresh Data", url: WORKER_LOGIN_URL }]] };
    if (level === "loud") {
      await sendTelegram(env,
        "⚠️ <b>QuantEdge data NOT refreshed</b> — cache still at <b>" + asOf +
        "</b> (expected " + expected + ").\nTap to refresh now:", kb);
    } else {
      await sendTelegram(env,
        "📊 <b>Market closed</b> — refresh QuantEdge so today's bar is captured.\n" +
        "Data as of: <b>" + asOf + "</b>. Tap to refresh:", kb);
    }
  } catch (e) { console.warn("[checkStaleAndAlert] " + (e && e.message)); }
}

// Route: GET /d1/verify?symbol=X — PARITY CHECK (Phase B go/no-go).
// Computes indicators from D1 AND from a fresh live fetch, returns both + diffs.
async function handleD1Verify(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  const token = await getToken(env);
  if (!token) return corsErr("No Kite token — log in first", 401);

  const url = new URL(request.url);
  const sym = (url.searchParams.get("symbol") || "").toUpperCase();
  if (!sym) return corsErr("Provide ?symbol=SYMBOL", 400);

  let tokenMap = {};
  try { const t = await env.KITE_STORE.get("qe_db_token_map"); if (t) tokenMap = JSON.parse(t); } catch (_) {}

  // Live path
  let live = null, liveErr = null;
  try { live = await pipeFetchOhlcvSymbol(env, token, sym, tokenMap[sym]); }
  catch (e) { liveErr = e.message; }

  // D1 path (uses the SAME shared math via pipeComputeIndicatorsFromCandles)
  let d1 = null, d1Err = null;
  try {
    const candles = await d1ReadCandles(env, sym);
    if (!candles) d1Err = "No D1 candles (absent/too few/stale)";
    else d1 = pipeComputeIndicatorsFromCandles(sym, candles);
  } catch (e) { d1Err = e.message; }

  // Diff the key indicators
  const fields = ["ema20","ema50","ema200","rsi14","atr14","adx14","stBull",
                  "volRatio","prox52w","emaStackBull","lastClose","candleCount"];
  const diffs = {};
  let maxRelDiff = 0;
  if (live && d1) {
    for (const f of fields) {
      const a = live[f], b = d1[f];
      if (typeof a === "number" && typeof b === "number") {
        const rel = Math.abs(a) > 1e-9 ? Math.abs(a - b) / Math.abs(a) : Math.abs(a - b);
        diffs[f] = { live: a, d1: b, relDiff: +(rel * 100).toFixed(4) + "%" };
        if (rel > maxRelDiff) maxRelDiff = rel;
      } else {
        diffs[f] = { live: a, d1: b, match: a === b };
      }
    }
  }
  // candleCount will differ (live ~248 vs D1 up to 400) — that's expected and
  // does NOT affect indicators (they use trailing windows). Flag it as info.
  const verdict = (live && d1 && maxRelDiff < 0.005) ? "PASS (within 0.5%)"
                : (live && d1) ? "REVIEW (diff >0.5% — investigate)"
                : "INCOMPLETE (one side missing)";

  return cors({
    status: "success", symbol: sym, verdict: verdict,
    max_rel_diff_pct: +(maxRelDiff * 100).toFixed(4),
    note: "candleCount differs by design (D1 stores more bars); indicators use trailing windows so values still match.",
    diffs: diffs, liveError: liveErr, d1Error: d1Err,
  });
}


// ─── OHLCV batch runner ───────────────────────────────────────────────────────
async function pipeFetchOhlcvBatch(env, token, symbols, audit, survive) {
  audit.log("S4_OHLCV", "", "START",
    "Fetching OHLCV for " + symbols.length + " Stream A candidates");

  // Commit 1: load symbol→token map ONCE (KV read, not a subrequest).
  let tokenMap = {};
  try {
    const raw = await env.KITE_STORE.get("qe_db_token_map");
    if (raw) tokenMap = JSON.parse(raw);
  } catch (e) { console.warn("[pipeFetchOhlcvBatch] token-map read failed:", e && e.message); }
  audit.log("S4_OHLCV", "", "TOKENMAP",
    "Loaded " + Object.keys(tokenMap).length + " cached tokens");

  const results = {};

  // D1: resolve the cache flag ONCE per run (not per symbol). If on, the per-symbol
  // path tries D1 first and falls back to live fetch on any miss/stale/error.
  const d1On = await d1Enabled(env);
  let d1Hits = 0, d1Misses = 0, liveFallbacksUsed = 0;
  audit.log("S4_OHLCV", "", "D1MODE", d1On ? "D1 cache ENABLED (live fallback on miss)" : "D1 cache OFF (live fetch)");

  for (let i = 0; i < symbols.length; i += PIPE_BATCH_SIZE) {
    const batch = symbols.slice(i, i + PIPE_BATCH_SIZE);

    const batchResults = await Promise.all(
      batch.map(async function(sym) {
        try {
          const instrToken = tokenMap[sym];
          let ohlcv = null, fromD1 = false;
          // D1 FAST PATH: read stored candles, compute via the SHARED math.
          if (d1On) {
            const candles = await d1ReadCandles(env, sym);
            if (candles) { ohlcv = pipeComputeIndicatorsFromCandles(sym, candles); fromD1 = true; }
          }
          // FALLBACK: live fetch (also the only path when flag off / QE_DB unbound).
          // GUARD (v4.18): when D1 is ON the queue is the FULL pool, so live
          // fallbacks must stay bounded — cap them at PIPE_MAX_OHLCV_CAP (the old
          // live budget). Without this, a degraded/empty D1 would fire hundreds of
          // slow Kite historical calls in one run. Soft counter: concurrent batch
          // members may overshoot by at most PIPE_BATCH_SIZE (10) — acceptable.
          if (!ohlcv) {
            if (d1On && liveFallbacksUsed >= PIPE_MAX_OHLCV_CAP) {
              throw new Error("D1 miss — live-fallback budget exhausted");
            }
            liveFallbacksUsed++;
            ohlcv = await pipeFetchOhlcvSymbol(env, token, sym, instrToken);
          }
          return { sym: sym, ok: true, data: ohlcv, fromD1: fromD1 };
        } catch (e) {
          return { sym: sym, ok: false, error: e.message };
        }
      })
    );

    for (let bi = 0; bi < batchResults.length; bi++) {
      const br = batchResults[bi];
      if (br.fromD1) d1Hits++; else if (br.ok) d1Misses++;
      if (br.ok) {
        results[br.sym] = br.data;
        audit.log("S4_OHLCV", br.sym, "OK",
          "RSI:" + (br.data.rsi14 !== null ? br.data.rsi14.toFixed(1) : "n/a")
          + " ADX:" + (br.data.adx14 !== null ? br.data.adx14.toFixed(1) : "n/a")
          + " ATR%:" + (br.data.atrPct !== null ? br.data.atrPct.toFixed(1) : "n/a"));
      } else {
        survive.drop(br.sym, "S4_OHLCV", "Fetch failed: " + br.error);
        audit.log("S4_OHLCV", br.sym, "FAIL", br.error);
      }
    }

    if (i + PIPE_BATCH_SIZE < symbols.length) {
      await new Promise(function(r) { setTimeout(r, PIPE_BATCH_DELAY); });
    }
  }

  const fetched = Object.keys(results).length;
  audit.log("S4_OHLCV", "", "DONE",
    "Fetched: " + fetched + "/" + symbols.length
    + (d1On ? (" | D1 hits: " + d1Hits + ", live fallbacks: " + liveFallbacksUsed) : ""));

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 5 — STREAM A TECHNICAL FILTERS (post-OHLCV)
//
// Applied after OHLCV is computed. These are the core trend-confirmation filters.
// ALL must pass for a symbol to reach RS Engine.
//
// Criteria:
//   B1. EMA stack bullish: EMA20 > EMA50 > EMA200 AND close > EMA20
//   B2. RSI 14 in range [45, 75]           (momentum confirmation, not overbought)
//   B3. ADX 14 >= 18                        (trend present)
//   B4. Supertrend bullish (direction = 1)  (trend direction confirmation)
//   B5. ATR% in range [0.5, 8.0]            (not too compressed, not erratic)
//   B6. Volume ratio >= 0.8                 (at least 80% of 20-day avg volume)
// ═══════════════════════════════════════════════════════════════════════════════
const SA_RSI_MIN   = 45;
const SA_RSI_MAX   = 75;
const SA_ADX_MIN   = 18;
const SA_ATR_MIN   = 0.5;
const SA_ATR_MAX   = 8.0;
const SA_VOL_RATIO = 0.8;

function pipeStreamATech(ohlcvMap, audit, survive) {
  const symbols = Object.keys(ohlcvMap);
  audit.log("S5_STREAM_A_TECH", "", "START",
    "Technical filters on " + symbols.length + " symbols");

  const passed   = [];
  const rejected = { ema: 0, rsi: 0, adx: 0, st: 0, atr: 0, vol: 0 };

  for (let i = 0; i < symbols.length; i++) {
    const sym  = symbols[i];
    const ohlcv = ohlcvMap[sym];
    let reason = null;

    if (!ohlcv.emaStackBull) {
      reason = "EMA stack not bullish (20>50>200>price failed)";
      rejected.ema++;
    } else if (ohlcv.rsi14 === null || ohlcv.rsi14 < SA_RSI_MIN || ohlcv.rsi14 > SA_RSI_MAX) {
      reason = "RSI " + (ohlcv.rsi14 !== null ? ohlcv.rsi14.toFixed(1) : "null")
             + " outside [" + SA_RSI_MIN + "–" + SA_RSI_MAX + "]";
      rejected.rsi++;
    } else if (ohlcv.adx14 === null || ohlcv.adx14 < SA_ADX_MIN) {
      reason = "ADX " + (ohlcv.adx14 !== null ? ohlcv.adx14.toFixed(1) : "null")
             + " < " + SA_ADX_MIN;
      rejected.adx++;
    } else if (!ohlcv.stBull) {
      reason = "Supertrend bearish";
      rejected.st++;
    } else if (ohlcv.atrPct === null || ohlcv.atrPct < SA_ATR_MIN || ohlcv.atrPct > SA_ATR_MAX) {
      reason = "ATR% " + (ohlcv.atrPct !== null ? ohlcv.atrPct.toFixed(1) : "null")
             + "% outside [" + SA_ATR_MIN + "–" + SA_ATR_MAX + "%]";
      rejected.atr++;
    } else if (ohlcv.volRatio !== null && ohlcv.volRatio < SA_VOL_RATIO) {
      reason = "Volume ratio " + ohlcv.volRatio.toFixed(2) + " < " + SA_VOL_RATIO;
      rejected.vol++;
    }

    if (reason) {
      survive.drop(sym, "S5_STREAM_A_TECH", reason);
      audit.log("S5_STREAM_A_TECH", sym, "REJECT", reason);
    } else {
      passed.push(sym);
      audit.log("S5_STREAM_A_TECH", sym, "PASS",
        "RSI:" + ohlcv.rsi14.toFixed(1)
        + " ADX:" + ohlcv.adx14.toFixed(1)
        + " ATR%:" + ohlcv.atrPct.toFixed(1));
    }
  }

  audit.log("S5_STREAM_A_TECH", "", "DONE",
    "Passed: " + passed.length
    + " | Rej EMA:" + rejected.ema + " RSI:" + rejected.rsi
    + " ADX:" + rejected.adx + " ST:" + rejected.st
    + " ATR:" + rejected.atr + " Vol:" + rejected.vol);

  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STREAM B — Hidden-Gem Discovery (B1–B5 + HGS)  [Commit 3 port, Commit 4 wiring]
// Ported VERBATIM from streamb-harness-v2.2.html (applyStreamBFilters + calcHGS),
// differential-tested green against streamb_golden_fixture_v1.json (5/5).
//
// SCOPE NOTE (MVP): Stream B runs on the SAME ohlcvMap as Stream A, i.e. only on
// symbols that survived the bhav pre-pass (pipeStreamAFast) and the OHLCV cap.
// Symbols dropped before Stage 4 are not seen by Stream B. Accepted for MVP.
//
// B2_MIN_AGE = 90 (production decision, matches fixture cfg_snapshot).
// ═══════════════════════════════════════════════════════════════════════════════
function streamBCfg() {
  return {
    SA_MIN_PRICE: 100, SA_MIN_VOL: 200000, SA_ADX_MIN: 18,
    SA_RSI_MIN: 45, SA_RSI_MAX: 75, SA_EMA_STACK: true,
    B2_PRICE_MIN: 20, B2_PRICE_MAX: 800, B2_MIN_AGE: 90,
    B3_VOL_RATIO: 1.5, B3_VOL_ACCEL: true, B3_MIN_MTV_LAKH: 5,
    B4_PROX_52W: 0.15, B4_RANGE_PCT: 0.12, B4_RSI_MIN: 40, B4_RSI_MAX: 68,
    B5_ADX_MIN: 10, B5_ADX_MAX: 28, B5_EMA20_GT_50: true, B5_PRICE_GT_E20: true,
  };
}

// Pure: production ohlcvMap entry -> harness-shaped object. No mutation of input.
function sbMapToHarnessShape(p) {
  return {
    symbol: p.symbol, last: p.lastClose, lastVol: p.lastVol,
    e20: p.ema20, e50: p.ema50, rsi: p.rsi14, adx: p.adx14,
    volRatio: p.volRatio, volAccel: p.volAccel, mtv: p.mtv,
    high52w: p.hi52w, pctBelow52w: p.pctBelow52w, range20pct: p.range20pct,
    emaStackBull: p.emaStackBull, rs1m: p.rs1m, candles: p.candleCount,
  };
}

function sbCalcHGS(data) {
  var score = 0;
  var vs = data.volRatio || 0;
  score += Math.min(20, Math.max(0, (vs - 1.0) * 13.3));
  var tightness = data.range20pct || 20;
  score += Math.min(20, Math.max(0, (15 - tightness) * 1.6));
  var rs = data.rs1m || 0;
  score += Math.min(20, Math.max(0, (rs + 0.05) * 200));
  var rsi = data.rsi || 50;
  var rsiScore = rsi >= 50 && rsi <= 62 ? 20 :
                 rsi >= 45 && rsi < 50  ? 10 :
                 rsi > 62 && rsi <= 68  ? 10 : 0;
  score += rsiScore;
  var prox = data.pctBelow52w || 30;
  score += prox >= 3 && prox <= 15 ? 20 :
           prox > 15 && prox <= 25 ? 8  : 0;
  return Math.round(Math.min(100, Math.max(0, score)));
}

function sbApplyFilters(ohlcv, cfg) {
  var result = {
    symbol: ohlcv.symbol,
    b1: false, b2: false, b3: false, b4: false, b5: false,
    b1reason:'', b2reason:'', b3reason:'', b4reason:'', b5reason:'',
    passAll: false,
  };
  var wouldPassSA = ohlcv.last >= cfg.SA_MIN_PRICE &&
                    ohlcv.lastVol >= cfg.SA_MIN_VOL &&
                    ohlcv.adx >= cfg.SA_ADX_MIN &&
                    ohlcv.rsi >= cfg.SA_RSI_MIN &&
                    ohlcv.rsi <= cfg.SA_RSI_MAX &&
                    ohlcv.emaStackBull;
  if (wouldPassSA) { result.b1 = false; result.b1reason = 'Passes Stream A (not a hidden gem)'; return result; }
  result.b1 = true;

  var price = ohlcv.last;
  var estAgeDays = Math.round(ohlcv.candles * 1.4);
  if (price < cfg.B2_PRICE_MIN) { result.b2 = false; result.b2reason = 'Price below min'; return result; }
  if (price > cfg.B2_PRICE_MAX) { result.b2 = false; result.b2reason = 'Price above max'; return result; }
  if (estAgeDays < cfg.B2_MIN_AGE) { result.b2 = false; result.b2reason = 'Listing age ' + estAgeDays + 'd < ' + cfg.B2_MIN_AGE; return result; }
  result.b2 = true;

  if (!ohlcv.volRatio || ohlcv.volRatio < cfg.B3_VOL_RATIO) { result.b3 = false; result.b3reason = 'Vol ratio below ' + cfg.B3_VOL_RATIO; return result; }
  if (cfg.B3_VOL_ACCEL && !ohlcv.volAccel) { result.b3 = false; result.b3reason = 'No volume acceleration'; return result; }
  if (ohlcv.mtv < cfg.B3_MIN_MTV_LAKH) { result.b3 = false; result.b3reason = 'MTV below ' + cfg.B3_MIN_MTV_LAKH + 'L/d'; return result; }
  result.b3 = true;

  if (ohlcv.pctBelow52w > cfg.B4_PROX_52W * 100) { result.b4 = false; result.b4reason = ohlcv.pctBelow52w.toFixed(1) + '% below 52w high'; return result; }
  if (ohlcv.range20pct > cfg.B4_RANGE_PCT * 100) { result.b4 = false; result.b4reason = '20d range ' + ohlcv.range20pct.toFixed(1) + '% too wide'; return result; }
  if (ohlcv.rsi < cfg.B4_RSI_MIN || ohlcv.rsi > cfg.B4_RSI_MAX) { result.b4 = false; result.b4reason = 'RSI outside band'; return result; }
  result.b4 = true;

  if (!ohlcv.adx || ohlcv.adx < cfg.B5_ADX_MIN) { result.b5 = false; result.b5reason = 'ADX below ' + cfg.B5_ADX_MIN; return result; }
  if (ohlcv.adx > cfg.B5_ADX_MAX) { result.b5 = false; result.b5reason = 'ADX above ' + cfg.B5_ADX_MAX; return result; }
  if (cfg.B5_EMA20_GT_50 && ohlcv.e20 && ohlcv.e50 && ohlcv.e20 <= ohlcv.e50) { result.b5 = false; result.b5reason = 'EMA20 not > EMA50'; return result; }
  if (cfg.B5_PRICE_GT_E20 && ohlcv.e20 && ohlcv.last <= ohlcv.e20) { result.b5 = false; result.b5reason = 'Price not > EMA20'; return result; }
  result.b5 = true;
  result.passAll = true;
  return result;
}

// Orchestrator: run Stream B over the production ohlcvMap. Returns candidate array.
// Pure read of ohlcvMap; logs to audit; drops to survive. Does NOT touch Stream A.
function pipeStreamBTech(ohlcvMap, audit, survive) {
  const cfg = streamBCfg();
  const symbols = Object.keys(ohlcvMap);
  audit.log("S5B_STREAM_B", "", "START", "Stream B over " + symbols.length + " symbols");
  const candidates = [];
  for (let i = 0; i < symbols.length; i++) {
    const sym = symbols[i];
    try {
      const shaped = sbMapToHarnessShape(ohlcvMap[sym]);
      const filt = sbApplyFilters(shaped, cfg);
      if (!filt.passAll) {
        const firstFail = !filt.b1 ? "B1" : !filt.b2 ? "B2" : !filt.b3 ? "B3" : !filt.b4 ? "B4" : "B5";
        const reason = filt.b1reason || filt.b2reason || filt.b3reason || filt.b4reason || filt.b5reason;
        survive.drop(sym, "S5B_STREAM_B", firstFail + ": " + reason);
        continue;
      }
      const hgs = sbCalcHGS({
        volRatio: shaped.volRatio, range20pct: shaped.range20pct,
        rs1m: shaped.rs1m, rsi: shaped.rsi, pctBelow52w: shaped.pctBelow52w
      });
      candidates.push({
        symbol: sym, hgs: hgs,
        last: shaped.last, pctBelow52w: shaped.pctBelow52w,
        volRatio: shaped.volRatio, mtv: shaped.mtv,
        rsi: shaped.rsi, adx: shaped.adx, range20pct: shaped.range20pct,
      });
      audit.log("S5B_STREAM_B", sym, "PASS", "HGS " + hgs);
    } catch (e) {
      // No silent failure — log and drop.
      audit.log("S5B_STREAM_B", sym, "ERROR", e.name + ": " + e.message);
      survive.drop(sym, "S5B_STREAM_B", "Exception: " + e.message);
    }
  }
  candidates.sort(function(a, b) { return b.hgs - a.hgs; });
  audit.log("S5B_STREAM_B", "", "DONE", candidates.length + " Stream B candidates");
  return candidates;
}


// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 6 — RS ENGINE (server-side)
//
// Relative Strength = 3-period weighted return vs Nifty 50.
// Weights: 1m=40%, 3m=35%, 6m=25% (recency-biased).
// RS Score = percentile rank of weighted return vs all stocks in this pipeline run.
// Cutoff: PIPE_RS_THRESHOLD (default 55th percentile).
//
// If Nifty closes are unavailable (fetch failed), fall back to absolute
// 1-month return rank only (RS_FALLBACK mode).
// ═══════════════════════════════════════════════════════════════════════════════

function pipeCalcRS(stockCloses, niftyCloses) {
  if (!stockCloses || stockCloses.length < 20) return 0;

  function periodReturn(closes, bars) {
    if (!closes || closes.length < bars + 1) return null;
    const end   = closes[closes.length - 1];
    const start = closes[closes.length - 1 - bars];
    if (!start || start === 0) return null;
    return (end - start) / start;
  }

  const s1m = periodReturn(stockCloses, 21);
  const s3m = periodReturn(stockCloses, 63);
  const s6m = periodReturn(stockCloses, 126);

  if (s1m === null) return 0;

  if (!niftyCloses || niftyCloses.length < 22) {
    // Fallback: absolute return only
    return s1m !== null ? Math.max(0, Math.min(100, (s1m + 0.10) * 500)) : 0;
  }

  const n1m = periodReturn(niftyCloses, 21);
  const n3m = periodReturn(niftyCloses, 63);
  const n6m = periodReturn(niftyCloses, 126);

  // Relative returns
  const r1m = s1m !== null && n1m !== null ? s1m - n1m : 0;
  const r3m = s3m !== null && n3m !== null ? s3m - n3m : 0;
  const r6m = s6m !== null && n6m !== null ? s6m - n6m : 0;

  // Weighted composite relative return
  const composite = (r1m * 0.40) + (r3m * 0.35) + (r6m * 0.25);
  return composite; // raw score — percentile rank computed after all stocks computed
}

function pipeRankRS(streamAPassed, ohlcvMap, niftyCloses, audit, survive) {
  audit.log("S6_RS", "", "START", "RS ranking for " + streamAPassed.length + " symbols");

  // Compute raw RS composite for each symbol
  const rsRaw = [];
  for (let i = 0; i < streamAPassed.length; i++) {
    const sym   = streamAPassed[i];
    const ohlcv = ohlcvMap[sym];
    const raw   = pipeCalcRS(ohlcv.closes, niftyCloses);
    rsRaw.push({ sym: sym, raw: raw });
  }

  // Sort ascending to get ranks
  const sorted = rsRaw.slice().sort(function(a, b) { return a.raw - b.raw; });

  // R5 fix: build a symbol→rank map once (O(n)) instead of findIndex per
  // element (O(n²)). Ties: equal raw values share the rank of the earliest
  // index holding that raw value — deterministic and stable.
  const rankBySym = {};
  let tieRank = 0;
  for (let k = 0; k < sorted.length; k++) {
    if (k > 0 && sorted[k].raw !== sorted[k - 1].raw) tieRank = k;
    rankBySym[sorted[k].sym] = tieRank;
  }

  // Assign percentile rank
  const ranked = rsRaw.map(function(item) {
    const rank = rankBySym[item.sym];
    const pct  = rsRaw.length > 1 ? (rank / (rsRaw.length - 1)) * 100 : 50;
    return { sym: item.sym, rsScore: Math.round(pct), rawRS: item.raw };
  });

  // Apply RS threshold cutoff
  const passed   = [];
  const rejected = [];

  for (let j = 0; j < ranked.length; j++) {
    const r = ranked[j];
    if (r.rsScore >= PIPE_RS_THRESHOLD) {
      passed.push(r);
      audit.log("S6_RS", r.sym, "PASS", "RS: " + r.rsScore + " (raw " + r.rawRS.toFixed(4) + ")");
    } else {
      survive.drop(r.sym, "S6_RS",
        "RS score " + r.rsScore + " < threshold " + PIPE_RS_THRESHOLD);
      audit.log("S6_RS", r.sym, "REJECT",
        "RS: " + r.rsScore + " < " + PIPE_RS_THRESHOLD);
      rejected.push(r.sym);
    }
  }

  audit.log("S6_RS", "", "DONE",
    "Passed: " + passed.length + " | Rejected: " + rejected.length);

  // Sort by rsScore descending
  passed.sort(function(a, b) { return b.rsScore - a.rsScore; });
  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 7 — SECTOR ENGINE
//
// Prevents sector concentration in final candidates.
// Max PIPE_MAX_SECTOR_N candidates per sector.
// Within each sector, keep highest RS score.
// ═══════════════════════════════════════════════════════════════════════════════
function pipeSectorFilter(rsRanked, ohlcvMap, audit, survive) {
  audit.log("S7_SECTOR", "", "START",
    "Sector concentration filter on " + rsRanked.length + " symbols");

  const sectorCount = {};
  const passed      = [];

  for (let i = 0; i < rsRanked.length; i++) {
    const r      = rsRanked[i];
    const sector = SECTOR_MAP[r.sym] || "OTHER";

    if (!sectorCount[sector]) sectorCount[sector] = 0;

    if (sectorCount[sector] < PIPE_MAX_SECTOR_N) {
      sectorCount[sector]++;
      passed.push(Object.assign({}, r, { sector: sector }));
      audit.log("S7_SECTOR", r.sym, "PASS",
        "Sector: " + sector + " (" + sectorCount[sector] + "/" + PIPE_MAX_SECTOR_N + ")");
    } else {
      survive.drop(r.sym, "S7_SECTOR",
        "Sector cap: " + sector + " already has " + PIPE_MAX_SECTOR_N + " candidates");
      audit.log("S7_SECTOR", r.sym, "REJECT",
        "Sector cap: " + sector + " (" + sectorCount[sector] + "/" + PIPE_MAX_SECTOR_N + ")");
    }
  }

  audit.log("S7_SECTOR", "", "DONE",
    "Passed: " + passed.length + " across "
    + Object.keys(sectorCount).filter(function(k) { return sectorCount[k] > 0; }).length
    + " sectors");

  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 8 — MERGE ENGINE + DISCOVERY SCORE
//
// Builds final candidate list for browser deep analysis.
// Discovery Score (server-side, mirrors browser calcDiscoveryScore logic):
//   RS Percentile   : 25 pts  (rsScore / 100 * 25)
//   Volume Surge    : 20 pts  (volRatio; 1.5x = full score)
//   Coil/Compression: 15 pts  (pctAboveE20 <= 5% and ATR% < 2.5%)
//   52-week Proximity: 15 pts (prox52w >= 80%)
//   Trend Quality   : 15 pts  (EMA stack + ADX + Supertrend)
//   Liquidity       : 10 pts  (daily volume >= 500K = full score)
//
// Sorts by discoveryScore descending, caps at PIPE_TOP_N.
// ═══════════════════════════════════════════════════════════════════════════════
function pipeCalcDiscoveryScore(ohlcv, rsScore) {
  let total = 0;
  const breakdown = {};

  // Factor 1: RS Percentile (25 pts)
  const rsPts = Math.round((rsScore / 100) * 25);
  breakdown.rs = rsPts;
  total += rsPts;

  // Factor 2: Volume Surge (20 pts) — volRatio 1.5x = full 20 pts
  let volPts = 0;
  if (ohlcv.volRatio !== null) {
    volPts = Math.min(20, Math.round((ohlcv.volRatio / 1.5) * 20));
  }
  breakdown.vol = volPts;
  total += volPts;

  // Factor 3: Coil / Compression (15 pts)
  // pctAboveE20 <= 5% AND atrPct < 2.5% = max score
  let coilPts = 0;
  if (ohlcv.pctAboveE20 !== null && ohlcv.atrPct !== null) {
    const coilScore = (ohlcv.pctAboveE20 <= 2 && ohlcv.atrPct < 1.5) ? 15
                    : (ohlcv.pctAboveE20 <= 5 && ohlcv.atrPct < 2.5) ? 10
                    : (ohlcv.pctAboveE20 <= 8)                        ? 5
                    : 0;
    coilPts = coilScore;
  }
  breakdown.coil = coilPts;
  total += coilPts;

  // Factor 4: 52-week Proximity (15 pts) — prox52w >= 95% = full; >= 80% = partial
  let proxPts = 0;
  if (ohlcv.prox52w !== null) {
    proxPts = ohlcv.prox52w >= 95 ? 15
            : ohlcv.prox52w >= 85 ? 10
            : ohlcv.prox52w >= 75 ? 5
            : 0;
  }
  breakdown.prox52w = proxPts;
  total += proxPts;

  // Factor 5: Trend Quality (15 pts)
  let trendPts = 0;
  if (ohlcv.emaStackBull) trendPts += 5;
  if (ohlcv.adx14 !== null && ohlcv.adx14 >= 25) trendPts += 5;
  else if (ohlcv.adx14 !== null && ohlcv.adx14 >= 18) trendPts += 3;
  if (ohlcv.stBull) trendPts += 5;
  breakdown.trend = trendPts;
  total += trendPts;

  // Factor 6: Liquidity (10 pts) — 500K daily vol = full score
  let liqPts = 0;
  if (ohlcv.lastVol >= 500000)      liqPts = 10;
  else if (ohlcv.lastVol >= 200000) liqPts = 6;
  else if (ohlcv.lastVol >= 100000) liqPts = 3;
  breakdown.liquidity = liqPts;
  total += liqPts;

  return { total: Math.min(100, total), breakdown: breakdown };
}

function pipeMerge(sectorFiltered, ohlcvMap, audit) {
  audit.log("S8_MERGE", "", "START", "Building " + sectorFiltered.length + " candidates");

  const candidates = [];

  for (let i = 0; i < sectorFiltered.length; i++) {
    const r     = sectorFiltered[i];
    const ohlcv = ohlcvMap[r.sym];
    if (!ohlcv) continue;

    const ds    = pipeCalcDiscoveryScore(ohlcv, r.rsScore);

    // Compute trade levels from ATR
    const atr   = ohlcv.atr14 || 0;
    const entry = ohlcv.lastClose;
    const sl    = atr > 0 ? parseFloat((entry - 1.5 * atr).toFixed(2)) : null;
    const t1    = atr > 0 ? parseFloat((entry + 2.0 * atr).toFixed(2)) : null;
    const t2    = atr > 0 ? parseFloat((entry + 3.5 * atr).toFixed(2)) : null;

    candidates.push({
      symbol:         r.sym,
      sector:         r.sector,
      rsScore:        r.rsScore,
      discoveryScore: ds.total,
      dsBreakdown:    ds.breakdown,
      // Price data for browser
      lastClose:      ohlcv.lastClose,
      entry:          parseFloat(entry.toFixed(2)),
      sl:             sl,
      t1:             t1,
      t2:             t2,
      atr:            atr !== null ? parseFloat(atr.toFixed(2)) : null,
      atrPct:         ohlcv.atrPct !== null ? parseFloat(ohlcv.atrPct.toFixed(2)) : null,
      // Indicators for browser card display
      rsi14:          ohlcv.rsi14 !== null ? parseFloat(ohlcv.rsi14.toFixed(1)) : null,
      adx14:          ohlcv.adx14 !== null ? parseFloat(ohlcv.adx14.toFixed(1)) : null,
      ema20:          ohlcv.ema20 !== null ? parseFloat(ohlcv.ema20.toFixed(2)) : null,
      ema50:          ohlcv.ema50 !== null ? parseFloat(ohlcv.ema50.toFixed(2)) : null,
      ema200:         ohlcv.ema200 !== null ? parseFloat(ohlcv.ema200.toFixed(2)) : null,
      stBull:         ohlcv.stBull,
      volRatio:       ohlcv.volRatio !== null ? parseFloat(ohlcv.volRatio.toFixed(2)) : null,
      pctAboveE20:    ohlcv.pctAboveE20 !== null ? parseFloat(ohlcv.pctAboveE20.toFixed(2)) : null,
      prox52w:        ohlcv.prox52w !== null ? parseFloat(ohlcv.prox52w.toFixed(1)) : null,
      // Pipeline metadata
      builtAt:        new Date().toISOString(),
      // closes omitted from candidates — too large; kept in per-symbol KV key
    });

    audit.log("S8_MERGE", r.sym, "CANDIDATE",
      "DS:" + ds.total + " RS:" + r.rsScore + " Sector:" + r.sector);
  }

  // Sort by discoveryScore descending
  candidates.sort(function(a, b) { return b.discoveryScore - a.discoveryScore; });

  // Cap at PIPE_TOP_N
  const final = candidates.slice(0, PIPE_TOP_N);
  audit.log("S8_MERGE", "", "DONE",
    "Final candidates: " + final.length + " (top " + PIPE_TOP_N + " by DS)");

  return final;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 9 — TELEGRAM SIGNAL DISPATCH
//
// Sends top PIPE_SIGNAL_TOP candidates as Telegram signals.
// FIX 2: Server-side signal integrity gate applied before dispatch.
// A candidate must pass all three structural checks to be dispatched as
// a WATCH-eligible signal. Candidates that fail are dispatched as
// WATCH_ONLY with a clear label — preventing a TG signal that the browser
// will later contradict with IGNORE.
//
// Gate criteria (mirrors the structural subset of browser finalDecision()):
//   G1. discoveryScore >= 60    (minimum score threshold)
//   G2. stBull === true         (Supertrend must be bullish — trend direction)
//   G3. adx14 >= 18             (trend must be present — not ranging/dead)
//
// Candidates below gate are still dispatched as WATCH_ONLY (informational)
// so the pipeline is transparent, but BUY button is suppressed at Telegram level.
// ═══════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 9.5 — QE SCORE GATE (v4.19) — PRO FILTER ON + ELITE ON
// The browser QuantEdge engine ported VERBATIM from the LIVE index.html (7587-line
// upload, 12-Jun-2026) so Telegram carries only candidates that survive the SAME
// screen the user runs: PRO FILTER ON + ELITE ON. Ported intact:
//   • helpers (ema/rsi/atr/adx/macd/supertrend/sma/aggregate/stats)
//   • signalEngine + tradeEngine + stockBacktest (walk-forward, trailing/breakeven)
//   • tradeStats, monteCarlo (real-trade fat-tail)
//   • applyProFilter (5 layers: structural / anti-failure / price / momentum / volume)
//   • computeExecutionDecision (Elite ENTER/WAIT/SKIP)
//   • finalDecision Rules 1-5 INCLUDING 4a/4b/4c Elite-SKIP rescue + 4d/3 baseScore
// _proFilterMode and _eliteMode are forced TRUE here (the user's production config),
// independent of the browser's UI defaults.
// Deviation (documented, immaterial): browser may fall back to mcProbabilistic when
// bt.total in [3,5); here mc=null, but Rule 3b (INSUFFICIENT_DATA, <5 trades) decides
// those cases first, so the gate verdict is identical and deterministic. Fundamentals/
// news/macro-prob shift winProb/EV display only, never score or these rules.
// Kill switch: KV QE_SCORE_GATE="off" → gate bypassed (dispatch as v4.18).
// ═══════════════════════════════════════════════════════════════════════════════
const QEGate = (function () {
const _proFilterMode = true;   // user production config: PRO FILTER ON
const _eliteMode     = true;   // user production config: ELITE ON
const QE={
  CAPITAL:100000,RISK_PCT:0.01,MAX_DAILY_RISK:0.05,
  MAX_CONCURRENT:5,MAX_SAME_DAY:2,SLIPPAGE:0.001,
  COMMISSION:20,MIN_BARS:50,MAX_HOLD:20,
  SIGNAL_COOLDOWN:5,RISK_FREE:0.065,TRADING_DAYS:250,MC_RUNS:1000
};
const WEIGHTS={trend:.30,volume:.20,breakout:.20,momentum:.15,strength:.15};

/* ── helpers (verbatim, live) ── */
function mean(a){return a.length?a.reduce((x,y)=>x+y,0)/a.length:0}
function std(a){if(a.length<2)return 0;const m=mean(a);return Math.sqrt(a.reduce((s,v)=>s+(v-m)**2,0)/(a.length-1))}
function downsideStd(a,t=0){const n=a.filter(v=>v<t);if(n.length<2)return 0;return Math.sqrt(n.reduce((s,v)=>s+(v-t)**2,0)/n.length)}
function safe(v,fb=0){return(!isFinite(v)||isNaN(v))?fb:v}
function last(a){for(let i=a.length-1;i>=0;i--)if(a[i]!=null)return a[i];return null}
function shuffle(a){const b=[...a];for(let i=b.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[b[i],b[j]]=[b[j],b[i]]}return b}
function ptile(s,p){var _i=Math.floor(s.length*p/100);return (_i>=0&&_i<s.length&&s[_i]!==undefined&&s[_i]!==null)?s[_i]:s[s.length-1];}

/* ── INDICATORS ── */
function ema(v,p){
  if(v.length<p)return v.map(()=>null);
  const k=2/(p+1);let val=v.slice(0,p).reduce((a,b)=>a+b,0)/p;
  const o=new Array(p-1).fill(null);o.push(val);
  for(let i=p;i<v.length;i++){val=v[i]*k+val*(1-k);o.push(val)}
  return o;
}
function rsi(c,p=14){
  const o=new Array(p).fill(null);let g=0,l=0;
  for(let i=1;i<=p;i++){const d=c[i]-c[i-1];d>0?g+=d:l+=Math.abs(d)}
  g/=p;l/=p;o.push(l===0?100:100-100/(1+g/l)); /* FIX: guard l===0 */
  for(let i=p+1;i<c.length;i++){
    const d=c[i]-c[i-1],gn=d>0?d:0,ln=d<0?Math.abs(d):0;
    g=(g*(p-1)+gn)/p;l=(l*(p-1)+ln)/p;o.push(l===0?100:100-100/(1+g/l));
  }
  return o;
}
function atr(H,L,C,p=14){
  const tr=[null];for(let i=1;i<C.length;i++)tr.push(Math.max(H[i]-L[i],Math.abs(H[i]-C[i-1]),Math.abs(L[i]-C[i-1])));
  const o=new Array(p).fill(null);let val=tr.slice(1,p+1).reduce((a,b)=>a+b,0)/p;o.push(val);
  for(let i=p+1;i<tr.length;i++){val=(val*(p-1)+tr[i])/p;o.push(val)}
  return o;
}
function macdCalc(c,f=12,s2=26,sg=9){
  const e12=ema(c,f),e26=ema(c,s2);
  const line=e12.map((v,i)=>(v!=null&&e26[i]!=null)?v-e26[i]:null);
  const valid=line.filter(v=>v!=null),off=line.length-valid.length;
  const sig=new Array(off).fill(null).concat(ema(valid,sg));
  return{line,signal:sig,hist:line.map((v,i)=>(v!=null&&sig[i]!=null)?v-sig[i]:null)};
}
function adxCalc(H,L,C,p=14){
  const n=C.length,pdm=[0],ndm=[0],tr=[0];
  for(let i=1;i<n;i++){
    const up=H[i]-H[i-1],dn=L[i-1]-L[i];
    pdm.push(up>dn&&up>0?up:0);ndm.push(dn>up&&dn>0?dn:0);
    tr.push(Math.max(H[i]-L[i],Math.abs(H[i]-C[i-1]),Math.abs(L[i]-C[i-1])));
  }
  const ws=(a,p)=>{const o=new Array(p).fill(null);let s=a.slice(0,p).reduce((x,y)=>x+y,0);o.push(s);for(let i=p;i<a.length;i++){s=s-s/p+a[i];o.push(s)}return o};
  const sTR=ws(tr,p),sPDM=ws(pdm,p),sNDM=ws(ndm,p);
  const dip=sTR.map((v,i)=>v>0?sPDM[i]/v*100:null);
  const dim=sTR.map((v,i)=>v>0?sNDM[i]/v*100:null);
  const dx=dip.map((p2,i)=>{if(p2==null||dim[i]==null)return null;const s=p2+dim[i];return s>0?Math.abs(p2-dim[i])/s*100:null});
  const vdx=dx.filter(v=>v!=null),adxRaw=ema(vdx,p);
  return{adx:new Array(dx.length-vdx.length).fill(null).concat(adxRaw),dip,dim};
}
function supertrend(H,L,C,p=10,m=2){
  const atrA=atr(H,L,C,p),dir=new Array(C.length).fill(1);let ub=0,lb=0,pUb=0,pLb=0;
  for(let i=p;i<C.length;i++){
    if(!atrA[i])continue;const hl=(H[i]+L[i])/2;
    ub=(hl+m*atrA[i]<pUb||C[i-1]>pUb)?hl+m*atrA[i]:pUb;
    lb=(hl-m*atrA[i]>pLb||C[i-1]<pLb)?hl-m*atrA[i]:pLb;
    if(C[i]>ub)dir[i]=1;else if(C[i]<lb)dir[i]=-1;else dir[i]=dir[i-1]||1;
    pUb=ub;pLb=lb;
  }
  return dir;
}
function sma(a,p){const o=new Array(p-1).fill(null);for(let i=p-1;i<a.length;i++)o.push(a.slice(i-p+1,i+1).reduce((x,y)=>x+y,0)/p);return o}
function aggregate(data,mode='weekly'){
  const g={};
  for(const d of data){
    const dt=new Date(d.t*1000);let key;
    if(mode==='weekly'){const day=dt.getDay(),diff=dt.getDate()-day+(day===0?-6:1);const mon=new Date(dt);mon.setDate(diff);key=mon.toISOString().slice(0,10)}
    else key=`${dt.getFullYear()}-${String(dt.getMonth()+1).padStart(2,'0')}`;
    if(!g[key])g[key]={t:d.t,o:d.o,h:d.h,l:d.l,c:d.c,v:d.v};
    else{g[key].h=Math.max(g[key].h,d.h);g[key].l=Math.min(g[key].l,d.l);g[key].c=d.c;g[key].v+=d.v}
  }
  return Object.values(g);
}

/* ── walk-forward engines (verbatim, live) ── */
function signalEngine(closes,highs,lows,volumes,i){
  if(i<QE.MIN_BARS)return null;
  const wc=closes.slice(0,i),wh=highs.slice(0,i),wl=lows.slice(0,i),wv=volumes.slice(0,i);
  if(wc.length<30)return null;
  const e8=last(ema(wc,8)),e21=last(ema(wc,21));
  const r14=last(rsi(wc,14)),a14=last(atr(wh,wl,wc,14));
  const volSMA=mean(wv.slice(-20)),curVol=wv[wv.length-1];
  if(!e8||!e21||!r14||!a14||a14<=0)return null;
  if(!(e8>e21))return null;
  if(!(r14>55&&r14<75))return null;     /* user's range */
  if(!(curVol>volSMA*1.2))return null;  /* volume filter */
  return{e8,e21,r14,atr:a14,curVol,volSMA};
}

/* Trade simulation — all bugs fixed */
function tradeEngine(data,i,equity){
  const H=data.map(d=>d.h),L=data.map(d=>d.l),C=data.map(d=>d.c);

  /* GOLDEN FORMULA (16-Jul-2026): entry/stop unified with the live signal formula —
     breakout above the 5-bar high (as of signal bar i, no lookahead — bar i IS "today"
     at signal time), swing-low/ATR stop. Matches entryPrice=Math.max(H.slice(-5))*1.005
     used live and in evaluate(). */
  const breakoutLevel=Math.max(...H.slice(Math.max(0,i-4),i+1))*1.005;
  const atrA=atr(H,L,C,14);
  const atrV=atrA[i]||((data[i].h-data[i].l)||breakoutLevel*0.02);
  const swingLow=Math.min(...L.slice(Math.max(0,i-9),i+1));
  const cmpAtSignal=data[i].c;
  const rawStop=Math.max(swingLow,atrV>0?cmpAtSignal-1.5*atrV:cmpAtSignal*0.95);
  const stop=rawStop<breakoutLevel?rawStop:breakoutLevel*0.92;
  const risk=breakoutLevel-stop;
  if(risk<=0||isNaN(risk))return null;

  /* Trigger simulation (no lookahead): scan forward from i+1 only — exactly what a
     live stop-buy order resting at breakoutLevel would experience. ENGINEERING_ASSUMPTION:
     5-bar trigger window, matching the existing SIGNAL_COOLDOWN convention. If the breakout
     never happens within the window, the setup is treated as expired — same as it would be
     live — and this signal produces no trade, not a forced entry. */
  const TRIGGER_WINDOW=5;
  let entry=null,nxt=null;
  for(let j=i+1;j<=Math.min(i+TRIGGER_WINDOW,data.length-1);j++){
    if(data[j].h>=breakoutLevel){
      entry=(data[j].o>breakoutLevel?data[j].o:breakoutLevel)*(1+QE.SLIPPAGE);
      nxt=j;break;
    }
  }
  if(entry===null)return null;

  /* FIX 3: dynamic equity position sizing */
  const qty=Math.max(1,Math.floor((equity*QE.RISK_PCT)/risk));
  const t1=entry+2*risk,t2=entry+3*risk;
  const hQ=Math.max(1,Math.floor(qty/2)),rQ=qty-hQ;

  let t1Hit=false,t1ExP=0,finalExit=0,exitIdx=nxt;
  const maxIdx=Math.min(nxt+QE.MAX_HOLD,data.length-1);

  let trailStop=stop; /* U4: trailing stop starts at ATR stop */
  for(let j=nxt+1;j<=maxIdx;j++){
    const hi=data[j].h,lo=data[j].l,cl=data[j].c;exitIdx=j;
    if(!t1Hit){
      if(lo<=trailStop){finalExit=Math.min(lo,trailStop)*(1-QE.SLIPPAGE);break}
      if(hi>=t1){
        t1Hit=true;
        t1ExP=t1*(1-QE.SLIPPAGE);
        trailStop=entry; /* U4: move stop to breakeven after T1 hit — protect profits */
        continue;
      }
    }else{
      if(lo<=trailStop){finalExit=Math.max(lo,trailStop)*(1-QE.SLIPPAGE);break}
      if(hi>=t2){finalExit=t2*(1-QE.SLIPPAGE);break}
    }
    if(j===maxIdx)finalExit=cl*(1-QE.SLIPPAGE);
  }

  let pnl=t1Hit?(t1ExP-entry)*hQ+(finalExit-entry)*rQ:(finalExit-entry)*qty;
  pnl-=QE.COMMISSION*2;
  const rm=safe(pnl/(risk*qty));
  return{
    entryTime:data[nxt].t,exitTime:data[exitIdx].t,
    entryBar:nxt,exitBar:exitIdx,
    entryPrice:parseFloat(entry.toFixed(2)),exitPrice:parseFloat((finalExit||entry).toFixed(2)),
    stopLoss:parseFloat(stop.toFixed(2)),t1:parseFloat(t1.toFixed(2)),t2:parseFloat(t2.toFixed(2)),
    qty,pnl:parseFloat(pnl.toFixed(2)),rMultiple:parseFloat(rm.toFixed(3)),
    barsHeld:exitIdx-nxt,t1Hit,result:pnl>0?'WIN':'LOSS',risk1R:parseFloat(risk.toFixed(2))
  };
}

/* Per-stock walk-forward */
function stockBacktest(data,symbol){
  const C=data.map(d=>d.c),H=data.map(d=>d.h),L=data.map(d=>d.l),V=data.map(d=>d.v);
  const trades=[];let cursor=QE.MIN_BARS,cooldown=0;
  let rollingEquity=QE.CAPITAL; /* U3: rolling equity — realistic position sizing */
  while(cursor<C.length-1){
    cooldown=Math.max(0,cooldown-1);
    if(cooldown>0){cursor++;continue}
    const sig=signalEngine(C,H,L,V,cursor);
    if(!sig){cursor++;continue}
    const t=tradeEngine(data,cursor,Math.max(rollingEquity,QE.CAPITAL*0.5));
    if(!t){cursor++;continue}
    trades.push({...t,symbol});
    rollingEquity=Math.max(rollingEquity+t.pnl,QE.CAPITAL*0.3); /* U3: update rolling equity */
    cursor=t.exitBar+1;cooldown=QE.SIGNAL_COOLDOWN;
  }
  return trades;
}

/* ── trade stats (verbatim, live) ── */
function tradeStats(trades){
  if(!trades||!trades.length)return{total:0,wins:0,winRate:0,expectancy:0,profitFactor:0};
  const wins=trades.filter(t=>t.result==='WIN'),losses=trades.filter(t=>t.result==='LOSS'),total=trades.length;
  const wr=parseFloat(safe(wins.length/total*100).toFixed(1));
  const aWR=wins.length?mean(wins.map(t=>t.rMultiple)):0;
  const aLR=losses.length?mean(losses.map(t=>Math.abs(t.rMultiple))):0;
  const exp=parseFloat(safe((wins.length/total)*aWR-(losses.length/total)*aLR).toFixed(3));
  const gW=wins.reduce((s,t)=>s+Math.max(0,t.pnl),0),gL=Math.abs(losses.reduce((s,t)=>s+Math.min(0,t.pnl),0));
  return{total,wins:wins.length,losses:losses.length,winRate:wr,expectancy:exp,profitFactor:gL>0?parseFloat(safe(gW/gL).toFixed(2)):0};
}

/* ── Commit A (v4.31): EDGE-CONFIDENCE INSTRUMENTATION — REPORT-ONLY ──────────────────
   Derives the standard error of expectancy and a 3-state edge class from the EXISTING
   backtest trades. Feeds NO verdict / score / ranking / gate (that is Commit C, not yet built).
   expSE     = std(per-trade R) / sqrt(n).
   edgeClass = one-sided band at t = QE_EDGE_T (provisional 1.0; final value locked in Commit C):
     PROVEN_POSITIVE : expectancy − t·SE > 0
     PROVEN_NEGATIVE : expectancy + t·SE < 0
     INDETERMINATE   : band straddles 0, or n < 5 (too few trades to classify). */
var QE_EDGE_T = 1.0;
function edgeConfidence(expectancy, rMults){
  var n = (rMults && rMults.length) ? rMults.length : 0;
  if(n < 5) return { expSE: null, edgeClass: 'INDETERMINATE' };
  var se = std(rMults) / Math.sqrt(n);
  var band = QE_EDGE_T * se;
  var cls = (expectancy - band > 0) ? 'PROVEN_POSITIVE'
          : (expectancy + band < 0) ? 'PROVEN_NEGATIVE'
          : 'INDETERMINATE';
  return { expSE: parseFloat(se.toFixed(4)), edgeClass: cls };
}

/* ── Commit D (v4.34): FRESH-BREAKOUT STRUCTURAL DETECTOR — WATCH-only admission, never BUY ──────
   Returns true ONLY if ALL criteria hold simultaneously (a-priori thresholds; NOT tuned to any
   benchmark or audit result):
     (1) long tight base: the 110-bar window ending 20 bars ago has full range < 30% of its mean;
     (2) volume-expansion breakout: price has cleared the base high AND last-5-bar avg volume
         exceeds 1.5x the base average volume;
     (3) price within 5% of the 250-bar (52-week) high;
     (4) trend structure: EMA20 > EMA50, price > EMA200, EMA20 rising over the last 5 bars.
   Used ONLY to admit such a stock to WATCH when the edge/MC gate would otherwise IGNORE it; it
   can never produce a BUY and never overrides the PROVEN_NEGATIVE invariant. */
function detectFreshBreakout(C, H, L, V){
  const n = C.length;
  if(n < 260) return false;
  const bStart = n - 130, bEnd = n - 20;
  let bHigh = -Infinity, bLow = Infinity, bCloseSum = 0, bVolSum = 0;
  for(let i = bStart; i < bEnd; i++){
    if(H[i] > bHigh) bHigh = H[i];
    if(L[i] < bLow)  bLow  = L[i];
    bCloseSum += C[i]; bVolSum += V[i];
  }
  const bCount = bEnd - bStart;
  const bMean = bCloseSum / bCount, bVolAvg = bVolSum / bCount;
  if(!(bMean > 0) || !(bVolAvg > 0)) return false;
  if(!(((bHigh - bLow) / bMean) < 0.30)) return false;        // (1) tight base
  const cmp = C[n - 1];
  if(!(cmp > bHigh)) return false;                            // (2a) cleared base resistance
  let recentVol = 0; for(let i = n - 5; i < n; i++) recentVol += V[i]; recentVol /= 5;
  if(!(recentVol > 1.5 * bVolAvg)) return false;              // (2b) volume expansion
  let hi250 = -Infinity; for(let i = n - 250; i < n; i++) if(H[i] > hi250) hi250 = H[i];
  if(!(cmp >= 0.95 * hi250)) return false;                    // (3) near 52wk high
  const e20arr = ema(C, 20), e50 = last(ema(C, 50)), e200 = last(ema(C, 200));
  const e20 = e20arr[n - 1], e20prev = e20arr[n - 6];
  if(!(e20 && e50 && e200 && e20prev)) return false;
  if(!(e20 > e50)) return false;                              // (4a)
  if(!(cmp > e200)) return false;                             // (4b)
  if(!(e20 > e20prev)) return false;                          // (4c) EMA20 rising
  return true;
}

/* ── Breakout Debug (v4.37): verbose, READ-ONLY mirror of detectFreshBreakout. Emits every
   criterion value WITHOUT short-circuiting, then calls the REAL detectFreshBreakout for a
   consistency check (the `match` field). detectFreshBreakout itself is UNCHANGED; this is used
   only by GET /breakout/debug for criterion-level visibility. */
function breakoutDebug(C, H, L, V){
  const round = function(x, d){ return (x==null || isNaN(x)) ? null : parseFloat(Number(x).toFixed(d==null?2:d)); };
  const n = C.length;
  const o = { bars: n };
  if(n < 260){
    o.insufficient = true; o.verbose_final = false;
    o.detector_final = detectFreshBreakout(C,H,L,V); o.match = (o.verbose_final === o.detector_final); return o;
  }
  const bStart = n - 130, bEnd = n - 20;
  let bHigh = -Infinity, bLow = Infinity, bCloseSum = 0, bVolSum = 0;
  for(let i = bStart; i < bEnd; i++){ if(H[i]>bHigh)bHigh=H[i]; if(L[i]<bLow)bLow=L[i]; bCloseSum+=C[i]; bVolSum+=V[i]; }
  const bCount = bEnd - bStart, bMean = bCloseSum/bCount, bVolAvg = bVolSum/bCount;
  const cmp = C[n-1];
  o.base_high = round(bHigh); o.base_low = round(bLow); o.base_mean = round(bMean); o.cmp = round(cmp);
  if(!(bMean>0) || !(bVolAvg>0)){
    o.degenerate = true; o.verbose_final = false;
    o.detector_final = detectFreshBreakout(C,H,L,V); o.match = (o.verbose_final === o.detector_final); return o;
  }
  let recentVol = 0; for(let i = n-5; i < n; i++) recentVol += V[i]; recentVol /= 5;
  let hi250 = -Infinity; for(let i = n-250; i < n; i++) if(H[i] > hi250) hi250 = H[i];
  const e20arr = ema(C,20), e50 = last(ema(C,50)), e200 = last(ema(C,200));
  const e20 = e20arr[n-1], e20prev = e20arr[n-6];
  o.base_width_pct          = round((bHigh - bLow)/bMean*100);
  o.breakout_above_base_pct = round((cmp - bHigh)/bHigh*100);
  o.vol_expansion_ratio     = round(recentVol/bVolAvg);
  o.dist_from_250high_pct   = round((hi250 - cmp)/hi250*100);
  o.ema20 = round(e20); o.ema50 = round(e50); o.ema200 = round(e200);
  const c_tight   = (bHigh - bLow)/bMean < 0.30;       // criterion 1: tight base < 30%
  const c_cleared = cmp > bHigh;                       // criterion 2a: cleared base resistance
  const c_vol     = recentVol > 1.5 * bVolAvg;         // criterion 2b: volume expansion > 1.5x
  const c_near    = cmp >= 0.95 * hi250;               // criterion 3: within 5% of 250-bar high
  const guard     = !!(e20 && e50 && e200 && e20prev);
  const c_2050    = guard && (e20 > e50);              // criterion 4a
  const c_p200    = guard && (cmp > e200);             // criterion 4b
  const c_rising  = guard && (e20 > e20prev);          // criterion 4c: EMA20 rising
  o.crit_tight_base       = c_tight;
  o.crit_cleared_base     = c_cleared;
  o.crit_volume_expansion = c_vol;
  o.crit_near_250high     = c_near;
  o.crit_ema20_gt_ema50   = c_2050;
  o.crit_price_gt_ema200  = c_p200;
  o.crit_ema20_rising     = c_rising;
  o.verbose_final  = !!(c_tight && c_cleared && c_vol && c_near && c_2050 && c_p200 && c_rising);
  o.detector_final = detectFreshBreakout(C, H, L, V);  // REAL detector — unchanged
  o.match          = (o.verbose_final === o.detector_final);
  return o;
}

/* ── Monte Carlo (verbatim, live) ── */
function monteCarlo(rMs,runs=QE.MC_RUNS,startEq=QE.CAPITAL){
  if(!rMs||rMs.length<3)return null;
  const losses=rMs.filter(r=>r<0),avgLoss=losses.length?mean(losses):-1,maxLoss=losses.length?Math.min(...losses):-3;
  const fatM=Math.abs(maxLoss/avgLoss)>2?Math.abs(maxLoss/avgLoss):2.5,baseWR=rMs.filter(r=>r>0).length/rMs.length;
  const outcomes=[];
  for(let i=0;i<runs;i++){
    const seq=shuffle(rMs);let eq=startEq,peak=startEq,maxDD=0,str=0,maxStr=0;
    for(let j=0;j<seq.length;j++){
      let rm=seq[j];
      if(rm<0&&Math.random()<0.05)rm=rm*fatM*(1+Math.random());
      if(str>=2&&rm>0&&Math.random()>baseWR*0.85)rm=avgLoss*(0.5+Math.random()*0.5);
      const risk=eq*QE.RISK_PCT;eq+=rm*risk;eq=Math.max(1,eq);
      if(eq>peak)peak=eq;const dd=(peak-eq)/peak*100;if(dd>maxDD)maxDD=dd;
      str=rm<0?str+1:0;maxStr=Math.max(maxStr,str);
    }
    outcomes.push({final:eq,maxDD,maxStreak:maxStr});
  }
  outcomes.sort((a,b)=>a.final-b.final);
  const finals=outcomes.map(o=>o.final),dds=outcomes.map(o=>o.maxDD),strs=outcomes.map(o=>o.maxStreak);
  return{
    runs,isReal:true,worst:Math.round(finals[0]),
    p10:Math.round(ptile(finals,10)),p25:Math.round(ptile(finals,25)),
    p50:Math.round(ptile(finals,50)),p75:Math.round(ptile(finals,75)),
    p90:Math.round(ptile(finals,90)),best:Math.round(finals[finals.length-1]),
    avgDD:parseFloat(safe(mean(dds)).toFixed(1)),
    avgStreak:parseFloat(safe(mean(strs)).toFixed(1)),worstStreak:Math.max(...strs),
    probProfit:parseFloat(safe(finals.filter(f=>f>startEq).length/runs*100).toFixed(1)),
    probRuin:parseFloat(safe(finals.filter(f=>f<startEq*0.5).length/runs*100).toFixed(1))
  };
}

/* ── Pro Filter (verbatim, live) ── */
function applyProFilter(baseScore, params){
  const {
    H, L, C, V,        // daily OHLCV arrays
    wkC, wkEma20,      // weekly close array + weekly EMA20
    moC, moEma20,      // monthly close array + monthly EMA20
    rsiArr,            // full daily RSI array
    adxArr, dipArr,    // full daily ADX, +DI arrays
    e21,               // daily EMA20 (closest to EMA20 available is e21)
    volSMA             // 20-day volume SMA
  } = params;

  const n = C.length;
  if(n < 15) return { adjustedScore:baseScore, baseScore, isRejected:false, bonusApplied:0, reasonCode:[], layers:{} };

  const _safeN = v => (v!=null&&!isNaN(v)&&isFinite(v)) ? v : 0;
  let adjustedScore = baseScore;
  let bonusApplied  = 0;
  const reasonCode  = [];

  // ── LAYER 1: Structural Integrity ──
  const wkN = wkC.length;
  const moN = moC.length;
  const weeklyStructOk  = wkN>0  && wkC[wkN-1]   > (wkEma20||0);
  const monthlyStructOk = moN>0  && moC[moN-1]    > (moEma20||0);
  const structOk = weeklyStructOk && monthlyStructOk;

  const layers = {
    structural:   structOk,
    priceAction:  false,
    momentumSeq:  false,
    volumeBehav:  false,
    antiFailure:  true
  };

  if(!structOk){
    reasonCode.push('STRUCT_FAIL');
    adjustedScore = Math.min(adjustedScore, 35);
    return { adjustedScore, baseScore, isRejected:true, bonusApplied:0, reasonCode, layers };
  }

  // ── LAYER 5: Anti-failure Gates (checked early — hard stops) ──
  // Gate 1: Recent breakdown — close today < close 10 days ago
  const c10ago = n >= 11 ? _safeN(C[n-11]) : 0;
  if(c10ago > 0 && _safeN(C[n-1]) < c10ago){
    reasonCode.push('BREAKDOWN');
    layers.antiFailure = false;
    adjustedScore = Math.min(adjustedScore, 35);
    return { adjustedScore, baseScore, isRejected:true, bonusApplied:0, reasonCode, layers };
  }

  // Gate 2: Extended — 3-tier system (v11)
  // Tier 1 MILD   (1.06-1.10 + ADX>=28): penalty -8pts, NOT rejected
  // Tier 2 WARN   (1.06-1.10 weak trend OR 1.10-1.15 strong): cap 65, NOT rejected
  // Tier 3 SEVERE (>1.15 OR >1.10 weak trend): hard veto
  const ema21val  = _safeN(e21);
  const lastADX   = adxArr && adxArr.length > 0 ? _safeN(adxArr[adxArr.length-1]) : 0;
  const extRatio  = ema21val > 0 ? _safeN(C[n-1]) / ema21val : 0;
  const strongTrend = lastADX >= 28;

  if(ema21val > 0 && extRatio > 1.06){
    layers.antiFailure = false;
    if(extRatio > 1.15 || (extRatio > 1.10 && !strongTrend)){
      // SEVERE — hard veto
      reasonCode.push('EXTENDED_SEVERE');
      adjustedScore = Math.min(adjustedScore, 35);
      return { adjustedScore, baseScore, isRejected:true, bonusApplied:0, reasonCode, layers };
    } else if(extRatio > 1.10 || !strongTrend){
      // WARN — cap score, allow through
      reasonCode.push('EXTENDED_WARN');
      adjustedScore = Math.min(adjustedScore, 65);
    } else {
      // MILD — penalty only, allow through
      reasonCode.push('EXTENDED_MILD');
      adjustedScore = Math.max(0, adjustedScore - 8);
    }
  }

  // ── LAYER 2: Price Behaviour — Higher High + Higher Low ──
  const hh = n>=2 && _safeN(H[n-1]) >= _safeN(H[n-2]);
  const hl = n>=2 && _safeN(L[n-1]) >= _safeN(L[n-2]);
  if(hh && hl){
    bonusApplied += 4;
    layers.priceAction = true;
  } else if(hh || hl){
    bonusApplied += 2;
    layers.priceAction = true;
  }

  // ── LAYER 3: Momentum Sequence ──
  // RSI rising over last 3 bars
  const rsiN = rsiArr ? rsiArr.filter(v=>v!=null) : [];
  const rsiRising = rsiN.length >= 4 &&
    rsiN[rsiN.length-1] > rsiN[rsiN.length-2] &&
    rsiN[rsiN.length-2] > rsiN[rsiN.length-4];

  // ADX rising over last 3 bars
  const adxN = adxArr ? adxArr.filter(v=>v!=null) : [];
  const adxRising3 = adxN.length >= 4 &&
    adxN[adxN.length-1] > adxN[adxN.length-4];

  // +DI rising (today vs yesterday)
  const dipN = dipArr ? dipArr.filter(v=>v!=null) : [];
  const dipRising = dipN.length >= 2 &&
    dipN[dipN.length-1] > dipN[dipN.length-2];

  const momentumCount = (rsiRising?1:0) + (adxRising3?1:0) + (dipRising?1:0);
  if(momentumCount === 3){ bonusApplied += 5; layers.momentumSeq = true; }
  else if(momentumCount === 2){ bonusApplied += 2; layers.momentumSeq = true; }

  // ── LAYER 4: Volume Behaviour — accumulation, not spike ──
  const vN   = V.length;
  const volToday  = _safeN(V[vN-1]);
  const volYest   = _safeN(V[vN-2]);
  const volSMAval = _safeN(volSMA);
  const volExpanding = vN>=2 && volToday > volYest;
  const volNotSpike  = volSMAval > 0 ? volToday < volSMAval * 2.0 : true;
  if(volExpanding && volNotSpike){
    bonusApplied += 3;
    layers.volumeBehav = true;
  }

  // Apply bonus — max 12 pts
  bonusApplied = Math.min(bonusApplied, 12);
  adjustedScore = Math.min(100, baseScore + bonusApplied);

  return {
    adjustedScore,
    baseScore,
    isRejected:  false,
    bonusApplied,
    reasonCode,  // empty = passed all checks
    layers
  };
}

/* ── Elite execution (verbatim, live) ── */
/* ── v4.27: CONFIRMED-EDGE WATCH TIER (mirrors index.html v33 _edgeWatchKind) ──
   Strong validated edge (expectancy>=0.2R, >=5 signals, not MC-vetoed) blocked ONLY by a
   timing overlay (EXTENDED_SEVERE above EMA20, or entryUnreachable >5% above CMP) → WATCH
   instead of IGNORE, so the server gate verdict matches the browser manual scan + load-and-
   analyse. Structural failures (STRUCT_FAIL/BREAKDOWN) never qualify. WATCH is still NOT a
   gate pass (out.pass keys on label==='BUY'), so this changes ZERO BUY signals / Telegram.
   Returns 'EXTENDED' | 'ENTRY' | null. Kill switch: QE_EDGE_WATCH=false. */
const QE_EDGE_WATCH = true;
function _edgeWatchKind(stock){
  if(!QE_EDGE_WATCH || !stock) return null;
  const bt = stock.bt;
  if(!bt || typeof bt.expectancy !== 'number') return null;
  if(bt.expectancy < 0.2 || (bt.total||0) < 5) return null;
  const mcProb = stock.mc ? stock.mc.probProfit : null;
  if(mcProb === 0 && (bt.total||0) >= 15) return null;
  const rc = (stock._proFilter && stock._proFilter.reasonCode) || [];
  const extendedOnly = stock.isRejected === true && rc.length > 0 && rc.every(function(r){ return /^EXTENDED/.test(r); });
  if(extendedOnly) return 'EXTENDED';
  if(stock.isRejected !== true && stock.entryUnreachable === true) return 'ENTRY';
  return null;
}

function computeExecutionDecision(stock){
  const isRejected = stock.isRejected === true;
  const base       = stock.baseScore;
  const pro        = stock.proAdjustedScore;
  // FIX 1: Read live finalDecision() label — never the cached stock.verdict field.
  // stock.verdict is a mutable field that may be stale or missing; finalDecision()
  // is the sole authoritative decision path.
  const fdLabel    = finalDecision(stock).label;

  // v4.27: Edge-WATCH — strong confirmed edge blocked only by a timing overlay → WAIT (not SKIP)
  const _ewk = _edgeWatchKind(stock);
  if(_ewk === 'EXTENDED')
    return { action:'WAIT', allocation:0, confidence:'MEDIUM', reason:'Extended above EMA20 — strong edge, wait for pullback' };
  if(_ewk === 'ENTRY')
    return { action:'WAIT', allocation:0, confidence:'MEDIUM', reason:'Entry above reach — strong edge, wait for pullback' };

  if(isRejected){
    const reasonCode = (Array.isArray(stock._proFilter && stock._proFilter.reasonCode)
      ? stock._proFilter.reasonCode.join(', ')
      : (stock._proFilter && stock._proFilter.reasonCode)) || "Filtered";
    return { action:'SKIP', allocation:0, confidence:'LOW', reason: reasonCode };
  }

  if(fdLabel === 'BUY' && base >= 70 && pro >= 60){
    return { action:'ENTER', allocation:100, confidence:'HIGH',
             reason:'Early momentum + strong trend' };
  }

  if(fdLabel === 'BUY' && base >= 60 && pro < 60){
    return { action:'WAIT', allocation:50, confidence:'MEDIUM',
             reason:'Strong trend but extended or late entry' };
  }

  if(fdLabel === 'BUY' && base < 60){
    return { action:'WAIT', allocation:0, confidence:'LOW',
             reason:'Momentum not strong enough' };
  }

  return { action:'SKIP', allocation:0, confidence:'LOW', reason:'No valid setup' };
}

/* ── finalDecision (verbatim, live, Rules 1-5 incl 4a/4b/4c rescue) ── */
function _fdIgnore(reason){ return { label:'IGNORE', reason: reason }; }
function finalDecision(stock){
  if(!stock) return { label:'IGNORE' };
  const mcProb = stock.mc ? stock.mc.probProfit : null;
  const mcZero = mcProb === 0;
  const mcWeak = mcProb !== null && mcProb > 0 && mcProb < 30;
  const hasStrongExp = stock.bt && stock.bt.expectancy >= 0.2;
  // F2 (v4.30): SYMMETRIC MC veto — mirror of index.html v38. Removes the mcZero-only
  // small-sample rescue so the veto is monotonic in MC: any MC<30% vetoes unless exp>=0.2R.
  const mcVeto = (mcZero || mcWeak) && !hasStrongExp;
  // Commit C (v4.33): confidence-aware expectancy gate — mirror of index.html v40. PROVEN-negative
  // (or unknown class) still hard-IGNORE'd; INDETERMINATE negative routed to WATCH at Rule 3c.
  const _expNeg    = stock.bt && stock.bt.expectancy < 0;
  const _indetNeg  = _expNeg && stock.bt.edgeClass === 'INDETERMINATE';
  const _provenNeg = _expNeg && !_indetNeg;
  // Commit D (v4.34): proven-negative absolute IGNORE (invariant); MC veto IGNORE unless a
  // fresh-breakout (all criteria, score>=60, reachable) → WATCH (never BUY). Mirror of index.html v41.
  if(_provenNeg) return _fdIgnore('NO_EDGE');
  if(mcVeto){
    const _fbScore = (_proFilterMode && stock.isRejected && stock.baseScore !== undefined) ? stock.baseScore : (stock.score || 0);
    if(stock.freshBreakout === true && _fbScore >= 60 && !stock.entryUnreachable) return { label:'WAIT', reason:'WATCH_FRESH_BREAKOUT' };
    return _fdIgnore('NO_EDGE');
  }
  if(stock.entryUnreachable){
    if(_edgeWatchKind(stock) === 'ENTRY') return { label:'WAIT', reason:'WATCH_EDGE_ENTRY' };
    return _fdIgnore('ENTRY_UNREACHABLE');
  }
  const _rule3Score = (_proFilterMode && stock.isRejected && stock.baseScore !== undefined)
    ? stock.baseScore : (stock.score || 0);
  if(_rule3Score < 60) return _fdIgnore('LOW_SCORE');
  if(stock.bt && stock.bt.total < 5) return _fdIgnore('INSUFFICIENT_DATA');
  if(_indetNeg) return { label:'WAIT', reason:'WATCH_INDETERMINATE' };
  if(_eliteMode && stock.execution && stock.execution.action === 'SKIP' && !stock.isRejected){
    const baseScore = stock.baseScore !== undefined ? stock.baseScore : (stock.score || 0);
    const edgeOK = stock.bt && stock.bt.expectancy > 0 && stock.bt.winRate >= 40;
    if(edgeOK && baseScore >= 80) return { label:'BUY', reason:'ELITE_SKIP_OVERRIDE_BUY' };
    if(edgeOK && baseScore >= 60) return { label:'WAIT', reason:'ELITE_SKIP_DOWNGRADE_WAIT' };
    return _fdIgnore('ELITE_SKIP');
  }
  if(stock.isRejected){
    if(_edgeWatchKind(stock) === 'EXTENDED') return { label:'WAIT', reason:'WATCH_EDGE_EXTENDED' };
    return _fdIgnore('PRO_FILTER_REJECTED');
  }
  return { label:'BUY', reason:'All criteria met' };
}

/* ── v4.30: RS adjustment — ported VERBATIM from index.html applyRSAdjustment for
   cron↔browser score parity. Bumps the pro score by the same RS rule the browser uses,
   so the cron gate score matches the card's displayed score (e.g. ANGELONE 79 → 87). ── */
function applyRSAdjustment(proScore, rsScore){
  if(rsScore === null || rsScore === undefined) return proScore;
  if(rsScore > 5)  return Math.min(100, proScore + 8);   // RS STRONG bonus
  if(rsScore < -5) return Math.max(0,   proScore - 12);  // RS WEAK penalty
  return proScore;                                        // NEUTRAL — no change
}

/* ── evaluate(): full analyseStock chain (score → proFilter → execution → finalDecision) ── */
function evaluate(rawCandles, regimeStr, rsScore){
  try {
    const data = rawCandles.map(function(c){
      const t = (typeof c[0]==='string') ? Math.floor(Date.parse(c[0])/1000) : c[0];
      return { t:t, o:c[1], h:c[2], l:c[3], c:c[4], v:c[5] };
    });
    const C=data.map(d=>d.c),H=data.map(d=>d.h),L=data.map(d=>d.l),V=data.map(d=>d.v);
    const n=C.length; if(n < QE.MIN_BARS+10) return { pass:false, reason:'INSUFFICIENT_DATA', score:0 };
    const cmp=C[n-1];

    /* score block — analyseStock verbatim */
    const e8=last(ema(C,8)),e21=last(ema(C,21)),e50=last(ema(C,50)),e100=last(ema(C,100)),e200=last(ema(C,200));
    const emaAligned=e8>e21&&e21>e50&&e50>e100&&e100>e200;
    const emaScore=(emaAligned?.5:0)+(cmp>e200?.2:0)+(cmp>e50?.15:0)+(cmp>e8?.15:0);
    const rsiD=last(rsi(C,14));
    const wk=aggregate(data,'weekly'),rsiW=last(rsi(wk.map(d=>d.c),14));
    const mo=aggregate(data,'monthly'),rsiM=last(rsi(mo.map(d=>d.c),14));
    const _rD=rsiD||50,_rW=rsiW||50,_rM=rsiM||50;
    const rsiScore=(_rD>=60&&_rD<=80?.5:_rD>=55?.3:_rD>=50?.1:0)+(_rW>=55?.25:_rW>=50?.1:0)+(_rM>=55?.25:_rM>=50?.1:0);
    const mc2=macdCalc(C);const macdBull=last(mc2.line)>last(mc2.signal);
    const momentumScore=(macdBull?.5:0)+(last(mc2.hist)>0?.2:0)+rsiScore*0.3;
    const atrA=atr(H,L,C,14);
    const rATR=mean(atrA.slice(-5).filter(v=>v)),lATR=mean(atrA.slice(-20).filter(v=>v));
    const rangeCoil=rATR>0&&lATR>0&&rATR<lATR*0.8;
    const _ac=adxCalc(H,L,C,14);
    const adxV=last(_ac.adx)||0,diP=last(_ac.dip)||0,diM=last(_ac.dim)||0;
    const adxRising=adxV>(_ac.adx.filter(v=>v!=null).slice(-2)[0]||0);
    const strengthScore=(adxV>25?.4:adxV>20?.2:0)+(diP>diM?.3:0)+(adxRising?.3:0);
    const volSMA=last(sma(V,20))||0,curVol=V[n-1],volRatio=volSMA>0?curVol/volSMA:0;
    const volumeScore=volRatio>=2?1:volRatio>=1.5?.7:volRatio>=1.2?.4:volRatio>=1?.2:0;
    const h5=Math.max(...H.slice(-5)),h120=Math.max(...H.slice(-120));
    const bo120=h5>h120*1.05,h52=Math.max(...H),near52=cmp>=h52*0.75;
    const stD=supertrend(H,L,C,10,2),stW=supertrend(wk.map(d=>d.h),wk.map(d=>d.l),wk.map(d=>d.c),10,2),stM=supertrend(mo.map(d=>d.h),mo.map(d=>d.l),mo.map(d=>d.c),10,2);
    const stBuy=stD[n-1]===1&&stW[stW.length-1]===1&&stM[stM.length-1]===1;
    const breakoutScore=(bo120?.35:0)+(cmp>Math.max(...H.slice(-22))?.20:0)+(stBuy?.25:stD[n-1]===1?.10:0)+(near52?.10:0)+(rangeCoil?.10:0);
    let raw=(emaScore*WEIGHTS.trend+volumeScore*WEIGHTS.volume+breakoutScore*WEIGHTS.breakout+momentumScore*WEIGHTS.momentum+strengthScore*WEIGHTS.strength)*100;
    if(regimeStr==='bull')raw=Math.min(100,raw*1.08);else if(regimeStr==='bear')raw=raw*0.82;
    const techScore=isNaN(raw)?0:Math.round(Math.min(100,Math.max(0,raw)));
    let score=techScore;

    const entryPrice=Math.max(...H.slice(-5))*1.005;
    const entryGapPct=safe((entryPrice-cmp)/cmp*100);
    const entryUnreachable=entryGapPct>5;
    if(entryUnreachable&&score>=70) score=Math.min(score,69);

    // GOLDEN FORMULA (16-Jul-2026): stop/targets, matching the browser's own scan
    // formula exactly (same swing-low/ATR stop, same 2R/3R targets) — evaluate()
    // previously only computed entryPrice, never the full trade levels.
    const swingLow=Math.min(...L.slice(-10));
    const _atrVNow=last(atr(H,L,C,14))||0;
    const rawStopNow=Math.max(swingLow,_atrVNow>0?cmp-1.5*_atrVNow:cmp*0.95);
    const stopLoss=rawStopNow<entryPrice?rawStopNow:entryPrice*0.92;
    const riskNow=Math.max(entryPrice-stopLoss,entryPrice*0.01);
    const t1Now=entryPrice+2*riskNow,t2Now=entryPrice+3*riskNow;

    /* backtest + MC */
    const btTrades=stockBacktest(data,'GATE');
    const btSt=tradeStats(btTrades);
    const btEC_maxDD=0;
    const _btEdgeConf=edgeConfidence(btSt.expectancy,btTrades.map(t=>t.rMultiple)); // Commit A: report-only, no verdict uses these
    const bt={total:btSt.total,wins:btSt.wins,winRate:btSt.winRate,expectancy:btSt.expectancy,
              expSE:_btEdgeConf.expSE,edgeClass:_btEdgeConf.edgeClass};
    const mcRes=btTrades.length>=5?monteCarlo(btTrades.map(t=>t.rMultiple)):null;

    /* backtest score-adjust — analyseStock verbatim */
    if(bt.total>=5){
      const exp=bt.expectancy||0, wr=bt.winRate||0;
      if(exp<0){ const penalty=Math.min(25,Math.round(Math.abs(exp)*15)); score=Math.max(0,techScore-penalty); if(exp<-0.5&&wr<45) score=Math.min(score,45); }
      else if(exp>0){ const bonus=Math.min(12,Math.round(exp*8)); score=Math.min(100,techScore+bonus); }
      score=Math.round(score);
    }

    /* PRO FILTER (always runs in app; here PRO is ON so it drives final score) */
    const baseScore=score;
    const _rsiFullArr=rsi(C,14);
    const {adx:_adxFull,dip:_dipFull}=adxCalc(H,L,C,14);
    const _wkCloses=wk.map(d=>d.c), _wkEma20=_wkCloses.length>=20?last(ema(_wkCloses,20)):null;
    const _moCloses=mo.map(d=>d.c), _moEma20=_moCloses.length>=20?last(ema(_moCloses,20)):null;
    const pf=applyProFilter(baseScore,{H,L,C,V,wkC:_wkCloses,wkEma20:_wkEma20,moC:_moCloses,moEma20:_moEma20,rsiArr:_rsiFullArr,adxArr:_adxFull,dipArr:_dipFull,e21,volSMA});
    let proAdjustedScore=pf.adjustedScore;
    // v4.30: RS parity — apply the browser's applyRSAdjustment so the cron gate score matches
    // the card's displayed score. Safe w.r.t. the verdict: computeExecutionDecision's SKIP
    // status is independent of the RS bump (SKIP only on isRejected / non-BUY fdLabel), so
    // finalDecision is unchanged; only the numeric score + the >=70 alert gate shift.
    if(_proFilterMode) proAdjustedScore=applyRSAdjustment(proAdjustedScore, rsScore);
    score=_proFilterMode?proAdjustedScore:baseScore;   // PRO ON (RS-adjusted, browser-parity)

    /* assemble stock object for execution + finalDecision (mirrors analyseStock) */
    const freshBreakout=detectFreshBreakout(C,H,L,V);   // Commit D: structural fresh-breakout flag (WATCH-only, never BUY)
    const stock={ score:score, baseScore:baseScore, proAdjustedScore:proAdjustedScore,
      isRejected:pf.isRejected, _proFilter:pf, entryUnreachable:entryUnreachable,
      bt:{total:bt.total, winRate:bt.winRate, expectancy:bt.expectancy, edgeClass:bt.edgeClass},
      freshBreakout:freshBreakout,
      mc:mcRes ? { probProfit:mcRes.probProfit } : null };
    stock.execution=computeExecutionDecision(stock);   // ELITE ON
    const fd=finalDecision(stock);

    const out={ score:score, baseScore:baseScore, proScore:proAdjustedScore,
      wr:bt.winRate, ev:bt.expectancy, mcProb:mcRes?mcRes.probProfit:null, btTotal:bt.total,
      expSE:bt.expSE, edgeClass:bt.edgeClass, freshBreakout:freshBreakout,
      elite:stock.execution.action, isRejected:pf.isRejected,
      entry:parseFloat(entryPrice.toFixed(2)), sl:parseFloat(stopLoss.toFixed(2)),
      t1:parseFloat(t1Now.toFixed(2)), t2:parseFloat(t2Now.toFixed(2)), cmp:parseFloat(cmp.toFixed(2)),
      proReason:(pf.reasonCode&&pf.reasonCode.length)?pf.reasonCode.join(','):'', label:fd.label, fdReason:fd.reason };
    // Gate PASS = app would show BUY (the only actionable, tradeable verdict)
    out.pass = (fd.label === 'BUY');
    if(!out.pass) out.reason = fd.reason || fd.label;
    return out;
  } catch(e){
    return { pass:true, reason:'GATE_ERROR', error:(e&&e.message) }; // fail-open
  }
}
/* ── evaluateDiff(): READ-ONLY layer differential (v4.39, for GET /diff/layers). ──────────────
   Reuses evaluate() UNCHANGED to get the production stock + verdict, reconstructs the stock object
   from evaluate's OWN output, asserts finalDecision(recon)===production label (fidelity self-check),
   then re-runs finalDecision on CLONES with ONE layer neutralized at a time. Touches NO production
   state: no const reassignment (_proFilterMode/_eliteMode stay true), no writes, no Telegram. Each
   layer is toggled purely via stock-object fields finalDecision already keys on:
     • MC-veto OFF    → mc=null                       (mcVeto cannot fire)
     • Pro Filter OFF → score=baseScore, isRejected=false (every _proFilterMode&& branch is gated by isRejected)
     • Elite OFF      → execution.action='ENTER'       (≠SKIP → Rule 4 skipped, identical to _eliteMode=false) */
function evaluateDiff(rawCandles, regimeStr, rsScore){
  const out = evaluate(rawCandles, regimeStr, rsScore);
  if(!out || out.label === undefined) return { ok:false, reason:(out && out.reason) || 'NO_VERDICT' };
  // entryUnreachable is the one finalDecision input not present in out — recompute it VERBATIM from evaluate.
  const _H = rawCandles.map(function(c){ return c[2]; });
  const _C = rawCandles.map(function(c){ return c[4]; });
  const _cmp = _C[_C.length - 1];
  const _entryPrice = Math.max.apply(null, _H.slice(-5)) * 1.005;
  const _entryGapPct = safe((_entryPrice - _cmp) / _cmp * 100);
  const entryUnreachable = _entryGapPct > 5;
  // Reconstruct exactly the fields finalDecision()/_edgeWatchKind() read.
  const stock = {
    score: out.score, baseScore: out.baseScore, isRejected: out.isRejected,
    entryUnreachable: entryUnreachable,
    bt: { total: out.btTotal, winRate: out.wr, expectancy: out.ev, edgeClass: out.edgeClass },
    mc: (out.mcProb !== null && out.mcProb !== undefined) ? { probProfit: out.mcProb } : null,
    freshBreakout: out.freshBreakout,
    execution: { action: out.elite },
    _proFilter: { reasonCode: out.proReason ? out.proReason.split(',') : [] }
  };
  const base = finalDecision(stock).label;
  const reconOK = (base === out.label);   // fidelity: reconstructed verdict must equal production verdict
  function clone(o){
    return { score:o.score, baseScore:o.baseScore, isRejected:o.isRejected, entryUnreachable:o.entryUnreachable,
      bt:{ total:o.bt.total, winRate:o.bt.winRate, expectancy:o.bt.expectancy, edgeClass:o.bt.edgeClass },
      mc:o.mc ? { probProfit:o.mc.probProfit } : null, freshBreakout:o.freshBreakout,
      execution:{ action:o.execution.action }, _proFilter:{ reasonCode:o._proFilter.reasonCode.slice() } };
  }
  const sMC = clone(stock);  sMC.mc = null;                                   // MC-veto OFF
  const noMC = finalDecision(sMC).label;
  const sPro = clone(stock); sPro.score = sPro.baseScore; sPro.isRejected = false;  // Pro Filter OFF
  const noPro = finalDecision(sPro).label;
  const sEl = clone(stock);  sEl.execution.action = 'ENTER';                  // Elite OFF
  const noElite = finalDecision(sEl).label;
  return { ok:true, reconOK:reconOK, base:base, prodLabel:out.label,
    noMC:noMC, noPro:noPro, noElite:noElite,
    ev:out.ev, mcProb:out.mcProb, edgeClass:out.edgeClass, score:out.score, baseScore:out.baseScore,
    btTotal:out.btTotal, isRejected:out.isRejected, freshBreakout:out.freshBreakout, elite:out.elite };
}

return { evaluate: evaluate, evaluateDiff: evaluateDiff, breakoutDebug: breakoutDebug, stockBacktest: stockBacktest, tradeStats: tradeStats, edgeConfidence: edgeConfidence };
})();

// ── Step C (v4.57): SINGLE SOURCE OF TRUTH for a run's breakout scan-mode ────
// scanModeOf() derives the mode from the run label; scanModeTag() maps a mode to its
// Telegram header tag. Both the consolidated signal tag AND qe_forward_track.scan_mode
// draw from these — no scattered literals. Legacy runs (09:30/12:00/14:30 discovery,
// MANUAL, HTTP) -> "discovery". scanModeTag("discovery") -> "" so their Telegram output
// is byte-identical to pre-v4.57 (the old null path produced no tag).
function scanModeOf(label){
  if (label && label.indexOf("post-close confirmed") !== -1) return "confirmed";
  if (label && label.indexOf("intraday forming")     !== -1) return "forming";
  return "discovery";
}
function scanModeTag(mode){
  if (mode === "confirmed") return `🟢 <b>DAY-CLOSE BREAKOUT · confirmed</b>\n`;
  if (mode === "forming")   return `🟡 <b>INTRADAY BREAKOUT · forming</b>\n`;
  return "";
}

// ── Q1(B) (v4.58): forming-bar VOLUME-PACE projection (KV-gated FORMING_VOL_PACE, default OFF) ──
// In forming mode today's last bar holds only PARTIAL session volume, deflating the gate's volume
// sub-score. When enabled, scale ONLY the last bar's volume up by the inverse elapsed-session fraction
// (NSE 09:15-15:30 IST = 375 min) to estimate the full-day figure, so the gate scores on projected
// volume. Returns a shallow-cloned candle array with only the last bar's volume changed — never mutates
// the input; never touches displayed CMP/volume (those come from ohlcvMap). Guards: last bar only, only
// if it is TODAY (IST), only forming mode, only flag on, only scale UP (frac in (0.05,1)); else returns
// the input unchanged. Simplification: linear time-pace (intraday volume is U-shaped — refine later only
// if measured edge warrants). The protected QEGate.evaluate is unchanged; only its INPUT is adjusted.
function projectFormingVolume(candles, scanMode, volPaceOn){
  if (!volPaceOn || scanMode !== "forming" || !candles || candles.length < 1) return candles;
  const last = candles[candles.length - 1];
  if (!last) return candles;
  const barDate  = String(last[0]).slice(0, 10);
  const istNow   = new Date(Date.now() + (5*60+30)*60000);
  const istToday = istNow.toISOString().slice(0, 10);
  if (barDate !== istToday) return candles;
  const istMins = istNow.getUTCHours()*60 + istNow.getUTCMinutes();
  const OPEN = 9*60+15, SPAN = (15*60+30) - (9*60+15);
  const frac = (istMins - OPEN) / SPAN;
  if (!(frac > 0.05 && frac < 1)) return candles;
  const out = candles.slice();
  const nb  = last.slice();
  const V = 5; // candle = [t,o,h,l,c,v]
  if (nb.length > V && typeof nb[V] === "number") nb[V] = Math.round(nb[V] / frac);
  out[out.length - 1] = nb;
  return out;
}

async function pipeDispatchTelegram(env, candidates, audit, scanMode) {
  if (!candidates || candidates.length === 0) {
    audit.log("S9_TELEGRAM", "", "SKIP", "No candidates to dispatch");
    return 0;
  }

  const top    = candidates.slice(0, PIPE_SIGNAL_TOP);
  const expiry = Date.now() + SIGNAL_TTL_MS;

  // Commit 3: build ONE consolidated message + ONE inline keyboard.
  // KV signal storage per candidate is retained (KV puts are NOT subrequests),
  // so callback verification still works for each signal individually. Only the
  // outbound Telegram fetch collapses from N → 1.
  const expiryStr = new Date(expiry).toLocaleTimeString("en-IN",
    { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" });

  const sections    = [];
  const keyboardRows = [];
  let   eligible    = 0;

  for (let i = 0; i < top.length; i++) {
    const c = top[i];

    // ── v4.30: UNIFIED alert gate — one rule on ALL paths (manual scan, load&analyse, cron,
    // /pipe/trigger): dispatch BUY iff the QE verdict is BUY AND the RS-adjusted QE score >= 70.
    // Replaces the old DS/ST/ADX integrity gate, which blocked genuine high-conviction BUYs
    // (e.g. ANGELONE — QE verdict BUY, score 87, but ADX 13 < 18 → was wrongly WATCH_ONLY).
    // Every candidate here is already a QE BUY-passer (qeDispatchList), so this keys on score>=70.
    const qePass    = !!(c.qe && c.qe.pass);
    const qeScore   = (c.qe && c.qe.qeScore != null) ? c.qe.qeScore : 0;
    const gatePass  = qePass && qeScore >= 70;
    const watchOnly = !gatePass;

    if (watchOnly) {
      const failReason = !qePass ? ("QE verdict " + ((c.qe && c.qe.label) || "—") + " (not BUY)")
                                 : ("QE score " + qeScore + " < 70");
      audit.log("S9_TELEGRAM", c.symbol, "GATE_FAIL",
        "Unified alert gate failed: " + failReason + " — dispatching WATCH_ONLY");
    } else {
      eligible++;
      audit.log("S9_TELEGRAM", c.symbol, "GATE_PASS",
        "QE BUY · score " + qeScore + " >= 70");
    }

    const signalId = genRunId();
    const hmac     = await signPayload(env, signalId, c.symbol, c.entry, expiry);

    // Store signal in KV for callback verification (per-signal, unchanged)
    try {
      await env.KITE_STORE.put(
        "qe_signal_" + signalId,
        JSON.stringify({
          signalId:  signalId,
          symbol:    c.symbol,
          entry:     c.entry,
          sl:        c.sl,
          t1:        c.t1,
          t2:        c.t2,
          qty:       1,
          cmp:       c.lastClose,
          expiry:    expiry,
          hmac:      hmac,
          source:    "pipeline_v4",
          watchOnly: watchOnly,
        }),
        { expirationTtl: 32400 }
      );
    } catch (e) { console.warn("[pipeDispatchTelegram] non-fatal:", e && e.message); }

    // ── Per-signal section (same content/formatting as before) ────────────────
    const gateStatusLine = watchOnly
      ? "⚠️ Gate: WATCH_ONLY (deep analysis required before BUY)\n"
      : "✅ Gate: DS✓ Supertrend✓ ADX✓\n";

    sections.push(
      `${watchOnly ? "⚠️" : "🔭"} <b>#${i + 1} ${c.symbol}</b>  [${c.sector}]\n`
      + `📊 Discovery Score: <b>${c.discoveryScore}/100</b>   📈 RS: <b>${c.rsScore}/100</b>\n`
      + (c.qe && c.qe.qeScore != null
          ? `🧠 QE: <b>${c.qe.qeScore}/100</b> (base ${c.qe.baseScore != null ? c.qe.baseScore : "—"}/pro ${c.qe.proScore != null ? c.qe.proScore : "—"}) · Elite ${c.qe.elite || "—"}\n`
            + `📐 WR ${c.qe.wr != null ? c.qe.wr : "—"}% · EV ${c.qe.ev != null ? c.qe.ev : "—"}R · MC ${c.qe.mcProb != null ? c.qe.mcProb : "—"}% · BT ${c.qe.btTotal != null ? c.qe.btTotal : "—"} · ${c.qe.basis || "1y"}\n`
          : "")
      + `💹 RSI: ${c.rsi14 !== null ? c.rsi14 : "—"}  ADX: ${c.adx14 !== null ? c.adx14 : "—"}  ST: ${c.stBull ? "🟢 Bull" : "🔴 Bear"}\n`
      + gateStatusLine
      + `💰 CMP: ₹${c.lastClose}   🎯 Entry: ₹${c.entry}  SL: ₹${c.sl !== null ? c.sl : "—"}\n`
      + `✅ T1: ₹${c.t1 !== null ? c.t1 : "—"}  T2: ₹${c.t2 !== null ? c.t2 : "—"}`
    );

    // Per-signal callback buttons (Watch / Skip), labelled with the symbol.
    // Telegram hard-limits callback_data to 64 bytes/button. The full trade
    // payload was ~175 bytes → Telegram rejected the whole message (send FAIL,
    // signalCount 0). Carry ONLY {a:action, s:signalId} (~32 bytes); the callback
    // handler reads the full signal back from KV (qe_signal_<signalId>, written above).
    keyboardRows.push([
      { text: "👀 " + c.symbol, callback_data: JSON.stringify({ a: "WATCH",  s: signalId }) },
      { text: "❌ Skip",        callback_data: JSON.stringify({ a: "REJECT", s: signalId }) },
    ]);
  }

  // ── Single consolidated message ─────────────────────────────────────────────
  const modeTag = scanModeTag(scanMode); // Step C (v4.57): single source of truth
  const header = modeTag
    + `🔭 <b>QuantEdge Signals</b> — ${eligible}/${top.length} gate-passed\n`
    + `⏱ Expires ${expiryStr} IST · open QuantEdge for deep analysis\n`
    + `━━━━━━━━━━━━━━━━━━━━\n`;
  const body   = sections.join("\n\n━━━━━━━━━━━━━━━━━━━━\n");
  const footer = `\n\n<i>Source: Server Pipeline v${QE_VERSION}</i>`;
  const msg    = header + body + footer;

  const keyboard = { inline_keyboard: keyboardRows };

  const ok = await sendTelegram(env, msg, keyboard);
  const sent = ok ? top.length : 0;
  audit.log("S9_TELEGRAM", "", "DONE",
    "Consolidated send " + (ok ? "OK" : "FAIL") + " — " + top.length + " signals in 1 message");
  return sent;
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIX 3: PIPELINE REGIME SNAPSHOT
// Derives a structural market regime from Nifty 50 daily closes.
// Mirrors the browser's fetchRegime() scoring logic (bull/sideways/bear).
// This snapshot is embedded in every KV signal so the browser uses the
// pipeline-time regime for deep analysis — not the browser's current regime.
//
// Returns: { regime: 'bull'|'sideways'|'bear', bullScore: N, ts: ISO }
// Falls back to 'sideways' if niftyCloses is unavailable.
// ═══════════════════════════════════════════════════════════════════════════════
function computePipelineRegime(niftyCloses, audit) {
  const fallback = { regime: "sideways", bullScore: 0, ts: new Date().toISOString(), source: "fallback" };

  if (!niftyCloses || niftyCloses.length < 50) {
    audit.log("REGIME_SNAP", "", "FALLBACK", "Insufficient Nifty bars — using sideways");
    return fallback;
  }

  try {
    const c   = niftyCloses;
    const n   = c.length;
    const cmp = c[n - 1];

    // EMA helpers (Wilder/standard)
    function emaLast(arr, period) {
      if (arr.length < period) return null;
      const k = 2 / (period + 1);
      let e = arr.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
      for (let i = period; i < arr.length; i++) { e = arr[i] * k + e * (1 - k); }
      return e;
    }
    function meanArr(arr) {
      return arr.reduce(function(a, b) { return a + b; }, 0) / arr.length;
    }

    const e50  = emaLast(c, 50);
    const e200 = emaLast(c, Math.min(200, c.length));

    // Use available data for trend metrics
    const recent20 = c.slice(-20);
    const prior40  = c.slice(-60, -20);
    const recentMean = meanArr(recent20);
    const priorMean  = prior40.length > 0 ? meanArr(prior40) : recentMean;
    const trend      = recentMean - priorMean;
    const mom5       = c[n - 1] - c[Math.max(0, n - 6)];

    let upDays = 0;
    for (let i = Math.max(1, n - 20); i < n; i++) { if (c[i] > c[i - 1]) upDays++; }
    const breadth = upDays / 20;

    // Bull score (mirrors browser logic)
    let bull = 0;
    if (e50 !== null   && cmp > e50)      bull += 2;
    if (e50 !== null   && e200 !== null && e50 > e200) bull += 2;
    if (trend > 0)     bull += 1;
    if (mom5 > 0)      bull += 1;
    if (breadth > 0.55) bull += 1;
    if (e200 !== null  && cmp > e200)     bull += 1;

    const regime = bull >= 5 ? "bull" : bull <= 2 ? "bear" : "sideways";

    return {
      regime:    regime,
      bullScore: bull,
      ts:        new Date().toISOString(),
      source:    "nifty_computed",
      niftyBars: n,
      cmp:       parseFloat(cmp.toFixed(2)),
      e50:       e50 !== null ? parseFloat(e50.toFixed(2)) : null,
      e200:      e200 !== null ? parseFloat(e200.toFixed(2)) : null,
    };
  } catch (e) {
    audit.log("REGIME_SNAP", "", "ERROR", "Regime computation failed: " + e.message);
    return fallback;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE WRAPPER — intraday re-scan crons (0 6, 0 9)
// Runs the full pipeline then sends a structured Telegram summary with
// bottleneck detection. The 09:30 cron (0 4) calls runFullPipeline directly
// and already has per-stage Telegram alerts; these re-scan runs need their
// own post-completion summary because they fire without a human watching.
// ═══════════════════════════════════════════════════════════════════════════════
// ── Fix 1: Token health guard (Stage 0) ──────────────────────────────────────
// One RELIANCE quote proves the token is alive BEFORE the pipeline spends a cycle
// on empty data. Every probe is written to the token_health black-box table. If the
// token is dead DURING market hours, alert Telegram; the caller aborts the run.
// Purely additive: touches no scoring, verdict, ranking, MC, gate, or detector logic.
async function tokenHealthCheck(env, token, label) {
  const ANCHOR = "RELIANCE";
  const nowUtc = new Date().toISOString();
  const istLbl = istStamp(Date.now());
  let alive = 0, price = null, httpStatus = null, detail = "";
  try {
    const q = await kiteRequest("GET", `/quote?i=NSE:${ANCHOR}`, null, token);
    httpStatus = q.status || null;
    const rec = q.ok && q.data && q.data.data && q.data.data["NSE:" + ANCHOR];
    if (q.ok && rec && typeof rec.last_price === "number" && rec.last_price > 0) {
      alive = 1; price = rec.last_price; detail = "OK";
    } else {
      detail = (q.data && q.data.message) || ("unexpected quote shape (status " + httpStatus + ")");
    }
  } catch (e) {
    detail = (e && e.message) || "exception";
  }
  // Black-box write — must never throw into the pipeline.
  try {
    await env.QE_DB.prepare(
      "INSERT INTO token_health (checked_at_utc, ist_label, trigger_src, alive, anchor, anchor_price, http_status, detail) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
    ).bind(nowUtc, istLbl, label || "unknown", alive, ANCHOR, price, httpStatus, String(detail).slice(0, 300)).run();
  } catch (e) { console.warn("[tokenHealthCheck] log write failed: " + (e && e.message)); }

  if (!alive) {
    // Only nag during live market hours (Mon–Fri 09:15–15:30 IST). Off-hours a dead
    // token is expected (daily login) — a loud alert then would just be noise.
    const istD   = new Date(Date.now() + (5 * 60 + 30) * 60000);
    const istMin = istD.getUTCHours() * 60 + istD.getUTCMinutes();
    const dow    = istD.getUTCDay(); // 0=Sun .. 6=Sat
    const inMarketHours = dow >= 1 && dow <= 5 && istMin >= (9 * 60 + 15) && istMin <= (15 * 60 + 30);
    if (inMarketHours) {
      const kb = { inline_keyboard: [[{ text: "🔄 Login & Refresh", url: WORKER_LOGIN_URL }]] };
      await sendTelegram(env,
        "⚠️ <b>Token expired — log in</b>\nThe " + (label || "scheduled") +
        " scan was aborted: Kite token is dead (" + String(detail).slice(0, 120) + ").\nTap to refresh:", kb);
    }
  }
  return { alive: alive === 1, price: price, httpStatus: httpStatus, detail: detail };
}

async function runPipelineWithSummary(env, label) {
  // ── DIAGNOSTIC HEARTBEAT (unconditional) ───────────────────────────────────
  // Proves the wrapper was entered at all. If this arrives but the summary does
  // not, the fault is between here and sendPipelineSummary. If this does NOT
  // arrive, the cron is not invoking the wrapper. Either way the next run tells
  // us the truth instead of failing silently.
  const hbOk = await sendTelegram(env, `🫀 <b>Re-scan started — ${label}</b>`);

  let result;
  try {
    // Manual triggers skip dedup so an after-hours re-run shows the SAME true
    // top candidates every time (data is frozen post-close). Crons keep dedup
    // to spread coverage across the day. Evidence: audit 811de142 showed manual
    // re-runs returning different candidates due to the analysed-today exclusion.
    const isManual = !!(label && label.indexOf("MANUAL") !== -1);
    const isForming = scanModeOf(label) === "forming"; // Q3 (v4.58): forming runs skip dedup so 14:00 re-scores 11:15 names on a more-formed bar
    result = await runFullPipeline(env, { skipDedup: isManual || isForming, closeFallback: isManual, label: label });
  } catch (e) {
    // Report the ACTUAL error — do not assume it is a missing token.
    await sendTelegram(env,
      `🔴 <b>Pipeline Threw — ${label}</b>\n\n`
      + `<b>${e.name || "Error"}:</b> ${e.message}\n`
      + `<code>${((e.stack || "").split("\n")[1] || "").trim().slice(0, 120)}</code>`
    );
    return { ok: false, error: e.message }; // return so HTTP callers can report it
  }

  // Wrap the summary so a failure INSIDE it is reported instead of swallowed.
  try {
    await sendPipelineSummary(env, result, label);
  } catch (e2) {
    await sendTelegram(env,
      `🟠 <b>Summary Failed — ${label}</b>\n\n`
      + `Pipeline ran OK (candidates: ${result && result.candidateCount}), `
      + `but summary build/send threw:\n`
      + `<b>${e2.name || "Error"}:</b> ${e2.message}\n`
      + `Heartbeat sent: ${hbOk}`
    );
  }

  // Manual-trigger only: a separate completion message listing selected
  // candidates by name. Gated to the MANUAL label so the in-market crons
  // (where the 50-subrequest budget is tightest) do NOT incur this extra send.
  if (label && label.indexOf("MANUAL") !== -1) {
    try {
      const cands = (result && result.candidates) || [];
      let msg;
      if (cands.length === 0) {
        msg = `✅ <b>Pipeline Complete — ${label}</b>\n`
            + `${result && result.candidateCount ? result.candidateCount : 0} candidates. `
            + `No candidate names available this run (empty or early-exit path).`;
      } else {
        const lines = cands.map(function(c, i) {
          return `${i + 1}. <b>${c.symbol}</b> — DS ${c.discoveryScore} [${c.sector}]`;
        }).join("\n");
        msg = `✅ <b>Pipeline Complete — ${label}</b>\n`
            + `${cands.length} candidate${cands.length === 1 ? "" : "s"} selected:\n\n${lines}`;
      }
      const okSend = await sendTelegram(env, msg);
      // If the completion send fails (e.g. subrequest budget exhausted), surface
      // it explicitly rather than swallowing — past silent failures hid the cause.
      if (!okSend) {
        await sendTelegram(env,
          `⚠️ <b>Completion message failed to send</b> (${label}). `
          + `Likely subrequest budget. Candidates: ${result && result.candidateCount}.`);
      }
    } catch (e3) {
      await sendTelegram(env,
        `⚠️ <b>Completion message threw</b> (${label}): ${e3 && e3.message}`);
    }
  }

  return result; // additive: cron path ignores this; HTTP trigger uses it
}

// ─── sendPipelineSummary ──────────────────────────────────────────────────────
// Sends a structured post-run Telegram summary.
// Bottleneck detection: compares funnel ratios at each stage.
// A bottleneck is flagged when a stage drops > 70% of symbols entering it.
// ─────────────────────────────────────────────────────────────────────────────
async function sendPipelineSummary(env, result, label) {
  // result shape from runFullPipeline():
  // { ok, runId, candidateCount, signalCount, stats }
  // stats: { universeCount, bhavCount, streamAFast, ohlcvQueue, ohlcvCapped,
  //          ohlcvFetched, streamATech, rsPassCount, streamACount,
  //          candidateCount, signalCount, survivorCount, niftyAvailable }

  const now = new Date();
  const ist = new Date(now.getTime() + 5.5 * 60 * 60 * 1000);
  const timeStr = ist.toISOString().slice(11, 16) + " IST";

  // ── Bug A fix: pipeline already messaged these paths — stay silent ─────────
  // runFullPipeline() sends its OWN sendTelegram on every ok:false early exit
  // (no bhav, no Stream A pass, all OHLCV failed) AND on the COMPLETED_EMPTY
  // path (no symbols passed technical filters). Sending again here would
  // produce a duplicate Telegram message on those days — violating the
  // no-duplicate-alerts requirement. This wrapper therefore owns messaging
  // ONLY for the full-success path (result.stats present).
  if (!result || !result.ok) {
    // Pipeline already sent a stage-specific failure/abort message. Do not re-send.
    return;
  }

  // ── Bug B fix: COMPLETED_EMPTY returns { ok:true, candidateCount:0,
  //    signalCount:0 } with NO stats and NO runId. runFullPipeline already
  //    sent "No stocks passed technical filters today" on this path.
  //    Re-sending a thin, blank-runId summary here is a duplicate + degraded
  //    message. Stay silent — pipeline owns this path's messaging.
  const s = result.stats;
  if (!s) {
    return;
  }

  // ── Funnel table ───────────────────────────────────────────────────────────
  const funnel = [
    { name: "Universe",       count: s.universeCount  || 0 },
    { name: "Bhav passed",    count: s.bhavCount      || 0 },
    { name: "Stream A fast",  count: s.streamAFast    || 0 },
    { name: "OHLCV queue",    count: s.ohlcvQueue     || 0 },
    { name: "OHLCV fetched",  count: s.ohlcvFetched   || 0 },
    { name: "Tech filters",   count: s.streamATech    || 0 },
    { name: "RS passed",      count: s.rsPassCount    || 0 },
    { name: "Sector filtered",count: s.streamACount   || 0 },
    { name: "Candidates",     count: s.candidateCount || 0 },
    { name: "QE gate (P+E)",  count: s.qeGatePassed != null ? s.qeGatePassed : (s.candidateCount || 0) },
    { name: "Signals sent",   count: s.signalCount    || 0 },
  ];

  // ── Bottleneck detection ───────────────────────────────────────────────────
  // Flag any stage that drops > 70% of the symbols it received.
  // Skip stages where the prior count is 0 (avoid div-by-zero).
  const BOTTLENECK_THRESHOLD = 0.70; // 70% drop = bottleneck
  const bottlenecks = [];

  for (let i = 1; i < funnel.length; i++) {
    const prev = funnel[i - 1].count;
    const curr = funnel[i].count;
    if (prev > 0) {
      const dropRate = (prev - curr) / prev;
      if (dropRate >= BOTTLENECK_THRESHOLD && prev >= 5) {
        // Only flag if the input was meaningful (≥5 symbols) to avoid noise
        // on the final funnel stages where small numbers are expected.
        bottlenecks.push({
          stage: funnel[i].name,
          from:  prev,
          to:    curr,
          pct:   Math.round(dropRate * 100),
        });
      }
    }
  }

  // ── Nifty regime ──────────────────────────────────────────────────────────
  let regimeStr = "—";
  try {
    const sigRaw = await env.KITE_STORE.get("qe_pipe_signals");
    if (sigRaw) {
      const sigs = JSON.parse(sigRaw);
      if (sigs.length > 0 && sigs[0].pipelineRegime) {
        const r = sigs[0].pipelineRegime;
        const icon = r.regime === "bull" ? "🟢" : r.regime === "bear" ? "🔴" : "🟡";
        regimeStr = icon + " " + r.regime.toUpperCase()
          + (r.cmp ? " (Nifty ₹" + r.cmp.toLocaleString("en-IN") + ")" : "");
      }
    }
  } catch (e) { console.warn("[sendPipelineSummary] non-fatal:", e && e.message); }

  // ── Build message ──────────────────────────────────────────────────────────
  const funnelLines = funnel
    .map(function(f) { return `  ${f.name.padEnd(16)}: ${f.count}`; })
    .join("\n");

  let bottleneckBlock = "";
  if (bottlenecks.length > 0) {
    bottleneckBlock = "\n\n⚠️ <b>Bottleneck Detected</b>\n"
      + bottlenecks.map(function(b) {
          return `  🔻 <b>${b.stage}</b>: ${b.from} → ${b.to} (${b.pct}% drop)`;
        }).join("\n");
  }

  const ohlcvCapLine = (s.ohlcvCapped && s.ohlcvCapped > 0)
    ? `\n⚡ OHLCV cap: ${s.ohlcvCapped} symbols dropped (CPU budget)`
    : "";

  const niftyLine = s.niftyAvailable === false
    ? "\n⚠️ Nifty data unavailable — RS ranking may be imprecise"
    : "";

  const statusIcon = s.candidateCount > 0 ? "✅" : "📊";

  // ── QE gate fail-reason tally (named breakdown of rejections) ──────────────
  let qeTallyLine = "";
  if (s.qeGateTally && Object.keys(s.qeGateTally).length) {
    const parts = Object.keys(s.qeGateTally)
      .filter(function(r){ return s.qeGateTally[r] > 0; })
      .map(function(r){ return `${r} ${s.qeGateTally[r]}`; });
    if (parts.length) qeTallyLine = `\n🧠 QE gate rejects: ${parts.join(" · ")}`;
  }

  // ── Named section 1: stocks passing Discovery score ───────────────────────
  const fmtList = function(arr, max){
    if (!arr || !arr.length) return "—";
    const shown = arr.slice(0, max);
    const extra = arr.length > max ? ` …+${arr.length - max} more` : "";
    return shown.join(", ") + extra;
  };
  const discoveryBlock = (s.discoveryNames && s.discoveryNames.length)
    ? `\n\n✅ <b>Passed Discovery (${s.discoveryNames.length})</b>\n${fmtList(s.discoveryNames, 25)}`
    : "";

  // ── Named section 2: stocks passing BOTH Discovery + QE (PRO+ELITE) ────────
  const bothBlock = `\n\n🎯 <b>Passed BOTH Discovery + QE (${(s.bothNames||[]).length})</b>\n${fmtList(s.bothNames, 25)}`;

  // ── Edge-Weighted Score (18-Jul-2026) — SHADOW ADD-ON, see computeEdgeWeightedScore
  // doc for full design. Discounts raw Score by empirical (Monte Carlo) + statistical
  // (expectancy/standard-error) confidence in the backtested edge. Not yet the number
  // QuantEdge decides anything on — for review. Score/finalDecision/BUY-WAIT-IGNORE and
  // the block above are completely unaffected; this is purely additive information.
  let edgeWeightedBlock = "";
  if (s.edgeWeightedEntries && s.edgeWeightedEntries.length) {
    const ewLines = s.edgeWeightedEntries.map(function(e) {
      return `${e.symbol} ${e.edgeWeightedScore} (MC ${Math.round(e.mcFactor*100)}%/t ${e.tStat})`;
    }).join(" · ");
    edgeWeightedBlock = `\n\n🧪 <b>Edge-Weighted Score (shadow — not yet the displayed score)</b>\n${ewLines}`;
  }

  // ── C (17-Jun v4.40): S5 volume near-miss visibility (audit/report only) ───
  // Names that cleared EVERY Stream-A technical gate EXCEPT the volume ratio
  // (reason begins "Volume ratio") — e.g. IIFLCAPS. Read-only surface; changes
  // NO filter, threshold, score, or verdict. Non-fatal if survivorship missing.
  let nearMissBlock = "";
  try {
    const survRaw = await env.KITE_STORE.get("qe_pipe_survivorship");
    if (survRaw) {
      const surv = JSON.parse(survRaw);
      const nm = surv.filter(function (e) {
        return e.stage === "S5_STREAM_A_TECH" && /^Volume ratio/.test(e.reason || "");
      }).map(function (e) { return e.symbol; });
      if (nm.length) {
        nearMissBlock = `\n\n🔎 <b>S5 volume near-miss (${nm.length})</b> — passed all gates except volume\n${fmtList(nm, 25)}`;
      }
    }
  } catch (e) { console.warn("[sendPipelineSummary] near-miss non-fatal:", e && e.message); }

  const msg = `${statusIcon} <b>Pipeline Complete — ${label}</b>\n`
    + `⏰ ${timeStr}\n`
    + `🔑 Run: <code>${(result.runId || "").slice(-8)}</code>\n`
    + `📶 Regime: ${regimeStr}\n\n`
    + `<b>Funnel</b>\n<code>\n${funnelLines}\n</code>`
    + ohlcvCapLine
    + niftyLine
    + qeTallyLine
    + bottleneckBlock
    + discoveryBlock
    + bothBlock
    + edgeWeightedBlock
    + nearMissBlock
    + (s.candidateCount === 0
        ? "\n\n📭 No candidates today — market filters too tight or broad weakness."
        : "");

  await sendTelegram(env, msg);
}

// ═══════════════════════════════════════════════════════════════════════════════
// COMMIT 2 — SMART MOMENTUM PRE-FILTER (candidate selection only)
//
// PURPOSE: prioritize which Stream-A-Fast survivors enter the expensive S4
// history fetch, under the free-tier subrequest budget. This layer ONLY decides
// selection/ordering. It does NOT touch QuantEdge score, BUY/WAIT/IGNORE,
// position sizing, entry/SL/T1/T2, or Telegram output.
//
// Frozen formula (approved): all inputs from bhav (full /quote) — available
// BEFORE S4. No RS/Sector (those don't exist pre-S4). No volume in the score
// (volume stays a hard FILTER, not a ranker).
// ═══════════════════════════════════════════════════════════════════════════════

const MOMENTUM_W = { m1: 0.25, m2: 0.25, m3: 0.25, m4: 0.10, m5: 0.15 };
const FINALRANK_MOMENTUM_W  = 0.85;
const FINALRANK_FRESHNESS_W = 0.15;
const PIPE_HISTORY_BUDGET   = 150; // max symbols into S4 per cron run.
                                   // P5 (paid plan): was 34, sized purely to hit the free-tier
                                   // 50-subrequest cap (1 heartbeat + 13 bhav + 1 nifty + 34 OHLCV
                                   // + 1 signals = 50). Paid tier = 1,000 subrequests, so that
                                   // ceiling is gone. New math: ~13 bhav + 1 nifty + 150 OHLCV +
                                   // overhead ≈ 166/1000 (83% headroom). Binding limits are now
                                   // 30s CPU (≈18s worst-case at BATCH_SIZE=10/DELAY=200) and Kite's
                                   // historical rate limit (one-retry-on-429 + survivorship log
                                   // absorb throttling). 150 is the safe per-run ceiling; wider
                                   // coverage comes from more runs/day (cross-run dedup exists),
                                   // not bigger single runs. Lifts daily coverage ~3.7%→~30% of the
                                   // liquid universe. Selection/ordering/scoring logic UNCHANGED.

function clamp01(x) { return Math.max(0, Math.min(1, x)); }

// MomentumScore — returns 0..100. Inputs from bhav[sym].
// b = { last_price, prev_close, day_open, day_high, day_low }
function pipeMomentumScore(b) {
  if (!b) return 0;
  const ltp = b.last_price, o = b.day_open, h = b.day_high, l = b.day_low, pc = b.prev_close;
  // Guard: need valid intraday range. If OHLC missing/degenerate, score 0.
  if (!(h > 0) || !(o > 0) || !(pc > 0) || h === l) return 0;

  const m1 = clamp01(ltp / h);                              // proximity to day-high
  const m2 = clamp01(((ltp - o) / o + 0.05) / 0.10);        // intraday momentum, ±5% window
  const m3 = clamp01((ltp - l) / (h - l));                  // position in day-range
  const m4 = clamp01(((h - l) / o) / 0.05);                 // range expansion, cap 5%
  const m5 = clamp01(((ltp - pc) / pc + 0.02) / 0.07);      // day change, -2%..+5% window

  const score = MOMENTUM_W.m1 * m1 + MOMENTUM_W.m2 * m2 + MOMENTUM_W.m3 * m3
              + MOMENTUM_W.m4 * m4 + MOMENTUM_W.m5 * m5;     // 0..1
  return Math.round(score * 1000) / 10;                      // 0..100, 1 decimal
}

// ── Daily dedup + freshness store ───────────────────────────────────────────
// KV key qe_analysed_<YYYYMMDD> holds { sym: priorMomentumScore }.
// TTL 26h → auto-resets each trading day (yesterday's key expires, today's is
// absent → every symbol eligible again). Cannot grow indefinitely: keyed per
// day, expires daily; size bounded by symbols analysed that day (≤ ~110).
function pipeTodayKey() {
  const ist = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  return "qe_analysed_" + ist.toISOString().slice(0, 10).replace(/-/g, "");
}

async function pipeLoadAnalysedToday(env) {
  try {
    const raw = await env.KITE_STORE.get(pipeTodayKey());
    return raw ? JSON.parse(raw) : {};
  } catch (_) { return {}; }
}

async function pipeSaveAnalysedToday(env, analysedMap) {
  try {
    await env.KITE_STORE.put(pipeTodayKey(), JSON.stringify(analysedMap),
      { expirationTtl: 26 * 60 * 60 }); // 26h → resets next trading day
  } catch (e) { console.warn("[pipeSaveAnalysedToday] non-fatal:", e && e.message); }
}

// FreshnessScore (0..100): rewards symbols NOT analysed yet today, and symbols
// whose momentum is RISING vs their prior-run score.
//   - never analysed today  → freshness 100 (max diversity reward)
//   - analysed, score rising → partial reward by delta
//   - analysed, score flat/falling → low freshness (already had its look)
// Bounded 0..100; cannot grow without limit (delta clamped, base capped).
function pipeFreshnessScore(sym, currentMomentum, analysedMap) {
  if (!(sym in analysedMap)) return 100;          // not yet analysed today
  const prior = analysedMap[sym];
  const delta = currentMomentum - prior;          // points (0..100 scale)
  // Rising fast → up to ~60; flat/falling → ~10 floor. Clamped.
  return clamp01((delta + 5) / 30) * 60 + 10;     // range ~10..70, never ≥ a fresh 100
}

// SINGLE SOURCE OF TRUTH for candidate ranking. Both the production selection
// path (runFullPipeline) and the /pipe/momentum/debug observer call THIS. There
// is no second copy of the scoring/assembly logic — debug cannot drift from
// production because they execute the identical function.
// Returns array sorted by finalRank desc, each: { sym, momentum, freshness, finalRank }.
function pipeRankCandidates(streamAFast, bhav, analysedMap) {
  return streamAFast.map(function(sym) {
    const b         = bhav[sym];
    const mom       = pipeMomentumScore(b);
    const fresh     = pipeFreshnessScore(sym, mom, analysedMap);
    const finalRank = FINALRANK_MOMENTUM_W * mom + FINALRANK_FRESHNESS_W * fresh;
    return { sym: sym, momentum: mom, freshness: fresh, finalRank: finalRank };
  }).sort(function(a, b) { return b.finalRank - a.finalRank; });
}

// ═══════════════════════════════════════════════════════════════════════════════
// FULL PIPELINE ORCHESTRATOR
// Runs all stages in sequence, writes results to KV at each checkpoint.
// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// EDGE-WEIGHTED SCORE (18-Jul-2026) — SHADOW / ADD-ON METRIC ONLY.
//
// Motivation: raw Score measures technical setup quality (trend/momentum/breakout/
// structure) and says nothing about whether the historical backtest actually
// confirms an edge. Real case that surfaced this (17-Jul-2026): BAJFINANCE scored
// 100 with a Monte Carlo profit probability of 37.7% (below breakeven), while
// BHARATFORG scored 89 with MC 88.8% and a PROVEN_POSITIVE edge class. The raw
// Score ranked the weaker-validated trade first. This is not a bug in Score — it
// was never designed to measure backtest confidence — but a reader has no way to
// see that distinction without manually cross-referencing edge_class/mc_prob/ev/
// exp_se, which is exactly the manual reconciliation this metric now automates.
//
// DESIGN, using ONLY already-computed fields, nothing new invented or backtested:
//   mcFactor   = mc_prob / 100                          — empirical (Monte Carlo simulation)
//   tStat      = ev / exp_se                             — statistical (is the expectancy
//                                                          estimate distinguishable from noise)
//   statFactor = clamp(0.5 + tStat/4, 0, 1)              — t-stat of 0 → 0.5 (uncertain),
//                                                          t-stat of ~2 (≈95% CI) → 1.0
//   confidenceFactor = (mcFactor + statFactor) / 2       — average of both lenses
//   edgeWeightedScore = round(score * confidenceFactor)
//
// THRESHOLD CLASSIFICATION: the /4 divisor and 0.5 baseline in statFactor are
// ENGINEERING_ASSUMPTION — no prior QuantEdge precedent, chosen so a t-stat of
// ~2 (a conventional "reasonably confident the effect is real" threshold) maps to
// full confidence, and a t-stat of 0 (no evidence either way) maps to neutral
// (half credit), not zero — a small backtest sample with an unproven-but-not-
// disproven edge shouldn't be scored as if it were empirically bad.
//
// SHADOW MODE: computed and logged/dispatched alongside the existing Score, in a
// clearly separate, clearly labeled section. Score, finalDecision, BUY/WAIT/IGNORE,
// and every existing field/format are completely untouched. This is an add-on for
// review, not yet the number QuantEdge decides anything on.
function computeEdgeWeightedScore(score, mcProb, ev, expSE) {
  if (score == null) return null;
  const mcFactor = (mcProb != null) ? Math.max(0, Math.min(1, mcProb / 100)) : 0.5; // unknown MC treated as neutral, not full credit
  const tStat = (ev != null && expSE != null && expSE > 0) ? ev / expSE : 0;
  const statFactor = Math.max(0, Math.min(1, 0.5 + tStat / 4));
  const confidenceFactor = (mcFactor + statFactor) / 2;
  const edgeWeightedScore = Math.round(score * confidenceFactor);
  return {
    edgeWeightedScore,
    confidenceFactor: Math.round(confidenceFactor * 1000) / 1000,
    mcFactor: Math.round(mcFactor * 1000) / 1000,
    statFactor: Math.round(statFactor * 1000) / 1000,
    tStat: Math.round(tStat * 1000) / 1000,
  };
}

async function runFullPipeline(env, opts) {
  const skipDedup = !!(opts && opts.skipDedup);
  const closeFallback = !!(opts && opts.closeFallback); // v4.22: manual-only
  const label    = (opts && opts.label) || "scheduled"; // v4.50 HOTFIX: trigger label for Stage 0 token guard (was undefined -> ReferenceError)
  const scanMode = scanModeOf(label); // Step C (v4.57): single derivation; feeds Telegram tag + qe_forward_track.scan_mode
  const runId    = genRunId();
  const startedAt = new Date().toISOString();
  const audit    = makePipeAudit();
  const survive  = makeSurvivorshipLog();

  audit.log("PIPELINE", "", "START", "Run ID: " + runId + " at " + startedAt);

  // Write initial status
  await writePipeStatus(env, "STARTING", 2, {
    runId:     runId,
    startedAt: startedAt,
  });
  await env.KITE_STORE.put("qe_pipe_run_id", runId);

  // ── Get Kite token ───────────────────────────────────────────────────────────
  let token;
  try {
    token = await getToken(env);
  } catch (e) {
    audit.log("PIPELINE", "", "ERROR", "No Kite token: " + e.message);
    await writePipeStatus(env, "FAILED", 0, {
      runId: runId, error: "No Kite token — login required",
    });
    await env.KITE_STORE.put("qe_pipe_audit", JSON.stringify(audit.getAll().slice(0, 500)));
    throw e;
  }

  // ── Stage 0: Token health guard (Fix 1) ──────────────────────────────────────
  // Abort BEFORE burning the pipeline if the token is dead. Writes token_health row.
  const _tok = await tokenHealthCheck(env, token, label);
  if (!_tok.alive) {
    audit.log("PIPELINE", "", "ABORT", "Stage 0: token dead — " + _tok.detail);
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "Token dead at Stage 0 — login required" });
    await env.KITE_STORE.put("qe_pipe_audit", JSON.stringify(audit.getAll().slice(0, 500)));
    return { ok: false, error: "Token dead — login required", tokenDead: true };
  }

  // ── Stage 1: Universe ────────────────────────────────────────────────────────
  const universe = await pipeLoadUniverse(env, audit);
  if (!universe) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "Universe not found in KV" });
    await env.KITE_STORE.put("qe_pipe_audit", JSON.stringify(audit.getAll().slice(0, 500)));
    await sendTelegram(env,
      `⚠️ <b>Pipeline Failed — Stage 1</b>\nUniverse not in KV. Run /universe/refresh first.`);
    return { ok: false, error: "Universe not found" };
  }

  // ── Stage 2: Bhav Copy ───────────────────────────────────────────────────────
  const bhav = await pipeBhavCopy(env, token, universe, audit, survive, { closeFallback: closeFallback });

  // ── Stage 3: Stream A Fast (bhav-only) ──────────────────────────────────────
  const bhavSymbols    = universe.filter(function(s) { return !!bhav[s]; });
  const streamAFast    = pipeStreamAFast(bhav, bhavSymbols, audit, survive);

  audit.log("PIPELINE", "", "CHECKPOINT",
    "Post-S3: " + streamAFast.length + " symbols entering OHLCV fetch");

  if (streamAFast.length === 0) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "No symbols passed Stream A Fast" });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await sendTelegram(env, `📭 <b>0 candidates</b> — ${universe.length} universe → ${Object.keys(bhav).length} passed bhav → 0 cleared Stream A. Token: OK (Stage 0 alive).`);
    return { ok: false, error: "No symbols passed Stream A Fast" };
  }

  // ── Commit 2: Smart Momentum pre-filter (replaces raw volume-cap) ──────────
  // Volume FILTER already applied in Stream A Fast (≥200k). Here we RANK the
  // survivors by MomentumScore + freshness, exclude symbols already analysed
  // earlier today, and take the top PIPE_HISTORY_BUDGET into S4. This decides
  // SELECTION ONLY — it never touches QuantEdge score or trade logic.
  const analysedToday = await pipeLoadAnalysedToday(env);

  // Score + rank all Stream-A-Fast survivors (shared ranker — see pipeRankCandidates)
  const ranked = pipeRankCandidates(streamAFast, bhav, analysedToday);

  // Exclude symbols already analysed today (dedup) — they keep their prior result.
  // skipDedup (manual trigger): bypass exclusion so an after-hours re-run shows
  // the same true top candidates every time, not a shrinking leftover pool.
  const notYetAnalysed = skipDedup
    ? ranked
    : ranked.filter(function(r) { return !(r.sym in analysedToday); });

  // Per-run history budget. Manual runs use a smaller budget than crons.
  // P5 (paid plan): was 30 (free-tier: 13 bhav + 30 history + 4 telegram = 47/50).
  // Paid tier removes that cap; raised to 60 — still conservative for an on-demand
  // run (faster turnaround than a full 150 cron run, lighter Kite-rate pressure).
  // v4.18: when the D1 cache is ON, the budget lifts to the ENTIRE ranked pool —
  // the cap existed only for Kite's historical rate limit + live-fetch CPU, and D1
  // reads have neither constraint. Subrequest math: ~600 D1 reads worst case + ~50
  // other subrequests ≈ 650, well under the paid 1,000/invocation. Live fallbacks
  // stay bounded at PIPE_MAX_OHLCV_CAP inside pipeFetchOhlcvBatch, so a degraded
  // D1 cannot exceed today's Kite load. Flag off → behavior byte-identical to v4.17.
  const d1FullCoverage = await d1Enabled(env);
  const historyBudget  = d1FullCoverage
    ? notYetAnalysed.length
    : (skipDedup ? 60 : PIPE_HISTORY_BUDGET);
  if (d1FullCoverage) {
    audit.log("S3B_BUDGET", "", "D1_FULL_COVERAGE",
      "D1 cache ON — budget lifted to full pool (" + notYetAnalysed.length + " symbols)");
  }

  // Select top N for this run's S4 history fetch
  const selected     = notYetAnalysed.slice(0, historyBudget);
  const ohlcvQueue    = selected.map(function(r) { return r.sym; });
  const ohlcvDropped  = notYetAnalysed.slice(historyBudget).map(function(r) { return r.sym; });

  // ── Observability only (traceability): full rank-list snapshot ──────────────
  // Records the rank/selection decision that was ALREADY made above. Pure
  // observer: reads finalized values, writes one KV key, assigns nothing back
  // into the pipeline. Does NOT touch filters/scoring/ranking/budget/selection.
  // KV put is NOT a subrequest → zero impact on the 50-subrequest cap.
  // Lets any future symbol be traced at the S3→S3B boundary from one key:
  //   presence  → passed Stream A Fast
  //   r         → momentum rank
  //   sel:false → excluded by history budget
  // Keyed by IST-date + runId so runs don't overwrite; 7-day TTL bounds growth.
  try {
    const selectedSet = {};
    for (let qi = 0; qi < ohlcvQueue.length; qi++) selectedSet[ohlcvQueue[qi]] = true;
    const rankSnapshot = ranked.map(function(r, idx) {
      const b = bhav[r.sym];
      return {
        s:   r.sym,
        r:   idx + 1,
        m:   r.momentum,
        sel: !!selectedSet[r.sym],
        px:  (b && typeof b.last_price === "number") ? b.last_price : null,
      };
    });
    const istDate = new Date(Date.now() + 5.5 * 60 * 60 * 1000)
      .toISOString().slice(0, 10).replace(/-/g, "");
    await env.KITE_STORE.put(
      "qe_pipe_rank_" + istDate + "_" + runId,
      JSON.stringify(rankSnapshot),
      { expirationTtl: 7 * 24 * 60 * 60 }); // 7 days
  } catch (e) {
    console.warn("[rankSnapshot] non-fatal:", e && e.message);
  }
  // ── End observability block ────────────────────────────────────────────────

  // Survivorship-log the not-selected (budget-capped) symbols with their score
  for (let di = 0; di < ohlcvDropped.length; di++) {
    const r = notYetAnalysed[historyBudget + di];
    survive.drop(ohlcvDropped[di], "S4_HISTORY_BUDGET",
      "Below history budget cutoff (rank " + (historyBudget + di + 1)
      + ", momentum " + (r ? r.momentum : "n/a") + ")");
  }

  // Commit 2 audit: momentum/rank metrics for every selected symbol
  for (let si = 0; si < selected.length; si++) {
    const r = selected[si];
    audit.log("S3B_MOMENTUM", r.sym, "SELECTED",
      "rank:" + (si + 1) + " momentum:" + r.momentum
      + " freshness:" + r.freshness.toFixed(1) + " final:" + r.finalRank.toFixed(1));
  }
  audit.log("PIPELINE", "", "MOMENTUM_RANK",
    "Ranked " + ranked.length + " | already-analysed-today " + Object.keys(analysedToday).length
    + " | eligible " + notYetAnalysed.length + " | selected " + ohlcvQueue.length
    + " | budget-dropped " + ohlcvDropped.length);

  if (ohlcvQueue.length === 0) {
    // All survivors already analysed today, or none scored — nothing new to fetch.
    await writePipeStatus(env, "COMPLETED_EMPTY", 100, {
      runId: runId, reason: "No new symbols to analyse (all done today or empty)",
    });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await sendTelegram(env, `📊 <b>Pipeline Complete</b>\nNo new candidates this run — all qualifying symbols already analysed today.`);
    return { ok: true, candidateCount: 0, signalCount: 0 };
  }

  // ── Stage 4: OHLCV fetch ─────────────────────────────────────────────────────
  // Also fetch Nifty closes (parallel with OHLCV batch)
  const niftyClosesPromise = pipeLoadNiftyCloses(env, token, audit);
  const ohlcvMap = await pipeFetchOhlcvBatch(env, token, ohlcvQueue, audit, survive);
  const niftyCloses = await niftyClosesPromise;

  // ── Fix 3: Compute and snapshot macro regime from Nifty data ────────────────
  // This regime snapshot travels with every pipeline signal so the browser
  // can use the pipeline-time regime for deterministic deep analysis scoring,
  // regardless of what time of day the user opens the Discovery panel.
  const pipelineRegime = computePipelineRegime(niftyCloses, audit);
  audit.log("PIPELINE", "", "MACRO_SNAP",
    "Pipeline regime: " + pipelineRegime.regime + " (bull:" + pipelineRegime.bullScore
    + " niftyBars:" + (niftyCloses ? niftyCloses.length : 0) + ")");
  // ── End Fix 3a ────────────────────────────────────────────────────────────

  // ── v4.24: PUBLISH regime as the SINGLE SOURCE OF TRUTH (qe_regime KV) ───────
  // Server cron, discovery bot, QE gate (Stage 9.5) and Telegram already score
  // under this one pipelineRegime. We now also expose it at GET /pipe/regime so
  // the browser MANUAL SCAN reads the identical regime instead of computing its
  // own (Yahoo) — eliminating the last cross-surface divergence. Additive; +1 KV
  // write/run; non-fatal on failure (browser falls back to its local structural).
  try {
    const _istDate = new Date(Date.now() + 5.5 * 60 * 60 * 1000).toISOString().slice(0, 10);
    await env.KITE_STORE.put("qe_regime", JSON.stringify({
      regime:    pipelineRegime.regime,
      bullScore: pipelineRegime.bullScore,
      ts:        pipelineRegime.ts,
      source:    pipelineRegime.source,
      niftyBars: pipelineRegime.niftyBars,
      cmp:       pipelineRegime.cmp,
      e50:       pipelineRegime.e50,
      e200:      pipelineRegime.e200,
      runDate:   _istDate,
    }));
    audit.log("PIPELINE", "", "REGIME_PUBLISH",
      "qe_regime written: " + pipelineRegime.regime + " (src:" + pipelineRegime.source + ")");
  } catch (e) { console.warn("[runFullPipeline] qe_regime write non-fatal:", e && e.message); }

  if (Object.keys(ohlcvMap).length === 0) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "All OHLCV fetches failed" });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await sendTelegram(env, `⚠️ <b>Pipeline: OHLCV fetch failed for all symbols</b>. Kite API may be throttling.`);
    return { ok: false, error: "All OHLCV fetches failed" };
  }

  // ── Stage 5: Stream A Technical ─────────────────────────────────────────────
  const streamATech = pipeStreamATech(ohlcvMap, audit, survive);

  // Write Stream A result to KV
  try {
    await env.KITE_STORE.put("qe_pipe_stream_a", JSON.stringify(streamATech));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 5B: Stream B Discovery (independent branch on same ohlcvMap) ───────
  // Runs in parallel intent to Stream A — does NOT consume or alter the Stream A
  // flow (RS/Sector/Merge below). Writes its own KV key for the Discovery panel.
  try {
    const streamBCandidates = pipeStreamBTech(ohlcvMap, audit, survive);
    const sbEvaluated = Object.keys(ohlcvMap).length;
    await env.KITE_STORE.put("qe_pipe_stream_b", JSON.stringify({
      generated_utc: new Date().toISOString(),
      run_id: runId,
      bhav_count: Object.keys(bhav).length,
      ohlcv_map_count: sbEvaluated,
      evaluated_count: sbEvaluated,
      count: streamBCandidates.length,
      candidates: streamBCandidates.slice(0, 50),
    }));
    audit.log("PIPELINE", "", "STREAM_B_KV",
      "Stream B wrote " + streamBCandidates.length + " candidates to qe_pipe_stream_b");
  } catch (e) {
    // Stream B failure must NOT break the Stream A pipeline.
    audit.log("PIPELINE", "", "STREAM_B_ERROR", e.name + ": " + e.message);
    try { await env.KITE_STORE.put("qe_pipe_stream_b", JSON.stringify({
      generated_utc: new Date().toISOString(), run_id: runId, count: 0,
      candidates: [], error: e.message,
    })); } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }
  }

  audit.log("PIPELINE", "", "CHECKPOINT",
    "Post-S5: " + streamATech.length + " symbols entering RS Engine");

  if (streamATech.length === 0) {
    await writePipeStatus(env, "COMPLETED_EMPTY", 100, {
      runId: runId, reason: "No symbols passed technical filters",
    });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await env.KITE_STORE.put("qe_pipe_signals",      JSON.stringify([]));
    await env.KITE_STORE.put("qe_pipe_candidates",   JSON.stringify([]));

    // Full funnel summary even on a zero-candidate day (13-Jul-2026 fix — see
    // memory #26/#27). Every count below was already computed earlier in this
    // same function call; this only stops discarding it. Funnel stops at Tech
    // Filters since nothing downstream (RS/Sector/Candidates/QE gate/Signals)
    // ever ran — those stages are correctly omitted, not zero-padded.
    const _now = new Date();
    const _ist = new Date(_now.getTime() + 5.5 * 60 * 60 * 1000);
    const _timeStr = _ist.toISOString().slice(11, 16) + " IST";
    const _regimeIcon = pipelineRegime.regime === "bull" ? "🟢" : pipelineRegime.regime === "bear" ? "🔴" : "🟡";
    const _regimeStr = _regimeIcon + " " + String(pipelineRegime.regime).toUpperCase()
      + (pipelineRegime.cmp ? " (Nifty ₹" + pipelineRegime.cmp.toLocaleString("en-IN") + ")" : "");

    const _funnel = [
      { name: "Universe",      count: universe.length },
      { name: "Bhav passed",   count: Object.keys(bhav).length },
      { name: "Stream A fast", count: streamAFast.length },
      { name: "OHLCV queue",   count: ohlcvQueue.length },
      { name: "OHLCV fetched", count: Object.keys(ohlcvMap).length },
      { name: "Tech filters",  count: streamATech.length },
    ];
    const _BOTTLENECK_THRESHOLD = 0.70; // matches sendPipelineSummary's threshold exactly
    const _bottlenecks = [];
    for (let i = 1; i < _funnel.length; i++) {
      const prev = _funnel[i - 1].count, curr = _funnel[i].count;
      if (prev > 0) {
        const dropRate = (prev - curr) / prev;
        if (dropRate >= _BOTTLENECK_THRESHOLD && prev >= 5) {
          _bottlenecks.push({ stage: _funnel[i].name, from: prev, to: curr, pct: Math.round(dropRate * 100) });
        }
      }
    }
    const _funnelLines = _funnel.map(function(f) { return `  ${f.name.padEnd(16)}: ${f.count}`; }).join("\n");
    let _bottleneckBlock = "";
    if (_bottlenecks.length > 0) {
      _bottleneckBlock = "\n\n⚠️ <b>Bottleneck Detected</b>\n"
        + _bottlenecks.map(function(b) { return `  🔻 <b>${b.stage}</b>: ${b.from} → ${b.to} (${b.pct}% drop)`; }).join("\n");
    }

    await sendTelegram(env,
      `📊 <b>Pipeline Complete — ${label}</b>\n`
      + `⏰ ${_timeStr}\n`
      + `🔑 Run: <code>${runId.slice(-8)}</code>\n`
      + `📶 Regime: ${_regimeStr}\n\n`
      + `<b>Funnel</b>\n<code>\n${_funnelLines}\n</code>`
      + _bottleneckBlock
      + `\n\nNo stocks passed Tech Filters today — 0 candidates, 0 signals. Market may be consolidating.`);
    return { ok: true, candidateCount: 0, signalCount: 0 };
  }

  // ── Stage 6: RS Engine ───────────────────────────────────────────────────────
  const rsRanked = pipeRankRS(streamATech, ohlcvMap, niftyCloses, audit, survive);

  // Write RS result to KV
  try {
    await env.KITE_STORE.put("qe_pipe_rs_ranked",
      JSON.stringify(rsRanked.map(function(r) {
        return { sym: r.sym, rsScore: r.rsScore };
      }))
    );
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  audit.log("PIPELINE", "", "CHECKPOINT",
    "Post-S6: " + rsRanked.length + " symbols entering Sector Engine");

  // ── Stage 7: Sector Engine ───────────────────────────────────────────────────
  const sectorFiltered = pipeSectorFilter(rsRanked, ohlcvMap, audit, survive);

  // Write sector map to KV
  const sectorMapOut = {};
  for (let si = 0; si < sectorFiltered.length; si++) {
    sectorMapOut[sectorFiltered[si].sym] = sectorFiltered[si].sector;
  }
  try {
    await env.KITE_STORE.put("qe_pipe_sector_map", JSON.stringify(sectorMapOut));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 8: Merge + Discovery Score ────────────────────────────────────────
  const candidates = pipeMerge(sectorFiltered, ohlcvMap, audit);

  // ── Stage 9.5: QE SCORE GATE — PRO FILTER ON + ELITE ON (v4.19) ──────────────
  // Telegram carries only candidates the user's live screen (PRO+ELITE) would call
  // BUY. Discovery-passing names AND both-passing names are captured for the
  // summary. Full results land in KV + the pipeline audit log. Kill: QE_SCORE_GATE=off.
  let qeGateOn = true;
  try { qeGateOn = ((await env.KITE_STORE.get("QE_SCORE_GATE")) || "on") !== "off"; } catch (_) {}
  // Q1(B) (v4.58): resolve the volume-pace kill-switch ONCE per run, only on forming runs (no KV
  // read on confirmed/discovery). Default OFF => no projection => live behavior = option (A).
  let volPaceOn = false;
  if (scanMode === "forming") {
    try { volPaceOn = ((await env.KITE_STORE.get("FORMING_VOL_PACE")) || "off") === "on"; } catch (_) {}
  }
  let qeDispatchList = candidates;
  let qeTally = null, discoveryNames = [], bothNames = [], edgeWeightedEntries = [];
  // Discovery-passing = every merged candidate (already cleared Discovery score upstream)
  discoveryNames = candidates.map(function(c){ return c.symbol + " (" + c.discoveryScore + ")"; });
  if (qeGateOn && candidates.length) {
    const regimeStr = (pipelineRegime && pipelineRegime.regime) || "sideways";
    qeTally = {};
    qeDispatchList = [];
    // v4.20: gate backtests on the SAME 2y window the browser uses. Load the
    // token map once (KV read) to fetch 2y candles per candidate; on any failure
    // fall back to the 1y _candles already in hand. The basis (2y vs 1y) is logged
    // per symbol so every verdict is traceable to the data it used.
    let _gateTokenMap = {};
    try { const _tm = await env.KITE_STORE.get("qe_db_token_map"); if (_tm) _gateTokenMap = JSON.parse(_tm); } catch (_) {}
    let _basis2y = 0, _basis1y = 0;
    const GATE_MAX = 40; // CPU guard; >40 candidates never occurs in practice
    for (let gi = 0; gi < candidates.length; gi++) {
      const cand = candidates[gi];
      if (gi >= GATE_MAX) { cand.qe = { pass: true, reason: "GATE_BUDGET" }; qeDispatchList.push(cand); bothNames.push(cand.symbol + " (DS " + cand.discoveryScore + ")"); continue; }
      const ind = ohlcvMap[cand.symbol];
      if (!ind || !ind._candles) { cand.qe = { pass: false, reason: "NO_CANDLES" }; qeTally.NO_CANDLES = (qeTally.NO_CANDLES||0)+1; audit.log("S9B_QEGATE", cand.symbol, "REJECT", "NO_CANDLES"); continue; }
      // Fetch 2y for browser-parity backtest; fall back to 1y _candles on failure.
      // v4.25: capture WHY 2y fell back (NO_TOKEN / HTTP_<status> / FEW_<n> / EXC_<msg>)
      let gateCandles = ind._candles, basis = "1y", b2reason = "";
      try {
        const r2y = await pipeFetch2yCandles(env, token, cand.symbol, _gateTokenMap[cand.symbol]);
        b2reason = (r2y && r2y.reason) || "NULL";
        if (r2y && r2y.candles && r2y.candles.length > ind._candles.length) { gateCandles = r2y.candles; basis = "2y"; }
      } catch (e) { b2reason = "GATE_EXC_" + (((e && e.message) || "?") + "").slice(0, 30); }
      if (basis === "2y") _basis2y++; else _basis1y++;
      gateCandles = projectFormingVolume(gateCandles, scanMode, volPaceOn); // Q1(B) (v4.58): no-op unless forming + FORMING_VOL_PACE=on
      const r = QEGate.evaluate(gateCandles, regimeStr, cand.rsScore);
      r.basis = basis;
      // Golden formula unification (16-Jul-2026): overwrite the Stage-8 rough ATR estimate
      // (entry=lastClose, computed before full candle history was available) with the same
      // breakout-formula entry/SL/T1/T2 evaluate() just computed from real candles — the
      // exact values the browser's card/GTT and the backtest now use too. This is what the
      // per-signal Telegram alert and qe_forward_track read (c.entry/c.sl/c.t1/c.t2), so this
      // is the fix that actually closes the Telegram/browser parity gap end to end.
      if (typeof r.entry === "number") cand.entry = r.entry;
      if (typeof r.sl === "number")    cand.sl    = r.sl;
      if (typeof r.t1 === "number")    cand.t1    = r.t1;
      if (typeof r.t2 === "number")    cand.t2    = r.t2;
      cand.qe = { pass: r.pass, label: r.label, qeScore: r.score, baseScore: r.baseScore, proScore: r.proScore,
                  wr: r.wr, ev: r.ev, mcProb: r.mcProb, btTotal: r.btTotal, elite: r.elite,
                  isRejected: r.isRejected, proReason: r.proReason, reason: r.reason || r.label, basis: r.basis,
                  // E1 (17-Jun v4.40): additive capture of engine outputs for forward measurement.
                  // These are read off the SAME r returned by QEGate.evaluate above — they do not
                  // alter pass/label/score/dispatch in any way.
                  edgeClass: r.edgeClass, freshBreakout: r.freshBreakout, expSE: r.expSE,
                  // Priority 1 fix (13-Jul-2026): b2reason was already computed above every run
                  // and only ever logged to the audit-log text string, then discarded — making the
                  // 1y-fallback root cause undiagnosable after the fact. Now captured so it reaches
                  // qe_forward_track.data_basis_reason and is queryable for any future occurrence.
                  dataBasisReason: b2reason,
                  // Edge-Weighted Score (18-Jul-2026) — shadow add-on, see function doc above.
                  edgeWeighted: r.score != null ? computeEdgeWeightedScore(r.score, r.mcProb, r.ev, r.expSE) : null };
      audit.log("S9B_QEGATE", cand.symbol, r.pass ? "PASS" : "REJECT",
        "QE:" + (r.score!=null?r.score:"-") + " base:" + (r.baseScore!=null?r.baseScore:"-") +
        " pro:" + (r.proScore!=null?r.proScore:"-") + " elite:" + (r.elite||"-") +
        " WR:" + (r.wr!=null?r.wr:"-") + " EV:" + (r.ev!=null?r.ev:"-") + "R MC:" + (r.mcProb!=null?r.mcProb:"-") +
        "% [" + r.basis + "] → " + (r.label||r.reason) + (r.proReason?(" ["+r.proReason+"]"):"")
        + " 2yfetch:" + b2reason);
      if (r.pass) {
        qeDispatchList.push(cand);
        bothNames.push(cand.symbol + " (DS " + cand.discoveryScore + "/QE " + (r.score!=null?r.score:"-") + ")");
        if (cand.qe.edgeWeighted) edgeWeightedEntries.push({ symbol: cand.symbol, ...cand.qe.edgeWeighted });
      }
      else { const rk = r.reason || r.label || "OTHER"; qeTally[rk] = (qeTally[rk]||0)+1; }
    }
    audit.log("S9B_QEGATE", "", "DONE",
      "PRO+ELITE gate: " + candidates.length + " in → " + qeDispatchList.length + " BUY | basis 2y:" + _basis2y + " 1y:" + _basis1y + " | " + JSON.stringify(qeTally));
  }

  // ── Persist COMPLETE QE-gate audit (every candidate, both gates, all reasons) ─
  // The shared pipeline audit log is capped at 500 entries; this dedicated key
  // guarantees the FULL gate decision history is captured per run. Dated key keeps
  // a rolling 14-day trail so past runs remain auditable. KV puts (not subrequests).
  try {
    const qeAudit = {
      runId: runId, ts: new Date().toISOString(),
      regime: (pipelineRegime && pipelineRegime.regime) || "sideways",
      discoveryPassed: candidates.length,
      bothPassed: qeDispatchList.length,
      tally: qeTally || {},
      // schema_version lets a future learning layer parse historical records safely.
      schema_version: 2,
      rows: candidates.map(function(c){
        return {
          symbol: c.symbol, sector: c.sector, ds: c.discoveryScore, rs: c.rsScore,
          // entry/levels captured so realized outcomes can be matched to predictions
          entry: c.entry, sl: c.sl, t1: c.t1, t2: c.t2, lastClose: c.lastClose,
          qe: c.qe ? {
            pass: c.qe.pass, label: c.qe.label, qeScore: c.qe.qeScore,
            base: c.qe.baseScore, pro: c.qe.proScore, elite: c.qe.elite,
            wr: c.qe.wr, ev: c.qe.ev, mc: c.qe.mcProb, bt: c.qe.btTotal,
            proReason: c.qe.proReason, reason: c.qe.reason, basis: c.qe.basis
          } : null
        };
      })
    };
    await env.KITE_STORE.put("qe_pipe_qegate", JSON.stringify(qeAudit));
    const dkey = "qe_pipe_qegate_" + new Date(Date.now() + 5.5*3600*1000).toISOString().slice(0,10) + "_" + String(runId).slice(-8);
    await env.KITE_STORE.put(dkey, JSON.stringify(qeAudit), { expirationTtl: 14 * 24 * 60 * 60 });
  } catch (e) { console.warn("[qegate audit persist] non-fatal:", e && e.message); }

  // ── E1 (17-Jun v4.40): Commit E forward-track snapshot — MEASUREMENT ONLY ───
  // Appends one row per candidate verdict to D1 qe_forward_track so realized
  // out-of-sample outcomes can be measured later (E2). Reads ONLY values already
  // computed above; changes NO verdict, score, threshold, filter, ranking, or
  // dispatch. v4.43: UPSERT keyed (snapshot_date,symbol) — a re-run UPGRADES the
  // day's row to the latest verdict, EXCEPT it never downgrades an existing 2y row
  // to 1y. This self-heals the case where a degraded 1y run (stale token map) writes
  // first and a corrected 2y run follows: the old INSERT OR IGNORE silently discarded
  // the correction, freezing the day at 1y. One batched D1 call (≤GATE_MAX rows).
  // Non-fatal on any failure.
  try {
    if (env.QE_DB && candidates && candidates.length) {
      const snapDate = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
      const nowIso   = new Date().toISOString();
      const fstmts   = [];
      for (let fi = 0; fi < candidates.length; fi++) {
        const c = candidates[fi]; const q = c.qe || {};
        fstmts.push(env.QE_DB.prepare(
          "INSERT INTO qe_forward_track " +
          "(run_id,snapshot_date,symbol,label,reason,score,base_score,edge_class,ev,exp_se,mc_prob,mc_veto,fresh_breakout,cmp,entry,sl,t1,t2,data_basis,created_ts,scan_mode,data_basis_reason,edge_weighted_score,edge_confidence_factor) " +
          "VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24) " +
          "ON CONFLICT(snapshot_date,symbol) DO UPDATE SET " +
          "run_id=excluded.run_id,label=excluded.label,reason=excluded.reason,score=excluded.score," +
          "base_score=excluded.base_score,edge_class=excluded.edge_class,ev=excluded.ev,exp_se=excluded.exp_se," +
          "mc_prob=excluded.mc_prob,mc_veto=excluded.mc_veto,fresh_breakout=excluded.fresh_breakout," +
          "cmp=excluded.cmp,entry=excluded.entry,sl=excluded.sl,t1=excluded.t1,t2=excluded.t2," +
          "data_basis=excluded.data_basis,created_ts=excluded.created_ts,scan_mode=excluded.scan_mode,data_basis_reason=excluded.data_basis_reason," +
          "edge_weighted_score=excluded.edge_weighted_score,edge_confidence_factor=excluded.edge_confidence_factor " +
          "WHERE NOT (qe_forward_track.data_basis='2y' AND excluded.data_basis='1y')"
        ).bind(
          String(runId || ""), snapDate, c.symbol,
          q.label    != null ? q.label    : null,
          q.reason   != null ? q.reason   : null,
          q.qeScore  != null ? q.qeScore  : null,
          q.baseScore!= null ? q.baseScore: null,
          q.edgeClass!= null ? q.edgeClass: null,
          q.ev       != null ? q.ev       : null,
          q.expSE    != null ? q.expSE    : null,
          q.mcProb   != null ? q.mcProb   : null,
          null, // mc_veto — not a discrete engine field; reserved (null) in E1
          q.freshBreakout != null ? (q.freshBreakout ? 1 : 0) : null,
          c.lastClose!= null ? c.lastClose: null,
          c.entry    != null ? c.entry    : null,
          c.sl       != null ? c.sl       : null,
          c.t1       != null ? c.t1       : null,
          c.t2       != null ? c.t2       : null,
          q.basis    != null ? q.basis    : null,
          nowIso,
          scanMode,
          q.dataBasisReason != null ? q.dataBasisReason : null,
          q.edgeWeighted != null ? q.edgeWeighted.edgeWeightedScore : null,
          q.edgeWeighted != null ? q.edgeWeighted.confidenceFactor : null
        ));
      }
      if (fstmts.length) await env.QE_DB.batch(fstmts);
    }
  } catch (e) { console.warn("[E1 forward-track] non-fatal:", e && e.message); }

  // Write candidates to KV — this is what the browser reads in Part 3
  try {
    await env.KITE_STORE.put("qe_pipe_candidates", JSON.stringify(candidates));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 9: Write signals to KV (browser-readable) ─────────────────────────
  // Signals are a trimmed version of candidates with HMAC-ready fields
  const signalsForKv = candidates.map(function(c) {
    return {
      symbol:         c.symbol,
      sector:         c.sector,
      discoveryScore: c.discoveryScore,
      rsScore:        c.rsScore,
      dsBreakdown:    c.dsBreakdown,
      lastClose:      c.lastClose,
      entry:          c.entry,
      sl:             c.sl,
      t1:             c.t1,
      t2:             c.t2,
      atr:            c.atr,
      atrPct:         c.atrPct,
      rsi14:          c.rsi14,
      adx14:          c.adx14,
      stBull:         c.stBull,
      volRatio:       c.volRatio,
      pctAboveE20:    c.pctAboveE20,
      prox52w:        c.prox52w,
      builtAt:        c.builtAt,
      source:         "pipeline_v4",
      // Fix 3: Pipeline-time regime snapshot — browser uses this for deterministic scoring
      pipelineRegime: pipelineRegime,
      // deepResult filled in by browser via POST /pipe/deep-result
      deepResult:     null,
    };
  });

  try {
    await env.KITE_STORE.put("qe_pipe_signals", JSON.stringify(signalsForKv));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 10: Survivorship write ────────────────────────────────────────────
  try {
    await env.KITE_STORE.put("qe_pipe_survivorship",
      JSON.stringify(survive.getAll().slice(0, 1000)));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Commit 2: persist dedup set + per-symbol metrics for recalibration ──────
  // Mark every symbol analysed THIS run into today's dedup map, storing its
  // momentum score (used as freshness baseline for later runs today).
  // candidates carry discoveryScore; selected[] carries momentum + rank.
  try {
    const dsBySym = {};
    for (let ci = 0; ci < candidates.length; ci++) {
      dsBySym[candidates[ci].symbol] = candidates[ci];
    }
    // Update dedup map: only symbols that PASSED (became candidates) are marked
    // "analysed today". Recall fix (11-Jun): previously every SELECTED symbol (all
    // that entered OHLCV) was marked, so a stock that FAILED the morning run was
    // deduped and never re-examined — even if it broke out cleanly by the afternoon
    // run (the freshest early-confirmed-breakout archetype). Now a failed symbol
    // stays eligible for later runs; it must still clear every S5/S6 filter to
    // signal, so this is pure recall gain with zero precision cost. Symbols absent
    // from the map get freshness 100 next run (correct: a fresh setup deserves a
    // full-priority look). dsBySym (built above) holds exactly the passing set.
    // skipDedup (manual): do NOT write — a manual re-run must not consume the
    // crons' daily coverage pool or affect their dedup state.
    if (!skipDedup) {
      for (let si = 0; si < selected.length; si++) {
        if (dsBySym[selected[si].sym]) {                       // only passers
          analysedToday[selected[si].sym] = selected[si].momentum;
        }
      }
      await pipeSaveAnalysedToday(env, analysedToday);
    }

    // Append per-symbol metrics to a daily metrics log for 2–4 week evaluation.
    // Records: ts, symbol, momentumScore, momentumRank, quantEdgeScore, decision.
    const metricsKey = "qe_metrics_" + (new Date(Date.now() + 5.5*60*60*1000)).toISOString().slice(0,10).replace(/-/g,"");
    let metricsLog = [];
    try {
      const raw = await env.KITE_STORE.get(metricsKey);
      if (raw) metricsLog = JSON.parse(raw);
    } catch (_) {}
    const nowIso = new Date().toISOString();
    for (let si = 0; si < selected.length; si++) {
      const r = selected[si];
      const cand = dsBySym[r.sym];
      let decision = "IGNORE"; // not a candidate after full analysis
      if (cand) {
        // v4.30: unified alert rule (same as pipeDispatchTelegram) — BUY iff QE verdict BUY AND RS-adjusted score >= 70.
        const _qeScore = (cand.qe && cand.qe.qeScore != null) ? cand.qe.qeScore : 0;
        const gatePass = !!(cand.qe && cand.qe.pass) && _qeScore >= 70;
        decision = gatePass ? "BUY" : "WAIT";
      }
      metricsLog.push({
        ts:        nowIso,
        runId:     runId,
        symbol:    r.sym,
        momentum:  r.momentum,
        momRank:   si + 1,
        qeScore:   cand ? cand.discoveryScore : null,
        decision:  decision,
      });
    }
    await env.KITE_STORE.put(metricsKey, JSON.stringify(metricsLog.slice(-500)),
      { expirationTtl: 35 * 24 * 60 * 60 }); // keep ~5 weeks for recalibration
  } catch (e) { console.warn("[runFullPipeline] metrics persist non-fatal:", e && e.message); }

  // ── Stage 11: Telegram Dispatch ─────────────────────────────────────────────
  // Step C (v4.57): scanMode derived once at the top of runFullPipeline via scanModeOf(label).
  const signalCount = await pipeDispatchTelegram(env, qeDispatchList, audit, scanMode);

  // ── Final: Write audit + run summary ────────────────────────────────────────
  const completedAt = new Date().toISOString();
  const lastRun = {
    runId:          runId,
    runDate:        completedAt.slice(0, 10),
    startedAt:      startedAt,
    completedAt:    completedAt,
    universeCount:  universe.length,
    bhavCount:      Object.keys(bhav).length,
    streamAFast:    streamAFast.length,
    ohlcvQueue:     ohlcvQueue.length,
    ohlcvCapped:    ohlcvDropped.length,
    ohlcvFetched:   Object.keys(ohlcvMap).length,
    streamATech:    streamATech.length,
    rsPassCount:    rsRanked.length,
    streamACount:   sectorFiltered.length,
    candidateCount: candidates.length,
    qeGatePassed:   qeDispatchList.length,
    qeGateTally:    qeTally,
    discoveryNames: discoveryNames,
    bothNames:      bothNames,
    edgeWeightedEntries: edgeWeightedEntries.slice().sort(function(a,b){ return b.edgeWeightedScore - a.edgeWeightedScore; }),
    signalCount:    signalCount,
    survivorCount:  survive.getAll().length,
    niftyAvailable: niftyCloses !== null,
  };

  try {
    await env.KITE_STORE.put("qe_pipe_last_run", JSON.stringify(lastRun));
    await env.KITE_STORE.put("qe_pipe_audit",
      JSON.stringify(audit.getAll().slice(0, 500)));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  await writePipeStatus(env, "COMPLETED", 100, {
    runId:          runId,
    completedAt:    completedAt,
    candidateCount: candidates.length,
    signalCount:    signalCount,
  });

  audit.log("PIPELINE", "", "COMPLETE",
    "Run " + runId + " done. Candidates: " + candidates.length
    + " Signals: " + signalCount
    + " Survivors dropped: " + survive.getAll().length);

  return {
    ok:             true,
    runId:          runId,
    candidateCount: candidates.length,
    signalCount:    signalCount,
    stats:          lastRun,
    // For the manual-trigger completion message (names + scores + decision).
    candidates:     candidates.map(function(c) {
      return { symbol: c.symbol, discoveryScore: c.discoveryScore, sector: c.sector };
    }),
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE HTTP ROUTE HANDLERS
// ═══════════════════════════════════════════════════════════════════════════════

// GET /pipe/trigger — manually trigger full pipeline run
async function handlePipeTrigger(env) {
  // Check token first
  let token;
  try { token = await getToken(env); } catch (e) {
    return corsErr("Kite token missing — login at /login first: " + e.message, 401);
  }

  // v4.23: RUN-LOCK — prevents a DUPLICATE manual run (and duplicate signal
  // dispatch). The /pipe/trigger GET is held open ~30-40s while the pipeline runs
  // synchronously; a network-layer retry of that idempotent GET (observed: two
  // "Re-scan started" heartbeats one run apart) would otherwise slip past the
  // status check during the pre-STARTING window and fire a SECOND runPipelineWith-
  // Summary (→ second heartbeat + second Telegram signal batch). We take a short-TTL
  // KV lock BEFORE the heartbeat (which lives inside runPipelineWithSummary) and
  // reject any trigger that finds the lock held. NOTE: KV is eventually consistent
  // and has no atomic compare-and-set, so this closes the realistic seconds-apart
  // retry — not a sub-millisecond simultaneous race (that would need a Durable
  // Object; revisit only if such duplicates are ever observed).
  const LOCK_KEY = "qe_pipe_lock";
  const LOCK_TTL = 180; // seconds; comfortably longer than a normal 30-40s run

  // 1) Reject if a manual run-lock is already held (and still fresh).
  try {
    const lockRaw = await env.KITE_STORE.get(LOCK_KEY);
    if (lockRaw) {
      const lock = JSON.parse(lockRaw);
      const lockAgeMs = Date.now() - (lock.ts || 0);
      if (lockAgeMs < LOCK_TTL * 1000) {
        return cors({
          status:  "already_running",
          message: "Pipeline run already in progress (started " + Math.round(lockAgeMs / 1000) + "s ago)",
          phase:   "LOCKED",
        });
      }
    }
  } catch (e) { console.warn("[handlePipeTrigger] lock read non-fatal:", e && e.message); }

  // 2) Reject if ANY pipeline (e.g. a cron) is mid-run. STARTING is a RUNNING state
  //    (v4.23 fix: it was previously in the OK-to-start list, leaving a gap).
  try {
    const statusRaw = await env.KITE_STORE.get("qe_pipe_status");
    if (statusRaw) {
      const status = JSON.parse(statusRaw);
      if (status.phase && !["COMPLETED", "FAILED", "COMPLETED_EMPTY"].includes(status.phase)) {
        const ageMs = Date.now() - new Date(status.updatedAt).getTime();
        // v4.26: phase-aware staleness. A real run leaves STARTING within seconds and
        // completes in ~60-90s. A status frozen in a running phase past these bounds is a
        // DEAD run (client disconnected mid-run -> Cloudflare cancelled it) and must NOT
        // block new triggers. The previous single 35-min window let a dead STARTING status
        // deadlock EVERY future manual trigger for over half an hour.
        const STALE_MS = status.phase === "STARTING" ? 3 * 60 * 1000 : 10 * 60 * 1000;
        if (ageMs < STALE_MS) {
          return cors({
            status:  "already_running",
            message: "Pipeline is already running: " + status.phase + " (" + Math.round(ageMs / 1000) + "s ago)",
            phase:   status.phase,
            pct:     status.pct,
          });
        }
        // Stale/dead run — fall through and start fresh (self-healing).
        console.warn("[handlePipeTrigger] overriding stale " + status.phase + " status " + Math.round(ageMs / 1000) + "s old");
      }
    }
  } catch (e) { console.warn("[handlePipeTrigger] non-fatal:", e && e.message); }

  // 3) Acquire the lock BEFORE runPipelineWithSummary fires its heartbeat.
  try {
    await env.KITE_STORE.put(LOCK_KEY, JSON.stringify({ ts: Date.now(), src: "manual" }),
      { expirationTtl: LOCK_TTL });
  } catch (e) { console.warn("[handlePipeTrigger] lock write non-fatal:", e && e.message); }

  // 4) Run the EXACT cron path: runPipelineWithSummary → runFullPipeline → … →
  //    Telegram. Identical function the 09:30/12:00/14:30 crons call; no alternate
  //    path. Synchronous here (manual). The lock is ALWAYS released in finally so a
  //    legitimate next run is never blocked for the full TTL.
  try {
    const result = await runPipelineWithSummary(env, "MANUAL UI trigger");
    if (!result || result.ok === false) {
      return corsErr("Pipeline error: " + ((result && result.error) || "unknown"), 500);
    }
    return cors({
      status:          "success",
      run_id:          result.runId,
      candidate_count: result.candidateCount,
      signal_count:    result.signalCount,
      stats:           result.stats,
      message:         `Pipeline complete. ${result.candidateCount} candidates, ${result.signalCount} signals sent.`,
    });
  } catch (e) {
    return corsErr("Pipeline error: " + e.message, 500);
  } finally {
    try { await env.KITE_STORE.delete(LOCK_KEY); } catch (_) {}
  }
}

// GET /pipe/status — current pipeline run status
async function handlePipeStatus(env) {
  try {
    const statusRaw  = await env.KITE_STORE.get("qe_pipe_status");
    const lastRunRaw = await env.KITE_STORE.get("qe_pipe_last_run");
    const runId      = await env.KITE_STORE.get("qe_pipe_run_id");

    const status  = statusRaw  ? JSON.parse(statusRaw)  : null;
    const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;

    return cors({
      status:   "success",
      current:  status,
      last_run: lastRun,
      run_id:   runId,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/regime — single-source-of-truth market regime snapshot (browser reads
// this so the manual scan scores under the SAME regime as cron/discovery/gate/Telegram)
async function handlePipeRegime(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_regime");
    if (!raw) {
      return cors({ status: "empty", regime: null,
        message: "No regime snapshot yet — run the pipeline once to publish qe_regime." });
    }
    return cors({ status: "success", regime: JSON.parse(raw) });
  } catch (e) {
    return corsErr("Regime read error: " + e.message, 500);
  }
}

// GET /pipe/signals — read completed signals from KV (browser polls this)
async function handlePipeSignals(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_signals");
    const signals = raw ? JSON.parse(raw) : [];
    const lastRunRaw = await env.KITE_STORE.get("qe_pipe_last_run");
    const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;
    return cors({
      status:   "success",
      count:    signals.length,
      signals:  signals,
      last_run: lastRun ? { runId: lastRun.runId, runDate: lastRun.runDate, completedAt: lastRun.completedAt } : null,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/candidates — read pre-deep-analysis candidates
async function handlePipeCandidates(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_candidates");
    const candidates = raw ? JSON.parse(raw) : [];
    return cors({
      status:     "success",
      count:      candidates.length,
      candidates: candidates,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/streamb/debug — read-only Stream B execution proof (Commit 4.5)
// Pure read of qe_pipe_stream_b. No writes. Does not touch Stream A or Stream B logic.
async function handlePipeStreamBDebug(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_stream_b");
    if (!raw) {
      return cors({
        status: "success",
        qe_pipe_stream_b_exists: false,
        message: "qe_pipe_stream_b not found — pipeline has not run since Stream B was deployed.",
      });
    }
    const sb = JSON.parse(raw);
    const cands = Array.isArray(sb.candidates) ? sb.candidates : [];
    return cors({
      status: "success",
      qe_pipe_stream_b_exists: true,
      pipeline_run_timestamp: sb.generated_utc || null,
      run_id: sb.run_id || null,
      bhav_universe_count: (sb.bhav_count !== undefined) ? sb.bhav_count : null,
      ohlcv_map_count: (sb.ohlcv_map_count !== undefined) ? sb.ohlcv_map_count : null,
      stream_b_evaluated_count: (sb.evaluated_count !== undefined) ? sb.evaluated_count : null,
      stream_b_candidate_count: (sb.count !== undefined) ? sb.count : cands.length,
      hgs_populated: cands.length > 0 ? cands.every(function(c){ return typeof c.hgs === "number"; }) : null,
      top_10_candidates: cands.slice(0, 10).map(function(c){
        return { symbol: c.symbol, hgs: c.hgs, last: c.last,
                 pctBelow52w: c.pctBelow52w, volRatio: c.volRatio, mtv: c.mtv,
                 rsi: c.rsi, adx: c.adx };
      }),
      error_in_run: sb.error || null,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/momentum/debug — TEMPORARY (Commit 2 validation)
// Scores ALL Stream-A-Fast survivors with the SAME production MomentumScore +
// freshness logic, returns full distribution + ranked table. Read-only: no
// history fetch, no signal write, no Telegram. Cost = bhav quotes only.
async function handleMomentumDebug(env) {
  try {
    let token;
    try { token = await getToken(env); }
    catch (e) { return corsErr("Kite token missing — login first: " + e.message, 401); }

    const audit   = makePipeAudit();
    const survive = makeSurvivorshipLog();

    const universe = await pipeLoadUniverse(env, audit);
    if (!universe) return corsErr("Universe not in KV — run /universe/refresh", 500);

    const bhav        = await pipeBhavCopy(env, token, universe, audit, survive);
    const bhavSymbols = universe.filter(function(s) { return !!bhav[s]; });
    const streamAFast = pipeStreamAFast(bhav, bhavSymbols, audit, survive);

    const analysedToday = await pipeLoadAnalysedToday(env);

    // SAME ranker as production — single source of truth, cannot drift.
    const rankedRaw = pipeRankCandidates(streamAFast, bhav, analysedToday);

    // Display-only: round for readability + attach rank index. Does NOT alter
    // scoring — operates on the output of the shared production ranker.
    const ranked = rankedRaw.map(function(r, i) {
      return { sym: r.sym, momentum: r.momentum,
               freshness: Math.round(r.freshness * 10) / 10,
               finalRank: Math.round(r.finalRank * 10) / 10,
               rank: i + 1 };
    });

    // Distribution statistics on raw MomentumScore
    const scores = ranked.map(function(r) { return r.momentum; }).slice().sort(function(a, b) { return a - b; });
    const n = scores.length;
    const pct = function(p) {
      if (n === 0) return null;
      const idx = Math.min(n - 1, Math.floor((p / 100) * n));
      return scores[idx];
    };
    const sum  = scores.reduce(function(a, b) { return a + b; }, 0);
    const mean = n ? sum / n : 0;
    const variance = n ? scores.reduce(function(a, b) { return a + (b - mean) * (b - mean); }, 0) / n : 0;
    const median = n ? (n % 2 ? scores[(n - 1) / 2] : (scores[n / 2 - 1] + scores[n / 2]) / 2) : 0;

    // Cutoff analysis at PIPE_HISTORY_BUDGET (raw-only, ignoring dedup, for distribution view)
    const r36 = ranked[PIPE_HISTORY_BUDGET - 1] || null;
    const r37 = ranked[PIPE_HISTORY_BUDGET] || null;
    const cutoffScore = r36 ? r36.finalRank : null;
    const within2 = (cutoffScore !== null)
      ? ranked.filter(function(r) { return Math.abs(r.finalRank - cutoffScore) <= 2; }).length : 0;

    // Raw-momentum-only top N vs finalRank top N (freshness impact)
    const byRaw = ranked.slice().sort(function(a, b) { return b.momentum - a.momentum; });
    const rawTopSet   = new Set(byRaw.slice(0, PIPE_HISTORY_BUDGET).map(function(r) { return r.sym; }));
    const finalTopSet = new Set(ranked.slice(0, PIPE_HISTORY_BUDGET).map(function(r) { return r.sym; }));
    const addedByFresh   = ranked.slice(0, PIPE_HISTORY_BUDGET).filter(function(r) { return !rawTopSet.has(r.sym); }).map(function(r){return r.sym;});
    const removedByFresh = byRaw.slice(0, PIPE_HISTORY_BUDGET).filter(function(r) { return !finalTopSet.has(r.sym); }).map(function(r){return r.sym;});
    const overlap = PIPE_HISTORY_BUDGET - addedByFresh.length;

    const payload = {
      status: "success",
      generated_utc: new Date().toISOString(),
      eligible_count: ranked.length,
      already_analysed_today: Object.keys(analysedToday).length,
      history_budget: PIPE_HISTORY_BUDGET,
      distribution: {
        min: scores[0] || 0, max: scores[n - 1] || 0,
        mean: Math.round(mean * 10) / 10, median: median,
        stddev: Math.round(Math.sqrt(variance) * 10) / 10,
        p50: pct(50), p75: pct(75), p90: pct(90), p95: pct(95), p99: pct(99),
      },
      cutoff: {
        rank36_finalRank: r36 ? r36.finalRank : null,
        rank36_momentum:  r36 ? r36.momentum : null,
        rank37_finalRank: r37 ? r37.finalRank : null,
        rank37_momentum:  r37 ? r37.momentum : null,
        spread_r1_r36:    (ranked[0] && r36) ? Math.round((ranked[0].finalRank - r36.finalRank) * 10) / 10 : null,
        spread_r10_r36:   (ranked[9] && r36) ? Math.round((ranked[9].finalRank - r36.finalRank) * 10) / 10 : null,
        spread_r36_r50:   (r36 && ranked[49]) ? Math.round((r36.finalRank - ranked[49].finalRank) * 10) / 10 : null,
        within_2pts_of_cutoff: within2,
      },
      freshness_impact: {
        overlap_raw_vs_final: overlap,
        overlap_pct: Math.round((overlap / PIPE_HISTORY_BUDGET) * 100),
        added_by_freshness: addedByFresh,
        removed_by_freshness: removedByFresh,
      },
      top50: ranked.slice(0, 50).map(function(r) {
        return { rank: r.rank, symbol: r.sym, momentum: r.momentum,
                 freshness: r.freshness, finalRank: r.finalRank };
      }),
    };

    // Persist for later inspection (TTL 7 days)
    try {
      await env.KITE_STORE.put("qe_momentum_debug", JSON.stringify(payload),
        { expirationTtl: 7 * 24 * 60 * 60 });
    } catch (_) {}

    return cors(payload);
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/audit — pipeline audit log for last run
async function handlePipeAudit(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_audit");
    const entries = raw ? JSON.parse(raw) : [];
    return cors({
      status:  "success",
      count:   entries.length,
      entries: entries,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/survivorship — eliminated stocks with reasons
async function handlePipeSurvivorship(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_survivorship");
    const entries = raw ? JSON.parse(raw) : [];

    // Group by stage for summary
    const byStage = {};
    for (let i = 0; i < entries.length; i++) {
      const e = entries[i];
      if (!byStage[e.stage]) byStage[e.stage] = 0;
      byStage[e.stage]++;
    }

    return cors({
      status:    "success",
      total:     entries.length,
      by_stage:  byStage,
      entries:   entries,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// C (17-Jun v4.40): GET /pipe/rejects — read-only focused view of S5_STREAM_A_TECH
// rejections, splitting "volume near-miss" (cleared every gate except the volume
// ratio, reason begins "Volume ratio" — e.g. IIFLCAPS) from other technical rejects.
// Pure read of the existing qe_pipe_survivorship log. Changes NO filter/threshold/
// score/verdict; introduces no new data — only surfaces what the pipeline already logs.
async function handlePipeRejects(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_survivorship");
    const entries = raw ? JSON.parse(raw) : [];
    const s5 = entries.filter(function (e) { return e.stage === "S5_STREAM_A_TECH"; });
    const volNearMiss = [];
    const otherTech   = [];
    for (let i = 0; i < s5.length; i++) {
      const e = s5[i];
      if (/^Volume ratio/.test(e.reason || "")) volNearMiss.push(e);
      else otherTech.push(e);
    }
    return cors({
      status:               "success",
      s5_total:             s5.length,
      volume_near_miss:     volNearMiss.length,
      other_tech_rejects:   otherTech.length,
      volume_near_miss_list: volNearMiss,
      other_tech_list:      otherTech,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// POST /pipe/deep-result — browser posts completed deep analysis result per symbol
// Browser runs analyseWithRetry(symbol) and posts the score/verdict/bt/mc result here.
// Worker merges it into qe_pipe_signals so the final signal has both DS and QS.
async function handlePipeDeepResult(request, env) {
  const authErr = await requireApiAuth(request, env); if (authErr) return authErr; // A4a
  let body;
  try { body = await request.json(); } catch (_) {
    return corsErr("Invalid JSON");
  }

  const { symbol, quantScore, verdict, bt, mc, entry, sl, t1, t2, targetDays } = body;
  if (!symbol) return corsErr("Required: symbol");

  try {
    // Read current signals
    const raw     = await env.KITE_STORE.get("qe_pipe_signals");
    const signals = raw ? JSON.parse(raw) : [];

    // Find this symbol in signals
    let found = false;
    for (let i = 0; i < signals.length; i++) {
      if (signals[i].symbol === symbol.toUpperCase()) {
        signals[i].deepResult = {
          quantScore:  quantScore  || null,
          verdict:     verdict     || null,
          bt:          bt          || null,
          mc:          mc          || null,
          entry:       entry       || signals[i].entry,
          sl:          sl          || signals[i].sl,
          t1:          t1          || signals[i].t1,
          t2:          t2          || signals[i].t2,
          targetDays:  targetDays  || null,
          analysedAt:  new Date().toISOString(),
        };
        found = true;
        break;
      }
    }

    if (!found) {
      return corsErr("Symbol not found in current pipeline signals: " + symbol, 404);
    }

    await env.KITE_STORE.put("qe_pipe_signals", JSON.stringify(signals));

    return cors({
      status:  "success",
      symbol:  symbol.toUpperCase(),
      message: "Deep result merged into pipeline signals",
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SCREENER.IN FUNDAMENTAL PARSER (server-side, Worker)
// Parses PE, ROE, Revenue Growth, Profit Growth, D/E, Market Cap from HTML.
// Cloudflare Workers have no DOM — uses regex on raw HTML instead of DOMParser.
// ═══════════════════════════════════════════════════════════════════════════════
function parseScreenerFundamentals(html, symbol) {
  try {
    var pe     = null;
    var roe    = null;
    var de     = null;
    var revGr  = null;
    var profGr = null;
    var mcap   = null;
    var industry = null;

    // ── Industry label ────────────────────────────────────────────────────────
    // Screener page contains: <a href="/screen/raw/?query=Industry+Name+%3D+...">Label</a>
    // The query param is the unique anchor — extract the link text as the label.
    var indMatch = /screen\/raw\/[^"]*[Ii]ndustry[^"]*"[^>]*>([^<]+)<\/a>/i.exec(html);
    if (indMatch) industry = indMatch[1].trim();

    // ── Key ratios: scan #top-ratios li blocks ────────────────────────────────
    // Pattern: <li ...><span class="name">Label</span>...<span class="number">Value</span>
    var ratioPattern = /<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*name[^"]*"[^>]*>([\s\S]*?)<\/span>[\s\S]*?<span[^>]*class="[^"]*number[^"]*"[^>]*>([\s\S]*?)<\/span>/gi;
    var match;
    while ((match = ratioPattern.exec(html)) !== null) {
      var lbl = match[1].replace(/<[^>]+>/g, '').trim().toLowerCase();
      var raw = match[2].replace(/<[^>]+>/g, '').replace(/,/g, '').trim();
      var val = parseFloat(raw);
      if (isNaN(val)) continue;

      if (/^stock p\/e$|^p\/e$/i.test(lbl))         pe  = val;
      if (/return on equity/i.test(lbl))             roe = val;
      if (/debt\s*\/\s*equity|debt to equity/i.test(lbl)) de = val;
      if (/market cap/i.test(lbl))                   mcap = val * 1e7; // Screener shows Cr
    }

    // ── Revenue & Profit Growth: scan P&L table rows ──────────────────────────
    // Find Sales/Revenue row and Net Profit row — get last 2 non-zero numeric values
    function extractGrowth(rowPattern) {
      var rowMatch = rowPattern.exec(html);
      if (!rowMatch) return null;
      var rowHtml = rowMatch[0];
      var cellPattern = /<td[^>]*>([\s\S]*?)<\/td>/gi;
      var cells = [];
      var cellMatch;
      while ((cellMatch = cellPattern.exec(rowHtml)) !== null) {
        var txt = cellMatch[1].replace(/<[^>]+>/g, '').replace(/,/g, '').trim();
        var num = parseFloat(txt);
        if (!isNaN(num) && num !== 0) cells.push(num);
      }
      if (cells.length < 2) return null;
      var curr = cells[cells.length - 1];
      var prev = cells[cells.length - 2];
      if (prev === 0) return null;
      return parseFloat(((curr - prev) / Math.abs(prev) * 100).toFixed(1));
    }

    // Sales/Revenue row
    var salesPattern = /<tr[^>]*>[\s\S]*?<td[^>]*>\s*(?:Sales|Revenue)\s*<\/td>([\s\S]*?)<\/tr>/i;
    revGr = extractGrowth(salesPattern);

    // Net Profit row
    var profitPattern = /<tr[^>]*>[\s\S]*?<td[^>]*>\s*(?:Net Profit|PAT)\s*<\/td>([\s\S]*?)<\/tr>/i;
    profGr = extractGrowth(profitPattern);

    // Return null if nothing was found
    if (pe === null && roe === null && revGr === null && profGr === null && de === null) {
      return null;
    }

    return { pe: pe, roe: roe, revGr: revGr, profGr: profGr, de: de, mcap: mcap,
             industry: industry, _source: "screener_worker" };
  } catch (_) {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN FETCH HANDLER — all v3.1 routes preserved exactly
// ═══════════════════════════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE ENGINE — P0 (Holdings Ingest & Visibility)
// Additive · feature-flagged (PORTFOLIO_INTEL_ENABLED, default OFF) · zero Buy-Engine impact.
// Data-only: no scoring, no verdict, no alerts. Frozen Architecture v1.0. Baseline kite.js v4.58.
// Kite v3 portfolio schema verified against kite.trade/docs/connect/v3/portfolio.
// ══════════════════════════════════════════════════════════════════════════════

async function pieFlagOn(env) {
  try { return (await env.KITE_STORE.get("PORTFOLIO_INTEL_ENABLED")) === "1"; }
  catch (_) { return false; }
}

async function pieLogStatus(env, obj) {
  try { await env.KITE_STORE.put("qe_holdings_last_run", JSON.stringify(obj)); } catch (_) {}
}

function pieNum(v, fb) { const n = Number(v); return isFinite(n) ? n : fb; }

// Data-source adapter boundary (ADR-012). Today: Kite. Returns {ok, rows} | {ok:false, reason}.
async function kiteFetchHoldings(token) {
  let r;
  try { r = await kiteRequest("GET", "/portfolio/holdings", null, token); }
  catch (_) { return { ok: false, reason: "NETWORK" }; }
  if (!r || !r.ok) return { ok: false, reason: "HTTP_" + ((r && r.status) || "ERR") };
  const rows = (r.data && Array.isArray(r.data.data)) ? r.data.data : null;
  if (rows === null) return { ok: false, reason: "SCHEMA" };
  return { ok: true, rows };
}

async function kiteFetchPositions(token) {
  let r;
  try { r = await kiteRequest("GET", "/portfolio/positions", null, token); }
  catch (_) { return { ok: false, reason: "NETWORK" }; }
  if (!r || !r.ok) return { ok: false, reason: "HTTP_" + ((r && r.status) || "ERR") };
  const net = (r.data && r.data.data && Array.isArray(r.data.data.net)) ? r.data.data.net : null;
  if (net === null) return { ok: false, reason: "SCHEMA" };
  return { ok: true, rows: net };
}

// Pure, deterministic. Union equity holdings + open equity positions (qty != 0).
// Holdings total = quantity + t1_quantity (T+2 settled + T+1 settling, per Kite docs).
// Positions (NSE/BSE only) override same-symbol holdings. Dedupe by symbol; sorted.
function normalizePortfolio(holdings, positions) {
  const map = new Map();
  const rowOf = (sym, isin, qty, avg, ltp, src) => ({
    symbol: sym, isin: isin || null, qty: qty, avg_price: avg, ltp: ltp,
    pnl_pct: avg > 0 ? Math.round(((ltp - avg) / avg) * 10000) / 100 : null, source: src
  });
  for (const h of (holdings || [])) {
    if (!h || !h.tradingsymbol) continue;
    const qty = pieNum(h.quantity, 0) + pieNum(h.t1_quantity, 0);
    if (qty === 0) continue;
    map.set(h.tradingsymbol, rowOf(h.tradingsymbol, h.isin, qty,
      pieNum(h.average_price, 0), pieNum(h.last_price, 0), "HOLDINGS"));
  }
  for (const p of (positions || [])) {
    if (!p || !p.tradingsymbol) continue;
    if (p.exchange !== "NSE" && p.exchange !== "BSE") continue; // equity-only (exclude NFO/MCX)
    const qty = pieNum(p.quantity, 0);
    if (qty === 0) continue; // closed intraday
    const prev = map.get(p.tradingsymbol);
    map.set(p.tradingsymbol, rowOf(p.tradingsymbol, p.isin || (prev && prev.isin) || null, qty,
      pieNum(p.average_price, 0), pieNum(p.last_price, 0), "POSITIONS"));
  }
  return Array.from(map.values()).sort((a, b) =>
    a.symbol < b.symbol ? -1 : (a.symbol > b.symbol ? 1 : 0));
}

// Idempotent sync: after this, qe_holdings == exactly `rows`.
// UPSERT (PK=symbol → no dupes) + DELETE absent (→ no stale). D1 batch = atomic (no partial writes).
async function pieSyncHoldings(env, rows) {
  const ts = new Date().toISOString();
  const stmts = [];
  for (const r of rows) {
    stmts.push(env.QE_DB.prepare(
      "INSERT INTO qe_holdings (symbol,isin,asset_class,qty,avg_price,ltp,pnl_pct,source,updated_ts) " +
      "VALUES (?,?,'EQUITY',?,?,?,?,?,?) " +
      "ON CONFLICT(symbol) DO UPDATE SET isin=excluded.isin,qty=excluded.qty,avg_price=excluded.avg_price," +
      "ltp=excluded.ltp,pnl_pct=excluded.pnl_pct,source=excluded.source,updated_ts=excluded.updated_ts"
    ).bind(r.symbol, r.isin, r.qty, r.avg_price, r.ltp, r.pnl_pct, r.source, ts));
  }
  if (rows.length) {
    const ph = rows.map(() => "?").join(",");
    stmts.push(env.QE_DB.prepare("DELETE FROM qe_holdings WHERE symbol NOT IN (" + ph + ")")
      .bind(...rows.map(r => r.symbol)));
  } else {
    stmts.push(env.QE_DB.prepare("DELETE FROM qe_holdings"));
  }
  await env.QE_DB.batch(stmts);
  return { count: rows.length, ts };
}

async function runHoldingsIngest(env) {
  if (!(await pieFlagOn(env))) return { ok: false, skipped: "FLAG_OFF" };
  let token;
  try { token = await getToken(env); }
  catch (_) { await pieLogStatus(env, { ok: false, reason: "NO_TOKEN", ts: new Date().toISOString() }); return { ok: false, reason: "NO_TOKEN" }; }
  const h = await kiteFetchHoldings(token);
  const p = await kiteFetchPositions(token);
  if (!h.ok || !p.ok) {                                    // require BOTH valid → no partial writes
    const reason = !h.ok ? ("HOLDINGS_" + h.reason) : ("POSITIONS_" + p.reason);
    await pieLogStatus(env, { ok: false, reason, ts: new Date().toISOString() });
    return { ok: false, reason };                           // last-good state preserved; recovers next run
  }
  let rows;
  try { rows = normalizePortfolio(h.rows, p.rows); }
  catch (_) { await pieLogStatus(env, { ok: false, reason: "NORMALIZE_ERR", ts: new Date().toISOString() }); return { ok: false, reason: "NORMALIZE_ERR" }; }
  try {
    const res = await pieSyncHoldings(env, rows);
    await pieLogStatus(env, { ok: true, count: res.count, ts: res.ts });
    return { ok: true, count: res.count };
  } catch (_) {
    await pieLogStatus(env, { ok: false, reason: "DB_ERR", ts: new Date().toISOString() });
    return { ok: false, reason: "DB_ERR" };                 // batch atomic → no partial write
  }
}

async function handlePortfolioStatus(env) {
  try {
    const q = await env.QE_DB.prepare(
      "SELECT symbol,isin,qty,avg_price,ltp,pnl_pct,source,updated_ts FROM qe_holdings ORDER BY symbol"
    ).all();
    let last = null;
    try { const raw = await env.KITE_STORE.get("qe_holdings_last_run"); last = raw ? JSON.parse(raw) : null; } catch (_) {}
    return cors({ ok: true, holdings: (q && q.results) || [], last_run: last });
  } catch (e) { return corsErr("portfolio_status_failed: " + e.message, 500); }
}

async function handlePortfolioRefresh(env) {
  return cors(await runHoldingsIngest(env));
}



// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE ENGINE — P1 (Deterministic Scoring · SHADOW MODE)
// Additive · flag-gated (PORTFOLIO_INTEL_ENABLED) · zero Buy-Engine impact · Claude-free score (ADR-009).
// Health Score (5 pillars) + Data Confidence GATE + hard triggers + INSUFFICIENT_DATA + Portfolio Memory.
// Production-dependent parameters are EXTERNALIZED to PIE_CONFIG (versioned, PROVISIONAL defaults).
// SHADOW: writes qe_holdings verdict cols + qe_holding_history. NO Telegram/alerts (alerts = P2).
// DEV-COMPLETE ≠ PRODUCTION-VALIDATED: provisional defaults require shadow calibration before RC final.
// ══════════════════════════════════════════════════════════════════════════════

const PIE_SCORE_VERSION = "pie-score-v1";
const PIE_CONFIG = {                       // versioned config — production tunes THIS, not the code
  version: "pie-cfg-v1",
  status: "PROVISIONAL",                   // → "CALIBRATED" after production shadow validation (P5)
  weights: {                               // pillar weights per regime (dynamic across, fixed within — ADR-007)
    "DEFAULT":  { trend:0.35, momentum:0.20, edge:0.20, risk:0.15, fundnews:0.10 },
    "RISK-OFF": { trend:0.30, momentum:0.15, edge:0.25, risk:0.20, fundnews:0.10 },
    "BULL":     { trend:0.40, momentum:0.25, edge:0.15, risk:0.10, fundnews:0.10 }
  },
  triggers:   { timeStopDays:20, timeStopR:0.5, volSpikeAtrMult:2.0 },
  confidence: { insufficientBelow:40, clampBelow:60, maxPriceAgeDays:4, minBars:50 },
  bands:      { strongHold:75, hold:60, watch:45, reduce:30 }
};

function pieClamp(x, lo, hi){ x = Number(x); if(!isFinite(x)) x = lo; return x<lo?lo:(x>hi?hi:x); }
function pieWeightsForRegime(regime){ return PIE_CONFIG.weights[regime] || PIE_CONFIG.weights.DEFAULT; }

// ---- Pillars (deterministic; PROVISIONAL formulas, config-tunable) ----
function piePillarTrend(t){
  let s=0;
  if(t.ema20!=null&&t.ema50!=null&&t.ema200!=null&&t.c!=null){
    if(t.c>t.ema20) s+=25; if(t.ema20>t.ema50) s+=20; if(t.ema50>t.ema200) s+=20;
  }
  if(t.adx!=null) s+=pieClamp((t.adx-15)*1.2,0,20);
  if(t.stBull===true) s+=10;
  if(t.rs!=null&&t.rs>0) s+=5;
  return pieClamp(Math.round(s),0,100);
}
function piePillarMomentum(t){
  let s=50;
  if(t.rsi!=null){ if(t.rsi>=50&&t.rsi<=70) s+=25; else if(t.rsi>70) s+=10; else s-=15; }
  if(t.c!=null&&t.ema20!=null) s+=(t.c>t.ema20)?15:-15;
  return pieClamp(Math.round(s),0,100);
}
function piePillarEdge(ctx){
  if(ctx.signalScore==null||ctx.currentScore==null) return 50;   // no baseline → neutral (provisional)
  return pieClamp(Math.round(60+(ctx.currentScore-ctx.signalScore)),0,100);
}
function piePillarRisk(ctx){
  let s=50;
  if(ctx.ltp!=null&&ctx.trailStop!=null&&ctx.trailStop>0){
    s+=pieClamp(((ctx.ltp-ctx.trailStop)/ctx.ltp*100)*3,-30,30);
  }
  if(ctx.rMultiple!=null) s+=pieClamp(ctx.rMultiple*5,-10,15);
  if(ctx.peak!=null&&ctx.ltp!=null&&ctx.peak>0) s-=pieClamp((ctx.peak-ctx.ltp)/ctx.peak*100,0,20);
  return pieClamp(Math.round(s),0,100);
}
function piePillarFundNews(ctx){
  if(ctx.fundScore==null) return 50;                              // provisional neutral when unavailable
  let s=ctx.fundScore; if(ctx.eventRisk===true) s-=20;
  return pieClamp(Math.round(s),0,100);
}
function computeHealthScore(ctx, weights){
  const t=ctx.tech||{};
  const p={ trend:piePillarTrend(t), momentum:piePillarMomentum(t), edge:piePillarEdge(ctx),
            risk:piePillarRisk(ctx), fundnews:piePillarFundNews(ctx) };
  // F1: score ONLY pillars whose inputs are populated; renormalise weights over the active set so
  // no pillar is silently pinned at a neutral constant. Deterministic; identical to the full-weight
  // result when all inputs are present. edge needs a live re-score baseline; fundnews needs fundamentals.
  const active={ trend:true, momentum:true, risk:true,
                 edge:(ctx.currentScore!=null&&ctx.signalScore!=null),
                 fundnews:(ctx.fundScore!=null) };
  let wsum=0; for(const k in active) if(active[k]) wsum+=weights[k];
  let acc=0; if(wsum>0){ for(const k in active) if(active[k]) acc+=p[k]*(weights[k]/wsum); }
  return { score:pieClamp(Math.round(acc),0,100), pillars:p,
           active_pillars:Object.keys(active).filter(k=>active[k]) };
}

// ---- Data Confidence GATE (ADR-003): deterministic coverage checklist → 0-100 + typed flags ----
function computeDataConfidence(ctx){
  const flags=[]; let pts=0; const cfg=PIE_CONFIG.confidence; const t=ctx.tech||{};
  const add=(cond,w,flag)=>{ if(cond) pts+=w; else flags.push(flag); };
  add(ctx.priceAgeDays!=null&&ctx.priceAgeDays<=cfg.maxPriceAgeDays,20,"STALE_PRICE");
  add(ctx.bars!=null&&ctx.bars>=cfg.minBars,15,"INSUFFICIENT_BARS");
  add(t.ema20!=null&&t.ema50!=null&&t.ema200!=null,15,"EMA_UNCOMPUTED");
  add(t.rsi!=null,10,"RSI_UNCOMPUTED");
  add(t.adx!=null,10,"ADX_UNCOMPUTED");
  add(t.stBull!=null,10,"SUPERTREND_UNCOMPUTED");
  add(ctx.fundScore!=null,10,"FUNDAMENTALS_MISSING");
  add(ctx.corpActionPending!==true,10,"CORP_ACTION_PENDING");
  return { confidence:pieClamp(pts,0,100), flags };
}

// ---- Corporate-action pre-filter (ADR-006): short-circuit technicals + confidence flag ----
function corpActionFilter(ctx){
  // PROVISIONAL: pending list is externally supplied (KV) in production. Heuristic fallback:
  // an unexplained large overnight gap w/o volume is flagged for confidence (never scored as crash).
  if(ctx.corpActionPending===true){ ctx.tech={}; }   // wipe technicals → pillars go neutral, not bearish
  return ctx;
}

// ---- Hard triggers (override the score — ADR-002) ----
function evaluateHardTriggers(ctx){
  const fired=[]; const t=ctx.tech||{}; const cfg=PIE_CONFIG.triggers;
  if(ctx.ltp!=null&&ctx.trailStop!=null&&ctx.trailStop>0&&ctx.ltp<ctx.trailStop) fired.push("STOP_BREACH");
  if(t.c!=null&&t.ema50!=null&&t.c<t.ema50) fired.push("TREND_BREAK");
  if(t.stBull===false) fired.push("TREND_BREAK_ST");
  if(ctx.signalT1!=null&&ctx.ltp!=null&&ctx.ltp>=ctx.signalT1) fired.push("T1_HIT");
  if(ctx.daysHeld!=null&&ctx.rMultiple!=null&&ctx.daysHeld>cfg.timeStopDays&&ctx.rMultiple<cfg.timeStopR) fired.push("TIME_STOP");
  if(ctx.atrExpansion!=null&&ctx.atrExpansion>cfg.volSpikeAtrMult) fired.push("VOL_SPIKE");
  return fired;
}

// ---- Verdict: triggers → confidence gate → bands (deterministic) ----
function evaluateVerdict(health, triggers, confidence){
  const cfg=PIE_CONFIG; const c=confidence.confidence;
  if(triggers.includes("STOP_BREACH")) return { verdict:"EXIT", reason:"STOP_BREACH" };
  if(c<cfg.confidence.insufficientBelow) return { verdict:"INSUFFICIENT_DATA", reason:"LOW_CONFIDENCE" };
  if(triggers.some(x=>["TREND_BREAK","TREND_BREAK_ST","T1_HIT","TIME_STOP"].includes(x)))
    return { verdict:"REDUCE", reason:triggers.join(",") };
  let v;
  if(health>=cfg.bands.strongHold) v="STRONG_HOLD";
  else if(health>=cfg.bands.hold) v="HOLD";
  else if(health>=cfg.bands.watch) v="WATCH";
  else if(health>=cfg.bands.reduce) v="REDUCE";
  else v="EXIT";
  if(c<cfg.confidence.clampBelow && v==="STRONG_HOLD") v="HOLD";      // confidence clamp
  if(triggers.includes("VOL_SPIKE") && (v==="STRONG_HOLD"||v==="HOLD")) v="WATCH";
  return { verdict:v, reason:(v==="STRONG_HOLD"||v==="HOLD")?"HEALTHY":"SCORE_BAND" };
}

// ---- Portfolio Memory: deterministic conviction slope ----
function pieConvictionTrend(series){   // series oldest→newest of {health}
  if(!series||series.length<3) return "STABLE";
  const n=series.length; let sx=0,sy=0,sxy=0,sxx=0;
  for(let i=0;i<n;i++){ sx+=i; sy+=series[i].health; sxy+=i*series[i].health; sxx+=i*i; }
  const den=n*sxx-sx*sx; const slope=den? (n*sxy-sx*sy)/den : 0;
  return slope>1?"IMPROVING":(slope<-1?"DETERIORATING":"STABLE");
}
async function updatePortfolioMemory(env, symbol, snap){
  const day=(snap.ts||new Date().toISOString()).slice(0,10);
  await env.QE_DB.prepare(
    "INSERT INTO qe_holding_history (symbol,snapshot_date,ts,ltp,r_multiple,health_score,data_confidence,"+
    "pillar_trend,pillar_momentum,pillar_edge,pillar_risk,pillar_fundnews,verdict,score_version) "+
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "+
    "ON CONFLICT(symbol,snapshot_date) DO UPDATE SET ts=excluded.ts,ltp=excluded.ltp,r_multiple=excluded.r_multiple,"+
    "health_score=excluded.health_score,data_confidence=excluded.data_confidence,pillar_trend=excluded.pillar_trend,"+
    "pillar_momentum=excluded.pillar_momentum,pillar_edge=excluded.pillar_edge,pillar_risk=excluded.pillar_risk,"+
    "pillar_fundnews=excluded.pillar_fundnews,verdict=excluded.verdict,score_version=excluded.score_version"
  ).bind(symbol,day,snap.ts,snap.ltp,snap.rMultiple,snap.health,snap.confidence,
         snap.pillars.trend,snap.pillars.momentum,snap.pillars.edge,snap.pillars.risk,snap.pillars.fundnews,
         snap.verdict,PIE_SCORE_VERSION).run();
}

// ---- Technicals: reuse buy-engine indicator primitives (indicator SSOT) ----
function pieTechFromSeries(C,H,L,lastDate,bars){   // pure indicator composition (indicator SSOT)
  const ageDays=lastDate?Math.round((Date.now()-new Date(lastDate).getTime())/86400000):null;
  // v4.66 FIX: pipeEma/pipeRsi/pipeAdx/pipeAtr return SCALARS (or null); pipeSupertrend returns
  // { direction, value } (or null). Previous code read them as arrays (x&&x.length?x[x.length-1]:null),
  // so every scalar/object coerced to null -> all indicators UNCOMPUTED on every holding. Consume as-is.
  const val=(x)=> (x!=null && isFinite(x)) ? x : null;
  const ema=(p)=>{ try{ return val(pipeEma(C,p)); }catch(_){ return null; } };
  let rsi=null,adx=null,atr=null,stObj=null;
  try{ rsi=val(pipeRsi(C,14)); }catch(_){}
  try{ adx=val(pipeAdx(H,L,C,14)); }catch(_){}
  try{ atr=val(pipeAtr(H,L,C,14)); }catch(_){}
  try{ const s=pipeSupertrend(H,L,C,10,2); stObj=(s&&isFinite(s.value))?s:null; }catch(_){}
  const stBull = stObj==null ? null : (stObj.direction===1);
  const tech={ c:C[bars-1], ema20:ema(20), ema50:ema(50), ema200:ema(200), rsi, adx,
               stBull, atr, rs:null };
  return { tech, bars, priceAgeDays:ageDays };
}
// F2: best-effort live OHLCV fallback for holdings outside the scan-universe cache. Reuses the
// pipeline's own fetch + cached instrument-token map. Failure -> null -> INSUFFICIENT_DATA (never silent).
async function pieFetchHoldingCandles(env, symbol){
  try{
    const token=await getToken(env);
    let tokenMap={}; try{ const t=await env.KITE_STORE.get("qe_db_token_map"); if(t) tokenMap=JSON.parse(t); }catch(_){}
    const instrToken=tokenMap[symbol];
    if(!instrToken){ console.error("[pie:tech] no instrument token for "+symbol); return null; }
    const res=await pipeFetchNCandles(env, token, symbol, instrToken, PIPE_OHLCV_RANGE+10);
    const cd=(res&&res.candles)||[];
    if(cd.length<2){ console.error("[pie:tech] live fetch thin "+symbol+": "+((res&&res.reason)||"?")); return null; }
    return { C:cd.map(x=>x[4]), H:cd.map(x=>x[2]), L:cd.map(x=>x[3]), lastDate:cd[cd.length-1][0], bars:cd.length };
  }catch(e){ console.error("[pie:tech] live fetch failed "+symbol+": "+((e&&e.message)||e)); return null; }
}
async function computeHoldingTechnicals(env, symbol){
  let rows=[];
  try {
    const q=await env.QE_DB.prepare("SELECT bar_date,o,h,l,c,v FROM ohlcv_daily WHERE symbol=? ORDER BY bar_date").bind(symbol).all();
    rows=(q&&q.results)||[];
  } catch(e){ console.error("[pie:tech] ohlcv read failed "+symbol+": "+((e&&e.message)||e)); }
  const minBars=(PIE_CONFIG.confidence&&PIE_CONFIG.confidence.minBars)||50;
  if(rows.length>=minBars)
    return pieTechFromSeries(rows.map(r=>r.c),rows.map(r=>r.h),rows.map(r=>r.l),rows[rows.length-1].bar_date,rows.length);
  const live=await pieFetchHoldingCandles(env, symbol);            // F2 fallback when cache is thin
  if(live) return pieTechFromSeries(live.C,live.H,live.L,live.lastDate,live.bars);
  if(rows.length>=2)
    return pieTechFromSeries(rows.map(r=>r.c),rows.map(r=>r.h),rows.map(r=>r.l),rows[rows.length-1].bar_date,rows.length);
  return { tech:{}, bars:rows.length, priceAgeDays:null };          // -> INSUFFICIENT_DATA via confidence gate
}

// ---- Signal context join (ADR-013 spirit: by symbol; ISIN/buy-date reserved) ----
async function resolveSignalContext(env, h){
  const out={ signalScore:null, signalEntry:null, trailStop:null, signalT1:null, rMultiple:null,
              daysHeld:null, currentScore:null, peak:null, atrExpansion:null,
              fundScore:null, eventRisk:false, corpActionPending:false };
  try{
    const q=await env.QE_DB.prepare(
      "SELECT snapshot_date,score,entry,sl,t1,t2 FROM qe_forward_track WHERE symbol=? ORDER BY snapshot_date DESC LIMIT 1"
    ).bind(h.symbol).all();
    const r=q&&q.results&&q.results[0];
    if(r){
      out.signalScore=r.score; out.signalEntry=r.entry; out.trailStop=r.sl; out.signalT1=r.t1;
    }
  }catch(e){ console.error("[pie:signalctx] "+((e&&e.message)||e)); }
  if(out.trailStop==null && h.avg_price!=null) out.trailStop=Math.round(h.avg_price*0.92*100)/100; // synth 8% stop (provisional)
  // F3: R-multiple from the user's ACTUAL cost basis (avg_price), not the signal entry.
  if(h.avg_price!=null && out.trailStop!=null && h.avg_price>out.trailStop && h.ltp!=null){
    out.rMultiple=Math.round(((h.ltp-h.avg_price)/(h.avg_price-out.trailStop))*100)/100;
  }
  // F3: days_held from the EARLIEST signal date (approx entry). No signal -> null -> TIME_STOP stays off.
  try{
    const q2=await env.QE_DB.prepare("SELECT MIN(snapshot_date) AS first_date FROM qe_forward_track WHERE symbol=?").bind(h.symbol).all();
    const fd=q2&&q2.results&&q2.results[0]&&q2.results[0].first_date;
    if(fd) out.daysHeld=Math.round((Date.now()-new Date(fd).getTime())/86400000);
  }catch(e){ console.error("[pie:daysheld] "+((e&&e.message)||e)); }
  return out;
}

// ---- Shadow orchestrator: score every holding, write snapshots, NO alerts ----
async function runPortfolioIntelligenceShadow(env){
  if(!(await pieFlagOn(env))) return { ok:false, skipped:"FLAG_OFF" };
  let regime="DEFAULT";
  try{ const rg=await env.KITE_STORE.get("qe_regime"); if(rg){ const ro=JSON.parse(rg); regime=ro.regime||ro.label||"DEFAULT"; } }catch(_){}
  const weights=pieWeightsForRegime(regime);
  let holdings=[];
  try{ const q=await env.QE_DB.prepare("SELECT symbol,isin,qty,avg_price,ltp FROM qe_holdings ORDER BY symbol").all(); holdings=(q&&q.results)||[]; }
  catch(_){ return { ok:false, reason:"NO_HOLDINGS_TABLE" }; }
  const ts=new Date().toISOString(); let scored=0, errors=0;
  for(const h of holdings){
    try{
      const tech=await computeHoldingTechnicals(env,h.symbol);
      let ctx=Object.assign({ symbol:h.symbol, ltp:h.ltp, avg_price:h.avg_price }, await resolveSignalContext(env,h), tech);
      ctx=corpActionFilter(ctx);
      const conf=computeDataConfidence(ctx);
      const hs=computeHealthScore(ctx,weights);
      const trig=evaluateHardTriggers(ctx);
      await piePersist(env, h, ctx, hs, conf, trig, ts);  // P2: score+evidence+hysteresis+alert
      scored++;
    }catch(e){ errors++; console.error("[pie:score] "+((h&&h.symbol)||"?")+": "+((e&&e.message)||e)); }
  }
  try{ await computePortfolioRisk(env); }catch(_){}  // P3: portfolio-level aggregation
  try{ await env.KITE_STORE.put("qe_pie_shadow_last_run", JSON.stringify({ ok:true, scored, errors, regime, cfg:PIE_CONFIG.version, ts })); }catch(_){}
  return { ok:true, scored, errors, regime };
}

// ---- Combined cycle: ingest (P0) → shadow score (P1). Self-gated. ----
async function runPortfolioCycle(env){
  const ing=await runHoldingsIngest(env);
  if(ing&&ing.ok) await runPortfolioIntelligenceShadow(env);
  return ing;
}

// ── v4.67: Portfolio pipeline orchestrator (Path 1) ─────────────────────────────
// Ordered, single-responsibility stages. Event-alerts (published inside the scoring
// engine via piePersist→dispatchPortfolioAlert) and the Executive Digest (stage 5)
// are two INDEPENDENT consumers of the SAME persisted qe_holdings state. The scoring
// engine is untouched; this only sequences existing stage functions and adds the
// digest + audit stages as new consumers. runPortfolioCycle() retained (independently
// callable). Stages are sequential awaits so the digest always reads freshly-persisted
// rows (no race). Digest failure is isolated (try/catch, runs AFTER persist+alerts) so
// it can never lose scores or suppress alerts.
// ═══════════════════════════════════════════════════════════════════════════════
// EOD GTT VALIDATION (16-Jul-2026) — read-only, never modifies/cancels a GTT itself.
//
// Motivation: GTT orders are placed off a signal at a point in time, but nothing
// re-checks them once placed. Real cases found by manual audit (16-Jul-2026):
//   GLAND    — signal flipped BUY→WAIT the same day the GTT was still active
//   ABCAPITAL— GTT resting on a 10-day-old signal that never even reached confirmed
//   FUSION   — price moved materially away from the entry the GTT was set against
// Validates whatever is CURRENTLY active on Zerodha (GET /gtt/triggers — already a
// working, existing call) against the freshest qe_forward_track evidence per symbol.
// No new logging table needed: this reconciles live GTTs against existing signal
// history, exactly the manual process just used for the audit above.
//
// PRICE_DRIFTED threshold: 2.5% — ENGINEERING_ASSUMPTION, no prior QuantEdge
// precedent. Chosen as a tolerance band roughly consistent with the breakout
// formula's own 0.5% buffer plus a trading day's typical intraday range for the
// liquid mid/small-caps this platform scans; not derived from a formal study.
const GTT_PRICE_DRIFT_PCT = 2.5; // ENGINEERING_ASSUMPTION

async function validateGttOrdersEOD(env) {
  let token;
  try { token = await getToken(env); } catch (e) { return { ok: false, error: "Token unavailable: " + (e && e.message) }; }

  let gtts = [];
  try {
    const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
    if (!ok) return { ok: false, error: (data && data.message) || "GTT fetch failed" };
    gtts = (data.data || []).filter(function (g) { return g.status === "active"; }).map(function (g) {
      return { trigger_id: g.id, symbol: g.condition && g.condition.tradingsymbol,
        trigger_price: g.condition && g.condition.trigger_values && g.condition.trigger_values[0],
        quantity: g.orders && g.orders[0] && g.orders[0].quantity,
        transaction_type: g.orders && g.orders[0] && g.orders[0].transaction_type,
        created_at: g.created_at };
    });
  } catch (e) { return { ok: false, error: "GTT fetch exception: " + (e && e.message) }; }

  if (!gtts.length) return { ok: true, results: [] };

  const results = [];
  for (const g of gtts) {
    if (!g.symbol) { results.push({ ...g, status_check: "UNKNOWN_SYMBOL", reason: "Could not read symbol from this GTT's condition block." }); continue; }

    // SELL-side GTTs are protective stops on EXISTING holdings, not new-entry signals —
    // "is the BUY signal still fresh" is the wrong question entirely for these. Validate
    // against qe_holdings instead: is the position still held, does the trigger match the
    // CURRENT trailing stop, does quantity match. Fixes the exact bug that misclassified
    // CHOICEIN's protective stop as a stale discovery-stage signal (17-Jul-2026).
    if (g.transaction_type === "SELL") {
      let holding = null;
      try {
        holding = await env.QE_DB.prepare("SELECT qty, trail_stop, verdict FROM qe_holdings WHERE symbol=?1 AND qty>0").bind(g.symbol).first();
      } catch (_) {}
      let statusCheck, reason;
      if (!holding) {
        statusCheck = "ORPHANED_STOP";
        reason = `No active holding found for ${g.symbol} — this SELL GTT may be resting on a position already closed elsewhere. Could execute an unintended sell if price reaches the trigger.`;
      } else if (holding.qty !== g.quantity) {
        statusCheck = "QTY_MISMATCH";
        reason = `GTT quantity (${g.quantity}) does not match current holding quantity (${holding.qty}) — position size changed since this stop was placed.`;
      } else if (holding.trail_stop != null && g.trigger_price != null && Math.abs(g.trigger_price - holding.trail_stop) / holding.trail_stop * 100 > GTT_PRICE_DRIFT_PCT) {
        statusCheck = "STOP_OUTDATED";
        reason = `GTT trigger ₹${g.trigger_price} no longer matches the current trailing stop ₹${holding.trail_stop} — the stop has moved since this order was placed.`;
      } else {
        statusCheck = "VALID";
        reason = `Matches current holding (qty ${holding.qty}, verdict ${holding.verdict}) and trailing stop ₹${holding.trail_stop}.`;
      }
      results.push({ ...g, status_check: statusCheck, reason, holding: holding || null });
      continue;
    }

    let latestAny = null, latestConfirmed = null;
    try {
      latestAny = await env.QE_DB.prepare(
        "SELECT snapshot_date, label, entry, scan_mode, created_ts FROM qe_forward_track WHERE symbol=?1 ORDER BY snapshot_date DESC, created_ts DESC LIMIT 1"
      ).bind(g.symbol).first();
      latestConfirmed = await env.QE_DB.prepare(
        "SELECT snapshot_date, label, entry, created_ts FROM qe_forward_track WHERE symbol=?1 AND scan_mode='confirmed' ORDER BY snapshot_date DESC LIMIT 1"
      ).bind(g.symbol).first();
    } catch (_) {}

    let statusCheck, reason;
    if (!latestAny) {
      statusCheck = "NO_EVIDENCE";
      reason = "No qe_forward_track record at all for this symbol — no evidence this GTT reflects a system-generated signal.";
    } else if (!latestConfirmed) {
      statusCheck = "NEVER_CONFIRMED";
      reason = `Only reached "${latestAny.scan_mode || "discovery"}" stage on ${latestAny.snapshot_date} — never validated by a confirmed (16:15) scan.`;
    } else {
      const freshness = _datasetFreshness(latestConfirmed.created_ts, "Confirmed signal");
      if (freshness.freshness_status === "EXPIRED") {
        statusCheck = "STALE";
        reason = `Last confirmed ${latestConfirmed.snapshot_date} — ${freshness.freshness_reason}`;
      } else if (latestConfirmed.label !== "BUY") {
        statusCheck = "SIGNAL_REVERSED";
        reason = `Latest confirmed signal (${latestConfirmed.snapshot_date}) is ${latestConfirmed.label}, not BUY.`;
      } else {
        const drift = (g.trigger_price != null && latestConfirmed.entry) ? Math.abs(g.trigger_price - latestConfirmed.entry) / latestConfirmed.entry * 100 : null;
        if (drift !== null && drift > GTT_PRICE_DRIFT_PCT) {
          statusCheck = "PRICE_DRIFTED";
          reason = `GTT trigger ₹${g.trigger_price} is ${drift.toFixed(1)}% from the latest confirmed entry ₹${latestConfirmed.entry} (${latestConfirmed.snapshot_date}).`;
        } else {
          statusCheck = "VALID";
          reason = `Matches confirmed BUY signal from ${latestConfirmed.snapshot_date}, entry ₹${latestConfirmed.entry}.`;
        }
      }
    }
    results.push({ ...g, status_check: statusCheck, reason, latest_confirmed: latestConfirmed || null, latest_any: latestAny || null });
  }
  return { ok: true, results };
}

async function sendGttValidationSummary(env) {
  let v;
  try { v = await validateGttOrdersEOD(env); } catch (e) { console.error("[EOD GTT] fatal:", e && e.message); return; }
  if (!v.ok) { console.error("[EOD GTT] validation failed:", v.error); return; }
  if (!v.results.length) return; // nothing active, nothing to report — no noise

  const icon = { VALID: "✅", SIGNAL_REVERSED: "🔴", PRICE_DRIFTED: "🟠", STALE: "🟡", NEVER_CONFIRMED: "🟡", NO_EVIDENCE: "⚪", UNKNOWN_SYMBOL: "⚪", ORPHANED_STOP: "🔴", QTY_MISMATCH: "🟠", STOP_OUTDATED: "🟠" };
  const needsAttention = v.results.filter(function (r) { return r.status_check !== "VALID"; });
  const lines = v.results.map(function (r) {
    return `${icon[r.status_check] || "⚪"} <b>${r.symbol || "?"}</b> — ${r.status_check}\n   Trigger: ₹${r.trigger_price != null ? r.trigger_price : "—"} · Qty: ${r.quantity != null ? r.quantity : "—"}\n   ${r.reason}`;
  });
  const header = needsAttention.length
    ? `⚠️ <b>EOD GTT Check</b> — ${needsAttention.length}/${v.results.length} need a look\n\n`
    : `✅ <b>EOD GTT Check</b> — all ${v.results.length} still match a confirmed signal\n\n`;
  try { await sendTelegram(env, header + lines.join("\n\n")); } catch (e) { console.error("[EOD GTT] Telegram send failed:", e && e.message); }
}

async function runPortfolioPipeline(env){
  const ts=new Date().toISOString(), t0=Date.now();
  const audit={ ts, stages:{} };
  // 1 — Refresh portfolio state
  const refresh=await runHoldingsIngest(env);
  audit.stages.refresh=refresh;
  if(!(refresh&&refresh.ok)){                       // gate: no scoring/reporting on stale state (mirrors runPortfolioCycle)
    audit.ok=false; audit.stopped_at="refresh"; audit.ms=Date.now()-t0;
    try{ await env.KITE_STORE.put("qe_pie_pipeline_last_run", JSON.stringify(audit)); }catch(_){}
    return { ok:false, stopped_at:"refresh", refresh };
  }
  // 2-4 — Calculate intelligence → persist → publish event alerts (existing engine; unchanged)
  let intel; try{ intel=await runPortfolioIntelligenceShadow(env); }
  catch(e){ intel={ ok:false, error:(e&&e.message)||String(e) }; console.error("[pie:pipeline] intelligence: "+((e&&e.message)||e)); }
  audit.stages.intelligence=intel;
  // 5 — Executive Portfolio Digest (independent reporting consumer; reads freshly-persisted state)
  let digest; try{ digest=await dispatchPortfolioDigest(env); }
  catch(e){ digest={ sent:false, error:(e&&e.message)||String(e) }; console.error("[pie:pipeline] digest: "+((e&&e.message)||e)); }
  audit.stages.digest=digest;
  // 5.5 — Executive Decision Intelligence Report (independent consumer; translates verdicts to decisions)
  let decision_report; try{ decision_report=await generateExecutiveDecisionReport(env); }
  catch(e){ decision_report={ sent:false, error:(e&&e.message)||String(e) }; console.error("[pie:pipeline] decision_report: "+((e&&e.message)||e)); }
  audit.stages.decision_report=decision_report;
  // 5.6 — Decision Intelligence Log (durable, append-only system of record; consumer of the
  // decision engine, not part of it. Approved v4.68 architecture — Telegram is presentation
  // only. A logging failure here must never block scoring, persistence, digest, or the report,
  // all of which have already completed by this point.)
  let decision_log;
  try{
    decision_log = (decision_report && decision_report.holdings_with_decisions)
      ? await persistDecisionLog(env, audit.ts, decision_report.holdings_with_decisions, decision_report.agg)
      : { ok:false, reason:"NO_DECISION_DATA", inserted:0 };
  } catch(e){ decision_log={ ok:false, error:(e&&e.message)||String(e), inserted:0 }; console.error("[pie:pipeline] decision_log: "+((e&&e.message)||e)); }
  audit.stages.decision_log=decision_log;
  // 5.7 — Decision Outcome Resolver (Phase 1.0, v4.75): resolves prior decisions'
  // real market outcomes against D1 ohlcv_daily. Reads qe_decision_log, NEVER writes
  // to it. Safe-by-default: no-ops unless KV PIE_OUTCOME_RESOLVER_ENABLED=1. A
  // failure here must never block scoring, persistence, digest, report, or log —
  // all of which have already completed by this point.
  let outcome_resolution;
  try{ outcome_resolution = await resolveDecisionOutcomes(env); }
  catch(e){ outcome_resolution={ ok:false, error:(e&&e.message)||String(e) }; console.error("[pie:pipeline] outcome_resolution: "+((e&&e.message)||e)); }
  audit.stages.outcome_resolution=outcome_resolution;
  // 6 — Audit / metrics
  audit.ok=true; audit.ms=Date.now()-t0;
  try{ await env.KITE_STORE.put("qe_pie_pipeline_last_run", JSON.stringify(audit)); }catch(_){}
  // 7 — Exit
  return { ok:true, refresh, intel, digest, decision_report:{sent:decision_report&&decision_report.sent,count:decision_report&&decision_report.count}, decision_log, outcome_resolution, ms:audit.ms };
}


// ══════════════════════════════════════════════════════════════════════════════
// EXECUTIVE DECISION INTELLIGENCE ENGINE (v4.68)
// Transforms Portfolio Intelligence Verdicts into actionable Executive Decisions
// ══════════════════════════════════════════════════════════════════════════════

function mapVerdictToDecision(verdict,health,pillars,triggers,conviction){
  // Hard stop — highest priority
  if(triggers&&triggers.some(t=>t==="STOP_BREACH")) return "EXIT_IMMEDIATELY";
  // Exit conditions
  if(verdict==="EXIT"||verdict==="INSUFFICIENT_DATA") return "SELL";
  if(verdict==="REDUCE"&&(health<45||triggers.length>0)) return "SELL";
  if(verdict==="REDUCE"&&health>=45) return "REDUCE";
  // Watch conditions  
  if(verdict==="WATCH"&&health>=60&&conviction==="IMPROVING") return "HOLD";
  if(verdict==="WATCH"&&health<60) return "REDUCE";
  // Strong hold conditions
  if(verdict==="STRONG_HOLD"&&health>=85&&conviction==="IMPROVING") return "ACCUMULATE";
  if(verdict==="STRONG_HOLD"&&health>=70&&health<85) return "HOLD";
  if(verdict==="STRONG_HOLD"&&health<70) return "HOLD";
  if(verdict==="HOLD") return "HOLD";
  return "HOLD"; // default
}
function computeRecommendationConfidence(decision,health,active_pillars,pillars,triggers,pending_verdict){
  let conf=85; // baseline
  if(!active_pillars||active_pillars.length<3) conf-=5;
  if(active_pillars&&active_pillars.length===5) conf+=5; // all pillars engaged
  // Weak pillar penalty
  (active_pillars||[]).forEach(p=>{
    const score=pillars[p]||0;
    if(score<40) conf-=8;
    if(score<60&&decision.match(/SELL|REDUCE/)) conf-=5; // conflicting weak signal
  });
  if(pending_verdict) conf-=10; // verdict is changing, confidence reduced
  if(triggers&&triggers.length>0) conf-=3; // triggers add noise
  return Math.max(50,Math.min(95,conf));
}
function generateReversalConditions(decision,health){
  const reversals={
    "ACCUMULATE": [
      `Health drops below 75 (de-escalation threshold)`,
      `Conviction shifts from IMPROVING to DETERIORATING`,
      `Risk pillar drops below 60`
    ],
    "HOLD": [
      `Health drops below 60 (two consecutive cycles)`,
      `Trend pillar drops below 50 AND price below 50DMA`,
      `REDUCE or EXIT trigger fires`
    ],
    "REDUCE": [
      `Health drops below 30`,
      `Stop-loss breached`,
      `Multiple red flags (2+ triggers)`
    ],
    "SELL": [
      `This is a critical decision. Reversal requires fundamental stabilization + 2 positive cycles`
    ],
    "EXIT_IMMEDIATELY": [
      `This is an emergency exit. Reversal highly unlikely in near term.`
    ]
  };
  return (reversals[decision]||["No specific reversals tracked"]).join(" | ");
}
function generateEvidenceSummary(pillars,strongest,weakest,verdict){
  const trend=pillars.trend||0,momentum=pillars.momentum||0,edge=pillars.edge||0,risk=pillars.risk||0;
  const summary=[];
  if(trend>=70) summary.push(`Trend ${trend} strong (price above MAs, ADX rising)`);
  else if(trend>=50) summary.push(`Trend ${trend} moderate`);
  else summary.push(`Trend ${trend} WEAK (concern)`);
  if(momentum>=70) summary.push(`Momentum ${momentum} bullish (RSI overbought, volume spike)`);
  else if(momentum>=50) summary.push(`Momentum ${momentum} neutral`);
  else summary.push(`Momentum ${momentum} weak`);
  if(edge>=60) summary.push(`Edge ${edge} present (backtest validated)`);
  else summary.push(`Edge ${edge} weak or unvalidated`);
  if(risk>=70) summary.push(`Risk ${risk} managed (defined stop, R/R positive)`);
  else if(risk>=50) summary.push(`Risk ${risk} moderate`);
  else summary.push(`Risk ${risk} elevated (stop-loss wide or not set)`);
  return summary.join(" | ");
}
function generatePortfolioAggregates(holdings, snapshot){
  const capital=holdings.reduce((sum,h)=>{const val=(h.qty||0)*(h.ltp||0); return sum+val;},0);
  const sorted=[...holdings].sort((a,b)=>{const av=(a.qty||0)*(a.ltp||0),bv=(b.qty||0)*(b.ltp||0); return bv-av;});
  const top1_pct=capital>0?Math.round(((sorted[0].qty||0)*(sorted[0].ltp||0))/capital*100):0;
  // ONE FACT, ONE COMPUTATION (Trust Audit v1.0 fix): top3_pct and portfolio_health were
  // previously reimplemented here independently of computePortfolioRisk() — same intent,
  // different rounding precision (integer here vs 2-decimal there), which meant Decision
  // Replay's own top3_pct_matches/portfolio_health_matches consistency checks could report
  // a mismatch even when nothing had actually changed. computePortfolioRisk() is the sole
  // authoritative computation (it's also the only WRITER of qe_portfolio_snapshot); this
  // function now reads its output rather than recomputing it. Fallback below only covers
  // the defensive case where no snapshot row exists yet (preserves prior formula exactly).
  const top3_pct = (snapshot && snapshot.top3_pct != null) ? snapshot.top3_pct : (() => {
    const top3_cap=(sorted.slice(0,3)||[]).reduce((sum,h)=>{return sum+((h.qty||0)*(h.ltp||0));},0);
    return capital>0?Math.round(top3_cap/capital*100):0;
  })();
  const health_avg = (snapshot && snapshot.portfolio_health != null) ? snapshot.portfolio_health :
    (holdings.length>0?Math.round(holdings.reduce((s,h)=>s+(h.health_score||50),0)/holdings.length):0);
  const decisions=holdings.map(h=>mapVerdictToDecision(h.verdict,h.health_score,JSON.parse(h.evidence_json?.pillars||"{}"),JSON.parse(h.triggers_json||"[]"),h.conviction_trend));
  const hold_accum=decisions.filter(d=>d.match(/HOLD|ACCUMULATE/)).length;
  const reduce_sell=decisions.filter(d=>d.match(/REDUCE|SELL|EXIT/)).length;
  return {
    capital,
    top1_pct,
    top3_pct,
    health_avg,
    core_pct:Math.round(hold_accum/(holdings.length||1)*100),
    concentration_flag:top3_pct>80?"EXCESSIVE":"MODERATE",
    highest_health:sorted[0],
    weakest_health:sorted[sorted.length-1]
  };
}
async function generateExecutiveDecisionReport(env){
  if(!(await pieAlertsOn(env))) return {sent:false,reason:"ALERTS_OFF"};
  let holdings=[];
  try{
    const q=await env.QE_DB.prepare("SELECT symbol,qty,avg_price,ltp,health_score,data_confidence,verdict,triggers_json,evidence_json,conviction_trend,trail_stop,r_multiple,updated_ts FROM qe_holdings WHERE qty>0 ORDER BY health_score DESC").all();
    holdings=(q&&q.results)||[];
  }catch(_){
    return {sent:false,reason:"NO_HOLDINGS"};
  }
  if(holdings.length===0) return {sent:false,reason:"NO_POSITIONS"};
  // Single authoritative snapshot read (computePortfolioRisk runs earlier in the same
  // pipeline invocation, stage "intelligence" — same row persistDecisionLog links to below).
  const snapshot = await _latestPortfolioSnapshot(env);
  // Generate per-holding decision intelligence
  const holdings_with_decisions=holdings.map(h=>{
    const ev=JSON.parse(h.evidence_json||"{}");
    const pillars=ev.pillars||{};
    const triggers=JSON.parse(h.triggers_json||"[]");
    const decision=mapVerdictToDecision(h.verdict,h.health_score,pillars,triggers,h.conviction_trend);
    const confidence=computeRecommendationConfidence(decision,h.health_score,ev.active_pillars,pillars,triggers,ev.pending_verdict);
    const risk_pct=Math.max(5,Math.min(95,100-confidence));
    const reversals=generateReversalConditions(decision,h.health_score);
    const evidence=generateEvidenceSummary(pillars,ev.strongest,ev.weakest,h.verdict);
    const action=(()=>{
      if(decision==="HOLD") return `Maintain position. Trail stop: ${h.trail_stop||"not set"}. Re-evaluate if health &lt;70.`;
      if(decision==="ACCUMULATE") return `Consider adding on any 2% weakness. Current position: ${h.qty} shares.`;
      if(decision==="REDUCE") return `Trim ${Math.max(1,Math.floor(h.qty*0.2))} shares (20%) on any strength.`;
      if(decision==="SELL") return `Exit on next 3-5% bounce. Do not hold through stop-loss.`;
      if(decision==="EXIT_IMMEDIATELY") return `Close position immediately. Do not wait.`;
      return "Monitor closely.";
    })();
    return {
      symbol:h.symbol,
      decision,
      health:h.health_score,
      confidence,
      risk_pct,
      evidence,
      changed:ev.what_changed||"No prior data",
      reversals,
      action,
      ltp:h.ltp,
      r_multiple:h.r_multiple,
      // ── Additive (v4.71): full reconstructable context for Decision Intelligence Log.
      // Report rendering above is unchanged; these fields are consumed only by persistDecisionLog().
      verdict:h.verdict,
      data_confidence:h.data_confidence,
      conviction_trend:h.conviction_trend,
      active_pillars:ev.active_pillars||null,
      strongest:ev.strongest||null,
      weakest:ev.weakest||null,
      pillars,
      triggers,
      why_changed:ev.why_changed||null
    };
  });
  // Portfolio aggregates
  const agg=generatePortfolioAggregates(holdings, snapshot);
  // Build report message (Telegram format, multipart for readability)
  const lines=[];
  lines.push("<b>🎯 QuantEdge Executive Decision Report</b>");
  lines.push("");
  lines.push(`<b>Portfolio Status</b>`);
  lines.push(`Health: ${agg.health_avg}/100 | Top-3: ${agg.top3_pct}% (${agg.concentration_flag})`);
  lines.push(`Core holdings: ${agg.core_pct}% | Highest: ${agg.highest_health.symbol} (${agg.highest_health.health_score})`);
  lines.push("");
  lines.push("<b>Holdings &amp; Decisions</b>");
  for(const h of holdings_with_decisions){
    const marker=h.decision.match(/SELL|EXIT/)?"🔴":(h.decision==="REDUCE"?"🟠":(h.decision==="ACCUMULATE"?"🟢":"⚪"));
    lines.push(`${marker} ${h.symbol} | ${h.decision} | H${h.health} | Conf ${h.confidence}%`);
    lines.push(`  Evidence: ${h.evidence}`);
    lines.push(`  Action: ${h.action}`);
    lines.push(`  If broken: ${h.reversals.substring(0,60)}...`);
    lines.push("");
  }
  lines.push("<b>Portfolio Decision</b>");
  const actions_required=holdings_with_decisions.filter(h=>h.decision.match(/SELL|EXIT|REDUCE/));
  if(actions_required.length===0) lines.push("✅ No trades required. Maintain current allocations.");
  else lines.push(`⚠️  ${actions_required.length} holding(s) require attention: ${actions_required.map(h=>h.symbol).join(", ")}`);
  lines.push(`Concentration recommendation: ${agg.top3_pct>80?`Reduce top holdings by 10-15% to reach &lt;75%`:`Within normal range`}`);
  lines.push("");
  lines.push("<i>PIE Executive Decision Intelligence | shadow-advisory</i>");
  const message=lines.join("\n");
  const telegramOk=await sendTelegram(env,message);
  return {sent:telegramOk,count:holdings.length,holdings_with_decisions,agg};
}

const DECISION_ENGINE_VERSION="v4.71";

// ── Decision Intelligence Log (v4.71) ───────────────────────────────────────
// APPROVED ARCHITECTURE (v4.68 design): Telegram is a presentation layer, not the
// system of record. This is the durable, append-only, machine-readable audit trail —
// one immutable row per holding per pipeline run — so any past recommendation can be
// fully reconstructed (decision, confidence, evidence, reversal conditions, action)
// without referring to Telegram. Pure consumer of generateExecutiveDecisionReport's
// output: does not touch scoring, the decision engine, or the report itself. Never
// UPDATEs or overwrites — INSERT only. A logging failure must never block scoring,
// digest, or the Telegram report — caller wraps this in try/catch and continues
// regardless of outcome.
async function persistDecisionLog(env, run_id, holdings_with_decisions, agg){
  if(!holdings_with_decisions||!holdings_with_decisions.length) return {ok:false,reason:"NO_DECISION_DATA",inserted:0};
  // Link to today's portfolio snapshot if one exists (computePortfolioRisk runs earlier in the
  // same pipeline invocation, stage "intelligence" — snapshot_date is keyed by calendar date).
  let snapshot_id=null;
  try{
    const dateKey=run_id.slice(0,10);
    const sq=await env.QE_DB.prepare("SELECT id FROM qe_portfolio_snapshot WHERE snapshot_date=? ORDER BY id DESC LIMIT 1").bind(dateKey).all();
    snapshot_id=(sq&&sq.results&&sq.results[0]&&sq.results[0].id)||null;
  }catch(_){ /* snapshot_id stays null — non-fatal, matches "if available" requirement */ }
  let inserted=0, errors=0;
  for(const h of holdings_with_decisions){
    try{
      await env.QE_DB.prepare(
        "INSERT INTO qe_decision_log (run_id,ts,symbol,snapshot_id,decision,verdict,health_score,"+
        "recommendation_confidence,data_confidence,conviction_trend,active_pillars,strongest_pillar,"+
        "weakest_pillar,pillars_json,triggers_json,evidence_summary,what_changed,why_changed,"+
        "reversal_conditions,recommended_action,portfolio_health,portfolio_top3_pct,"+
        "portfolio_concentration_flag,engine_version,ltp,r_multiple) "+
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      ).bind(
        run_id, run_id, h.symbol, snapshot_id, h.decision, h.verdict, h.health,
        h.confidence, h.data_confidence, h.conviction_trend,
        JSON.stringify(h.active_pillars), h.strongest, h.weakest,
        JSON.stringify(h.pillars), JSON.stringify(h.triggers), h.evidence, h.changed, h.why_changed,
        h.reversals, h.action, agg.health_avg, agg.top3_pct,
        agg.concentration_flag, DECISION_ENGINE_VERSION, h.ltp, h.r_multiple
      ).run();
      inserted++;
    }catch(e){ errors++; console.error("[pie:decision_log] "+h.symbol+": "+((e&&e.message)||e)); }
  }
  return {ok:errors===0, inserted, errors, snapshot_id};
}

// ── Decision Intelligence Log — read-only query routes (v4.72) ─────────────────
// Deliberately NOT a general SQL endpoint: exactly two hardcoded, parameterized SELECTs.
// Symbol is always bound as a parameter, never concatenated — no injection surface even
// in principle. Frontend (index.html Decision Log panel) is the only consumer.
async function handleDecisionSymbols(env){
  try{
    // Additive (v4.73): rich per-symbol summary for the Decision Explorer's searchable list —
    // held/closed status + last activity, derived entirely from qe_decision_log + a held-check
    // against qe_holdings. No new table. `symbols` (plain array) kept for back-compat.
    const q=await env.QE_DB.prepare(
      "SELECT s.symbol,"+
      " (SELECT MAX(ts) FROM qe_decision_log WHERE symbol=s.symbol) AS last_ts,"+
      " (SELECT decision FROM qe_decision_log WHERE symbol=s.symbol ORDER BY ts DESC LIMIT 1) AS last_decision,"+
      " (SELECT health_score FROM qe_decision_log WHERE symbol=s.symbol ORDER BY ts DESC LIMIT 1) AS last_health,"+
      " CASE WHEN h.symbol IS NOT NULL THEN 1 ELSE 0 END AS held"+
      " FROM (SELECT DISTINCT symbol FROM qe_decision_log) s"+
      " LEFT JOIN qe_holdings h ON h.symbol=s.symbol AND h.qty>0"+
      " ORDER BY held DESC, last_ts DESC"
    ).all();
    const rows=(q&&q.results)||[];
    return cors({
      ok:true,
      symbols:rows.map(r=>r.symbol),                       // back-compat: plain array
      holdings:rows.map(r=>({ symbol:r.symbol, last_ts:r.last_ts, last_decision:r.last_decision,
                               last_health:r.last_health, held:!!r.held }))
    });
  }catch(e){ return corsErr("decision_symbols_failed: "+e.message, 500); }
}
async function handleDecisionHistory(url, env){
  const symbol=(url.searchParams.get("symbol")||"").trim().toUpperCase();
  if(!symbol) return corsErr("Required: symbol", 400);
  const limit=Math.max(1,Math.min(100, parseInt(url.searchParams.get("limit")||"30",10)||30));  // v4.73: 20→30, covers a full month of daily runs for the Decision Journey view
  try{
    const q=await env.QE_DB.prepare(
      "SELECT ts,decision,verdict,health_score,recommendation_confidence,data_confidence,conviction_trend,"+
      "active_pillars,strongest_pillar,weakest_pillar,pillars_json,triggers_json,evidence_summary,what_changed,"+
      "why_changed,reversal_conditions,recommended_action,portfolio_health,portfolio_top3_pct,"+
      "portfolio_concentration_flag,engine_version,ltp,r_multiple FROM qe_decision_log WHERE symbol=? ORDER BY ts DESC LIMIT ?"
    ).bind(symbol, limit).all();
    return cors({ ok:true, symbol, count:((q&&q.results)||[]).length, history:(q&&q.results)||[] });
  }catch(e){ return corsErr("decision_history_failed: "+e.message, 500); }
}

// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE ENGINE — P2 (Explainability + Verdict Alerts)
// Additive · flag-gated · zero Buy-Engine impact. Builds the evidence bundle + three-question
// triad (what changed / why / monitor-next) and dispatches Telegram ONLY on verdict change, with
// hysteresis. Alerts gated by a SEPARATE flag (PIE_ALERTS_ENABLED, default OFF) so RC1 stays
// alert-silent until shadow validation. Alert/hysteresis thresholds externalized to PIE_CONFIG.alerts.
// ══════════════════════════════════════════════════════════════════════════════

PIE_CONFIG.alerts = { hysteresisCycles: 2, escalations: ["EXIT","REDUCE","INSUFFICIENT_DATA"] };

async function pieAlertsOn(env){
  try { return (await env.KITE_STORE.get("PIE_ALERTS_ENABLED")) === "1"; }
  catch (_) { return false; }
}

// ---- Explainability: evidence bundle (engine-generated; Claude renders it, never fabricates it) ----
function pieNearestThreshold(ctx){
  const c=[];
  if(ctx.ltp!=null&&ctx.trailStop!=null) c.push({level:"trail_stop",value:ctx.trailStop,dist:Math.round(((ctx.ltp-ctx.trailStop)/ctx.ltp)*10000)/100});
  if(ctx.ltp!=null&&ctx.signalT1!=null&&ctx.ltp<ctx.signalT1) c.push({level:"T1",value:ctx.signalT1,dist:Math.round(((ctx.signalT1-ctx.ltp)/ctx.ltp)*10000)/100});
  if(ctx.tech&&ctx.tech.c!=null&&ctx.tech.ema50!=null) c.push({level:"ema50",value:ctx.tech.ema50,dist:Math.round(((ctx.tech.c-ctx.tech.ema50)/ctx.tech.c)*10000)/100});
  c.sort((a,b)=>Math.abs(a.dist)-Math.abs(b.dist));
  return c.length?c[0]:null;
}
function pieWhatChanged(prev, health, verdict){
  if(!prev||prev.health_score==null) return "First evaluation.";
  const dh=health-prev.health_score, arrow=dh>=0?"+":"";
  const vc=(prev.verdict&&prev.verdict!==verdict)?`${prev.verdict} -> ${verdict}`:"verdict unchanged";
  return `Health ${arrow}${dh} (${prev.health_score} -> ${health}); ${vc}.`;
}
function pieWhyChanged(prev, hs, triggers, conf, verdict){
  if(triggers.length) return `Triggered: ${triggers.join(", ")}.`;
  if(verdict.verdict==="INSUFFICIENT_DATA") return `Data confidence ${conf.confidence} below floor; missing: ${conf.flags.join(", ")||"n/a"}.`;
  const p=hs.pillars, r=Object.keys(p).sort((a,b)=>p[a]-p[b]);
  return `Strongest ${r[r.length-1]} (${p[r[r.length-1]]}); weakest ${r[0]} (${p[r[0]]}).`;
}
function buildEvidenceBundle(ctx, hs, conf, triggers, verdict, prev){
  const p=hs.pillars, ranked=Object.keys(p).sort((a,b)=>p[a]-p[b]);
  return {
    verdict:verdict.verdict, reason:verdict.reason, health:hs.score,
    confidence:conf.confidence, confidence_flags:conf.flags,
    pillars:p, active_pillars:hs.active_pillars||null, weakest:ranked[0], strongest:ranked[ranked.length-1], triggers,
    edge_decay:(ctx.signalScore!=null&&ctx.currentScore!=null)?{from:ctx.signalScore,to:ctx.currentScore}:null,
    what_changed:pieWhatChanged(prev, hs.score, verdict.verdict),
    why_changed:pieWhyChanged(prev, hs, triggers, conf, verdict),
    monitor_next:pieNearestThreshold(ctx),
    score_version:PIE_SCORE_VERSION, cfg_version:PIE_CONFIG.version
  };
}

// ---- Hysteresis: a non-escalation verdict must persist N cycles before it flips (anti-flap) ----
function pieVerdictWithHysteresis(newV, prevV, prevPendingV, prevPendingCount){
  const cfg=PIE_CONFIG.alerts;
  if(!prevV||newV===prevV) return { effective:newV, pendingV:null, pendingCount:0 };
  if(cfg.escalations.includes(newV)) return { effective:newV, pendingV:null, pendingCount:0 }; // risk-off applies now
  const count=(prevPendingV===newV)?(prevPendingCount+1):1;
  if(count>=cfg.hysteresisCycles) return { effective:newV, pendingV:null, pendingCount:0 };
  return { effective:prevV, pendingV:newV, pendingCount:count };
}

// ---- Telegram verdict-change alert (effective-verdict change only; alert-flag gated) ----
async function dispatchPortfolioAlert(env, sym, ev, prevVerdict){
  if(!(await pieAlertsOn(env))) return { sent:false, reason:"ALERTS_OFF" };
  if(ev.verdict===prevVerdict) return { sent:false, reason:"NO_CHANGE" };
  const mon=ev.monitor_next?`\nMonitor: ${ev.monitor_next.level} @ ${ev.monitor_next.value} (${ev.monitor_next.dist}%)`:"";
  const flg=(ev.confidence_flags&&ev.confidence_flags.length)?`\nData: ${ev.confidence_flags.join(", ")}`:"";
  const msg=`<b>${sym} -> ${ev.verdict}</b>\nHealth ${ev.health} | Confidence ${ev.confidence}\n`
    +`${ev.what_changed}\n${ev.why_changed}${mon}${flg}\n<i>PIE ${ev.score_version} shadow-advisory</i>`;
  await sendTelegram(env, msg);
  return { sent:true };
}

// ---- Integration: persist score + evidence + hysteresis, update memory, alert on change ----
async function piePersist(env, h, ctx, hs, conf, trig, ts){
  const vr=evaluateVerdict(hs.score, trig, conf);
  let prev=null;
  try{ const q=await env.QE_DB.prepare("SELECT verdict,health_score,pending_verdict,pending_count FROM qe_holdings WHERE symbol=?").bind(h.symbol).all(); prev=(q&&q.results&&q.results[0])||null; }catch(_){}
  const hyst=pieVerdictWithHysteresis(vr.verdict, prev&&prev.verdict, prev&&prev.pending_verdict, (prev&&prev.pending_count)||0);
  const ev=buildEvidenceBundle(ctx, hs, conf, trig, { verdict:hyst.effective, reason:vr.reason }, prev);
  await env.QE_DB.prepare(
    "UPDATE qe_holdings SET health_score=?,data_confidence=?,confidence_flags=?,verdict=?,triggers_json=?,"+
    "evidence_json=?,what_changed=?,why_changed=?,monitor_next=?,pending_verdict=?,pending_count=?,"+
    "r_multiple=?,trail_stop=?,days_held=?,score_version=?,updated_ts=? WHERE symbol=?"
  ).bind(hs.score,conf.confidence,JSON.stringify(conf.flags),hyst.effective,JSON.stringify(trig),
         JSON.stringify(ev),ev.what_changed,ev.why_changed,JSON.stringify(ev.monitor_next),
         hyst.pendingV,hyst.pendingCount,ctx.rMultiple,ctx.trailStop,ctx.daysHeld,PIE_SCORE_VERSION,ts,h.symbol).run();
  await updatePortfolioMemory(env,h.symbol,{ ts, ltp:h.ltp, rMultiple:ctx.rMultiple, health:hs.score, confidence:conf.confidence, pillars:hs.pillars, verdict:hyst.effective });
  // F4: derive conviction trend from history (incl. this cycle) and persist it — no longer a dead column.
  try{
    const hq=await env.QE_DB.prepare("SELECT health_score FROM qe_holding_history WHERE symbol=? ORDER BY snapshot_date DESC LIMIT 10").bind(h.symbol).all();
    const series=(((hq&&hq.results)||[]).map(r=>({health:r.health_score}))).reverse();
    await env.QE_DB.prepare("UPDATE qe_holdings SET conviction_trend=? WHERE symbol=?").bind(pieConvictionTrend(series),h.symbol).run();
  }catch(e){ console.error("[pie:trend] "+((e&&e.message)||e)); }
  await dispatchPortfolioAlert(env, h.symbol, ev, prev&&prev.verdict);
  return hyst.effective;
}


// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE ENGINE — P3 (Portfolio-Level Risk + Daily Digest)
// Additive · flag-gated · zero Buy-Engine impact. Aggregates concentration, sector exposure,
// value-weighted portfolio health → qe_portfolio_snapshot. Digest reuses the P2 alert flag.
// Sector source is externalized (KV qe_sector_map); UNKNOWN when unavailable (PROVISIONAL).
// ══════════════════════════════════════════════════════════════════════════════

async function pieSectorMap(env){
  try { const raw=await env.KITE_STORE.get("qe_sector_map"); return raw?JSON.parse(raw):{}; }
  catch(_){ return {}; }
}

// Deterministic portfolio-level aggregation. Returns the snapshot object (also persisted).
async function computePortfolioRisk(env){
  let rows=[];
  try { const q=await env.QE_DB.prepare("SELECT symbol,qty,avg_price,ltp,health_score FROM qe_holdings WHERE qty>0").all(); rows=(q&&q.results)||[]; }
  catch(_){ return { ok:false, reason:"NO_HOLDINGS_TABLE" }; }
  const sectors=await pieSectorMap(env);
  const ts=new Date().toISOString();
  let totalValue=0, totalInvested=0, wHealth=0, wHealthDen=0;
  const valued=[];
  for(const r of rows){
    const qty=Number(r.qty)||0, ltp=Number(r.ltp)||0, avg=Number(r.avg_price)||0;
    const value=qty*ltp;
    totalValue+=value; totalInvested+=qty*avg;
    if(r.health_score!=null){ wHealth+=r.health_score*value; wHealthDen+=value; }
    valued.push({ symbol:r.symbol, value, sector:sectors[r.symbol]||"UNKNOWN" });
  }
  valued.sort((a,b)=>b.value-a.value);
  const pct=(v)=> totalValue>0 ? Math.round((v/totalValue)*10000)/100 : 0;
  const topName=valued.length?pct(valued[0].value):0;
  const top3=pct(valued.slice(0,3).reduce((s,x)=>s+x.value,0));
  const sectorAgg={};
  for(const v of valued){ sectorAgg[v.sector]=(sectorAgg[v.sector]||0)+v.value; }
  const sectorPct={}; for(const k in sectorAgg) sectorPct[k]=pct(sectorAgg[k]);
  const snap={
    ts, holdings_count:rows.length,
    total_invested:Math.round(totalInvested*100)/100,
    total_value:Math.round(totalValue*100)/100,
    total_pnl_pct: totalInvested>0 ? Math.round(((totalValue-totalInvested)/totalInvested)*10000)/100 : 0,
    top_name_pct:topName, top3_pct:top3, sector_json:JSON.stringify(sectorPct),
    portfolio_health: wHealthDen>0 ? Math.round(wHealth/wHealthDen) : null
  };
  try {
    await env.QE_DB.prepare(
      "INSERT INTO qe_portfolio_snapshot (snapshot_date,ts,total_invested,total_value,total_pnl_pct,"+
      "holdings_count,top_name_pct,top3_pct,sector_json,portfolio_health) VALUES (?,?,?,?,?,?,?,?,?,?) "+
      "ON CONFLICT(snapshot_date) DO UPDATE SET ts=excluded.ts,total_invested=excluded.total_invested,"+
      "total_value=excluded.total_value,total_pnl_pct=excluded.total_pnl_pct,holdings_count=excluded.holdings_count,"+
      "top_name_pct=excluded.top_name_pct,top3_pct=excluded.top3_pct,sector_json=excluded.sector_json,"+
      "portfolio_health=excluded.portfolio_health"
    ).bind(ts.slice(0,10),ts,snap.total_invested,snap.total_value,snap.total_pnl_pct,snap.holdings_count,
           snap.top_name_pct,snap.top3_pct,snap.sector_json,snap.portfolio_health).run();
  } catch(_){ return Object.assign({ ok:false, reason:"SNAPSHOT_WRITE" }, snap); }
  return Object.assign({ ok:true }, snap);
}

// Daily digest (alert-flag gated; shadow = silent). Concise per-holding verdicts + one risk line.
async function dispatchPortfolioDigest(env){
  if(!(await pieAlertsOn(env))) return { sent:false, reason:"ALERTS_OFF" };
  let holdings=[];
  try { const q=await env.QE_DB.prepare("SELECT symbol,verdict,health_score,conviction_trend FROM qe_holdings WHERE qty>0 ORDER BY health_score DESC").all(); holdings=(q&&q.results)||[]; }
  catch(_){ return { sent:false, reason:"NO_HOLDINGS" }; }
  const risk=await computePortfolioRisk(env);
  const arrow=(t)=> t==="IMPROVING"?"^":(t==="DETERIORATING"?"v":"-");
  const lines=holdings.map(h=>`${h.symbol}: ${h.verdict||"?"} (H${h.health_score==null?"-":h.health_score} ${arrow(h.conviction_trend)})`).join("\n");
  const riskLine=risk&&risk.ok?`\nPortfolio health ${risk.portfolio_health==null?"-":risk.portfolio_health} | top-name ${risk.top_name_pct}% | top-3 ${risk.top3_pct}%`:"";
  await sendTelegram(env, `<b>QuantEdge Portfolio Digest</b>\n${lines}${riskLine}\n<i>PIE shadow-advisory</i>`);
  return { sent:true, count:holdings.length };
}


// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE ENGINE — P4 (Claude Enrichment Routes)
// Additive · zero Buy-Engine impact. Read routes expose holdings/evidence/memory/risk to Claude.
// POST /portfolio/claude-note writes ONLY claude_note/claude_flag — NEVER verdict/score
// (schema-enforced ADR-001/ADR-009 boundary: Claude explains, the engine decides).
// ══════════════════════════════════════════════════════════════════════════════

async function pieAuthOk(request, env){
  let provided=null;
  try{ const h=request.headers&&request.headers.get&&request.headers.get("Authorization"); if(h&&h.indexOf("Bearer ")===0) provided=h.slice(7).trim(); }catch(_){}
  if(!provided){ try{ provided=new URL(request.url).searchParams.get("token"); }catch(_){} }
  let expected=null; try{ expected=await env.KITE_STORE.get("PIE_API_TOKEN"); }catch(_){}
  if(!expected) return false;                                   // fail-closed: token unset -> deny
  if(!provided || provided.length!==expected.length) return false;
  let diff=0; for(let i=0;i<expected.length;i++) diff|=expected.charCodeAt(i)^provided.charCodeAt(i);
  return diff===0;                                              // constant-time comparison
}
async function handlePortfolioHolding(url, env){
  const sym=url.searchParams.get("symbol"); if(!sym) return corsErr("symbol required",400);
  try{
    const q=await env.QE_DB.prepare("SELECT * FROM qe_holdings WHERE symbol=?").bind(sym).all();
    const row=(q&&q.results&&q.results[0])||null;
    let ev=null; if(row&&row.evidence_json){ try{ ev=JSON.parse(row.evidence_json); }catch(_){} }
    return cors({ ok:true, holding:row, evidence:ev });
  }catch(e){ return corsErr("holding_failed: "+e.message,500); }
}
async function handlePortfolioMemory(url, env){
  const sym=url.searchParams.get("symbol"); if(!sym) return corsErr("symbol required",400);
  try{
    const q=await env.QE_DB.prepare("SELECT snapshot_date,ts,ltp,r_multiple,health_score,data_confidence,verdict FROM qe_holding_history WHERE symbol=? ORDER BY snapshot_date").bind(sym).all();
    const rows=(q&&q.results)||[];
    return cors({ ok:true, symbol:sym, history:rows, trend:pieConvictionTrend(rows.map(r=>({health:r.health_score}))) });
  }catch(e){ return corsErr("memory_failed: "+e.message,500); }
}
async function handlePortfolioRiskRoute(env){
  try{
    const q=await env.QE_DB.prepare("SELECT * FROM qe_portfolio_snapshot ORDER BY snapshot_date DESC LIMIT 1").all();
    return cors({ ok:true, risk:(q&&q.results&&q.results[0])||null });
  }catch(e){ return corsErr("risk_failed: "+e.message,500); }
}
// Claude enrichment write — STRICTLY claude_note/claude_flag. Cannot touch verdict/health/score.
async function handleClaudeNote(request, env){
  let body; try{ body=await request.json(); }catch(_){ return corsErr("invalid json",400); }
  const sym=body&&body.symbol; if(!sym) return corsErr("symbol required",400);
  const note=(body.note!=null)?String(body.note).slice(0,2000):null;
  const flag=(body.flag!=null)?String(body.flag).slice(0,200):null;
  try{
    await env.QE_DB.prepare("UPDATE qe_holdings SET claude_note=?,claude_flag=? WHERE symbol=?").bind(note,flag,sym).run();
    return cors({ ok:true, updated:sym, note_len:note?note.length:0, flag:flag });
  }catch(e){ return corsErr("claude_note_failed: "+e.message,500); }
}


// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE ENGINE — P5 (Calibration SCAFFOLD · governance mechanism)
// Offline, batch, outcome-measured, HUMAN-GATED (ADR-008). Replays qe_holding_history vs
// qe_trade_outcomes and PROPOSES config changes with evidence. NEVER auto-applies: applying a
// proposal = a manual, versioned PIE_CONFIG bump. Min-sample threshold externalized.
// PRODUCTION-VALIDATED only after real outcome history accrues (returns INSUFFICIENT_DATA until then).
// ══════════════════════════════════════════════════════════════════════════════

async function computeCalibrationProposal(env){
  let outcomes=[];
  try{ const q=await env.QE_DB.prepare("SELECT symbol,signal_label,return_r,return_pct,status FROM qe_trade_outcomes WHERE status='CLOSED'").all(); outcomes=(q&&q.results)||[]; }
  catch(_){ return { ok:false, reason:"NO_OUTCOMES_TABLE" }; }
  let minSamples=30;
  try{ const m=await env.KITE_STORE.get("PIE_CALIB_MIN_SAMPLES"); if(m) minSamples=Number(m)||30; }catch(_){}
  if(outcomes.length<minSamples){
    return { ok:true, status:"INSUFFICIENT_DATA", samples:outcomes.length, min:minSamples,
             proposal:null, cfg_version:PIE_CONFIG.version,
             note:"Calibration requires production outcome history; mechanism ready, awaiting data." };
  }
  const withR=outcomes.filter(o=>o.return_r!=null);
  const avgR = withR.length ? Math.round((withR.reduce((s,o)=>s+o.return_r,0)/withR.length)*1000)/1000 : null;
  const winRate = Math.round((outcomes.filter(o=>(o.return_pct||0)>0).length/outcomes.length)*1000)/1000;
  const proposal={ measured:{ samples:outcomes.length, avgR, winRate },
    proposed_cfg_version:PIE_CONFIG.version+"-candidate",
    note:"PROVISIONAL measurement. Human review + explicit versioned PIE_CONFIG bump required before effect." };
  try{ await env.KITE_STORE.put("qe_pie_calibration_proposal", JSON.stringify({ ts:new Date().toISOString(), proposal })); }catch(_){}
  return { ok:true, status:"PROPOSED", samples:outcomes.length, proposal };   // proposal only — never applied here
}
async function handleCalibration(env){ return cors(await computeCalibrationProposal(env)); }

// ═══════════════════════════════════════════════════════════════════════════════
// FUNDAMENTALS — server-side Screener.in fetch (v4.74)
//
// REPLACES: index.html's 4-proxy CORS chain (allorigins.win / corsproxy.io /
// codetabs.com / thingproxy.freeboard.io). Workers fetch server-to-server, so
// no CORS proxy is needed at all — CORS is a browser-enforced restriction and
// does not apply to Worker-to-Screener requests. This removes three third-party
// hop dependencies entirely (reliability + no third party sees your traffic).
//
// PARSING: Workers have no DOMParser. Uses the native HTMLRewriter streaming
// parser (built into the Workers runtime, zero bundle cost) with the *same*
// CSS selectors and label-matching regexes the old browser-side
// _parseScreenerHTML() used (#top-ratios li .name/.number, section#profit-loss
// table rows) — so field-extraction logic is unchanged, only the parsing
// engine moved from DOM to streaming-HTML.
//
// CACHING: fundamentals move slowly (quarterly results), so successful parses
// are cached in KITE_STORE for 20h. This cuts Screener.in load, gives graceful
// degradation on a transient Screener outage, and keeps repeated frontend
// requests (e.g. re-opening a card) fast. Cache misses fall through to a live
// fetch; a live-fetch failure returns null (never a stale/wrong partial guess).
// ═══════════════════════════════════════════════════════════════════════════════
const FUNDAMENTALS_CACHE_TTL_SEC = 20 * 60 * 60; // 20h — fundamentals refresh quarterly, not intraday

async function _parseScreenerViaRewriter(response) {
  const ratios = [];   // [{name, number}] — one per #top-ratios li
  const plRows = [];   // [{cells:[{text}]}] — rows from section#profit-loss table
  let curLi = null;
  let inPL = false;
  let curRow = null;

  class LiHandler {
    element(el) {
      curLi = { name: "", number: "" };
      const ref = curLi;
      el.onEndTag(() => { ratios.push(ref); curLi = null; });
    }
  }
  class NameHandler   { text(t) { if (curLi) curLi.name   += t.text; } }
  class NumberHandler { text(t) { if (curLi) curLi.number += t.text; } }
  class SectionHandler {
    element(el) {
      const id = (el.getAttribute("id") || "").toLowerCase();
      if (id === "profit-loss") {
        inPL = true;
        el.onEndTag(() => { inPL = false; });
      }
    }
  }
  class RowHandler {
    element(el) {
      if (!inPL) return;
      curRow = { cells: [] };
      const ref = curRow;
      el.onEndTag(() => { if (inPL) plRows.push(ref); curRow = null; });
    }
  }
  class CellHandler {
    element(el) {
      if (!inPL || !curRow) return;
      curRow.cells.push({ text: "" });
    }
    text(t) {
      if (!inPL || !curRow || !curRow.cells.length) return;
      curRow.cells[curRow.cells.length - 1].text += t.text;
    }
  }

  const rewriter = new HTMLRewriter()
    .on("#top-ratios li",           new LiHandler())
    .on("#top-ratios li .name",     new NameHandler())
    .on("#top-ratios li .number",   new NumberHandler())
    .on("section",                  new SectionHandler())
    .on("section table tr",         new RowHandler())
    .on("section table tr td",      new CellHandler())
    .on("section table tr th",      new CellHandler());

  const transformed = rewriter.transform(response);
  await transformed.text(); // must drain the stream — handlers fire during consumption, not on .transform()

  // ── Key ratios (same label regexes as the retired browser-side parser) ──
  let pe = null, roe = null, de = null, mcapRaw = null;
  for (const r of ratios) {
    const lbl = (r.name || "").trim();
    const val = parseFloat((r.number || "").replace(/,/g, "").trim());
    if (isNaN(val)) continue;
    if (/^stock p\/e$/i.test(lbl))            pe = val;
    if (/^roe$|return on equity/i.test(lbl))  roe = val;
    if (/debt.{0,5}equity/i.test(lbl))        de = val;
    if (/market cap/i.test(lbl))              mcapRaw = val * 1e7; // Screener shows crores → rupees
  }

  // ── Revenue / profit growth from the Profit & Loss table, last two cols ──
  let revGr = null, profGr = null;
  for (const row of plRows) {
    if (!row.cells.length) continue;
    const rowLbl = (row.cells[0].text || "").trim().toLowerCase();
    const vals = [];
    for (let i = row.cells.length - 1; i >= 1; i--) {
      const v = parseFloat((row.cells[i].text || "").replace(/,/g, ""));
      if (!isNaN(v) && v !== 0) { vals.push(v); if (vals.length === 2) break; }
    }
    if (vals.length < 2) continue;
    const [curr, prev] = vals; // curr = latest col, prev = prior col
    const growth = prev !== 0 ? ((curr - prev) / Math.abs(prev)) * 100 : null;
    if (/^sales|^revenue|^net sales/i.test(rowLbl) && revGr === null)  revGr  = growth;
    if (/net profit|pat\b/i.test(rowLbl)           && profGr === null) profGr = growth;
  }

  if (pe === null && roe === null && revGr === null && profGr === null && de === null) return null;
  return { pe, roe, revGr, profGr, de, mcap: mcapRaw };
}

async function fetchScreenerFundamentalsServer(symbol) {
  const clean = symbol.replace(/\.NS$|\.BO$/i, "").toUpperCase().trim();
  const urls = [
    `https://www.screener.in/company/${clean}/consolidated/`,
    `https://www.screener.in/company/${clean}/`,
  ];
  for (const targetUrl of urls) {
    try {
      const ctrl  = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 8000);
      let res;
      try {
        res = await fetch(targetUrl, {
          signal: ctrl.signal,
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept": "text/html",
          },
        });
      } finally { clearTimeout(timer); }
      if (!res.ok) continue;

      // Guard checks (404 / login-wall) on a cloned body — must happen before
      // the rewriter consumes the original stream, since a Response body can
      // only be read once.
      const guardText = await res.clone().text();
      if (!guardText || guardText.length < 500) continue;
      if (guardText.includes("Page not found") || guardText.includes("page-not-found")) continue;
      if (guardText.includes('id="login-form"') || (guardText.includes("csrfmiddlewaretoken") && guardText.includes("password"))) continue;

      const parsed = await _parseScreenerViaRewriter(res);
      if (parsed) { parsed._source = "screener"; return parsed; }
    } catch (_) { continue; }
  }
  return null;
}

async function fetchFundamentalsWithCache(symbol, env) {
  const clean = symbol.replace(/\.NS$|\.BO$/i, "").toUpperCase().trim();
  const cacheKey = `fund_v1:${clean}`;
  try {
    const cached = await env.KITE_STORE.get(cacheKey);
    if (cached) return { ...JSON.parse(cached), _cached: true };
  } catch (_) { /* cache miss/corrupt — fall through to live fetch */ }

  const fresh = await fetchScreenerFundamentalsServer(clean);
  if (fresh) {
    try {
      await env.KITE_STORE.put(cacheKey, JSON.stringify(fresh), { expirationTtl: FUNDAMENTALS_CACHE_TTL_SEC });
    } catch (_) { /* cache write failure is non-fatal — data still returned fresh */ }
    return { ...fresh, _cached: false };
  }
  return null;
}

async function handleFundamentals(url, env) {
  const symbol = url.searchParams.get("symbol");
  if (!symbol) return corsErr("Missing symbol parameter");
  try {
    const data = await fetchFundamentalsWithCache(symbol, env);
    if (!data) return corsErr(`Fundamentals not found for ${symbol}`, 404);
    return cors({ status: "success", data });
  } catch (e) {
    return corsErr(e.message || "Fundamentals fetch failed", 502);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION OUTCOME RESOLVER — Phase 1.0 (v4.75)
//
// PURPOSE: for every row in qe_decision_log, automatically resolve what the
// symbol's price actually did over configurable forward horizons, using the
// existing D1 ohlcv_daily cache (already populated daily by the "Refresh"
// pipeline stage — no new price feed integration was needed). Writes to a new
// table, qe_decision_outcomes; qe_decision_log itself is NEVER modified —
// immutability of the original recommendation is preserved by construction.
//
// SCOPE BOUNDARY (mandatory, approved design): this resolver computes and
// persists OBJECTIVE, UNINTERPRETED facts only — reference/eval price, MFE,
// MAE, volatility, benchmark-relative return, market regime at evaluation.
// It never computes "was this decision correct," accuracy, or any judgment —
// that is explicitly reserved for Phase 1.1 (Decision Quality Analytics).
//
// HORIZON-AGNOSTIC: evaluation windows (trading days) come from KV
// PIE_OUTCOME_WINDOWS (comma list, e.g. "5,10,20,40,60"). Adding a new
// horizon requires a KV edit only — zero code change, per approved amendment 1.
//
// BENCHMARK PROXY: D1 has no historical NIFTY 50 index series (the live
// regime pipeline fetches Nifty from Yahoo into a 4h KV cache with no
// history retained). NIFTYBEES (Nifty 50 ETF, full continuous D1 history)
// is used as the benchmark/regime proxy — disclosed, not hidden. Regime
// classification reuses computePipelineRegime() UNCHANGED (protected
// function untouched — this only calls it with retroactive closes).
// ═══════════════════════════════════════════════════════════════════════════════

const OUTCOME_RESOLVER_VERSION   = "resolver-v1.0";
const OUTCOME_EVAL_METHOD_CLOSE  = "CLOSE_N_TRADING_DAYS";
const OUTCOME_BENCHMARK_SYMBOL   = "NIFTYBEES"; // Nifty 50 ETF — proxy, see module header
const OUTCOME_DELIST_STALE_DAYS  = 21;          // calendar days a symbol can lag the market before DELISTED_HEURISTIC
const OUTCOME_GAP_FLAG_DAYS      = 10;           // calendar-day gap between consecutive bars that flags RESOLVED_WITH_DATA_GAP

function _istDateStr(msOrIso) {
  const ms = typeof msOrIso === "number" ? msOrIso : new Date(msOrIso).getTime();
  return new Date(ms + 5.5 * 60 * 60 * 1000).toISOString().slice(0, 10); // same idiom used elsewhere in this file
}

async function _outcomeResolverConfig(env) {
  let enabled = false;
  try { enabled = (await env.KITE_STORE.get("PIE_OUTCOME_RESOLVER_ENABLED")) === "1"; } catch (_) {}
  let windows = [10, 20];
  try {
    const raw = await env.KITE_STORE.get("PIE_OUTCOME_WINDOWS");
    if (raw) {
      const parsed = raw.split(",").map(s => parseInt(s.trim(), 10)).filter(n => Number.isFinite(n) && n > 0);
      if (parsed.length) windows = Array.from(new Set(parsed)).sort((a, b) => a - b);
    }
  } catch (_) {}
  return { enabled, windows };
}

// Bars strictly after `fromDate` (exclusive), ascending, capped at `limit`.
async function _barsAfter(env, symbol, fromDate, limit) {
  const q = await env.QE_DB.prepare(
    "SELECT bar_date,o,h,l,c,v FROM ohlcv_daily WHERE symbol=?1 AND bar_date>?2 ORDER BY bar_date ASC LIMIT ?3"
  ).bind(symbol, fromDate, limit).all();
  return (q && q.results) || [];
}
// Nearest bar on or before `onOrBeforeDate` (for benchmark lookups).
async function _barOnOrBefore(env, symbol, onOrBeforeDate) {
  const q = await env.QE_DB.prepare(
    "SELECT bar_date,c FROM ohlcv_daily WHERE symbol=?1 AND bar_date<=?2 ORDER BY bar_date DESC LIMIT 1"
  ).bind(symbol, onOrBeforeDate).first();
  return q || null;
}
// Exact bar on a given date (for benchmark eval-date alignment).
async function _barOn(env, symbol, date) {
  const q = await env.QE_DB.prepare(
    "SELECT bar_date,c FROM ohlcv_daily WHERE symbol=?1 AND bar_date=?2 LIMIT 1"
  ).bind(symbol, date).first();
  return q || null;
}

function _stdevPct(returns) {
  if (!returns.length) return null;
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((a, b) => a + (b - mean) * (b - mean), 0) / returns.length;
  return Math.sqrt(variance);
}

// Regime as of `uptoDate`, via NIFTYBEES closes, reusing computePipelineRegime() UNCHANGED.
async function _regimeAsOf(env, uptoDate) {
  try {
    const q = await env.QE_DB.prepare(
      "SELECT c FROM ohlcv_daily WHERE symbol=?1 AND bar_date<=?2 ORDER BY bar_date DESC LIMIT 260"
    ).bind(OUTCOME_BENCHMARK_SYMBOL, uptoDate).all();
    const closes = ((q && q.results) || []).map(r => r.c).reverse(); // ascending order, as computePipelineRegime expects
    if (closes.length < 50) return null;
    const r = computePipelineRegime(closes, { log: () => {} });
    return (r && r.regime) || null;
  } catch (_) { return null; }
}

// Resolve every (decision, window) pair not yet in a terminal state.
// Fully additive; never touches qe_decision_log. Errors are isolated per-row —
// one bad symbol cannot abort the batch (mirrors persistDecisionLog's pattern).
async function resolveDecisionOutcomes(env) {
  const cfg = await _outcomeResolverConfig(env);
  if (!cfg.enabled) return { ok: true, skipped: true, reason: "DISABLED (set KV PIE_OUTCOME_RESOLVER_ENABLED=1)" };

  let decisions = [];
  try {
    const q = await env.QE_DB.prepare(
      "SELECT id,run_id,ts,symbol,decision,ltp,engine_version FROM qe_decision_log ORDER BY id ASC"
    ).all();
    decisions = (q && q.results) || [];
  } catch (e) { return { ok: false, error: "qe_decision_log read failed: " + ((e && e.message) || e) }; }

  if (!decisions.length) return { ok: true, processed: 0, resolved: 0, pending: 0, errors: 0, windows: cfg.windows };

  // Market-wide latest bar (for DELISTED_HEURISTIC staleness comparisons) — one query, reused for all rows.
  let marketLatestBar = null;
  try {
    const mq = await env.QE_DB.prepare("SELECT MAX(bar_date) AS d FROM ohlcv_daily").first();
    marketLatestBar = mq && mq.d;
  } catch (_) {}

  const maxWindow = cfg.windows[cfg.windows.length - 1];
  let resolvedCount = 0, pendingCount = 0, errorCount = 0;

  for (const d of decisions) {
    try {
      // Existing outcome rows for this decision, keyed by window — skip terminal states, retry PENDING.
      let existing = {};
      try {
        const eq = await env.QE_DB.prepare(
          "SELECT eval_window_days,resolution_status FROM qe_decision_outcomes WHERE decision_log_id=?1 AND evaluation_method=?2"
        ).bind(d.id, OUTCOME_EVAL_METHOD_CLOSE).all();
        for (const r of (eq && eq.results) || []) existing[r.eval_window_days] = r.resolution_status;
      } catch (_) {}

      const windowsToProcess = cfg.windows.filter(w => {
        const st = existing[w];
        return !st || st === "PENDING"; // terminal states (RESOLVED_*, DELISTED_HEURISTIC, MISSING_DATA) are never revisited
      });
      if (!windowsToProcess.length) continue;

      const decisionDate = _istDateStr(d.ts);
      const referencePrice = d.ltp;
      if (referencePrice == null) { errorCount++; continue; } // cannot resolve without a reference price — skip, don't guess

      // One fetch per decision, reused across every window (amendment 7 — compute in one pass, avoid repeated scans).
      const bars = await _barsAfter(env, d.symbol, decisionDate, maxWindow);

      // Staleness check for DELISTED_HEURISTIC — only relevant if data is currently insufficient.
      let symbolIsStale = false;
      if (marketLatestBar) {
        const symLatest = bars.length ? bars[bars.length - 1].bar_date : null;
        const refDate = symLatest || decisionDate;
        const daysBehind = (new Date(marketLatestBar) - new Date(refDate)) / 86400000;
        symbolIsStale = daysBehind > OUTCOME_DELIST_STALE_DAYS;
      }

      for (const w of windowsToProcess) {
        if (bars.length < w) {
          const status = symbolIsStale ? "DELISTED_HEURISTIC" : "PENDING";
          await env.QE_DB.prepare(
            "INSERT INTO qe_decision_outcomes (decision_log_id,symbol,decision,decision_date,decision_engine_version," +
            "resolver_version,evaluation_method,eval_window_days,reference_price,bars_available,resolution_status,resolved_ts) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) " +
            "ON CONFLICT(decision_log_id,eval_window_days,evaluation_method) DO UPDATE SET " +
            "bars_available=excluded.bars_available, resolution_status=excluded.resolution_status, resolved_ts=excluded.resolved_ts"
          ).bind(
            d.id, d.symbol, d.decision, decisionDate, d.engine_version || null,
            OUTCOME_RESOLVER_VERSION, OUTCOME_EVAL_METHOD_CLOSE, w, referencePrice, bars.length,
            status, new Date().toISOString()
          ).run();
          pendingCount++;
          continue;
        }

        // ── Resolvable: compute all objective evidence in one pass over the window ──
        const windowBars = bars.slice(0, w);
        const evalBar = windowBars[w - 1];
        const evalPrice = evalBar.c;
        const priceChangePct = ((evalPrice - referencePrice) / referencePrice) * 100;

        let mfe = -Infinity, mae = Infinity; // per audit convention: min needs +Infinity, max needs -Infinity
        for (const b of windowBars) {
          mfe = Math.max(mfe, ((b.h - referencePrice) / referencePrice) * 100);
          mae = Math.min(mae, ((b.l - referencePrice) / referencePrice) * 100);
        }

        // Daily returns for volatility (reference_price as day 0), annualized stdev.
        const closesForVol = [referencePrice, ...windowBars.map(b => b.c)];
        const dailyReturns = [];
        for (let i = 1; i < closesForVol.length; i++) dailyReturns.push((closesForVol[i] - closesForVol[i - 1]) / closesForVol[i - 1]);
        const sd = _stdevPct(dailyReturns);
        const volatilityAnnualizedPct = sd != null ? sd * Math.sqrt(252) * 100 : null;

        // Data-gap detection: >OUTCOME_GAP_FLAG_DAYS calendar days between consecutive bars (incl. decision_date anchor).
        let hasGap = false;
        const dateChain = [decisionDate, ...windowBars.map(b => b.bar_date)];
        for (let i = 1; i < dateChain.length; i++) {
          if ((new Date(dateChain[i]) - new Date(dateChain[i - 1])) / 86400000 > OUTCOME_GAP_FLAG_DAYS) { hasGap = true; break; }
        }

        // Benchmark (NIFTYBEES proxy) — reference at/before decision_date, eval on the same eval_bar_date.
        let benchRef = null, benchEval = null, benchReturnPct = null, benchRelativePct = null;
        try {
          const brRow = await _barOnOrBefore(env, OUTCOME_BENCHMARK_SYMBOL, decisionDate);
          const beRow = await _barOn(env, OUTCOME_BENCHMARK_SYMBOL, evalBar.bar_date) ||
                        await _barOnOrBefore(env, OUTCOME_BENCHMARK_SYMBOL, evalBar.bar_date);
          if (brRow && beRow) {
            benchRef = brRow.c; benchEval = beRow.c;
            benchReturnPct = ((benchEval - benchRef) / benchRef) * 100;
            benchRelativePct = priceChangePct - benchReturnPct;
          }
        } catch (_) { /* benchmark unavailable — leave null, never fabricated */ }

        const regime = await _regimeAsOf(env, evalBar.bar_date);

        await env.QE_DB.prepare(
          "INSERT INTO qe_decision_outcomes (decision_log_id,symbol,decision,decision_date,decision_engine_version," +
          "resolver_version,evaluation_method,eval_window_days,reference_price,eval_bar_date,eval_price,price_change_pct," +
          "mfe_pct,mae_pct,volatility_annualized_pct,bars_available,benchmark_symbol,benchmark_reference_price," +
          "benchmark_eval_price,benchmark_return_pct,benchmark_relative_pct,regime_at_evaluation,resolution_status,resolved_ts) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
          "ON CONFLICT(decision_log_id,eval_window_days,evaluation_method) DO UPDATE SET " +
          "eval_bar_date=excluded.eval_bar_date, eval_price=excluded.eval_price, price_change_pct=excluded.price_change_pct," +
          "mfe_pct=excluded.mfe_pct, mae_pct=excluded.mae_pct, volatility_annualized_pct=excluded.volatility_annualized_pct," +
          "bars_available=excluded.bars_available, benchmark_symbol=excluded.benchmark_symbol," +
          "benchmark_reference_price=excluded.benchmark_reference_price, benchmark_eval_price=excluded.benchmark_eval_price," +
          "benchmark_return_pct=excluded.benchmark_return_pct, benchmark_relative_pct=excluded.benchmark_relative_pct," +
          "regime_at_evaluation=excluded.regime_at_evaluation, resolution_status=excluded.resolution_status, resolved_ts=excluded.resolved_ts"
        ).bind(
          d.id, d.symbol, d.decision, decisionDate, d.engine_version || null,
          OUTCOME_RESOLVER_VERSION, OUTCOME_EVAL_METHOD_CLOSE, w, referencePrice, evalBar.bar_date, evalPrice, priceChangePct,
          mfe, mae, volatilityAnnualizedPct, windowBars.length, OUTCOME_BENCHMARK_SYMBOL, benchRef,
          benchEval, benchReturnPct, benchRelativePct, regime,
          hasGap ? "RESOLVED_WITH_DATA_GAP" : "RESOLVED_NORMAL_WINDOW", new Date().toISOString()
        ).run();
        resolvedCount++;
      }
    } catch (e) {
      errorCount++;
      console.error("[outcome_resolver] " + d.symbol + " (decision_log_id=" + d.id + "): " + ((e && e.message) || e));
    }
  }

  return { ok: true, processed: decisions.length, resolved: resolvedCount, pending: pendingCount, errors: errorCount, windows: cfg.windows };
}

async function handleOutcomeResolverRun(env) {
  try { return cors(await resolveDecisionOutcomes(env)); }
  catch (e) { return corsErr(e.message || "Outcome resolver failed", 502); }
}

async function handleOutcomeResolverStatus(env) {
  try {
    const cfg = await _outcomeResolverConfig(env);
    const q = await env.QE_DB.prepare(
      "SELECT resolution_status, COUNT(*) AS n FROM qe_decision_outcomes GROUP BY resolution_status"
    ).all();
    const byStatus = {};
    for (const r of (q && q.results) || []) byStatus[r.resolution_status] = r.n;
    const latest = await env.QE_DB.prepare(
      "SELECT MAX(resolved_ts) AS ts FROM qe_decision_outcomes WHERE resolved_ts IS NOT NULL"
    ).first();
    return cors({
      ok: true, enabled: cfg.enabled, windows: cfg.windows,
      resolver_version: OUTCOME_RESOLVER_VERSION, benchmark_symbol: OUTCOME_BENCHMARK_SYMBOL,
      by_status: byStatus, last_resolved_ts: (latest && latest.ts) || null,
    });
  } catch (e) { return corsErr(e.message || "Status query failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION EVALUATION POLICY — Phase 1.1a (v4.76)
//
// Internal architectural abstraction, NOT a product feature. This is the ONLY place
// that interprets whether a decision's realized outcome was objectively favorable.
// Decision Quality Analytics (1.1b, below) consumes this and does no interpretation
// of its own. A future Decision Calibration Engine (Phase 1.2, not built here) will
// consume the same policy outputs — it must never reimplement these rules.
//
// Pure functions only: no DB access, no aggregation, no side effects. Input is a
// single resolved row from qe_decision_outcomes (+ optional decision-log context);
// output is a fixed-shape, fully-documented classification. Rows that are not yet
// in a terminal RESOLVED_* state return null — never force-classified, never guessed.
//
// EXPECTATION MAPPING (the one genuine judgment call in this file — isolated here,
// not scattered through analytics):
//   BULLISH  (STRONG_BUY, BUY, ACCUMULATE)      — expects price_change_pct > 0
//   BEARISH  (SELL, REDUCE, EXIT_IMMEDIATELY)   — expects price_change_pct < 0
//                                                  (confirms de-risking was justified)
//   NEUTRAL_STABLE (HOLD)                       — expects no severe drawdown while held;
//                                                  correct if mae_pct >= HOLD_ACCEPTABLE_DRAWDOWN_PCT
// A HOLD that suffers a severe drawdown anyway is classified FALSE_NEGATIVE (a missed
// de-risk signal) rather than left unclassified — this is what lets HOLD accuracy be
// measured at all. The threshold is a named, isolated constant, not a magic number
// buried in analytics.
// ═══════════════════════════════════════════════════════════════════════════════

const DECISION_EVAL_POLICY_VERSION   = "policy-v1.0";
const HOLD_ACCEPTABLE_DRAWDOWN_PCT   = -10; // institutional judgment call, isolated here — adjust only in this one place

const DECISION_EXPECTATION = {
  STRONG_BUY: "BULLISH", BUY: "BULLISH", ACCUMULATE: "BULLISH",
  HOLD: "NEUTRAL_STABLE",
  REDUCE: "BEARISH", SELL: "BEARISH", EXIT_IMMEDIATELY: "BEARISH",
};

// Evaluates ONE resolved outcome row into an objective classification.
// `ctx` (optional) carries decision_log fields not present on the outcome row itself
// (r_multiple, recommendation_confidence) — still sourced only from qe_decision_log,
// per the mandated evidence boundary; no third table is introduced.
function evaluateDecisionOutcome(outcomeRow, ctx) {
  if (!outcomeRow) return null;
  const st = outcomeRow.resolution_status;
  if (st !== "RESOLVED_NORMAL_WINDOW" && st !== "RESOLVED_WITH_DATA_GAP") return null; // only resolved evidence is ever evaluated

  const expectation = DECISION_EXPECTATION[outcomeRow.decision];
  const pct = outcomeRow.price_change_pct, mae = outcomeRow.mae_pct, mfe = outcomeRow.mfe_pct;
  ctx = ctx || {};

  let correct, classification;
  if (expectation === "BULLISH") {
    correct = pct > 0;
    classification = correct ? "TRUE_POSITIVE" : "FALSE_POSITIVE";
  } else if (expectation === "BEARISH") {
    correct = pct < 0;
    classification = correct ? "TRUE_POSITIVE" : "FALSE_POSITIVE"; // "positive" = the defensive call was justified
  } else if (expectation === "NEUTRAL_STABLE") {
    correct = mae >= HOLD_ACCEPTABLE_DRAWDOWN_PCT;
    classification = correct ? "TRUE_NEGATIVE" : "FALSE_NEGATIVE"; // "negative" = correctly predicted no crisis
  } else {
    return { decision_log_id: outcomeRow.decision_log_id, symbol: outcomeRow.symbol, decision: outcomeRow.decision,
      eval_window_days: outcomeRow.eval_window_days, expectation: null, correct: null,
      classification: "UNKNOWN_DECISION_TYPE", policy_version: DECISION_EVAL_POLICY_VERSION };
  }

  const upside_captured_pct   = expectation === "BULLISH" ? Math.max(0, pct) : null;
  const downside_avoided_pct  = expectation === "BEARISH" ? Math.max(0, -pct) : null;
  // Profit protection only means something for a defensive call made while the position
  // already had unrealized gains (r_multiple > 0 at decision time) AND price subsequently
  // fell — i.e., exiting locked in gains that would otherwise have eroded.
  const profit_protected_pct  = (expectation === "BEARISH" && ctx.r_multiple > 0 && pct < 0) ? Math.max(0, -pct) : null;

  return {
    decision_log_id: outcomeRow.decision_log_id, symbol: outcomeRow.symbol, decision: outcomeRow.decision,
    eval_window_days: outcomeRow.eval_window_days, expectation, correct, classification,
    price_change_pct: pct, mae_pct: mae, mfe_pct: mfe,
    upside_captured_pct, downside_avoided_pct, profit_protected_pct,
    benchmark_relative_pct: outcomeRow.benchmark_relative_pct,
    recommendation_confidence: ctx.recommendation_confidence != null ? ctx.recommendation_confidence : null,
    resolution_status: st, policy_version: DECISION_EVAL_POLICY_VERSION,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION QUALITY ANALYTICS — Phase 1.1b (v4.76)
//
// Pure consumer of evaluateDecisionOutcome() — computes no interpretation of its own,
// only counts/averages/buckets objective classifications. Computed on-demand at
// request time (no new table, no persistence, no pipeline stage) — this is a
// read-only report over qe_decision_log + qe_decision_outcomes exactly as directed;
// zero write risk, zero new evidence table.
//
// NOT built here (explicitly out of scope for Phase 1.1): Decision Calibration
// (no threshold recalibration proposal), Profit Protection Engine, Capital Rotation.
// "confidence_calibration_inputs" below is a grouped-evidence report only — it feeds
// a future calibration engine, it does not calibrate anything itself.
// ═══════════════════════════════════════════════════════════════════════════════

async function computeDecisionQualityAnalytics(env, windowFilter) {
  let logRows = [];
  try {
    const q = await env.QE_DB.prepare(
      "SELECT id,ts,symbol,decision,recommendation_confidence,r_multiple FROM qe_decision_log ORDER BY id ASC"
    ).all();
    logRows = (q && q.results) || [];
  } catch (e) { return { ok: false, error: "qe_decision_log read failed: " + ((e && e.message) || e) }; }

  let outcomeRows = [];
  try {
    const sql = windowFilter
      ? "SELECT * FROM qe_decision_outcomes WHERE eval_window_days=?1"
      : "SELECT * FROM qe_decision_outcomes";
    const q = windowFilter
      ? await env.QE_DB.prepare(sql).bind(windowFilter).all()
      : await env.QE_DB.prepare(sql).all();
    outcomeRows = (q && q.results) || [];
  } catch (e) { return { ok: false, error: "qe_decision_outcomes read failed: " + ((e && e.message) || e) }; }

  const logById = {};
  for (const l of logRows) logById[l.id] = l;

  // Evaluate every resolved outcome row via the Policy layer (single source of interpretation).
  const evaluations = [];
  for (const o of outcomeRows) {
    const ctx = logById[o.decision_log_id] || {};
    const ev = evaluateDecisionOutcome(o, { r_multiple: ctx.r_multiple, recommendation_confidence: ctx.recommendation_confidence });
    if (ev) evaluations.push(ev);
  }

  // ── Per decision-type, per window: accuracy + false positive/negative rates ──
  const byDecision = {}; // { [decision]: { [window]: { total, correct, tp, fp, tn, fn, upside:[], downside:[], profit_protected:[] } } }
  for (const ev of evaluations) {
    byDecision[ev.decision] = byDecision[ev.decision] || {};
    const bucket = byDecision[ev.decision][ev.eval_window_days] = byDecision[ev.decision][ev.eval_window_days] ||
      { total: 0, correct: 0, TRUE_POSITIVE: 0, FALSE_POSITIVE: 0, TRUE_NEGATIVE: 0, FALSE_NEGATIVE: 0,
        upside_captured_pct: [], downside_avoided_pct: [], profit_protected_pct: [] };
    bucket.total++;
    if (ev.correct) bucket.correct++;
    if (bucket[ev.classification] !== undefined) bucket[ev.classification]++;
    if (ev.upside_captured_pct != null) bucket.upside_captured_pct.push(ev.upside_captured_pct);
    if (ev.downside_avoided_pct != null) bucket.downside_avoided_pct.push(ev.downside_avoided_pct);
    if (ev.profit_protected_pct != null) bucket.profit_protected_pct.push(ev.profit_protected_pct);
  }
  const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
  const decisionQuality = {};
  for (const decision of Object.keys(byDecision)) {
    decisionQuality[decision] = {};
    for (const window of Object.keys(byDecision[decision])) {
      const b = byDecision[decision][window];
      decisionQuality[decision][window] = {
        total_evaluated: b.total,
        accuracy: b.total ? b.correct / b.total : null,
        true_positive: b.TRUE_POSITIVE, false_positive: b.FALSE_POSITIVE,
        true_negative: b.TRUE_NEGATIVE, false_negative: b.FALSE_NEGATIVE,
        avg_upside_captured_pct: avg(b.upside_captured_pct),
        avg_downside_avoided_pct: avg(b.downside_avoided_pct),
        avg_profit_protected_pct: avg(b.profit_protected_pct),
        profit_protected_events: b.profit_protected_pct.length,
      };
    }
  }

  // ── Decision effectiveness: simple objective aggregate (documented, not weighted/opinionated) ──
  // = correct / total across ALL decision types combined, per window. No subjective weighting applied.
  const effectivenessByWindow = {};
  for (const ev of evaluations) {
    effectivenessByWindow[ev.eval_window_days] = effectivenessByWindow[ev.eval_window_days] || { total: 0, correct: 0 };
    effectivenessByWindow[ev.eval_window_days].total++;
    if (ev.correct) effectivenessByWindow[ev.eval_window_days].correct++;
  }
  const decisionEffectiveness = {};
  for (const w of Object.keys(effectivenessByWindow)) {
    const e = effectivenessByWindow[w];
    decisionEffectiveness[w] = { total_evaluated: e.total, effectiveness: e.total ? e.correct / e.total : null };
  }

  // ── Confidence calibration inputs: grouped evidence only, no recalibration performed ──
  const confBuckets = {}; // { [window]: { "50-60": {total,correct}, ... } }
  for (const ev of evaluations) {
    if (ev.recommendation_confidence == null) continue;
    const lo = Math.floor(ev.recommendation_confidence / 10) * 10;
    const label = `${lo}-${lo + 10}`;
    confBuckets[ev.eval_window_days] = confBuckets[ev.eval_window_days] || {};
    confBuckets[ev.eval_window_days][label] = confBuckets[ev.eval_window_days][label] || { total: 0, correct: 0 };
    confBuckets[ev.eval_window_days][label].total++;
    if (ev.correct) confBuckets[ev.eval_window_days][label].correct++;
  }
  const confidenceCalibrationInputs = {};
  for (const w of Object.keys(confBuckets)) {
    confidenceCalibrationInputs[w] = {};
    for (const label of Object.keys(confBuckets[w])) {
      const b = confBuckets[w][label];
      confidenceCalibrationInputs[w][label] = { total: b.total, correct: b.correct, empirical_accuracy: b.total ? b.correct / b.total : null };
    }
  }

  // ── Average holding period: purely from qe_decision_log timestamps (first entry → terminal
  // SELL/EXIT_IMMEDIATELY entry per symbol). No third evidence table introduced. ──
  const bySymbol = {};
  for (const l of logRows) { bySymbol[l.symbol] = bySymbol[l.symbol] || []; bySymbol[l.symbol].push(l); }
  const holdingDays = [];
  for (const symbol of Object.keys(bySymbol)) {
    const rows = bySymbol[symbol].sort((a, b) => new Date(a.ts) - new Date(b.ts));
    const first = rows[0];
    const terminal = rows.find(r => r.decision === "SELL" || r.decision === "EXIT_IMMEDIATELY");
    if (first && terminal && terminal !== first) {
      holdingDays.push((new Date(terminal.ts) - new Date(first.ts)) / 86400000);
    }
  }
  const avgHoldingPeriodDays = holdingDays.length ? avg(holdingDays) : null;

  return {
    ok: true,
    policy_version: DECISION_EVAL_POLICY_VERSION,
    evaluated_count: evaluations.length,
    decision_log_rows: logRows.length,
    outcome_rows: outcomeRows.length,
    decision_quality: decisionQuality,
    decision_effectiveness: decisionEffectiveness,
    confidence_calibration_inputs: confidenceCalibrationInputs,
    avg_holding_period_days: avgHoldingPeriodDays,
    holding_period_sample_size: holdingDays.length,
    note: holdingDays.length === 0 ? "No symbol has yet reached a terminal SELL/EXIT_IMMEDIATELY decision — holding period not yet measurable." : undefined,
  };
}

async function handleDecisionQualityAnalytics(url, env) {
  try {
    const windowParam = url.searchParams.get("window");
    const windowFilter = windowParam ? parseInt(windowParam, 10) : null;
    return cors(await computeDecisionQualityAnalytics(env, windowFilter));
  } catch (e) { return corsErr(e.message || "Decision quality analytics failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION CALIBRATION ENGINE — Phase 1.2 (v4.77)
//
// Pipeline position: Decision Engine → Decision Log → Outcome Resolver (1.0) →
// Evaluation Policy (1.1a) → Quality Analytics (1.1b) → CALIBRATION ENGINE (here).
// Consumes only qe_decision_log + qe_decision_outcomes, and reuses
// evaluateDecisionOutcome() (Phase 1.1a) UNCHANGED for classification — this engine
// adds zero new interpretation of "was a decision correct," it only asks "does the
// evidence justify adjusting a production parameter." No new evidence table.
//
// GOVERNANCE (mandatory, non-negotiable): this engine RECOMMENDS ONLY. It never
// writes to PIE_CONFIG, mapVerdictToDecision, computeRecommendationConfidence, or
// any production threshold. Every output's approval_status defaults to PROPOSED
// (or NOT_APPLICABLE for a NO_CALIBRATION result) and requires a human-executed,
// versioned PIE_CONFIG bump before it can ever take effect — identical governance
// posture to the existing computeCalibrationProposal() scaffold (line ~8192,
// untouched by this phase), just applied to the new, automated evidence layer
// instead of manually-logged trades.
//
// GUARDRAILS (applied uniformly to every parameter, before any recommendation can
// be generated — implements all 5 mandated categories):
//   1. Minimum evidence      — sample size >= DECISION_CALIBRATION_MIN_SAMPLES (KV, default 30)
//   2. Stability over time   — evidence must span >= 3 distinct decision dates (protects
//                               against a single clustered run masquerading as a trend —
//                               directly relevant today, since all current evidence is
//                               from one date)
//   3. Overfitting protection— evidence split in half by date; the accuracy direction
//                               (above/below 50%) must agree in BOTH halves, a crude but
//                               real out-of-sample check
//   4. Cross-regime consistency — regime-sensitive parameters additionally require the
//                               effect to not be contradicted by another regime with
//                               comparable sample size
//   5. Statistical significance — two-proportion z-test (95% CI) against a neutral
//                               baseline before a gap is treated as real, not noise
// If ANY guardrail fails, the parameter returns NO_CALIBRATION with the specific
// reason — never a forced recommendation.
// ═══════════════════════════════════════════════════════════════════════════════

const CALIBRATION_ENGINE_VERSION           = "calib-v1.0";
const DECISION_CALIBRATION_MIN_DATES       = 3;    // stability guardrail
const DECISION_CALIBRATION_Z_THRESHOLD     = 1.96; // 95% two-tailed significance

async function _calibrationConfig(env) {
  let minSamples = 30, minSectorSamples = 60; // sector held to a stricter bar — directive's own caution
  try { const v = await env.KITE_STORE.get("DECISION_CALIBRATION_MIN_SAMPLES"); if (v) minSamples = Number(v) || minSamples; } catch (_) {}
  try { const v = await env.KITE_STORE.get("DECISION_CALIBRATION_MIN_SECTOR_SAMPLES"); if (v) minSectorSamples = Number(v) || minSectorSamples; } catch (_) {}
  return { minSamples, minSectorSamples };
}

// One-proportion z-test: is empirical accuracy significantly different from a 50% neutral baseline?
function _zTestVsBaseline(correctCount, n, p0) {
  if (!n) return null;
  p0 = p0 == null ? 0.5 : p0;
  const pHat = correctCount / n;
  const se = Math.sqrt((p0 * (1 - p0)) / n);
  if (se === 0) return null;
  return (pHat - p0) / se;
}

// The 5 mandated guardrails, applied to one evidence group. Never mutates input.
function _applyCalibrationGuardrails(evidenceGroup, minSamples) {
  const n = evidenceGroup.length;
  if (n < minSamples) return { pass: false, reason: `INSUFFICIENT_EVIDENCE: ${n} samples, minimum ${minSamples} required` };

  const distinctDates = new Set(evidenceGroup.map(e => e.decision_date)).size;
  if (distinctDates < DECISION_CALIBRATION_MIN_DATES) {
    return { pass: false, reason: `INSUFFICIENT_TIME_SPREAD: evidence spans only ${distinctDates} distinct decision date(s), minimum ${DECISION_CALIBRATION_MIN_DATES} required — a single clustered run cannot establish a stable trend` };
  }

  const sorted = [...evidenceGroup].sort((a, b) => new Date(a.decision_date) - new Date(b.decision_date));
  const mid = Math.floor(sorted.length / 2);
  const firstHalf = sorted.slice(0, mid), secondHalf = sorted.slice(mid);
  const accOf = arr => arr.length ? arr.filter(e => e.correct).length / arr.length : null;
  const accFirst = accOf(firstHalf), accSecond = accOf(secondHalf);
  if (accFirst != null && accSecond != null) {
    const flipsSide = (accFirst > 0.5 && accSecond < 0.5) || (accFirst < 0.5 && accSecond > 0.5); // exact 0.5 in either half = no clear side, let the significance test decide instead
    if (flipsSide) {
      return { pass: false, reason: `UNSTABLE_ACROSS_TIME: split-half accuracy disagrees (${(accFirst * 100).toFixed(1)}% earlier vs ${(accSecond * 100).toFixed(1)}% later) — effect does not replicate out-of-sample` };
    }
  }

  const correctCount = evidenceGroup.filter(e => e.correct).length;
  const z = _zTestVsBaseline(correctCount, n, 0.5);
  const significant = z != null && Math.abs(z) >= DECISION_CALIBRATION_Z_THRESHOLD;
  if (!significant) {
    return { pass: false, reason: `NOT_STATISTICALLY_SIGNIFICANT: z=${z != null ? z.toFixed(2) : "n/a"}, need |z|>=${DECISION_CALIBRATION_Z_THRESHOLD} (95% CI) vs neutral baseline` };
  }

  return { pass: true, n, distinctDates, accuracy: correctCount / n, z, correctCount };
}

// Builds one CalibrationRecommendation. `interpretFn(stats, groupKey)` returns
// {recommended_value, expected_portfolio_impact, risks_and_trade_offs} for a PASSING
// group, or null to force NO_CALIBRATION even after guardrails pass (e.g. accuracy
// gap too small to be actionable).
function _buildRecommendation(parameterKey, groupKey, currentValue, evidenceGroup, cfg, interpretFn, regimeSet) {
  const minSamples = parameterKey === "SECTOR_SENSITIVITY" ? cfg.minSectorSamples : cfg.minSamples;
  const guard = _applyCalibrationGuardrails(evidenceGroup, minSamples);
  const base = {
    recommendation_id: `${parameterKey}:${groupKey}:${CALIBRATION_ENGINE_VERSION}`,
    parameter: parameterKey, group: groupKey, current_value: currentValue,
    sample_size: evidenceGroup.length,
    applicable_regimes: regimeSet && regimeSet.size ? Array.from(regimeSet) : "ALL",
    recommendation_version: CALIBRATION_ENGINE_VERSION,
    generated_ts: new Date().toISOString(),
  };
  if (!guard.pass) {
    return { ...base, status: "NO_CALIBRATION", recommended_value: null, supporting_evidence: null,
      statistical_confidence: null, expected_portfolio_impact: null, risks_and_trade_offs: null,
      reason: guard.reason, approval_status: "NOT_APPLICABLE" };
  }
  const interp = interpretFn(guard, groupKey);
  if (!interp) {
    return { ...base, status: "NO_CALIBRATION", recommended_value: null,
      supporting_evidence: { n: guard.n, accuracy: guard.accuracy, z: guard.z },
      statistical_confidence: Math.abs(guard.z), expected_portfolio_impact: null, risks_and_trade_offs: null,
      reason: "GUARDRAILS_PASSED_BUT_EFFECT_NOT_ACTIONABLE: statistically significant but the measured gap is too small to warrant a parameter change",
      approval_status: "NOT_APPLICABLE" };
  }
  return { ...base, status: "PROPOSED", recommended_value: interp.recommended_value,
    supporting_evidence: { n: guard.n, accuracy: guard.accuracy, correct: guard.correctCount, z: guard.z, distinct_decision_dates: guard.distinctDates },
    statistical_confidence: Math.abs(guard.z), expected_portfolio_impact: interp.expected_portfolio_impact,
    risks_and_trade_offs: interp.risks_and_trade_offs, reason: "GUARDRAILS_PASSED", approval_status: "PROPOSED" };
}

// Fixed NO_CALIBRATION for a parameter whose required evidence isn't yet captured by
// the schema — an honest, complete result (a real evidence-gap detection), not a stub.
function _infrastructureGapRecommendation(parameterKey, currentValue, reason) {
  return {
    recommendation_id: `${parameterKey}:ALL:${CALIBRATION_ENGINE_VERSION}`,
    parameter: parameterKey, group: "ALL", current_value: currentValue, status: "NO_CALIBRATION",
    recommended_value: null, sample_size: 0, applicable_regimes: "ALL", statistical_confidence: null,
    supporting_evidence: null, expected_portfolio_impact: null, risks_and_trade_offs: null,
    reason: "INSUFFICIENT_INFRASTRUCTURE: " + reason,
    recommendation_version: CALIBRATION_ENGINE_VERSION, approval_status: "NOT_APPLICABLE",
    generated_ts: new Date().toISOString(),
  };
}

// Gathers the joined, policy-evaluated evidence set once, shared by every analyzer below.
async function _gatherCalibrationEvidence(env) {
  const logQ = await env.QE_DB.prepare(
    "SELECT id,ts,symbol,decision,health_score,recommendation_confidence,r_multiple,pillars_json,strongest_pillar,weakest_pillar FROM qe_decision_log"
  ).all();
  const logRows = (logQ && logQ.results) || [];
  const logById = {}; for (const l of logRows) logById[l.id] = l;

  const outQ = await env.QE_DB.prepare("SELECT * FROM qe_decision_outcomes").all();
  const outcomeRows = (outQ && outQ.results) || [];

  const evaluations = [];
  for (const o of outcomeRows) {
    const ctx = logById[o.decision_log_id] || {};
    const ev = evaluateDecisionOutcome(o, { r_multiple: ctx.r_multiple, recommendation_confidence: ctx.recommendation_confidence }); // Phase 1.1a, reused unchanged
    if (!ev || ev.correct == null) continue; // unresolved or UNKNOWN_DECISION_TYPE — not usable evidence
    let pillars = null; try { pillars = ctx.pillars_json ? JSON.parse(ctx.pillars_json) : null; } catch (_) {}
    evaluations.push({ ...ev, health_score: ctx.health_score, strongest_pillar: ctx.strongest_pillar,
      pillars, sector: SECTOR_MAP[o.symbol] || "OTHER", regime: o.regime_at_evaluation || null, r_multiple: ctx.r_multiple });
  }
  return evaluations;
}

// ── The 11 calibratable parameter categories ────────────────────────────────────
async function computeCalibrationRecommendations(env) {
  const cfg = await _calibrationConfig(env);
  let evaluations;
  try { evaluations = await _gatherCalibrationEvidence(env); }
  catch (e) { return { ok: false, error: "Evidence gathering failed: " + ((e && e.message) || e) }; }

  const recommendations = [];
  const groupAndRecommend = (parameterKey, currentValue, groupFn, interpretFn) => {
    const groups = {};
    for (const ev of evaluations) {
      const g = groupFn(ev);
      if (g == null) continue;
      groups[g] = groups[g] || [];
      groups[g].push(ev);
    }
    for (const groupKey of Object.keys(groups)) {
      const group = groups[groupKey];
      const regimeSet = new Set(group.map(e => e.regime).filter(Boolean));
      recommendations.push(_buildRecommendation(parameterKey, groupKey, currentValue, group, cfg, interpretFn, regimeSet));
    }
    if (!Object.keys(groups).length) {
      recommendations.push({ recommendation_id: `${parameterKey}:NONE:${CALIBRATION_ENGINE_VERSION}`, parameter: parameterKey,
        group: "NONE", current_value: currentValue, status: "NO_CALIBRATION", recommended_value: null, sample_size: 0,
        applicable_regimes: "ALL", statistical_confidence: null, supporting_evidence: null, expected_portfolio_impact: null,
        risks_and_trade_offs: null, reason: "INSUFFICIENT_EVIDENCE: no resolved outcomes available for this parameter yet",
        recommendation_version: CALIBRATION_ENGINE_VERSION, approval_status: "NOT_APPLICABLE", generated_ts: new Date().toISOString() });
    }
  };

  const gapPct = (accuracy) => Math.round(Math.abs(accuracy - 0.5) * 1000) / 10; // pp deviation from neutral, for impact text

  // 1. Confidence thresholds — is stated confidence well-calibrated against empirical accuracy?
  groupAndRecommend("CONFIDENCE_BASELINE", "baseline=85 (computeRecommendationConfidence)",
    ev => { const lo = Math.floor((ev.recommendation_confidence || 0) / 10) * 10; return `conf_${lo}-${lo + 10}`; },
    (guard, groupKey) => {
      const statedMid = parseInt(groupKey.split("_")[1], 10) + 5;
      const gap = Math.round((statedMid - guard.accuracy * 100) * 10) / 10;
      if (Math.abs(gap) < 10) return null; // gap not material
      return { recommended_value: `adjust baseline/penalty weights toward empirical accuracy (~${(guard.accuracy * 100).toFixed(1)}% observed vs ~${statedMid}% stated)`,
        expected_portfolio_impact: `${guard.n} decisions in this confidence band show a ${gapPct(guard.accuracy)}pp deviation from neutral; confidence is currently ${gap > 0 ? "overstated" : "understated"} by ~${Math.abs(gap).toFixed(1)}pp`,
        risks_and_trade_offs: "Recalibrating confidence changes downstream messaging/UI trust signals only, not the decision itself — low execution risk, but affects how strongly Siva should weight the stated confidence." };
    });

  // 2. Health score thresholds — PIE_CONFIG.bands
  groupAndRecommend("HEALTH_SCORE_BANDS", JSON.stringify(PIE_CONFIG.bands),
    ev => { const h = ev.health_score; if (h == null) return null; const lo = Math.floor(h / 10) * 10; return `health_${lo}-${lo + 10}`; },
    (guard, groupKey) => {
      if (guard.accuracy >= 0.5) return null; // only flag bands that underperform — bands doing fine need no change
      return { recommended_value: `review PIE_CONFIG.bands near ${groupKey} — accuracy below neutral in this health range`,
        expected_portfolio_impact: `${guard.n} decisions with health in ${groupKey} show only ${(guard.accuracy * 100).toFixed(1)}% accuracy`,
        risks_and_trade_offs: "Moving a band threshold reclassifies verdicts for future holdings in this health range — requires a versioned PIE_CONFIG bump and re-validation, not a silent change." };
    });

  // 3. Indicator weightings — PIE_CONFIG.weights, grouped by strongest_pillar
  groupAndRecommend("INDICATOR_WEIGHTING", JSON.stringify(PIE_CONFIG.weights),
    ev => ev.strongest_pillar || null,
    (guard, groupKey) => {
      if (guard.accuracy >= 0.5) return null;
      return { recommended_value: `consider down-weighting '${groupKey}' pillar in PIE_CONFIG.weights — underperforms as the strongest signal`,
        expected_portfolio_impact: `${guard.n} decisions where '${groupKey}' was the strongest pillar show only ${(guard.accuracy * 100).toFixed(1)}% accuracy`,
        risks_and_trade_offs: "Pillar weights are shared across ALL regimes' weight sets structurally — changing one regime's weight for this pillar does not automatically fix others; each must be evaluated separately once evidence allows." };
    });

  // 4. Decision score cutoffs — schema gap (discovery-time score not persisted in decision_log)
  recommendations.push(_infrastructureGapRecommendation("DECISION_SCORE_CUTOFF", "not tracked in PIE_CONFIG today",
    "the discovery-time QEGate score is not persisted in qe_decision_log (only portfolio health_score is) — measuring this parameter would require extending the decision log schema, which is out of scope for a calibration engine that must not modify Phase 1.0/1.1"));

  // 5. Regime sensitivity — does the CURRENT regime-conditional weighting hold up per regime?
  groupAndRecommend("REGIME_SENSITIVITY", JSON.stringify(PIE_CONFIG.weights),
    ev => ev.regime ? `regime_${ev.regime}` : null,
    (guard, groupKey) => {
      if (guard.accuracy >= 0.5) return null;
      return { recommended_value: `review PIE_CONFIG.weights for regime '${groupKey.replace("regime_", "")}' — decisions evaluated in this regime underperform`,
        expected_portfolio_impact: `${guard.n} decisions evaluated while regime was ${groupKey.replace("regime_", "")} show only ${(guard.accuracy * 100).toFixed(1)}% accuracy`,
        risks_and_trade_offs: "Regime is reconstructed retroactively via the NIFTYBEES proxy (documented in Phase 1.0), not the live regime at decision time — treat as directional evidence, cross-check against the live regime badge before acting." };
    });

  // 6. Sector sensitivity — stricter min sample, via existing SECTOR_MAP (no new storage)
  groupAndRecommend("SECTOR_SENSITIVITY", "no sector-specific weighting in PIE_CONFIG today",
    ev => ev.sector || null,
    (guard, groupKey) => {
      if (guard.accuracy >= 0.5) return null;
      return { recommended_value: `consider a sector-level caution flag for '${groupKey}'`,
        expected_portfolio_impact: `${guard.n} decisions in sector '${groupKey}' show only ${(guard.accuracy * 100).toFixed(1)}% accuracy`,
        risks_and_trade_offs: "Sector evidence is held to a stricter minimum sample (default 60 vs 30) precisely because sector effects are easy to overfit to a handful of names — treat any PROPOSED sector recommendation with extra scrutiny even after guardrails pass." };
    });

  // 7. Holding-period guidance — compare accuracy across the configured eval windows
  groupAndRecommend("HOLDING_PERIOD_GUIDANCE", "2-4 week swing horizon (product assumption, not in PIE_CONFIG)",
    ev => `${ev.decision}_window_${ev.eval_window_days}d`,
    (guard, groupKey) => {
      return { recommended_value: `${groupKey}: ${(guard.accuracy * 100).toFixed(1)}% accuracy at this horizon (comparative — review against other windows for the same decision type once all are populated)`,
        expected_portfolio_impact: `${guard.n} decisions measured at this specific horizon`,
        risks_and_trade_offs: "This reports per-window accuracy for comparison, not a recommended horizon change by itself — a genuine holding-period recommendation requires comparing multiple windows for the SAME decision type side by side, which needs evidence across every configured PIE_OUTCOME_WINDOWS value simultaneously." };
    });

  // 8. Profit-protection sensitivity — BEARISH decisions only, bucketed by r_multiple at decision time.
  // Only meaningful where a defensive call was made while a position already had unrealized gains.
  groupAndRecommend("PROFIT_PROTECTION_SENSITIVITY", "no explicit r_multiple trigger threshold in PIE_CONFIG today",
    ev => { if (ev.expectation !== "BEARISH" || ev.r_multiple == null) return null; const lo = Math.floor(ev.r_multiple); return `r_multiple_${lo}-${lo + 1}`; },
    (guard, groupKey) => {
      return { recommended_value: `${groupKey}: ${(guard.accuracy * 100).toFixed(1)}% of defensive calls made at this profit level were justified`,
        expected_portfolio_impact: `${guard.n} SELL/REDUCE/EXIT decisions made while r_multiple was in ${groupKey}`,
        risks_and_trade_offs: "Informs whether the r_multiple level at which QuantEdge starts recommending defensive action should shift — no automatic trigger threshold exists in PIE_CONFIG today, so any change requires first defining one, versioned, before this evidence can be applied." };
    });

  // 9. Risk weighting — PIE_CONFIG.weights[*].risk, grouped by risk-pillar score band
  groupAndRecommend("RISK_WEIGHTING", JSON.stringify(Object.fromEntries(Object.entries(PIE_CONFIG.weights).map(([k, v]) => [k, v.risk]))),
    ev => { const r = ev.pillars && ev.pillars.risk; if (r == null) return null; const lo = Math.floor(r / 20) * 20; return `risk_pillar_${lo}-${lo + 20}`; },
    (guard, groupKey) => {
      if (guard.accuracy >= 0.5) return null;
      return { recommended_value: `review risk-pillar weighting — decisions with risk pillar in ${groupKey} underperform`,
        expected_portfolio_impact: `${guard.n} decisions with risk pillar score in this band show only ${(guard.accuracy * 100).toFixed(1)}% accuracy`,
        risks_and_trade_offs: "Risk pillar feeds health_score which feeds the decision itself — changing this weight has second-order effects on every other pillar's relative influence, not just risk." };
    });

  // 10. Capital-allocation preferences — infrastructure gap (no capital/sizing outcome evidence stored)
  recommendations.push(_infrastructureGapRecommendation("CAPITAL_ALLOCATION_PREFERENCE", "not tracked",
    "neither qe_decision_log nor qe_decision_outcomes stores capital amounts or position-sizing outcomes — this parameter is out of the current two-table evidence boundary entirely; would require Phase 2 (Portfolio Optimizer) evidence, not Phase 1.2's"));

  // 11. Signal reliability by regime — decision-type × regime reliability table
  groupAndRecommend("SIGNAL_RELIABILITY_BY_REGIME", "decisions treated uniformly regardless of regime today",
    ev => ev.regime ? `${ev.decision}_in_${ev.regime}` : null,
    (guard, groupKey) => {
      return { recommended_value: `${groupKey}: ${(guard.accuracy * 100).toFixed(1)}% empirical reliability`,
        expected_portfolio_impact: `${guard.n} decisions of this type evaluated in this regime`,
        risks_and_trade_offs: "A reliability table, not an automatic weighting change — informs how much extra scrutiny Siva should apply to a given decision type under a given regime, a human-in-the-loop signal rather than a config parameter." };
    });

  return {
    ok: true, engine_version: CALIBRATION_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    evidence_evaluated: evaluations.length, min_samples_config: cfg,
    proposed_count: recommendations.filter(r => r.status === "PROPOSED").length,
    no_calibration_count: recommendations.filter(r => r.status === "NO_CALIBRATION").length,
    recommendations,
  };
}

async function handleCalibrationRecommendations(env) {
  try { return cors(await computeCalibrationRecommendations(env)); }
  catch (e) { return corsErr(e.message || "Calibration recommendations failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PROFIT PROTECTION ENGINE — Phase 2.0 (v4.78)
//
// Pipeline position: Decision Engine → Decision Log → Outcome Resolver (1.0) →
// Evaluation Policy (1.1a) → Quality Analytics (1.1b) → Calibration (1.2) →
// PROFIT PROTECTION ENGINE (here). Read-only consumer of every upstream layer;
// modifies none of them. No new table — computed on-demand, zero writes.
//
// SCOPE BOUNDARY (guardrail: never duplicate Decision Engine responsibilities):
// this engine only evaluates holdings the Decision Engine currently says to
// CONTINUE (STRONG_BUY/BUY/ACCUMULATE/HOLD) AND that are profitable (r_multiple>0
// in qe_holdings, the live portfolio snapshot). If the Decision Engine has already
// called REDUCE/SELL/EXIT_IMMEDIATELY, that IS the protective action — this engine
// explicitly defers rather than issuing a second, possibly conflicting opinion.
//
// NOT A FIXED-PROFIT-TARGET ENGINE (mandatory guardrail): r_multiple/pnl_pct are
// used ONLY as the profitability eligibility filter and as contextual evidence in
// the output — they are never part of the classification logic itself. The
// classification is driven entirely by already-computed evidence QuantEdge
// produces today: pillar scores, conviction trend, proximity to the monitored
// stop/level, and portfolio concentration — all read from qe_holdings.evidence_json
// and the latest qe_decision_log row per symbol, NOT recomputed.
//
// REGIME- AND PORTFOLIO-AWARE (mandatory guardrails): current regime (read from
// the same qe_pipe_regime KV key the live pipeline itself reads — not recomputed)
// tightens the signal threshold in RISK-OFF; portfolio_concentration_flag (already
// computed and stored per decision by persistDecisionLog) counts as an erosion
// signal — neither is ignored.
//
// ENRICHMENT, NOT A GATE: Decision Quality Analytics (1.1b) and Calibration (1.2)
// are consulted for contextual evidence (matched by r_multiple bucket / decision×
// regime) but never block a classification — Phase 1's own audit already
// established that calibration will legitimately return NO_CALIBRATION until more
// evidence accrues, and this engine must still function on current live evidence
// in that case, not go silent.
// ═══════════════════════════════════════════════════════════════════════════════

const PROFIT_PROTECTION_ENGINE_VERSION = "pp-v1.0";
const PP_RISK_PILLAR_CAUTION_BELOW  = 60; // named, isolated — NOT a profit-percentage rule
const PP_TREND_PILLAR_CAUTION_BELOW = 60; // named, isolated
const PP_NEAR_STOP_PCT              = 3;  // named, isolated — % distance to monitored stop/level
const PP_CONTINUE_DECISIONS = new Set(["STRONG_BUY", "BUY", "ACCUMULATE", "HOLD"]);

// Latest qe_decision_log row per symbol — reused as "today's decision," never recomputed.
async function _latestDecisionsBySymbol(env) {
  const q = await env.QE_DB.prepare(
    "SELECT d.* FROM qe_decision_log d INNER JOIN " +
    "(SELECT symbol, MAX(ts) AS max_ts FROM qe_decision_log GROUP BY symbol) latest " +
    "ON d.symbol = latest.symbol AND d.ts = latest.max_ts"
  ).all();
  const rows = (q && q.results) || [];
  const bySymbol = {}; for (const r of rows) bySymbol[r.symbol] = r;
  return bySymbol;
}

// Same KV key and parsing the live pipeline itself reads (line ~7641) — reused, not recomputed.
async function _currentRegimeForPP(env) {
  try { const rg = await env.KITE_STORE.get("qe_regime"); if (rg) { const ro = JSON.parse(rg); return ro.regime || ro.label || "DEFAULT"; } } catch (_) {}
  return "DEFAULT";
}

function _ppRisksFor(classification, signals) {
  if (classification === "LET_PROFITS_RUN") return "No erosion signals present today, but this is a point-in-time read — regime or conviction can shift before the next pipeline run; re-evaluate daily, don't treat as a standing guarantee.";
  if (classification === "HOLD") return "Evidence is currently mixed but below the protection threshold — a single additional erosion signal next run would move this to PARTIAL_PROFIT_PROTECTION.";
  if (classification === "PARTIAL_PROFIT_PROTECTION") return `Signals present (${signals.join(", ")}) may reflect genuine deterioration or short-term noise — partial protection balances both without fully exiting a still-plausible winner.`;
  if (classification === "FULL_PROFIT_PROTECTION") return `Multiple compounding signals (${signals.join(", ")}) — the risk of giving back accumulated profit currently outweighs the case for continued participation; opportunity cost if this reflects noise rather than a genuine reversal.`;
  return null;
}

function _evaluateProfitProtection(holding, decisionLogRow, regime, calibEnrichment) {
  const symbol = holding.symbol;
  const base = { symbol, engine_version: PROFIT_PROTECTION_ENGINE_VERSION, generated_ts: new Date().toISOString() };

  if (holding.r_multiple == null || holding.r_multiple <= 0) return null; // not profitable — out of scope, not an error

  if (!decisionLogRow) {
    return { ...base, recommendation: "NO_CHANGE", confidence: null, explanation: "No logged decision found for this symbol yet.",
      reason: "INSUFFICIENT_EVIDENCE", supporting_evidence: null, risks: null, reversal_conditions: null };
  }
  if (!PP_CONTINUE_DECISIONS.has(decisionLogRow.decision)) {
    return { ...base, recommendation: "DEFERRED_TO_DECISION_ENGINE", confidence: decisionLogRow.recommendation_confidence != null ? decisionLogRow.recommendation_confidence : null,
      explanation: `Decision Engine already recommends ${decisionLogRow.decision} for this holding — Profit Protection Engine does not duplicate or override an active de-risking call.`,
      reason: "DECISION_ENGINE_ALREADY_PROTECTIVE", supporting_evidence: { current_decision: decisionLogRow.decision },
      risks: null, reversal_conditions: decisionLogRow.reversal_conditions || null };
  }

  let evidence; try { evidence = holding.evidence_json ? JSON.parse(holding.evidence_json) : null; } catch (_) { evidence = null; }
  if (!evidence || !evidence.pillars) {
    return { ...base, recommendation: "NO_CHANGE", confidence: null, explanation: "Pillar evidence unavailable for this holding in qe_holdings.",
      reason: "INSUFFICIENT_EVIDENCE", supporting_evidence: null, risks: null, reversal_conditions: decisionLogRow.reversal_conditions || null };
  }

  const pillars = evidence.pillars;
  const convictionTrend = holding.conviction_trend || decisionLogRow.conviction_trend;
  const monitorDist = evidence.monitor_next && typeof evidence.monitor_next.dist === "number" ? evidence.monitor_next.dist : null;

  const signals = [];
  if (convictionTrend === "WEAKENING") signals.push("conviction_weakening");
  if (pillars.risk != null && pillars.risk < PP_RISK_PILLAR_CAUTION_BELOW) signals.push("risk_pillar_low");
  if (pillars.trend != null && pillars.trend < PP_TREND_PILLAR_CAUTION_BELOW) signals.push("trend_pillar_weakening");
  if (monitorDist != null && monitorDist < PP_NEAR_STOP_PCT) signals.push("near_stop_level");
  if (decisionLogRow.portfolio_concentration_flag) signals.push("portfolio_concentration");

  // Regime-aware threshold — RISK-OFF tightens by one signal. Never ignores regime.
  const riskOff = regime === "RISK-OFF";
  const partialThreshold = riskOff ? 1 : 2;
  const fullThreshold = riskOff ? 2 : 3;

  let classification;
  if (signals.length === 0 && convictionTrend === "IMPROVING") classification = "LET_PROFITS_RUN";
  else if (signals.length < partialThreshold) classification = "HOLD";
  else if (signals.length < fullThreshold) classification = "PARTIAL_PROFIT_PROTECTION";
  else classification = "FULL_PROFIT_PROTECTION";

  const confidence = decisionLogRow.recommendation_confidence != null ? decisionLogRow.recommendation_confidence
                    : (holding.data_confidence != null ? holding.data_confidence : null);

  return {
    ...base, recommendation: classification, confidence,
    explanation: `${signals.length} erosion signal(s) detected${signals.length ? " (" + signals.join(", ") + ")" : ""} — regime=${regime} (thresholds: partial>=${partialThreshold}, full>=${fullThreshold} signals). Classification is driven by pillar/conviction/stop-proximity evidence, not by the ${holding.r_multiple.toFixed(2)}R unrealized profit itself.`,
    reason: "EVALUATED", risks: _ppRisksFor(classification, signals),
    reversal_conditions: decisionLogRow.reversal_conditions || null,
    supporting_evidence: {
      r_multiple: holding.r_multiple, pnl_pct: holding.pnl_pct, health_score: holding.health_score,
      conviction_trend: convictionTrend, pillars, monitor_next: evidence.monitor_next || null,
      regime, signals, portfolio_concentration_flag: !!decisionLogRow.portfolio_concentration_flag,
      calibration_enrichment: calibEnrichment,
    },
  };
}

// Canonical active-holdings reader (UAT fix, Bug #1/#2). qe_holdings has no
// status/is_active column — qty is the only signal a position is still held.
// Closed positions are left at negative qty (not deleted). Every engine that
// ═══════════════════════════════════════════════════════════════════════════════
// PLATFORM FRESHNESS CONTRACT — Phase 2, market-aware revision (13-Jul-2026)
//
// "A recommendation is only valid if both the analysis and the underlying data are
// current. Stale intelligence must never drive an investment decision. Weekends
// and market holidays must never cause valid market intelligence to expire."
//
// ONE shared contract for every time-sensitive dataset: source_timestamp,
// freshness_status (LIVE/AGING/EXPIRED/UNAVAILABLE), freshness_reason, next_action.
// Does not touch QEGate, Health Score, Monte Carlo, or Decision Engine internals —
// this only gates whether their OUTPUT is presented as an active recommendation.
//
// MARKET-AWARE, NOT CALENDAR-DAY: freshness is measured in scheduled pipeline
// CHECKPOINTS missed, not raw elapsed days. The checkpoint is 16:15 IST on a
// trading day — verified against this file's own scheduled() handler: qe_holdings/
// qe_decision_log/qe_portfolio_snapshot are written ONLY by runPortfolioPipeline(),
// which is invoked from exactly ONE cron branch: cron === "45 10 * * MON-FRI" (=
// 16:15 IST). This is not a new assumption; it is the actual, already-deployed
// schedule, confirmed by tracing every branch of scheduled(). Trading days are
// Mon-Fri, matching this file's own cron day-of-week patterns ("2-6" under
// Cloudflare's 1=Sun..7=Sat convention) — the same definition already in production
// use, not invented here.
//   LIVE        — 0 checkpoints missed since source_timestamp (still the latest
//                 completed run; e.g. Friday's data checked any time over the
//                 weekend or Monday morning before 16:15).
//   AGING       — exactly 1 checkpoint missed (e.g. Monday's 16:15 has passed but
//                 the data hasn't refreshed yet — one missed run, within tolerance).
//   EXPIRED     — 2+ checkpoints missed (the pipeline has failed to produce fresh
//                 output across multiple consecutive scheduled runs).
//   UNAVAILABLE — no source_timestamp exists at all.
// DISCLOSED LIMITATION: trading-day detection is Mon-Fri only. No NSE market
// holiday calendar exists anywhere in this codebase (the pre-existing market-hours
// guard in tokenHealthCheck has the identical limitation) — a holiday (e.g. Diwali)
// will be incorrectly treated as a trading day and could show one extra AGING/
// EXPIRED cycle until the next real trading day's run arrives. Not a new gap
// introduced here; consistent with the codebase's existing, accepted simplification.
const FRESHNESS_CHECKPOINT_IST_HOUR = 16, FRESHNESS_CHECKPOINT_IST_MIN = 15; // matches cron "45 10 * * MON-FRI"
const FRESHNESS_AGING_CHECKPOINTS   = 1; // ENGINEERING_ASSUMPTION
const FRESHNESS_EXPIRED_CHECKPOINTS = 2; // ENGINEERING_ASSUMPTION

function _istCalendarDate(d) { return new Date((d instanceof Date ? d : new Date(d)).getTime() + 5.5 * 3600 * 1000).toISOString().slice(0, 10); }
function _isIstTradingDay(dateStr) { const dow = new Date(dateStr + "T12:00:00Z").getUTCDay(); return dow !== 0 && dow !== 6; } // Mon-Fri only; see disclosed limitation above
function _checkpointUtcMs(dateStr) { return new Date(dateStr + "T00:00:00.000Z").getTime() + (FRESHNESS_CHECKPOINT_IST_HOUR * 60 + FRESHNESS_CHECKPOINT_IST_MIN) * 60000 - 5.5 * 3600 * 1000; }
// Counts trading-day 16:15 IST checkpoints strictly AFTER the source's own checkpoint that
// have already occurred by "nowMs" — i.e. how many scheduled pipeline runs have completed
// (or should have) since this data was produced. 14-day bound comfortably covers any
// realistic holiday cluster without an unbounded loop.
function _countMissedCheckpoints(sourceIstDate, nowMs) {
  let count = 0, cursorMs = _checkpointUtcMs(sourceIstDate);
  for (let i = 0; i < 14; i++) {
    cursorMs += 24 * 3600 * 1000;
    const cursorDateStr = new Date(cursorMs + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
    if (!_isIstTradingDay(cursorDateStr)) continue;
    const cp = _checkpointUtcMs(cursorDateStr);
    if (cp <= nowMs) count++; else break; // future checkpoints haven't happened; later ones can't have either
  }
  return count;
}
function _nextCheckpointLabel(nowMs) {
  let cursorMs = nowMs;
  for (let i = 0; i < 14; i++) {
    const dateStr = new Date(cursorMs + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
    if (_isIstTradingDay(dateStr) && _checkpointUtcMs(dateStr) > nowMs) return `${dateStr} 16:15 IST`;
    cursorMs += 24 * 3600 * 1000;
  }
  return "the next scheduled trading-day run";
}

function _datasetFreshness(sourceTimestampIso, datasetLabel) {
  if (!sourceTimestampIso) {
    return { source_timestamp: null, freshness_status: "UNAVAILABLE", freshness_reason: `No ${datasetLabel} data found.`,
      next_action: `Run today's market scan, or await ${_nextCheckpointLabel(Date.now())}.` };
  }
  const sourceIst = _istCalendarDate(sourceTimestampIso);
  const missed = _countMissedCheckpoints(sourceIst, Date.now());
  let status, reason, nextAction;
  if (missed === 0) {
    status = "LIVE"; reason = `${datasetLabel} reflects the most recently completed scheduled pipeline run.`;
    nextAction = null;
  } else if (missed <= FRESHNESS_AGING_CHECKPOINTS) {
    status = "AGING"; reason = `${datasetLabel} is ${missed} scheduled checkpoint behind (last evidence: ${sourceTimestampIso}).`;
    nextAction = `Await ${_nextCheckpointLabel(Date.now())} — this is within the expected gap for one missed or pending run.`;
  } else {
    status = "EXPIRED"; reason = `${datasetLabel} is ${missed} scheduled checkpoints behind (last evidence: ${sourceTimestampIso}) — the pipeline has not produced fresh output across multiple expected runs.`;
    nextAction = `Run today's market scan manually, or check pipeline health — do not rely on this evidence until it refreshes.`;
  }
  return { source_timestamp: sourceTimestampIso, freshness_status: status, freshness_reason: reason, next_action: nextAction };
}

async function _maxHoldingsUpdatedTs(env) {
  try { const q = await env.QE_DB.prepare("SELECT MAX(updated_ts) AS t FROM qe_holdings WHERE qty>0").all(); return (q && q.results && q.results[0] && q.results[0].t) || null; }
  catch (_) { return null; }
}
async function _maxForwardTrackDate(env) {
  try { const q = await env.QE_DB.prepare("SELECT MAX(snapshot_date) AS d FROM qe_forward_track").all(); return (q && q.results && q.results[0] && q.results[0].d) || null; }
  catch (_) { return null; }
}

async function _activeHoldings(env) {
  const q = await env.QE_DB.prepare("SELECT * FROM qe_holdings WHERE qty>0 ORDER BY symbol").all();
  return (q && q.results) || [];
}

// Single authoritative read of the latest portfolio-level snapshot row — the ONE
// source for top3_pct / portfolio_health / total_value / top_name_pct everywhere
// they're needed. computePortfolioRisk() is the only WRITER of this row; every
// consumer reads it here rather than recomputing top3_pct/health from qe_holdings
// independently (fixes the dual-implementation SSOT defect found in the Trust Audit —
// generatePortfolioAggregates previously reimplemented this formula with different
// rounding, causing Decision Replay's own top3_pct_matches/portfolio_health_matches
// checks to report false mismatches even when nothing had actually changed).
async function _latestPortfolioSnapshot(env) {
  try {
    const q = await env.QE_DB.prepare("SELECT * FROM qe_portfolio_snapshot ORDER BY snapshot_date DESC LIMIT 1").all();
    return (q && q.results && q.results[0]) || null;
  } catch (_) { return null; }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE CONTEXT (request-scoped) — Trust Audit v1.0 architecture.
//
// One instance is created ONCE per inbound portfolio request (at the route handler)
// and threaded through every downstream engine as `ctx`. This is NOT a cache: it
// carries no TTL, is never stored anywhere outside the request closure that created
// it, and is discarded the moment that request's response is returned. Each call to
// createPortfolioContext(env) produces an independent object — no global state, no
// cross-request persistence, nothing shared between concurrent requests.
//
// Every method memoizes its own PROMISE (not the resolved value) on first call, so
// concurrent awaits (e.g. inside a Promise.all) share one in-flight computation
// instead of racing to compute the same fact twice. All formulas below are
// byte-identical to their pre-existing standalone implementations — this only
// removes redundant re-invocation of the same computation within one request.
//
// ONE FACT. ONE COMPUTATION. MANY CONSUMERS. — every future portfolio engine should
// read facts it needs from ctx rather than calling computeXxx(env) directly,
// whenever that fact might already be in flight elsewhere in the same request.
// ═══════════════════════════════════════════════════════════════════════════════
function createPortfolioContext(env) {
  const cache = {};
  function memo(key, fn) {
    if (!(key in cache)) cache[key] = fn();
    return cache[key];
  }
  const ctx = {
    env,
    holdings:                   () => memo("holdings",                   () => _activeHoldings(env)),
    holdingsFreshness:          () => memo("holdingsFreshness",          async () => _datasetFreshness(await _maxHoldingsUpdatedTs(env), "Portfolio holdings/decision data")),
    forwardTrackFreshness:      () => memo("forwardTrackFreshness",      async () => _datasetFreshness(await _maxForwardTrackDate(env), "Capital Rotation candidate scan")),
    decisionsBySymbol:          () => memo("decisionsBySymbol",          () => _latestDecisionsBySymbol(env)),
    regime:                     () => memo("regime",                     () => _currentRegimeForPP(env)),
    snapshot:                   () => memo("snapshot",                   () => _latestPortfolioSnapshot(env)),
    capitalConstraints:         () => memo("capitalConstraints",         () => _capitalConstraints(env)),
    portfolioCapital:           () => memo("portfolioCapital",           () => _portfolioCapital(env, ctx)),
    positionIntelligence:       () => memo("positionIntelligence",       () => _positionIntelligence(env, ctx)),
    capitalFoundation:          () => memo("capitalFoundation",          () => computePortfolioCapitalFoundation(env, ctx)),
    profitProtection:           () => memo("profitProtection",           () => computeProfitProtectionRecommendations(env, ctx)),
    capitalRotation:            () => memo("capitalRotation",            () => computeCapitalRotationRecommendations(env, ctx)),
    calibrationRecommendations: () => memo("calibrationRecommendations", () => computeCalibrationRecommendations(env)),
    optimizationConstraints:    () => memo("optimizationConstraints",    () => evaluateOptimizationConstraints(env, ctx)),
    activeObjectives:           () => memo("activeObjectives",           () => _activeObjectives(env)),
    optimizationEvidence:       () => memo("optimizationEvidence",       () => buildOptimizationEvidence(env, ctx)),
    portfolioOptimization:      () => memo("portfolioOptimization",      () => computePortfolioOptimization(env, ctx)),
    portfolioMemoryAll:         () => memo("portfolioMemoryAll",         () => computePortfolioMemory(env, null, ctx)),
    decisionEvolutionAll:       () => memo("decisionEvolutionAll",       () => computeDecisionEvolution(env, null, ctx)),
    portfolioStory:             () => memo("portfolioStory",             () => computePortfolioStory(env, ctx)),
    executiveBriefing:          () => memo("executiveBriefing",          () => computeExecutiveBriefing(env, ctx)),
    executiveIntelligence:      () => memo("executiveIntelligence",      () => computeExecutiveIntelligence(env, ctx)),
    morningBriefing:            () => memo("morningBriefing",            () => computeMorningBriefing(env, ctx)),
    portfolioPerformance:       () => memo("portfolioPerformance",       () => computePortfolioPerformance(env, ctx)),
  };
  return ctx;
}

async function computeProfitProtectionRecommendations(env, ctx) {
  const freshness = await ctx.holdingsFreshness();
  if (freshness.freshness_status === "EXPIRED" || freshness.freshness_status === "UNAVAILABLE") {
    return { ok: true, engine_version: PROFIT_PROTECTION_ENGINE_VERSION, generated_ts: new Date().toISOString(),
      status: "RECOMMENDATION_EXPIRED", last_evidence_ts: freshness.source_timestamp, reason: freshness.freshness_reason, next_action: freshness.next_action,
      freshness, recommendations: [],
      regime: null, holdings_evaluated: 0, profitable_holdings: 0, by_recommendation: {} };
  }

  let holdings = [];
  try { holdings = await ctx.holdings(); }
  catch (e) { return { ok: false, error: "qe_holdings read failed: " + ((e && e.message) || e) }; }

  let decisionsBySymbol = {};
  try { decisionsBySymbol = await ctx.decisionsBySymbol(); }
  catch (e) { return { ok: false, error: "qe_decision_log read failed: " + ((e && e.message) || e) }; }

  const regime = await ctx.regime();

  // Enrichment only, never a gate — reused UNCHANGED from Phase 1.1b / 1.2, no reimplementation.
  let calibration = null, analytics = null;
  try { calibration = await ctx.calibrationRecommendations(); } catch (_) {}
  try { analytics = await computeDecisionQualityAnalytics(env, null); } catch (_) {}
  const calibRecs = (calibration && calibration.recommendations) || [];

  const recommendations = [];
  for (const h of holdings) {
    const dlog = decisionsBySymbol[h.symbol];
    let calibEnrichment = null;
    if (h.r_multiple != null && dlog) {
      const rBucket = `r_multiple_${Math.floor(h.r_multiple)}-${Math.floor(h.r_multiple) + 1}`;
      const regimeGroup = `${dlog.decision}_in_${regime}`;
      const matches = calibRecs.filter(r =>
        (r.parameter === "PROFIT_PROTECTION_SENSITIVITY" && r.group === rBucket) ||
        (r.parameter === "SIGNAL_RELIABILITY_BY_REGIME" && r.group === regimeGroup));
      calibEnrichment = {
        matched_recommendations: matches.map(m => ({ parameter: m.parameter, group: m.group, status: m.status, reason: m.reason })),
        decision_effectiveness_context: (analytics && analytics.ok) ? analytics.decision_effectiveness : null,
      };
    }
    const result = _evaluateProfitProtection(h, dlog, regime, calibEnrichment);
    if (result) recommendations.push(result);
  }

  return {
    ok: true, engine_version: PROFIT_PROTECTION_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    regime, holdings_evaluated: holdings.length, profitable_holdings: recommendations.length,
    by_recommendation: recommendations.reduce((acc, r) => { acc[r.recommendation] = (acc[r.recommendation] || 0) + 1; return acc; }, {}),
    recommendations,
  };
}

async function handleProfitProtection(env) {
  try { return cors(await computeProfitProtectionRecommendations(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Profit protection evaluation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO CAPITAL INTELLIGENCE FOUNDATION — Phase 2.5 (v4.79)
//
// PURE INFRASTRUCTURE — no buy/sell/rotation/reallocation recommendation exists
// anywhere in this module. It provides a single source of truth for portfolio
// capital and per-position economics that Capital Rotation, Portfolio Optimizer,
// Executive Briefing/Cockpit, and Wealth Progress will consume instead of each
// reimplementing their own capital math.
//
// REUSE, NOT RECOMPUTATION: Total Portfolio Value / Invested Capital come from the
// existing qe_portfolio_snapshot (computePortfolioRisk(), already run daily by the
// pipeline) — read-only, not recalculated here. Per-holding cost basis, current
// value, holding age (days_held), R-multiple, and conviction all come directly from
// qe_holdings — none of these are recomputed. Current decision comes from the
// latest qe_decision_log row per symbol, same reuse pattern as Phase 2.0.
//
// [UPDATED 13-Jul-2026] Available Cash / Utilised Margin / Total Funds are now LIVE
// Zerodha Kite Connect Funds/Margins data (equity segment) — see _fetchZerodhaFunds().
// The old manually-maintained KV workflow (PORTFOLIO_AVAILABLE_CASH) is retired
// entirely; Zerodha is the single source of truth for capital per architectural
// decision. Current Portfolio Value / Current Invested Value still reuse the
// existing qe_portfolio_snapshot (computePortfolioRisk(), already run daily by the
// pipeline) — read-only, not recalculated here. Per-holding cost basis, current
// value, holding age (days_held), R-multiple, and conviction all come directly from
// qe_holdings — none of these are recomputed. Current decision comes from the
// latest qe_decision_log row per symbol, same reuse pattern as Phase 2.0.
//
// NO FABRICATED CERTAINTY: if the live Kite funds call fails for any reason (expired
// session, network error, malformed response), the capital object returns
// {ok:false, status:"CAPITAL_INFORMATION_UNAVAILABLE", reason, as_of_ts} — never an
// estimated, cached, or last-known substitute value. Pending Capital Commitments is
// similarly honest: qe_gtt_log is a mixed-purpose KV audit log (entry approvals +
// exit-bracket arming) with no clean structural distinction between "capital already
// deployed" and "capital pending on an unfilled entry" — rather than build a fragile
// heuristic parse of a log not designed for this, pending_capital_commitments reports
// 0 with an explicit reason. This mirrors how Phase 1.2 handled
// CAPITAL_ALLOCATION_PREFERENCE: report the gap, don't fake data.
//
// THRESHOLD CLASSIFICATION (per Siva's standing rule — every threshold classified
// Derived / Portfolio management practice / Engineering assumption / Product
// assumption, with justification):
//   max_portfolio_concentration_pct = 80  → DERIVED. Matches the existing hardcoded
//     top3_pct>80 "EXCESSIVE" threshold already live in generateExecutiveDecisionReport
//     (line ~7863). Exposed here for future engines to read as config; the live
//     pipeline's own inline threshold is NOT modified or rewired to this KV key —
//     that would touch frozen/existing code, out of scope for an infra-only phase.
//     A future phase can wire them together; until then this is a documented,
//     disclosed duplication of a single number, not two independently-tunable ones.
//   max_position_weight_pct = 25          → PRODUCT ASSUMPTION, no prior QuantEdge
//     precedent; common institutional single-name concentration guideline.
//   max_sector_exposure_pct = 30          → PRODUCT ASSUMPTION. Deliberately distinct
//     from PIPE_MAX_SECTOR_N (=5), which caps discovery-scan CANDIDATE COUNT per
//     sector at screen time, not held-portfolio WEIGHT per sector — different
//     mechanism, not reused, to avoid conflating two unrelated constraints.
//   min_cash_reserve = 0 (absolute ₹)     → ENGINEERING ASSUMPTION. No capital data
//     existed before this phase; 0 is the only safe, non-invasive default.
//   max_concurrent_new_positions = 3      → PRODUCT ASSUMPTION, no prior precedent.
// All five are KV-overridable; none are enforced anywhere in this phase — exposure
// only, per the explicit "do not implement optimization yet" instruction.
// ═══════════════════════════════════════════════════════════════════════════════

const CAPITAL_FOUNDATION_VERSION = "capfound-v1.0";

async function _capitalConstraints(env) {
  const defaults = {
    max_position_weight_pct:      { value: 25, classification: "PRODUCT_ASSUMPTION" },
    max_sector_exposure_pct:      { value: 30, classification: "PRODUCT_ASSUMPTION" },
    min_cash_reserve:             { value: 0,  classification: "ENGINEERING_ASSUMPTION" },
    max_portfolio_concentration_pct: { value: 80, classification: "DERIVED (matches existing top3_pct>80 EXCESSIVE threshold, generateExecutiveDecisionReport)" },
    max_concurrent_new_positions: { value: 3,  classification: "PRODUCT_ASSUMPTION" },
  };
  const kvKeys = {
    max_position_weight_pct: "PORTFOLIO_MAX_POSITION_WEIGHT_PCT",
    max_sector_exposure_pct: "PORTFOLIO_MAX_SECTOR_EXPOSURE_PCT",
    min_cash_reserve: "PORTFOLIO_CASH_RESERVE",
    max_portfolio_concentration_pct: "PORTFOLIO_MAX_CONCENTRATION_PCT",
    max_concurrent_new_positions: "PORTFOLIO_MAX_CONCURRENT_NEW_POSITIONS",
  };
  const out = {};
  for (const key of Object.keys(defaults)) {
    let value = defaults[key].value;
    try { const kv = await env.KITE_STORE.get(kvKeys[key]); if (kv != null && kv !== "") { const n = Number(kv); if (!isNaN(n)) value = n; } } catch (_) {}
    out[key] = { value, default: defaults[key].value, classification: defaults[key].classification, kv_key: kvKeys[key] };
  }
  return out;
}

// Live Zerodha Funds/Margins fetch — the ONLY network call this integration makes.
// Kite Connect schema (verified against https://kite.trade/docs/connect/v3/user/,
// example response cross-checked field-by-field before this was written):
//   equity.available.live_balance — net balance actually usable right now (Kite's own
//     docs example shows this is numerically identical to equity.net: opening_balance
//     minus utilised.debits). This is "Available Cash" for a cash-market-only account.
//   equity.utilised.debits — total currently utilised/blocked amount. This is "Utilised Margin".
//   equity.available.cash — the ledger/opening cash total BEFORE utilisation is subtracted
//     (confirmed in Kite's own example: cash = live_balance + utilised.debits exactly,
//     245431.6 = 99725.05 + 145706.55). This is "Total Funds".
// Only the equity segment is fetched — QuantEdge is cash-market equity only, no F&O/commodity.
// NEVER throws: every failure path returns {ok:false, reason, as_of_ts} for the caller to
// handle uniformly. Never estimates or substitutes a stale/cached value on failure.
async function _fetchZerodhaFunds(env) {
  const as_of_ts = new Date().toISOString();
  let token;
  try { token = await getToken(env); }
  catch (e) { return { ok: false, reason: (e && e.message) || "Kite session token unavailable — login required at /login.", as_of_ts }; }

  let resp;
  try {
    resp = await fetch(`${KITE_API_BASE}/user/margins/equity`, {
      method: "GET",
      headers: { "X-Kite-Version": "3", Authorization: kiteAuthHeader(token) },
    });
  } catch (e) {
    return { ok: false, reason: "Network error calling Kite funds API: " + ((e && e.message) || "unknown"), as_of_ts };
  }

  let data;
  try { data = await resp.json(); }
  catch (_) { return { ok: false, reason: `Kite funds API returned non-JSON response (HTTP ${resp.status})`, as_of_ts }; }

  if (!resp.ok || data.status !== "success" || !data.data) {
    return { ok: false, reason: "Kite funds API error: " + ((data && data.message) || `HTTP ${resp.status}`), as_of_ts };
  }

  const eq = data.data;
  if (eq.enabled !== true) return { ok: false, reason: "Equity segment not enabled on this Kite account.", as_of_ts };

  const availableCash  = eq.available && eq.available.live_balance != null ? eq.available.live_balance : null;
  const utilisedMargin = eq.utilised  && eq.utilised.debits       != null ? eq.utilised.debits       : null;
  const totalFunds      = eq.available && eq.available.cash        != null ? eq.available.cash        : null;
  if (availableCash == null || utilisedMargin == null || totalFunds == null) {
    return { ok: false, reason: "Kite funds API response missing expected equity.available/utilised fields.", as_of_ts };
  }
  return { ok: true, available_cash: availableCash, utilised_margin: utilisedMargin, total_funds: totalFunds, as_of_ts };
}

// ═══════════════════════════════════════════════════════════════════════════════
// CANONICAL CAPITAL OBJECT — Capital Foundation v2.0 (13-Jul-2026 architectural decision)
//
// Zerodha (Kite Connect) is the single source of truth for capital information.
// PORTFOLIO_AVAILABLE_CASH (the old manually-maintained KV workflow) is retired —
// no manual cash entry exists anywhere in this codebase from this version forward.
//
// This is the ONE canonical capital object every downstream engine must consume:
// Portfolio Optimizer, Capital Rotation, Executive Story/Briefing/Cockpit, Copilot,
// and the upcoming Wealth Progress Engine all read this exact shape via
// ctx.portfolioCapital() — none independently computes or reinterprets capital.
//
// ONE FACT. ONE SOURCE. MANY CONSUMERS. NO FABRICATED CERTAINTY: if live Zerodha
// funds cannot be retrieved, this returns {ok:false, status:CAPITAL_INFORMATION_
// UNAVAILABLE, reason, as_of_ts} — never an estimated, inferred, or last-known value.
// All other portfolio intelligence (health scores, decisions, holdings) is computed
// independently of this object and continues to function when capital is unavailable.
async function _portfolioCapital(env, ctx) {
  const funds = await _fetchZerodhaFunds(env);

  if (!funds.ok) {
    return {
      ok: false,
      status: "CAPITAL_INFORMATION_UNAVAILABLE",
      reason: funds.reason,
      as_of_ts: funds.as_of_ts,
      data_freshness: "UNAVAILABLE",
    };
  }

  // Current Portfolio Value / Current Invested Value reuse the SAME qe_portfolio_snapshot
  // row computePortfolioRisk() writes — not recomputed here (ONE FACT, ONE COMPUTATION).
  const snap = await ctx.snapshot();
  const currentPortfolioValue = snap ? snap.total_value : null;
  const currentInvestedValue  = snap ? snap.total_invested : null;

  const constraints = await ctx.capitalConstraints();
  const minCashReserveFloor = constraints.min_cash_reserve.value;

  // Capital Deployed % / Cash Reserve % — % of the TRUE combined pool (cash + invested
  // equity value), not Kite's "Total Funds" (which is a cash-ledger concept only and does
  // not include the value of shares already purchased — using it as the denominator would
  // understate how much of the real capital pool is deployed).
  const totalPool = (currentInvestedValue != null) ? (currentInvestedValue + funds.available_cash) : null;
  const capitalDeployedPct = (totalPool != null && totalPool > 0) ? Math.round((currentInvestedValue / totalPool) * 10000) / 100 : null;
  const cashReservePct = capitalDeployedPct != null ? Math.round((100 - capitalDeployedPct) * 100) / 100 : null;

  // Remaining Buying Capacity — available cash above the configured minimum reserve floor.
  // Cash-market-only account (no leverage), so buying capacity is cash-limited, not margin-multiplied.
  const remainingBuyingCapacity = Math.max(0, funds.available_cash - minCashReserveFloor);

  return {
    ok: true,
    status: "LIVE",
    available_cash: Math.round(funds.available_cash * 100) / 100,
    utilised_margin: Math.round(funds.utilised_margin * 100) / 100,
    total_funds: Math.round(funds.total_funds * 100) / 100,
    current_portfolio_value: currentPortfolioValue,
    current_invested_value: currentInvestedValue,
    current_value_note: currentPortfolioValue == null ? "No qe_portfolio_snapshot row yet — run the daily pipeline first" : null,
    capital_deployed_pct: capitalDeployedPct,
    cash_reserve_pct: cashReservePct,
    remaining_buying_capacity: Math.round(remainingBuyingCapacity * 100) / 100,
    min_cash_reserve_floor: minCashReserveFloor,
    broker_data_ts: funds.as_of_ts,
    data_freshness: "LIVE",
  };
}

// Per-holding Position Intelligence — every field either reused verbatim from
// qe_holdings/qe_decision_log or simple deterministic arithmetic on those fields.
async function _positionIntelligence(env, ctx) {
  let holdings = [];
  try { holdings = await ctx.holdings(); }
  catch (e) { return { ok: false, error: "qe_holdings read failed: " + ((e && e.message) || e) }; }

  let decisionsBySymbol = {};
  try { decisionsBySymbol = await ctx.decisionsBySymbol(); } catch (_) {} // reused from Phase 2.0, unchanged

  let totalValue = null;
  try { const cap = await ctx.portfolioCapital(); totalValue = (cap && cap.ok) ? cap.current_portfolio_value : null; } catch (_) {}

  return holdings.map(h => {
    const qty = Number(h.qty) || 0, ltp = Number(h.ltp) || 0, avg = Number(h.avg_price) || 0;
    const costBasis = qty * avg, currentValue = qty * ltp, unrealizedPnl = currentValue - costBasis;
    const portfolioWeightPct = (totalValue != null && totalValue > 0) ? Math.round((currentValue / totalValue) * 10000) / 100 : null;
    const capitalEfficiencyAnnualizedPct = (h.days_held != null && h.days_held > 0 && h.pnl_pct != null)
      ? Math.round((h.pnl_pct / h.days_held) * 365 * 100) / 100 : null;
    const dlog = decisionsBySymbol[h.symbol];
    return {
      symbol: h.symbol,
      cost_basis: Math.round(costBasis * 100) / 100,
      current_value: Math.round(currentValue * 100) / 100,
      unrealized_pnl: Math.round(unrealizedPnl * 100) / 100,
      unrealized_pnl_pct: h.pnl_pct,
      portfolio_weight_pct: portfolioWeightPct,
      invested_amount: Math.round(costBasis * 100) / 100,
      holding_age_days: h.days_held,
      r_multiple: h.r_multiple,
      capital_efficiency_annualized_pct: capitalEfficiencyAnnualizedPct,
      capital_efficiency_note: "Time-normalized return (pnl_pct / days_held * 365) — lets positions of different ages be compared on capital productivity, not just raw return.",
      current_decision: dlog ? dlog.decision : null,
      current_conviction: h.conviction_trend || (dlog ? dlog.conviction_trend : null),
      health_score: h.health_score,
    };
  });
}

async function computePortfolioCapitalFoundation(env, ctx) {
  const capital = await ctx.portfolioCapital();
  const positions = await ctx.positionIntelligence();
  if (positions && positions.ok === false) return positions;
  const constraints = await ctx.capitalConstraints();
  return {
    ok: true, engine_version: CAPITAL_FOUNDATION_VERSION, generated_ts: new Date().toISOString(),
    capital, positions, constraints,
  };
}

// Opportunity Interface — pure comparison SHAPE, no ranking/scoring/recommendation.
// `current_symbol` is looked up from real, reused data (qe_holdings + latest decision).
// Candidate-side fields are supplied by the caller (a future engine's own live
// discovery-scan evidence) since candidate opportunities are not persisted anywhere
// in QuantEdge today — this phase defines the comparison structure, not a new
// opportunity data source.
async function buildOpportunityComparison(env, currentSymbol, candidate, ctx) {
  let holding = null;
  try {
    const q = await env.QE_DB.prepare("SELECT * FROM qe_holdings WHERE symbol=?1 AND qty>0").bind(currentSymbol).first();
    holding = q || null;
  } catch (_) {}
  if (!holding) return { ok: false, error: `No current holding found for symbol ${currentSymbol}` };

  const decisionsBySymbol = await ctx.decisionsBySymbol();
  const dlog = decisionsBySymbol[currentSymbol];
  // NOTE: this route has always computed position intelligence with totalValue=null
  // (portfolio_weight_pct intentionally omitted here — this is a structural symbol
  // comparison, not a weighted-portfolio view). Preserve that exact behavior: reuse
  // ctx's holdings/decisions reads, but force portfolioCapital() to resolve null so
  // _positionIntelligence's weight calc stays disabled, byte-identical to before.
  const positions = await _positionIntelligence(env, { holdings: ctx.holdings, decisionsBySymbol: ctx.decisionsBySymbol, portfolioCapital: async () => null });
  const posIntel = positions.find(p => p.symbol === currentSymbol) || null;

  return {
    ok: true, engine_version: CAPITAL_FOUNDATION_VERSION, generated_ts: new Date().toISOString(),
    note: "Structural comparison only — no ranking, score, or replacement recommendation. Interpretation belongs to the Capital Rotation Engine.",
    current_holding: {
      symbol: currentSymbol, health_score: holding.health_score, r_multiple: holding.r_multiple,
      conviction_trend: holding.conviction_trend, decision: dlog ? dlog.decision : null,
      recommendation_confidence: dlog ? dlog.recommendation_confidence : null,
      capital_efficiency_annualized_pct: posIntel ? posIntel.capital_efficiency_annualized_pct : null,
      current_value: posIntel ? posIntel.current_value : null,
    },
    candidate_opportunity: {
      symbol: candidate.symbol || null, health_score: candidate.health_score != null ? Number(candidate.health_score) : null,
      r_multiple: candidate.r_multiple != null ? Number(candidate.r_multiple) : null,
      decision: candidate.decision || null,
      recommendation_confidence: candidate.recommendation_confidence != null ? Number(candidate.recommendation_confidence) : null,
      capital_efficiency_annualized_pct: null, // candidates have no holding history — cannot be computed, honestly null
      source_note: "Caller-supplied — candidate opportunities are not persisted anywhere in QuantEdge today.",
    },
  };
}

async function handleCapitalFoundation(env) {
  try { return cors(await computePortfolioCapitalFoundation(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Capital foundation computation failed", 502); }
}

async function handleOpportunityComparison(url, env) {
  const currentSymbol = url.searchParams.get("current_symbol");
  if (!currentSymbol) return corsErr("Missing current_symbol parameter");
  const candidate = {
    symbol: url.searchParams.get("candidate_symbol"),
    health_score: url.searchParams.get("candidate_health"),
    r_multiple: url.searchParams.get("candidate_r_multiple"),
    decision: url.searchParams.get("candidate_decision"),
    recommendation_confidence: url.searchParams.get("candidate_confidence"),
  };
  try { return cors(await buildOpportunityComparison(env, currentSymbol, candidate, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Opportunity comparison failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CAPITAL ROTATION ENGINE — Phase 3.0 (v4.80)
//
// Pipeline position: ... → Decision Calibration (1.2) → Profit Protection (2.0) →
// Portfolio Capital Intelligence (2.5) → CAPITAL ROTATION ENGINE (here). Read-only
// consumer of every upstream layer; modifies none. No new table, computed on-demand.
//
// CANDIDATE SOURCE (correction to the Phase 2.5 assumption): qe_forward_track — the
// Discovery Engine's own persisted scan output, already live and queryable — was
// checked before writing any code here. It IS a real, structured candidate source
// (symbol, score, base_score, edge_class from the existing Backtest Edge Confidence
// classifier, per-run snapshot_date), so this engine reads it directly rather than
// requiring caller-supplied candidate data the way Phase 2.5's generic
// buildOpportunityComparison() does (that function remains as-is, untouched, for
// callers with their own candidate source).
//
// METHODOLOGY DISCLOSURE (mandatory, not buried): a discovery-scan `score` and a
// portfolio `health_score` are DIFFERENT methodologies measuring different things —
// entry-setup technical quality vs. current portfolio-context health. This engine
// never treats them as directly interchangeable numbers. The candidate's score/
// edge_class is used ONLY as a qualification GATE (does a genuinely strong candidate
// exist at all), never as the driver of the rotation decision itself — that
// structurally enforces "must never rotate purely because another stock has a
// higher technical score."
//
// DECISION LOGIC (deterministic, multi-signal, never single-metric): counts
// retention-favoring signals (conviction IMPROVING, profit protection says
// LET_PROFITS_RUN, high capital efficiency, high recommendation confidence) against
// rotation-favoring signals (conviction WEAKENING, profit protection says PARTIAL/
// FULL protection, over-concentrated position weight, low/negative capital
// efficiency, health in the WATCH band or below, an applicable PROPOSED calibration
// flag) on the CURRENT HOLDING alone. A qualifying candidate (score>=75 AND
// edge_class=PROVEN_POSITIVE, named thresholds) is a REQUIRED gate before any
// rotation tier is reachable, but the tier itself (MONITOR/PARTIAL/FULL) is set by
// the holding-side signal balance, not by candidate score margin. Ties and mixed
// evidence default to NO_ROTATION / RETAIN_POSITION — inactivity is the safe default.
//
// GUARDRAILS ENFORCED: profit protection LET_PROFITS_RUN force-overrides to
// RETAIN_POSITION regardless of any candidate (never ignore profit protection).
// Portfolio concentration is a first-class signal (never ignored). Capital
// efficiency is a first-class signal (never ignored). Current conviction is a
// first-class signal (never ignored). Calibration evidence is consulted, matched by
// decision×regime (never ignored, though today it will almost always be
// NO_CALIBRATION — Phase 1's sparse-evidence state, disclosed since Phase 1.2's
// audit, not new). Decision Engine is never duplicated — a holding already at
// REDUCE/SELL/EXIT_IMMEDIATELY defers rather than getting a second, competing call.
// ═══════════════════════════════════════════════════════════════════════════════

const CAPITAL_ROTATION_ENGINE_VERSION = "rotation-v1.0";
const CR_CANDIDATE_MIN_SCORE   = 75;   // named, isolated — candidate qualification gate, not the rotation driver
const CR_CAPITAL_EFF_HIGH_PCT  = 50;   // named — capital_efficiency_annualized_pct above this counts as a retention signal
const CR_CAPITAL_EFF_LOW_PCT   = 0;    // named — at/below this counts as a rotation signal
const CR_HIGH_CONFIDENCE       = 80;   // named — recommendation_confidence above this counts as a retention signal

// Today's (or most recent) qualifying candidates, excluding symbols already held.
async function _rotationCandidates(env, heldSymbols) {
  let dateRow = null;
  try { dateRow = await env.QE_DB.prepare("SELECT MAX(snapshot_date) AS d FROM qe_forward_track").first(); } catch (_) {}
  const latestDate = dateRow && dateRow.d;
  if (!latestDate) return [];
  let rows = [];
  try {
    const q = await env.QE_DB.prepare(
      "SELECT symbol, score, base_score, edge_class, label FROM qe_forward_track WHERE snapshot_date=?1 AND label='BUY' ORDER BY score DESC"
    ).bind(latestDate).all();
    rows = (q && q.results) || [];
  } catch (_) { return []; }
  const heldSet = new Set(heldSymbols);
  return rows.filter(r => !heldSet.has(r.symbol));
}

function _bestQualifyingCandidate(candidates) {
  const qualifying = candidates.filter(c => c.score >= CR_CANDIDATE_MIN_SCORE && c.edge_class === "PROVEN_POSITIVE");
  return qualifying.length ? qualifying[0] : null; // already sorted by score DESC
}

function _evaluateRotation(holding, posIntel, decisionLogRow, ppResult, constraints, snapCapital, regime, calibEnrichment, candidate) {
  const symbol = holding.symbol;
  const base = { symbol, engine_version: CAPITAL_ROTATION_ENGINE_VERSION, generated_ts: new Date().toISOString() };

  if (!decisionLogRow) {
    return { ...base, recommendation: "NO_ROTATION", confidence: null, explanation: "No logged decision found for this symbol yet.",
      reason: "INSUFFICIENT_EVIDENCE", current_holding: null, proposed_replacement: null, supporting_evidence: null,
      expected_portfolio_improvement: null, expected_risks: null, reversal_conditions: null };
  }
  if (decisionLogRow.decision === "SELL" || decisionLogRow.decision === "EXIT_IMMEDIATELY") {
    return { ...base, recommendation: "NO_ROTATION", confidence: decisionLogRow.recommendation_confidence,
      explanation: `Decision Engine already recommends ${decisionLogRow.decision} — Capital Rotation Engine does not duplicate or override an active exit call.`,
      reason: "DECISION_ENGINE_ALREADY_ACTING", current_holding: { symbol, decision: decisionLogRow.decision },
      proposed_replacement: null, supporting_evidence: null, expected_portfolio_improvement: null, expected_risks: null,
      reversal_conditions: decisionLogRow.reversal_conditions || null };
  }

  const ppRec = ppResult ? ppResult.recommendation : null; // Phase 2.0, reused unchanged
  if (ppRec === "LET_PROFITS_RUN") {
    return { ...base, recommendation: "RETAIN_POSITION", confidence: decisionLogRow.recommendation_confidence,
      explanation: "Profit Protection Engine says LET_PROFITS_RUN — Capital Rotation Engine never overrides an active profit-protection signal in favor of rotation.",
      reason: "PROFIT_PROTECTION_OVERRIDE", current_holding: { symbol, decision: decisionLogRow.decision, profit_protection: ppRec },
      proposed_replacement: null, supporting_evidence: { profit_protection: ppRec }, expected_portfolio_improvement: null,
      expected_risks: null, reversal_conditions: decisionLogRow.reversal_conditions || null };
  }

  if (!posIntel) {
    return { ...base, recommendation: "NO_ROTATION", confidence: null, explanation: "Position intelligence unavailable for this holding.",
      reason: "INSUFFICIENT_EVIDENCE", current_holding: null, proposed_replacement: null, supporting_evidence: null,
      expected_portfolio_improvement: null, expected_risks: null, reversal_conditions: decisionLogRow.reversal_conditions || null };
  }

  // ── Retention-favoring signals (evidence FOR keeping current holding) ──
  const retentionSignals = [];
  const convictionTrend = holding.conviction_trend || decisionLogRow.conviction_trend;
  if (convictionTrend === "IMPROVING") retentionSignals.push("conviction_improving");
  if (posIntel.capital_efficiency_annualized_pct != null && posIntel.capital_efficiency_annualized_pct > CR_CAPITAL_EFF_HIGH_PCT) retentionSignals.push("high_capital_efficiency");
  if (decisionLogRow.recommendation_confidence != null && decisionLogRow.recommendation_confidence > CR_HIGH_CONFIDENCE) retentionSignals.push("high_decision_confidence");

  // ── Rotation-favoring signals (evidence AGAINST keeping, on the holding itself) ──
  const rotationSignals = [];
  if (convictionTrend === "WEAKENING") rotationSignals.push("conviction_weakening");
  if (ppRec === "PARTIAL_PROFIT_PROTECTION" || ppRec === "FULL_PROFIT_PROTECTION") rotationSignals.push(`profit_protection_${ppRec.toLowerCase()}`);
  if (posIntel.portfolio_weight_pct != null && posIntel.portfolio_weight_pct > constraints.max_position_weight_pct.value) rotationSignals.push("position_over_weight_limit");
  if (snapCapital.top3_pct != null && snapCapital.top3_pct > constraints.max_portfolio_concentration_pct.value) rotationSignals.push("portfolio_concentration_excessive");
  if (posIntel.capital_efficiency_annualized_pct != null && posIntel.capital_efficiency_annualized_pct <= CR_CAPITAL_EFF_LOW_PCT) rotationSignals.push("low_capital_efficiency");
  if (holding.health_score != null && holding.health_score < PIE_CONFIG.bands.watch) rotationSignals.push("health_below_watch_band");
  if (calibEnrichment && calibEnrichment.some(c => c.status === "PROPOSED")) rotationSignals.push("calibration_flagged_unreliable");

  const netSignal = rotationSignals.length - retentionSignals.length;
  const candidateQualifies = !!candidate;

  let classification, proposedReplacement = null;
  if (!candidateQualifies) {
    classification = "NO_ROTATION";
  } else if (retentionSignals.length >= rotationSignals.length) {
    classification = "RETAIN_POSITION";
  } else if (netSignal === 1) {
    classification = "MONITOR_ROTATION"; proposedReplacement = candidate.symbol;
  } else if (netSignal >= 2 && netSignal <= 3) {
    classification = "PARTIAL_ROTATION"; proposedReplacement = candidate.symbol;
  } else {
    classification = "FULL_ROTATION"; proposedReplacement = candidate.symbol;
  }

  // Concentration-impact estimate — deterministic arithmetic on already-known weights, not a new model.
  const weightFreed = classification === "FULL_ROTATION" ? posIntel.portfolio_weight_pct
                     : classification === "PARTIAL_ROTATION" ? (posIntel.portfolio_weight_pct || 0) / 2 : 0;
  const estimatedTop3AfterPct = (snapCapital.top3_pct != null && weightFreed) ? Math.round((snapCapital.top3_pct - weightFreed) * 100) / 100 : null;

  const risksText = classification === "NO_ROTATION"
    ? "No candidate currently meets the qualification bar (score>=" + CR_CANDIDATE_MIN_SCORE + " and PROVEN_POSITIVE edge) — this reflects today's discovery scan, not a permanent judgment."
    : classification === "RETAIN_POSITION"
    ? "Retention evidence currently outweighs rotation evidence despite a qualifying candidate existing — re-evaluate if the balance shifts."
    : `Candidate ${candidate ? candidate.symbol : "?"} has no holding history in this portfolio — its score/edge_class reflects a discovery-time technical setup, not a proven capital-efficiency track record the way the current holding has. Rotation risk includes giving up a demonstrated position for an unproven one.`;

  return {
    ...base, recommendation: classification, confidence: decisionLogRow.recommendation_confidence,
    explanation: `${rotationSignals.length} rotation-favoring signal(s) vs ${retentionSignals.length} retention-favoring signal(s) on the current holding` +
      (candidateQualifies ? `; qualifying candidate ${candidate.symbol} (score=${candidate.score}, edge_class=${candidate.edge_class})` : "; no qualifying candidate found") +
      `. Candidate score is a qualification gate only — it does not by itself determine the rotation tier.`,
    reason: "EVALUATED",
    current_holding: { symbol, decision: decisionLogRow.decision, conviction_trend: convictionTrend, health_score: holding.health_score,
      portfolio_weight_pct: posIntel.portfolio_weight_pct, capital_efficiency_annualized_pct: posIntel.capital_efficiency_annualized_pct,
      profit_protection: ppRec },
    proposed_replacement: proposedReplacement ? { symbol: proposedReplacement, score: candidate.score, base_score: candidate.base_score, edge_class: candidate.edge_class } : null,
    supporting_evidence: { retention_signals: retentionSignals, rotation_signals: rotationSignals, net_signal: netSignal,
      regime, calibration_enrichment: calibEnrichment, constraints_referenced: { max_position_weight_pct: constraints.max_position_weight_pct.value, max_portfolio_concentration_pct: constraints.max_portfolio_concentration_pct.value } },
    expected_portfolio_improvement: classification === "NO_ROTATION" || classification === "RETAIN_POSITION" ? null :
      `Estimated top3 concentration ${snapCapital.top3_pct != null ? snapCapital.top3_pct + "%" : "?"} → ${estimatedTop3AfterPct != null ? estimatedTop3AfterPct + "%" : "?"} if executed; qualitative improvement in conviction/efficiency per signals above.`,
    expected_risks: risksText,
    reversal_conditions: decisionLogRow.reversal_conditions || null,
  };
}

async function computeCapitalRotationRecommendations(env, ctx) {
  const freshness = await ctx.holdingsFreshness();
  if (freshness.freshness_status === "EXPIRED" || freshness.freshness_status === "UNAVAILABLE") {
    return { ok: true, engine_version: CAPITAL_ROTATION_ENGINE_VERSION, generated_ts: new Date().toISOString(),
      status: "RECOMMENDATION_EXPIRED", last_evidence_ts: freshness.source_timestamp, reason: freshness.freshness_reason, next_action: freshness.next_action,
      freshness, recommendations: [],
      regime: null, candidates_considered: 0, best_qualifying_candidate: null, candidate_freshness: await ctx.forwardTrackFreshness(), by_recommendation: {} };
  }

  const foundation = await ctx.capitalFoundation();
  if (!foundation.ok) return { ok: false, error: "Portfolio Capital Foundation unavailable: " + (foundation.error || "unknown") };

  let holdingsRaw = [];
  try { holdingsRaw = await ctx.holdings(); }
  catch (e) { return { ok: false, error: "qe_holdings read failed: " + ((e && e.message) || e) }; }

  const decisionsBySymbol = await ctx.decisionsBySymbol();
  const regime = await ctx.regime();
  const ppResults = await ctx.profitProtection();
  const ppBySymbol = {}; if (ppResults.ok) for (const r of ppResults.recommendations) ppBySymbol[r.symbol] = r;
  const calibration = await ctx.calibrationRecommendations();
  const calibRecs = (calibration && calibration.recommendations) || [];

  const heldSymbols = holdingsRaw.map(h => h.symbol);
  const candidateFreshness = await ctx.forwardTrackFreshness();
  // FRESH_DATA_REQUIRED: a stale forward-track scan must never source a live "deploy cash
  // into this new stock" recommendation — this is the exact mechanism that let a 3-day-old
  // PROVEN_POSITIVE verdict surface as an active BUY when the live scan had since flipped to
  // IGNORE with negative expectancy. Existing-holding rotation evaluation below is unaffected
  // (gated separately, above, on holdings freshness only).
  const candidateFresh = candidateFreshness.freshness_status !== "EXPIRED" && candidateFreshness.freshness_status !== "UNAVAILABLE";
  const candidates = candidateFresh ? await _rotationCandidates(env, heldSymbols) : [];
  const bestCandidate = candidateFresh ? _bestQualifyingCandidate(candidates) : null;

  const posIntelBySymbol = {};
  for (const p of foundation.positions) posIntelBySymbol[p.symbol] = p;
  const snapForRotation = await ctx.snapshot(); // top3_pct is a concentration fact, sourced from the SSOT snapshot, not the capital object

  const recommendations = [];
  for (const h of holdingsRaw) {
    const dlog = decisionsBySymbol[h.symbol];
    const regimeGroup = dlog ? `${dlog.decision}_in_${regime}` : null;
    const calibEnrichment = regimeGroup ? calibRecs.filter(r => r.parameter === "SIGNAL_RELIABILITY_BY_REGIME" && r.group === regimeGroup) : [];
    const result = _evaluateRotation(h, posIntelBySymbol[h.symbol], dlog, ppBySymbol[h.symbol], foundation.constraints, snapForRotation || {}, regime, calibEnrichment, bestCandidate);
    recommendations.push(result);
  }

  return {
    ok: true, engine_version: CAPITAL_ROTATION_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    regime, candidates_considered: candidates.length, best_qualifying_candidate: bestCandidate, candidate_freshness: candidateFreshness,
    by_recommendation: recommendations.reduce((acc, r) => { acc[r.recommendation] = (acc[r.recommendation] || 0) + 1; return acc; }, {}),
    recommendations,
  };
}

async function handleCapitalRotation(env) {
  try { return cors(await computeCapitalRotationRecommendations(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Capital rotation evaluation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO OPTIMIZATION FRAMEWORK — Phase 3.5 (v4.81)
//
// INFRASTRUCTURE ONLY — this module contains zero recommendation logic. Every
// function here either (a) returns static, reusable configuration, (b) evaluates
// an already-true fact against an existing threshold (satisfied/violated — a
// status check, not a decision), or (c) aggregates upstream phases' existing
// outputs by direct function call. No function in this file computes an optimal
// allocation, proposes a trade, or suggests what a hypothetical action should be —
// the one "what-if" interface (below) requires the caller to fully specify the
// hypothetical; it only reports whether that caller-specified hypothetical would
// be feasible against existing constraints.
//
// Introduces ZERO new numeric thresholds. Every constraint value reused here comes
// from Phase 2.5's already-classified (Derived/Portfolio-practice/Engineering-
// assumption/Product-assumption) constraints — this phase adds no new judgment
// calls, which is itself a deliberate signal that it has stayed inside its
// "infrastructure only" boundary rather than drifting into optimization.
//
// No new table. Zero writes. Computed on-demand throughout.
// ═══════════════════════════════════════════════════════════════════════════════

const OPTIMIZATION_FRAMEWORK_VERSION = "optframe-v1.0";

// ── 1. OPTIMIZATION OBJECTIVES — pure catalog/config, no scoring or weighting logic ──
// Each objective names the EXISTING evidence field(s) that inform it — nothing here
// is computed; these are pointers into upstream phases' already-produced outputs.
const OPTIMIZATION_OBJECTIVE_REGISTRY = {
  capital_preservation:      { description: "Avoid giving back accumulated profit / large drawdowns.", direction: "maximize", evidence_source: "Phase 1.1 decision_quality.avg_downside_avoided_pct; Phase 2.0 profit_protection recommendation" },
  risk_adjusted_return:      { description: "Return generated relative to capital and time deployed.", direction: "maximize", evidence_source: "Phase 2.5 position.capital_efficiency_annualized_pct; qe_holdings.r_multiple" },
  portfolio_quality:         { description: "Overall health of the held portfolio.", direction: "maximize", evidence_source: "qe_portfolio_snapshot.portfolio_health" },
  concentration_reduction:   { description: "Reduce over-concentration in single names or sectors.", direction: "minimize", evidence_source: "qe_portfolio_snapshot.top3_pct vs Phase 2.5 max_portfolio_concentration_pct constraint" },
  capital_efficiency:        { description: "How productively deployed capital is generating return per unit time.", direction: "maximize", evidence_source: "Phase 2.5 position.capital_efficiency_annualized_pct" },
  diversification:           { description: "Spread of capital across sectors.", direction: "maximize", evidence_source: "SECTOR_MAP[symbol] per holding (existing static map, Phase 1.2 reuse) — NOT qe_portfolio_snapshot.sector_json, which is currently degenerate ({\"UNKNOWN\":100}), a known pre-existing gap in frozen code, not fixed here" },
  cash_reserve_preservation: { description: "Maintain the configured minimum cash reserve.", direction: "maximize", evidence_source: "Capital Foundation capital.available_cash (live Zerodha) vs capital.min_cash_reserve_floor" },
};

async function _activeObjectives(env) {
  let active = Object.keys(OPTIMIZATION_OBJECTIVE_REGISTRY); // default: all, in registry order (no implied priority)
  try {
    const kv = await env.KITE_STORE.get("PORTFOLIO_OPTIMIZATION_ACTIVE_OBJECTIVES");
    if (kv) {
      const parsed = kv.split(",").map(s => s.trim()).filter(k => OPTIMIZATION_OBJECTIVE_REGISTRY[k]);
      if (parsed.length) active = parsed; // order in KV = priority order, caller-defined, not computed
    }
  } catch (_) {}
  return active.map(key => ({ key, ...OPTIMIZATION_OBJECTIVE_REGISTRY[key] }));
}

// ── 2. OPTIMIZATION CONSTRAINTS — evaluation layer only, never enforcement ──
// Reuses Phase 2.5's exact constraint definitions/values. For each, computes
// current status (SATISFIED/VIOLATED) against real current data — a status
// report, not a gate: nothing here blocks, rejects, or modifies anything.
function _sectorWeights(positions) {
  const weights = {};
  for (const p of positions) {
    const sector = SECTOR_MAP[p.symbol] || "OTHER";
    weights[sector] = (weights[sector] || 0) + (p.portfolio_weight_pct || 0);
  }
  return weights;
}

async function evaluateOptimizationConstraints(env, ctx) {
  const foundation = await ctx.capitalFoundation();
  if (!foundation.ok) return { ok: false, error: "Portfolio Capital Foundation unavailable: " + (foundation.error || "unknown") };

  const c = foundation.constraints;
  const results = [];

  // max_position_weight_pct — which holdings (if any) exceed it
  const overWeightHoldings = foundation.positions.filter(p => p.portfolio_weight_pct != null && p.portfolio_weight_pct > c.max_position_weight_pct.value);
  results.push({ constraint: "max_position_weight_pct", threshold: c.max_position_weight_pct.value, classification: c.max_position_weight_pct.classification,
    status: overWeightHoldings.length ? "VIOLATED" : "SATISFIED", violating_holdings: overWeightHoldings.map(p => ({ symbol: p.symbol, value: p.portfolio_weight_pct })) });

  // max_portfolio_concentration_pct — top3_pct (read from the same qe_portfolio_snapshot row computePortfolioRisk writes — unchanged SSOT)
  const snapForConstraints = await ctx.snapshot();
  const top3 = snapForConstraints ? snapForConstraints.top3_pct : null;
  results.push({ constraint: "max_portfolio_concentration_pct", threshold: c.max_portfolio_concentration_pct.value, classification: c.max_portfolio_concentration_pct.classification,
    status: (top3 != null && top3 > c.max_portfolio_concentration_pct.value) ? "VIOLATED" : (top3 == null ? "UNKNOWN" : "SATISFIED"), current_value: top3 });

  // min_cash_reserve — evaluable only when the live Zerodha capital object is available
  const availableCash = foundation.capital.ok ? foundation.capital.available_cash : null;
  results.push({ constraint: "min_cash_reserve", threshold: c.min_cash_reserve.value, classification: c.min_cash_reserve.classification,
    status: availableCash == null ? "UNKNOWN" : (availableCash < c.min_cash_reserve.value ? "VIOLATED" : "SATISFIED"),
    current_value: availableCash, note: availableCash == null ? "CAPITAL_INFORMATION_UNAVAILABLE — " + (foundation.capital.reason || "live Zerodha funds data not available") : null });

  // max_sector_exposure_pct — via SECTOR_MAP, not the degenerate snapshot sector_json
  const sectorWeights = _sectorWeights(foundation.positions);
  const violatingSectors = Object.entries(sectorWeights).filter(([, w]) => w > c.max_sector_exposure_pct.value);
  results.push({ constraint: "max_sector_exposure_pct", threshold: c.max_sector_exposure_pct.value, classification: c.max_sector_exposure_pct.classification,
    status: violatingSectors.length ? "VIOLATED" : "SATISFIED", sector_weights: sectorWeights,
    violating_sectors: violatingSectors.map(([sector, w]) => ({ sector, weight_pct: Math.round(w * 100) / 100 })) });

  // max_concurrent_new_positions — not evaluable without a notion of "new positions this period"; honestly reported
  results.push({ constraint: "max_concurrent_new_positions", threshold: c.max_concurrent_new_positions.value, classification: c.max_concurrent_new_positions.classification,
    status: "NOT_APPLICABLE", note: "No new-position tracking window exists yet — this constraint applies to future position-sizing/execution decisions, not the current static snapshot." });

  return { ok: true, engine_version: OPTIMIZATION_FRAMEWORK_VERSION, generated_ts: new Date().toISOString(), constraints: results };
}

// ── 3. OPTIMIZATION EVIDENCE — pure aggregation, zero recomputation ──
// Calls each upstream phase's existing compute function directly and merges by
// symbol. No new evidence is derived here beyond simple grouping.
async function buildOptimizationEvidence(env, ctx) {
  const [foundation, profitProtection, capitalRotation, calibration] = await Promise.all([
    ctx.capitalFoundation(),
    ctx.profitProtection(),
    ctx.capitalRotation(),
    ctx.calibrationRecommendations(),
  ]);
  if (!foundation.ok) return { ok: false, error: "Portfolio Capital Foundation unavailable: " + (foundation.error || "unknown") };

  const decisionsBySymbol = await ctx.decisionsBySymbol();
  const ppBySymbol = {}; if (profitProtection.ok) for (const r of profitProtection.recommendations) ppBySymbol[r.symbol] = r;
  const rotBySymbol = {}; if (capitalRotation.ok) for (const r of capitalRotation.recommendations) rotBySymbol[r.symbol] = r;

  const bySymbol = foundation.positions.map(p => ({
    symbol: p.symbol,
    position_intelligence: p, // Phase 2.5
    decision: decisionsBySymbol[p.symbol] ? { decision: decisionsBySymbol[p.symbol].decision, confidence: decisionsBySymbol[p.symbol].recommendation_confidence, reversal_conditions: decisionsBySymbol[p.symbol].reversal_conditions } : null, // Decision Engine, via Decision Log
    profit_protection: ppBySymbol[p.symbol] || null, // Phase 2.0
    capital_rotation: rotBySymbol[p.symbol] || null, // Phase 3.0
  }));

  return {
    ok: true, engine_version: OPTIMIZATION_FRAMEWORK_VERSION, generated_ts: new Date().toISOString(),
    capital: foundation.capital, constraints: foundation.constraints,
    calibration_summary: calibration.ok ? { proposed_count: calibration.proposed_count, no_calibration_count: calibration.no_calibration_count } : null,
    by_symbol: bySymbol,
  };
}

// ── 4. OPTIMIZATION INTERFACE — feasibility check for a CALLER-SUPPLIED hypothetical ──
// This does not decide what the hypothetical should be. It only answers: given this
// fully-specified proposed change, which constraints apply, and would they be
// satisfied? Never computes or suggests an allocation itself.
async function evaluateOptimizationFeasibility(env, proposedAction, ctx) {
  const { symbol, proposed_weight_pct } = proposedAction;
  if (!symbol || proposed_weight_pct == null || isNaN(Number(proposed_weight_pct))) {
    return { ok: false, error: "proposedAction requires symbol and a numeric proposed_weight_pct — this interface evaluates a caller-specified hypothetical, it does not generate one." };
  }
  const foundation = await ctx.capitalFoundation();
  if (!foundation.ok) return { ok: false, error: "Portfolio Capital Foundation unavailable: " + (foundation.error || "unknown") };

  const c = foundation.constraints;
  const currentPos = foundation.positions.find(p => p.symbol === symbol);
  const newWeight = Number(proposed_weight_pct);
  const currentTop3 = (await ctx.snapshot())?.top3_pct ?? null;
  // Estimated resulting concentration impact — simple deterministic arithmetic (swap this symbol's weight), not an optimization.
  const weightDelta = currentPos ? newWeight - (currentPos.portfolio_weight_pct || 0) : newWeight;
  const estimatedTop3 = currentTop3 != null ? Math.round((currentTop3 + weightDelta) * 100) / 100 : null;

  const applicableConstraints = [
    { constraint: "max_position_weight_pct", threshold: c.max_position_weight_pct.value,
      feasible: newWeight <= c.max_position_weight_pct.value, proposed_value: newWeight },
    { constraint: "max_portfolio_concentration_pct", threshold: c.max_portfolio_concentration_pct.value,
      feasible: estimatedTop3 == null || estimatedTop3 <= c.max_portfolio_concentration_pct.value, estimated_value: estimatedTop3 },
  ];
  const sector = SECTOR_MAP[symbol] || "OTHER";
  applicableConstraints.push({ constraint: "max_sector_exposure_pct", threshold: c.max_sector_exposure_pct.value, sector,
    note: "Full sector-weight recomputation requires the evidence bundle (see /portfolio/optimization/evidence) — this endpoint reports the position-level and portfolio-concentration checks only." });

  const overallFeasible = applicableConstraints.filter(x => x.feasible === false).length === 0;

  return {
    ok: true, engine_version: OPTIMIZATION_FRAMEWORK_VERSION, generated_ts: new Date().toISOString(),
    note: "Feasibility check only — evaluates the caller-supplied hypothetical against existing constraints. Does not recommend, rank, or generate a proposed allocation.",
    proposed_action: { symbol, proposed_weight_pct: newWeight, current_weight_pct: currentPos ? currentPos.portfolio_weight_pct : null },
    feasible: overallFeasible,
    applicable_constraints: applicableConstraints,
    supporting_evidence: { current_position: currentPos || null, current_top3_pct: currentTop3, estimated_top3_pct_after: estimatedTop3 },
  };
}

async function handleOptimizationObjectives(env) {
  try { return cors({ ok: true, engine_version: OPTIMIZATION_FRAMEWORK_VERSION, active_objectives: await _activeObjectives(env) }); }
  catch (e) { return corsErr(e.message || "Objectives lookup failed", 502); }
}
async function handleOptimizationConstraints(env) {
  try { return cors(await evaluateOptimizationConstraints(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Constraint evaluation failed", 502); }
}
async function handleOptimizationEvidence(env) {
  try { return cors(await buildOptimizationEvidence(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Evidence aggregation failed", 502); }
}
async function handleOptimizationFeasibility(url, env) {
  const proposedAction = { symbol: url.searchParams.get("symbol"), proposed_weight_pct: url.searchParams.get("proposed_weight_pct") };
  try { return cors(await evaluateOptimizationFeasibility(env, proposedAction, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Feasibility evaluation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO OPTIMIZER — Phase 4.0 (v4.82)
//
// ARCHITECTURAL DISTINCTION FROM PHASE 2.0/3.0: those engines loop per-holding and
// emit one recommendation per symbol. This engine evaluates the PORTFOLIO AS A
// WHOLE — it emits a small set of portfolio-scoped recommendations (concentration,
// diversification, cash posture, position-weight rebalancing), each of which may
// NAME specific holdings as evidence, but the trigger and framing are always
// portfolio-level constraint/objective status, never an independent per-holding
// quality judgment. That is the concrete implementation of "must not evaluate
// holdings independently."
//
// NON-OVERRIDE GUARDRAIL, enforced structurally, not by promise: this engine never
// re-derives whether a specific holding is good or bad. Wherever a recommendation
// touches a specific symbol, it reads that symbol's ALREADY-COMPUTED Capital
// Rotation (3.0) and Profit Protection (2.0) verdicts and either (a) aligns with
// them, or (b) if portfolio-level and position-level evidence disagree, reports the
// tension explicitly as a fact requiring human judgment — it does not pick a side.
// A holding with Profit Protection = LET_PROFITS_RUN can never be named as a
// trim/reduce target here, full stop, regardless of its portfolio weight.
//
// Reuses Phase 3.5's evidence aggregator and constraint evaluator directly (zero
// recomputation of anything), Phase 3.0's rotation output, Phase 2.0's protection
// output. No new table, zero writes, computed on-demand.
// ═══════════════════════════════════════════════════════════════════════════════

const PORTFOLIO_OPTIMIZER_VERSION = "optimizer-v1.0";

function _violationMagnitudeBand(current, threshold) {
  if (current == null || threshold == null || threshold === 0) return null;
  const pctOver = ((current - threshold) / threshold) * 100;
  if (pctOver > 50) return "HIGH";
  if (pctOver > 10) return "MEDIUM";
  return "LOW";
}

async function computePortfolioOptimization(env, ctx) {
  const evidence = await ctx.optimizationEvidence();
  if (!evidence.ok) return { ok: false, error: "Optimization evidence unavailable: " + (evidence.error || "unknown") };
  const constraints = await ctx.optimizationConstraints();
  if (!constraints.ok) return { ok: false, error: "Constraint evaluation unavailable: " + (constraints.error || "unknown") };
  const rotation = await ctx.capitalRotation();
  const objectives = await ctx.activeObjectives();

  const cByType = {}; for (const c of constraints.constraints) cByType[c.constraint] = c;
  const bySymbol = {}; for (const s of evidence.by_symbol) bySymbol[s.symbol] = s;

  const recommendations = [];

  // ── REDUCE_CONCENTRATION (portfolio-level) ──
  const concC = cByType["max_portfolio_concentration_pct"];
  if (concC && concC.status === "VIOLATED") {
    const topContributors = [...evidence.by_symbol]
      .sort((a, b) => (b.position_intelligence.portfolio_weight_pct || 0) - (a.position_intelligence.portfolio_weight_pct || 0)).slice(0, 3)
      .map(s => ({ symbol: s.symbol, weight_pct: s.position_intelligence.portfolio_weight_pct,
        capital_rotation: s.capital_rotation ? s.capital_rotation.recommendation : null,
        profit_protection: s.profit_protection ? s.profit_protection.recommendation : null }));
    const actionable = topContributors.filter(t => t.profit_protection !== "LET_PROFITS_RUN" && t.capital_rotation && !["RETAIN_POSITION", "NO_ROTATION"].includes(t.capital_rotation));
    const protectedFromAction = topContributors.filter(t => t.profit_protection === "LET_PROFITS_RUN");
    recommendations.push({
      type: "REDUCE_CONCENTRATION", scope: "PORTFOLIO",
      confidence_band: _violationMagnitudeBand(concC.current_value, concC.threshold),
      explanation: `Portfolio concentration constraint violated: top3_pct=${concC.current_value}% vs threshold ${concC.threshold}%. Top contributors: ${topContributors.map(t => `${t.symbol} (${t.weight_pct}%)`).join(", ")}.` +
        (actionable.length ? ` Capital Rotation already recommends action on ${actionable.map(a => a.symbol).join(", ")} — executing those would directly help address this constraint.` :
          protectedFromAction.length ? ` ${protectedFromAction.map(p => p.symbol).join(", ")} ${protectedFromAction.length > 1 ? "are" : "is"} protected by Profit Protection (LET_PROFITS_RUN) and cannot be named as a reduction target here — this is a genuine tension between portfolio-level concentration and position-level conviction requiring human judgment, not something this engine resolves by overriding either upstream engine.` :
          ` Capital Rotation currently supports retaining all top contributors — the same tension applies: this engine does not override Capital Rotation's position-level evidence.`),
      supporting_evidence: { constraint: concC, top_contributors: topContributors },
      objectives_addressed: ["concentration_reduction"],
    });
  }

  // ── IMPROVE_DIVERSIFICATION (portfolio-level) ──
  const sectorC = cByType["max_sector_exposure_pct"];
  if (sectorC && sectorC.status === "VIOLATED") {
    recommendations.push({
      type: "IMPROVE_DIVERSIFICATION", scope: "PORTFOLIO",
      confidence_band: "MEDIUM",
      explanation: `Sector exposure constraint violated for: ${sectorC.violating_sectors.map(v => `${v.sector} (${v.weight_pct}%)`).join(", ")}, threshold ${sectorC.threshold}%. Full sector weights: ${JSON.stringify(sectorC.sector_weights)}.`,
      supporting_evidence: { constraint: sectorC },
      objectives_addressed: ["diversification"],
    });
  }

  // ── HOLD_CASH / DEPLOY_CASH (portfolio-level) ──
  const cashC = cByType["min_cash_reserve"];
  const availableCash = evidence.capital.ok ? evidence.capital.available_cash : null;
  if (!evidence.capital.ok) {
    recommendations.push({
      type: "NO_OPTIMIZATION_REQUIRED", scope: "CASH",
      confidence_band: null,
      explanation: "Cash posture cannot be optimized — CAPITAL_INFORMATION_UNAVAILABLE: " + (evidence.capital.reason || "live Zerodha funds data not available") + " (as of " + evidence.capital.as_of_ts + ").",
      supporting_evidence: { available_cash: null, capital_status: "CAPITAL_INFORMATION_UNAVAILABLE", reason: evidence.capital.reason }, objectives_addressed: ["cash_reserve_preservation"],
    });
  } else if (cashC && cashC.status === "VIOLATED") {
    recommendations.push({ type: "HOLD_CASH", scope: "PORTFOLIO", confidence_band: "HIGH",
      explanation: `Cash reserve constraint violated: available_cash=${availableCash} below configured reserve ${cashC.threshold}. No further deployment recommended until reserve is restored.`,
      supporting_evidence: { constraint: cashC }, objectives_addressed: ["cash_reserve_preservation", "capital_preservation"] });
  } else {
    const deployable = evidence.capital.remaining_buying_capacity;
    const candidateFreshness = rotation.ok ? rotation.candidate_freshness : null;
    const candidateStale = candidateFreshness && (candidateFreshness.freshness_status === "EXPIRED" || candidateFreshness.freshness_status === "UNAVAILABLE");
    const bestCandidate = rotation.ok ? rotation.best_qualifying_candidate : null;
    if (deployable != null && deployable > 0 && candidateStale) {
      recommendations.push({ type: "FRESH_DATA_REQUIRED", scope: "CASH", confidence_band: null,
        last_evidence_ts: candidateFreshness.source_timestamp, next_action: candidateFreshness.next_action,
        explanation: `Deployable capital of ${deployable} is available, but Capital Rotation's candidate scan is ${candidateFreshness.freshness_reason} A recommendation will not be made on stale evidence.`,
        freshness: candidateFreshness, supporting_evidence: { deployable_capital: deployable }, objectives_addressed: ["capital_efficiency"] });
    } else if (deployable != null && deployable > 0 && bestCandidate) {
      // Priority 3 fix (13-Jul-2026): size the allocation against the SAME
      // max_position_weight_pct constraint already used elsewhere to flag overweight
      // holdings (25%, PRODUCT_ASSUMPTION — not a new threshold). Previously
      // suggested_allocation was simply 100% of deployable cash, which could recommend
      // concentrating the entire portfolio into one new position — in tension with a
      // constraint the platform already enforces on existing holdings. Solves for the
      // allocation amount X such that X / (current_portfolio_value + X) <= maxWeight.
      const maxWeightPct = cByType["max_position_weight_pct"] ? cByType["max_position_weight_pct"].threshold : null;
      const currentPortfolioValue = evidence.capital.ok ? evidence.capital.current_portfolio_value : null;
      let weightCap = null, sizingNote = null;
      if (maxWeightPct != null && currentPortfolioValue != null) {
        const w = maxWeightPct / 100;
        weightCap = Math.round((w * currentPortfolioValue) / (1 - w) * 100) / 100;
      }
      const sizedAllocation = weightCap != null ? Math.max(0, Math.min(deployable, weightCap)) : deployable;
      if (weightCap != null && weightCap < deployable) {
        sizingNote = `Capped by your ${maxWeightPct}% max single-position weight limit (would otherwise be ₹${deployable}, full deployable cash).`;
      } else if (weightCap == null) {
        sizingNote = `Not weight-capped — no current portfolio value on record yet to size against; this is full deployable cash, not a sized position.`;
      }

      // Phase 4 (18-Jul-2026): Portfolio Fit gate. A technically-qualified candidate must
      // still improve THIS portfolio's actual allocation before it becomes an active
      // recommendation — "Discovery signals alone should never trigger recommendations."
      let fit = null;
      try { fit = await computePortfolioFit(env, ctx, bestCandidate.symbol, sizedAllocation); } catch (_) {}

      if (fit && fit.ok && fit.verdict === "WORSENS_CONCENTRATION") {
        recommendations.push({ type: "PORTFOLIO_FIT_REJECTED", scope: "PORTFOLIO", confidence_band: null,
          candidate_symbol: bestCandidate.symbol, portfolio_fit: fit,
          explanation: `${bestCandidate.symbol} qualified technically (score=${bestCandidate.score}, edge_class=${bestCandidate.edge_class}) but fails Portfolio Fit: ${fit.reason}`,
          one_line_reason: `Technically qualified but does not fit this portfolio right now: ${fit.reason}`,
          supporting_evidence: { candidate: bestCandidate, portfolio_fit: fit }, objectives_addressed: ["risk_adjusted_return"] });
      } else {
        recommendations.push({ type: "DEPLOY_CASH", scope: "PORTFOLIO", confidence_band: "MEDIUM",
        // Trust refinement (13-Jul-2026), requirement #3: surface the recommendation as an
        // immediately actionable card (candidate, confidence, one-line reason) rather than a
        // static type-name badge. confidence reuses the SAME score Capital Rotation already
        // qualified this candidate on (CR_CANDIDATE_MIN_SCORE + PROVEN_POSITIVE edge_class) —
        // not a newly invented metric. one_line_reason is deterministically templated from
        // already-computed fields, not free-form text.
        candidate_symbol: bestCandidate.symbol,
        recommended_action: bestCandidate.label,
        suggested_allocation: sizedAllocation,
        sizing_note: sizingNote,
        deployable_capital: deployable,
        confidence: bestCandidate.score,
        source_engine: ["Capital Rotation Engine", "Live Zerodha Funds"],
        freshness: candidateFreshness,
        portfolio_fit: fit, // Phase 4 — surfaced for transparency even when it passes
        one_line_reason: `Highest-scoring PROVEN_POSITIVE-edge candidate (score=${bestCandidate.score}) among un-held BUY-labeled stocks — ₹${sizedAllocation} sized to your ${maxWeightPct != null ? maxWeightPct + "% max position weight" : "deployable cash"}.`,
        explanation: `Deployable capital of ${deployable} available above reserve, and Capital Rotation has identified a qualifying candidate (${bestCandidate.symbol}, score=${bestCandidate.score}, edge_class=${bestCandidate.edge_class}). Allocation sized against max_position_weight_pct (${maxWeightPct}%) rather than suggesting full deployable cash into one position. This references Capital Rotation's existing candidate evaluation rather than independently assessing the opportunity.`,
        supporting_evidence: { deployable_capital: deployable, weight_cap: weightCap, max_position_weight_pct: maxWeightPct, candidate: bestCandidate }, objectives_addressed: ["capital_efficiency", "risk_adjusted_return"] });
      }
    }
  }

  // ── INCREASE_POSITION (portfolio-level: underweight + strong retention evidence + cash available) ──
  if (evidence.capital.ok && evidence.capital.remaining_buying_capacity > 0) {
    const weights = evidence.by_symbol.map(s => s.position_intelligence.portfolio_weight_pct || 0).filter(w => w > 0);
    const avgWeight = weights.length ? weights.reduce((a, b) => a + b, 0) / weights.length : null;
    const candidates = evidence.by_symbol.filter(s => {
      const w = s.position_intelligence.portfolio_weight_pct;
      const eff = s.position_intelligence.capital_efficiency_annualized_pct;
      const rot = s.capital_rotation ? s.capital_rotation.recommendation : null;
      const pp = s.profit_protection ? s.profit_protection.recommendation : null;
      return avgWeight != null && w != null && w < avgWeight && eff != null && eff > 50 && rot === "RETAIN_POSITION" && pp !== "FULL_PROFIT_PROTECTION";
    });
    if (candidates.length) {
      recommendations.push({ type: "INCREASE_POSITION", scope: "PORTFOLIO",
        confidence_band: "MEDIUM",
        explanation: `${candidates.map(c => c.symbol).join(", ")} ${candidates.length > 1 ? "are" : "is"} below average portfolio weight (${avgWeight.toFixed(2)}%) despite strong capital efficiency and a Capital Rotation RETAIN_POSITION verdict — deployable capital exists to increase weighting. This defers entirely to Capital Rotation's existing retention verdict rather than independently re-scoring these holdings.`,
        supporting_evidence: { avg_weight_pct: avgWeight, candidates: candidates.map(c => ({ symbol: c.symbol, weight_pct: c.position_intelligence.portfolio_weight_pct, capital_efficiency_annualized_pct: c.position_intelligence.capital_efficiency_annualized_pct })) },
        objectives_addressed: ["capital_efficiency", "risk_adjusted_return"] });
    }
  }

  // ── MAINTAIN_POSITION (portfolio-level default for holdings untouched by any issue above) ──
  const namedSymbols = new Set();
  for (const r of recommendations) {
    if (r.supporting_evidence && r.supporting_evidence.top_contributors) for (const t of r.supporting_evidence.top_contributors) namedSymbols.add(t.symbol);
    if (r.supporting_evidence && r.supporting_evidence.candidates) for (const c of r.supporting_evidence.candidates) namedSymbols.add(c.symbol);
  }
  const untouched = evidence.by_symbol.filter(s => !namedSymbols.has(s.symbol));
  if (untouched.length) {
    recommendations.push({ type: "MAINTAIN_POSITION", scope: "PORTFOLIO", confidence_band: "LOW",
      explanation: `${untouched.map(s => s.symbol).join(", ")} ${untouched.length > 1 ? "are" : "is"} not implicated in any portfolio-level constraint violation or rebalancing signal — no change indicated.`,
      supporting_evidence: { symbols: untouched.map(s => s.symbol) }, objectives_addressed: ["portfolio_quality"] });
  }

  if (!recommendations.length) {
    recommendations.push({ type: "NO_OPTIMIZATION_REQUIRED", scope: "PORTFOLIO", confidence_band: null,
      explanation: "No portfolio-level constraint violations or rebalancing signals found in current evidence.",
      supporting_evidence: { constraints: constraints.constraints }, objectives_addressed: [] });
  }

  const snapForSummary = await ctx.snapshot();
  return {
    ok: true, engine_version: PORTFOLIO_OPTIMIZER_VERSION, generated_ts: new Date().toISOString(),
    portfolio_summary: { portfolio_health: snapForSummary ? snapForSummary.portfolio_health : null, top3_pct: snapForSummary ? snapForSummary.top3_pct : null,
      holdings_count: evidence.by_symbol.length, available_cash: availableCash, deployable_capital: evidence.capital.ok ? evidence.capital.remaining_buying_capacity : null,
      capital_status: evidence.capital.ok ? "LIVE" : "CAPITAL_INFORMATION_UNAVAILABLE" },
    active_objectives: objectives.map(o => o.key),
    recommendations,
  };
}

async function handlePortfolioOptimization(env) {
  try { return cors(await computePortfolioOptimization(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Portfolio optimization failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO MEMORY ENGINE — Institutional Memory foundation (v4.83)
//
// SEQUENCING NOTE: the original roadmap listed this 4th within "Institutional
// Memory" (after Decision Replay/Narrative/Portfolio Story), but its own spec says
// Copilot/Replay/Calibration must consume it rather than reconstruct history
// independently — a dependency pointing backwards. Siva approved building it first;
// product roadmap unchanged, only implementation order.
//
// PURE MEMORY LAYER — verified by construction: no function in this module scores,
// ranks, recommends, or explains. Every field is either (a) a direct read of an
// already-stored value, (b) a chronological list of already-stored values, or (c) a
// mechanical transition detection between two ADJACENT already-stored values (e.g.
// "conviction_trend changed from STABLE to IMPROVING on date X" is a logged fact,
// not a judgment). current_thesis_snapshot is a factual bundle of the latest
// already-computed signals — explicitly NOT a new synthesized validity verdict;
// that interpretation remains the Decision Engine's exclusive responsibility.
//
// NO NEW TABLE — every underlying source is already append-only/immutable
// (qe_decision_log, qe_decision_outcomes, qe_holding_history, qe_trade_outcomes).
// This engine composes them on-demand; persisting a derived copy would duplicate
// data that's already the single source of truth, violating "do not duplicate
// historical reconstruction logic." Same zero-new-table pattern as every phase
// since 1.1.
//
// REUSE MAP (checked against live schema before writing any code):
//   original_thesis, decision_journey        → qe_decision_log (append-only)
//   health_evolution                         → qe_holding_history (daily-granularity
//                                               health/pillar snapshots — a better-
//                                               resolution source than decision_log,
//                                               which only has one row per pipeline run)
//   outcome_history                          → qe_decision_outcomes (Phase 1.0)
//   realized_performance                     → qe_trade_outcomes, status=CLOSED
//                                               (pre-roadmap Trade Tracker — reused,
//                                               not the same mechanism as Phase 1.0's
//                                               automated resolver, disclosed as such)
//   unrealized_performance                   → Phase 2.5 position intelligence
//   current_thesis_snapshot.profit_protection→ Phase 2.0, current_thesis_snapshot.
//     capital_rotation                         capital_rotation → Phase 3.0
// ═══════════════════════════════════════════════════════════════════════════════

const PORTFOLIO_MEMORY_ENGINE_VERSION = "memory-v1.0";

// Reused ordering from the roadmap's own 7-state decision vocabulary — not a new
// invented hierarchy, purely for detecting whether consecutive decisions moved up
// or down this existing scale.
const DECISION_RANK = { STRONG_BUY: 6, BUY: 5, ACCUMULATE: 4, HOLD: 3, REDUCE: 2, SELL: 1, EXIT_IMMEDIATELY: 0 };

async function _fullDecisionJourney(env, symbol) {
  const q = await env.QE_DB.prepare(
    "SELECT ts,decision,verdict,health_score,recommendation_confidence,conviction_trend,what_changed,why_changed,reversal_conditions,engine_version FROM qe_decision_log WHERE symbol=?1 ORDER BY ts ASC"
  ).bind(symbol).all();
  return (q && q.results) || [];
}
async function _healthEvolution(env, symbol) {
  const q = await env.QE_DB.prepare(
    "SELECT snapshot_date,health_score,data_confidence,pillar_trend,pillar_momentum,pillar_edge,pillar_risk,pillar_fundnews,verdict FROM qe_holding_history WHERE symbol=?1 ORDER BY snapshot_date ASC"
  ).bind(symbol).all();
  return (q && q.results) || [];
}
async function _outcomeHistoryForMemory(env, symbol) {
  const q = await env.QE_DB.prepare(
    "SELECT decision,decision_date,eval_window_days,resolution_status,price_change_pct,mfe_pct,mae_pct,benchmark_relative_pct,regime_at_evaluation FROM qe_decision_outcomes WHERE symbol=?1 ORDER BY decision_date ASC, eval_window_days ASC"
  ).bind(symbol).all();
  return (q && q.results) || [];
}
async function _realizedPerformance(env, symbol) {
  const q = await env.QE_DB.prepare(
    "SELECT buy_date,buy_price,sell_date,sell_price,qty,return_pct,return_r,hold_days,exit_reason,status FROM qe_trade_outcomes WHERE symbol=?1 ORDER BY buy_date ASC"
  ).bind(symbol).all();
  return (q && q.results) || [];
}

// Mechanical transition detection over adjacent decision_journey entries — a fact
// log ("X changed from A to B on date D"), never an interpretation of why or
// whether it matters.
function _deriveThesisEvents(journey) {
  const events = [];
  for (let i = 1; i < journey.length; i++) {
    const prev = journey[i - 1], cur = journey[i];
    if (prev.conviction_trend !== cur.conviction_trend) {
      if (cur.conviction_trend === "IMPROVING") events.push({ ts: cur.ts, event_type: "STRENGTHENING", trigger: "conviction_trend", from: prev.conviction_trend, to: cur.conviction_trend });
      else if (cur.conviction_trend === "WEAKENING") events.push({ ts: cur.ts, event_type: "WEAKENING", trigger: "conviction_trend", from: prev.conviction_trend, to: cur.conviction_trend });
    }
    const prevRank = DECISION_RANK[prev.decision], curRank = DECISION_RANK[cur.decision];
    if (prevRank != null && curRank != null && prevRank !== curRank) {
      events.push({ ts: cur.ts, event_type: curRank > prevRank ? "STRENGTHENING" : "WEAKENING", trigger: "decision", from: prev.decision, to: cur.decision });
    }
  }
  return events;
}

async function computePortfolioMemory(env, symbolFilter, ctx) {
  let symbols;
  if (symbolFilter) {
    symbols = [symbolFilter];
  } else {
    let q; try { q = await env.QE_DB.prepare(
      "SELECT DISTINCT d.symbol FROM qe_decision_log d INNER JOIN qe_holdings h ON h.symbol=d.symbol AND h.qty>0 ORDER BY d.symbol ASC"
    ).all(); }
    catch (e) { return { ok: false, error: "qe_decision_log read failed: " + ((e && e.message) || e) }; }
    symbols = ((q && q.results) || []).map(r => r.symbol);
  }
  if (!symbols.length) return { ok: true, engine_version: PORTFOLIO_MEMORY_ENGINE_VERSION, generated_ts: new Date().toISOString(), symbols_covered: 0, records: [] };

  const foundation = await ctx.capitalFoundation();
  const posBySymbol = {}; if (foundation.ok) for (const p of foundation.positions) posBySymbol[p.symbol] = p;
  const ppResults = await ctx.profitProtection();
  const ppBySymbol = {}; if (ppResults.ok) for (const r of ppResults.recommendations) ppBySymbol[r.symbol] = r;
  const rotResults = await ctx.capitalRotation();
  const rotBySymbol = {}; if (rotResults.ok) for (const r of rotResults.recommendations) rotBySymbol[r.symbol] = r;

  const records = [];
  for (const symbol of symbols) {
    let journey; try { journey = await _fullDecisionJourney(env, symbol); } catch (_) { journey = []; }
    if (!journey.length) continue;
    let healthEvo = []; try { healthEvo = await _healthEvolution(env, symbol); } catch (_) {}
    let outcomeHist = []; try { outcomeHist = await _outcomeHistoryForMemory(env, symbol); } catch (_) {}
    let realized = []; try { realized = await _realizedPerformance(env, symbol); } catch (_) {}
    const thesisEvents = _deriveThesisEvents(journey);
    const first = journey[0], latest = journey[journey.length - 1];

    records.push({
      symbol,
      original_thesis: { ts: first.ts, decision: first.decision, verdict: first.verdict, health_score: first.health_score,
        reversal_conditions: first.reversal_conditions, engine_version: first.engine_version },
      decision_journey: journey,
      health_evolution: healthEvo.length ? healthEvo : null,
      health_evolution_source: healthEvo.length ? "qe_holding_history (daily granularity)" : "unavailable — no qe_holding_history rows for this symbol yet",
      outcome_history: outcomeHist,
      realized_performance: realized.filter(r => r.status === "CLOSED"),
      realized_performance_source: "qe_trade_outcomes (pre-roadmap Trade Tracker, manually-logged closes) — a different mechanism than Phase 1.0's automated resolver, disclosed as such",
      unrealized_performance: posBySymbol[symbol] || null,
      thesis_events: thesisEvents,
      current_thesis_snapshot: {
        as_of_ts: latest.ts, decision: latest.decision, conviction_trend: latest.conviction_trend, health_score: latest.health_score,
        profit_protection: ppBySymbol[symbol] ? ppBySymbol[symbol].recommendation : null,
        capital_rotation: rotBySymbol[symbol] ? rotBySymbol[symbol].recommendation : null,
        note: "Factual snapshot of the latest already-computed signals — not a new synthesized validity judgment. Interpretation remains the Decision Engine's exclusive responsibility.",
      },
    });
  }

  return { ok: true, engine_version: PORTFOLIO_MEMORY_ENGINE_VERSION, generated_ts: new Date().toISOString(), symbols_covered: records.length, records };
}

async function handleInstitutionalMemory(url, env) {
  const symbol = url.searchParams.get("symbol");
  try { return cors(await computePortfolioMemory(env, symbol, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Portfolio memory computation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION REPLAY ENGINE — Phase 5.0 (v4.84)
//
// PURPOSE: given a specific historical decision (by qe_decision_log.id), faithfully
// reconstruct what QuantEdge knew and decided AT THAT MOMENT — never today's state,
// never today's algorithms. Historical truth over current correctness.
//
// ARCHITECTURAL FINDING (disclosed, not worked around): most of this roadmap's
// engines — Profit Protection (2.0), Portfolio Capital Intelligence (2.5), Capital
// Rotation (3.0), Optimization Framework (3.5), Portfolio Optimizer (4.0), Decision
// Calibration (1.2), Decision Quality Analytics (1.1) — were deliberately built as
// LIVE-ONLY, ZERO-PERSISTENCE computations (a design choice justified and approved
// at each phase: "computed on-demand, zero new tables, zero regression risk"). That
// design choice has a real consequence for Replay: none of those engines' outputs
// were ever persisted at any past moment, so there is no historical trace to
// reconstruct. This is an INFRASTRUCTURE LIMITATION of the current architecture,
// not a missing-data gap specific to any one decision — every replay reports
// HISTORICAL DATA NOT AVAILABLE for those dimensions, with this exact reason, and
// this engine NEVER calls those engines' live compute functions to paper over the
// gap (that would be "applying today's logic to yesterday's decision," explicitly
// forbidden).
//
// WHAT IS GENUINELY REPLAYABLE (append-only/immutable by construction, checked
// against live schema before writing any code): qe_decision_log itself (frozen at
// write — decision, verdict, health, conviction, pillars, triggers, evidence,
// reversal conditions, engine_version), qe_portfolio_snapshot (joined via the
// decision's own snapshot_id — NOT today's latest snapshot), qe_holding_history
// (point-in-time per symbol+date — NOT qe_holdings, which is current-only),
// qe_decision_outcomes (resolved evidence about what happened AFTER — presented in
// a clearly separate, explicitly-labeled section, never merged into "what was known
// at decision time"), qe_trade_outcomes (genuinely dated historical trade records).
//
// MARKET REGIME: qe_decision_log has no regime column — the literal live regime the
// pipeline read at that exact moment was never persisted and is not recoverable.
// What IS available is a deterministic reconstruction via the same method Phase 1.0
// already established and disclosed (_regimeAsOf — NIFTYBEES closes up to the
// decision date, fed into the unchanged computePipelineRegime()). Reused directly,
// not reimplemented, and clearly labeled "reconstructed" rather than "as recorded,"
// since it depends only on data that existed by that date, not on anything learned
// since.
// ═══════════════════════════════════════════════════════════════════════════════

const DECISION_REPLAY_ENGINE_VERSION = "replay-v1.0";
const HISTORICAL_DATA_NOT_AVAILABLE = "HISTORICAL_DATA_NOT_AVAILABLE";

async function _decisionLogById(env, id) {
  const q = await env.QE_DB.prepare("SELECT * FROM qe_decision_log WHERE id=?1").bind(id).first();
  return q || null;
}
async function _portfolioSnapshotById(env, snapshotId) {
  if (snapshotId == null) return null;
  const q = await env.QE_DB.prepare("SELECT * FROM qe_portfolio_snapshot WHERE id=?1").bind(snapshotId).first();
  return q || null;
}
// Nearest qe_holding_history row on or before the decision date — never after (would
// inject future knowledge into "state at decision time").
async function _holdingStateAsOf(env, symbol, onOrBeforeDate) {
  const q = await env.QE_DB.prepare(
    "SELECT * FROM qe_holding_history WHERE symbol=?1 AND snapshot_date<=?2 ORDER BY snapshot_date DESC LIMIT 1"
  ).bind(symbol, onOrBeforeDate).first();
  return q || null;
}

async function replayDecision(env, decisionLogId) {
  if (decisionLogId == null || isNaN(Number(decisionLogId))) {
    return { ok: false, error: "A numeric decision_log_id is required." };
  }
  const decision = await _decisionLogById(env, Number(decisionLogId));
  if (!decision) return { ok: false, error: `No decision found for decision_log_id=${decisionLogId}` };

  const decisionDate = _istDateStr(decision.ts); // reused from Phase 1.0, unchanged

  // ── What QuantEdge decided, exactly as recorded — the decision_log row itself is
  // already immutable-at-write, so this is a direct read, not a reconstruction. ──
  let pillars = null; try { pillars = decision.pillars_json ? JSON.parse(decision.pillars_json) : null; } catch (_) {}
  let triggers = null; try { triggers = decision.triggers_json ? JSON.parse(decision.triggers_json) : null; } catch (_) {}
  const decisionAsRecorded = {
    decision_log_id: decision.id, run_id: decision.run_id, ts: decision.ts, symbol: decision.symbol,
    decision: decision.decision, verdict: decision.verdict, health_score: decision.health_score,
    recommendation_confidence: decision.recommendation_confidence, data_confidence: decision.data_confidence,
    conviction_trend: decision.conviction_trend, active_pillars: decision.active_pillars,
    strongest_pillar: decision.strongest_pillar, weakest_pillar: decision.weakest_pillar, pillars,
    triggers, evidence_summary: decision.evidence_summary, what_changed: decision.what_changed,
    why_changed: decision.why_changed, reversal_conditions: decision.reversal_conditions,
    recommended_action: decision.recommended_action, engine_version: decision.engine_version,
    ltp: decision.ltp, r_multiple: decision.r_multiple,
  };

  // ── Portfolio context AT THAT MOMENT — via the decision's own snapshot_id, never
  // today's latest snapshot. ──
  const snapshot = await _portfolioSnapshotById(env, decision.snapshot_id);
  const portfolioSnapshotAtDecision = snapshot ? {
    snapshot_date: snapshot.snapshot_date, total_invested: snapshot.total_invested, total_value: snapshot.total_value,
    total_pnl_pct: snapshot.total_pnl_pct, holdings_count: snapshot.holdings_count, top_name_pct: snapshot.top_name_pct,
    top3_pct: snapshot.top3_pct, sector_json: snapshot.sector_json, portfolio_health: snapshot.portfolio_health,
    cross_check_vs_decision_log: { // both frozen at decision time, from two independent columns — purely observational
      portfolio_health_matches: snapshot.portfolio_health === decision.portfolio_health,
      top3_pct_matches: snapshot.top3_pct === decision.portfolio_top3_pct,
    },
  } : HISTORICAL_DATA_NOT_AVAILABLE;
  if (!snapshot) { /* explicit: decision.snapshot_id was null, or the linked snapshot no longer resolves */ }

  // ── Holding state AT THAT MOMENT — qe_holding_history, never qe_holdings (current only). ──
  const histRow = await _holdingStateAsOf(env, decision.symbol, decisionDate);
  const holdingStateAtDecision = histRow ? {
    snapshot_date: histRow.snapshot_date, ltp: histRow.ltp, r_multiple: histRow.r_multiple,
    health_score: histRow.health_score, data_confidence: histRow.data_confidence,
    pillars: { trend: histRow.pillar_trend, momentum: histRow.pillar_momentum, edge: histRow.pillar_edge,
      risk: histRow.pillar_risk, fundnews: histRow.pillar_fundnews },
    verdict: histRow.verdict, is_exact_date_match: histRow.snapshot_date === decisionDate,
  } : HISTORICAL_DATA_NOT_AVAILABLE;

  // ── Market regime — reconstructed via Phase 1.0's exact method, clearly labeled. ──
  const regimeReconstructed = await _regimeAsOf(env, decisionDate); // reused unchanged from Phase 1.0
  const marketRegime = {
    reconstructed_regime: regimeReconstructed != null ? regimeReconstructed : HISTORICAL_DATA_NOT_AVAILABLE,
    method: "Deterministic recomputation via NIFTYBEES closes up to the decision date, using the unchanged computePipelineRegime() — same method as Phase 1.0's Decision Outcome Resolver.",
    caveat: "This is a RECONSTRUCTION, not the literal live regime value the pipeline read at that exact moment — qe_decision_log has no regime column, so the actual live value was never persisted and cannot be recovered. This reconstruction depends only on data that existed by the decision date, so it does not inject future knowledge.",
  };

  // ── Engines with NO historical persistence — honestly reported, never live-computed here. ──
  const NOT_AVAILABLE_REASON = "This engine computes live/on-demand only and has never persisted a historical snapshot at any past moment — an infrastructure limitation of the current architecture, not a gap specific to this decision. Replay never calls this engine's live function to backfill this field, since that would apply today's logic to yesterday's decision.";
  const capitalContext = { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: NOT_AVAILABLE_REASON, engine: "Portfolio Capital Intelligence Foundation (Phase 2.5)" };
  const optimizationContext = { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: NOT_AVAILABLE_REASON, engine: "Portfolio Optimization Framework (3.5) / Portfolio Optimizer (4.0)" };
  const capitalRotationContext = { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: NOT_AVAILABLE_REASON, engine: "Capital Rotation Engine (Phase 3.0)" };
  const profitProtectionContext = { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: NOT_AVAILABLE_REASON, engine: "Profit Protection Engine (Phase 2.0)" };
  const calibrationContext = { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: NOT_AVAILABLE_REASON, engine: "Decision Calibration Engine (Phase 1.2)" };
  const decisionQualityContext = { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: NOT_AVAILABLE_REASON, engine: "Decision Quality Analytics (Phase 1.1)" };

  // ── Genuinely historical, dated evidence — reused directly, not recomputed. ──
  let outcomeRows = []; try {
    const q = await env.QE_DB.prepare("SELECT * FROM qe_decision_outcomes WHERE decision_log_id=?1").bind(decision.id).all();
    outcomeRows = (q && q.results) || [];
  } catch (_) {}
  let tradeRows = []; try { tradeRows = await _realizedPerformance(env, decision.symbol); } catch (_) {} // reused from Portfolio Memory Engine, unchanged
  const relatedTradeRecord = tradeRows.filter(t => t.buy_date && t.buy_date <= decisionDate).slice(-1)[0] || HISTORICAL_DATA_NOT_AVAILABLE;

  return {
    ok: true, engine_version: DECISION_REPLAY_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    replay_note: "This is a reconstruction of what QuantEdge knew and decided AT THE DECISION TIMESTAMP — not current state, not current algorithms. 'subsequent_resolved_outcome' below is explicitly hindsight evidence (known only after the fact) and is kept separate from the decision reconstruction above it.",
    decision_as_recorded: decisionAsRecorded,
    portfolio_snapshot_at_decision: portfolioSnapshotAtDecision,
    holding_state_at_decision: holdingStateAtDecision,
    market_regime: marketRegime,
    capital_context: capitalContext,
    optimization_context: optimizationContext,
    capital_rotation_context: capitalRotationContext,
    profit_protection_context: profitProtectionContext,
    calibration_context: calibrationContext,
    decision_quality_context: decisionQualityContext,
    related_trade_record: relatedTradeRecord,
    // ── Explicitly separated: this is HINDSIGHT evidence, known only after the decision, never part of "what was known at decision time" above. ──
    subsequent_resolved_outcome: {
      note: "KNOWN ONLY IN HINDSIGHT — was NOT available at decision time. Kept separate to preserve the historical-truth boundary.",
      outcomes: outcomeRows.length ? outcomeRows : HISTORICAL_DATA_NOT_AVAILABLE,
    },
  };
}

async function handleDecisionReplay(url, env) {
  const decisionLogId = url.searchParams.get("decision_log_id");
  try { return cors(await replayDecision(env, decisionLogId)); }
  catch (e) { return corsErr(e.message || "Decision replay failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION NARRATIVE ENGINE — Phase 6.0 (v4.85)
//
// PURPOSE: answer exactly 5 mandated questions about a historical decision, using
// ONLY evidence that already exists — no scoring, no re-decision, no free-form
// generation. Every answer is a deterministic template filled with fields already
// present on the Decision Replay Engine's output.
//
// NOT AN LLM / NOT FREE-FORM TEXT GENERATION: every sentence below is built from a
// fixed template string with values substituted from already-recorded fields. The
// same decision_log_id always produces byte-identical narrative text (validated
// before delivery, see changelog) — the only thing that varies between calls is the
// generated_ts metadata field, never the narrative content itself.
//
// STRICT LAYERING: consumes replayDecision() (Phase 5.0) and computePortfolioMemory()
// (Portfolio Memory Engine) via direct calls — never re-queries qe_decision_log,
// qe_portfolio_snapshot, or qe_holding_history directly, and never re-runs
// mapVerdictToDecision/computeRecommendationConfidence or any other decision logic.
// Reuses _deriveThesisEvents() and DECISION_RANK (Portfolio Memory Engine) and
// PIE_CONFIG.bands (read-only reference, never re-executed) — zero new judgment
// thresholds introduced.
//
// Q2/Q3 ("why not stronger/weaker") are answered by CITING already-recorded fields
// (weakest_pillar, strongest_pillar, conviction_trend, health_score vs the existing
// band table, reversal_conditions) — never by re-running the Decision Engine to see
// what it would have said. If the decision was already at the strongest or weakest
// possible tier, that's reported as a structural fact, not "missing evidence."
// ═══════════════════════════════════════════════════════════════════════════════

const DECISION_NARRATIVE_ENGINE_VERSION = "narrative-v1.0";

function _bandOf(healthScore) {
  if (healthScore == null) return null;
  const b = PIE_CONFIG.bands; // read-only reference, reused, never re-executed as decision logic
  if (healthScore >= b.strongHold) return "strongHold";
  if (healthScore >= b.hold) return "hold";
  if (healthScore >= b.watch) return "watch";
  if (healthScore >= b.reduce) return "reduce";
  return "below_reduce";
}

function _buildNarrative(replay, previousJourneyEntry) {
  const d = replay.decision_as_recorded;
  const questions = {};

  // ── Q1: Why was this decision made? ──
  if (d.evidence_summary || d.health_score != null) {
    questions.why_this_decision = {
      answer: `Recorded decision: ${d.decision} (verdict: ${d.verdict}). Health score at decision time was ${d.health_score != null ? d.health_score : "not recorded"}, with recommendation confidence ${d.recommendation_confidence != null ? d.recommendation_confidence : "not recorded"}. Strongest contributing pillar recorded as '${d.strongest_pillar || "not recorded"}'.` +
        (d.evidence_summary ? ` Recorded evidence summary: "${d.evidence_summary}"` : ""),
      evidence_used: ["decision", "verdict", "health_score", "recommendation_confidence", "strongest_pillar", "evidence_summary"],
    };
  } else {
    questions.why_this_decision = { answer: HISTORICAL_DATA_NOT_AVAILABLE, evidence_used: [] };
  }

  // ── Q2: Why was a stronger decision not issued? ──
  const rank = DECISION_RANK[d.decision]; // reused from Portfolio Memory Engine
  if (rank === 6) { // STRONG_BUY — already the strongest tier
    questions.why_not_stronger = { answer: "Not applicable — the recorded decision (STRONG_BUY) was already the strongest available tier.", evidence_used: ["decision"] };
  } else if (d.weakest_pillar != null && d.conviction_trend != null) {
    questions.why_not_stronger = {
      answer: `A stronger decision was not issued. Recorded weakest pillar was '${d.weakest_pillar}' — the evidence model requires uniformly strong active pillars for a higher tier, and this was not the case. Recorded conviction trend was '${d.conviction_trend}'` +
        (d.conviction_trend !== "IMPROVING" ? ", not IMPROVING — stronger tiers are associated with strengthening conviction in the evidence model." : ", which was IMPROVING; other recorded evidence nonetheless did not support a stronger tier at this time."),
      evidence_used: ["decision", "weakest_pillar", "conviction_trend"],
    };
  } else {
    questions.why_not_stronger = { answer: HISTORICAL_DATA_NOT_AVAILABLE, evidence_used: [] };
  }

  // ── Q3: Why was a weaker decision not issued? ──
  if (rank === 0) { // EXIT_IMMEDIATELY — already the weakest tier
    questions.why_not_weaker = { answer: "Not applicable — the recorded decision (EXIT_IMMEDIATELY) was already the weakest available tier.", evidence_used: ["decision"] };
  } else if (d.health_score != null) {
    const band = _bandOf(d.health_score);
    questions.why_not_weaker = {
      answer: `A weaker decision was not issued. Recorded health score was ${d.health_score}, placing it in the '${band}' band (PIE_CONFIG.bands, referenced not re-executed) rather than a lower band that would be associated with a weaker tier.` +
        (d.reversal_conditions ? ` Recorded reversal conditions had not been triggered at this time: "${d.reversal_conditions}"` : " No reversal conditions were recorded for this decision."),
      evidence_used: ["decision", "health_score", "reversal_conditions"],
    };
  } else {
    questions.why_not_weaker = { answer: HISTORICAL_DATA_NOT_AVAILABLE, evidence_used: [] };
  }

  // ── Q4: What changed since the previous decision? ──
  if (!previousJourneyEntry) {
    questions.what_changed = { answer: "Not applicable — this is the earliest recorded decision for this symbol; no prior decision exists for comparison.", evidence_used: [] };
  } else {
    const events = _deriveThesisEvents([previousJourneyEntry, { ts: d.ts, decision: d.decision, conviction_trend: d.conviction_trend }]); // reused from Portfolio Memory Engine, unchanged
    const healthDelta = (d.health_score != null && previousJourneyEntry.health_score != null) ? Math.round((d.health_score - previousJourneyEntry.health_score) * 100) / 100 : null;
    const confDelta = (d.recommendation_confidence != null && previousJourneyEntry.recommendation_confidence != null) ? Math.round((d.recommendation_confidence - previousJourneyEntry.recommendation_confidence) * 100) / 100 : null;
    const parts = [];
    if (previousJourneyEntry.decision !== d.decision) parts.push(`Decision changed from ${previousJourneyEntry.decision} to ${d.decision}.`);
    if (previousJourneyEntry.conviction_trend !== d.conviction_trend) parts.push(`Conviction trend changed from ${previousJourneyEntry.conviction_trend} to ${d.conviction_trend}.`);
    if (healthDelta != null) parts.push(`Health score changed by ${healthDelta >= 0 ? "+" : ""}${healthDelta} (${previousJourneyEntry.health_score} → ${d.health_score}).`);
    if (confDelta != null) parts.push(`Recommendation confidence changed by ${confDelta >= 0 ? "+" : ""}${confDelta} (${previousJourneyEntry.recommendation_confidence} → ${d.recommendation_confidence}).`);
    if (d.why_changed) parts.push(`Recorded reason: "${d.why_changed}"`);
    questions.what_changed = {
      answer: parts.length ? parts.join(" ") : "No factual differences were recorded between the previous and current decision on the tracked fields.",
      evidence_used: ["decision_journey (previous vs current)", "why_changed"], derived_events: events,
    };
  }

  // ── Q5: What would have to change before this recommendation changes? ──
  if (d.reversal_conditions) {
    questions.what_would_change = { answer: `Recorded reversal conditions: "${d.reversal_conditions}"`, evidence_used: ["reversal_conditions"] };
  } else {
    questions.what_would_change = { answer: HISTORICAL_DATA_NOT_AVAILABLE, evidence_used: [] };
  }

  return questions;
}

async function generateDecisionNarrative(env, decisionLogId, ctx) {
  const replay = await replayDecision(env, decisionLogId); // Phase 5.0, reused unchanged
  if (!replay.ok) return { ok: false, error: "Replay unavailable: " + (replay.error || "unknown") };

  const symbol = replay.decision_as_recorded.symbol;
  const memory = await computePortfolioMemory(env, symbol, ctx); // Portfolio Memory Engine, reused unchanged
  let previousJourneyEntry = null;
  if (memory.ok && memory.records && memory.records[0]) {
    const journey = memory.records[0].decision_journey || [];
    const before = journey.filter(j => j.ts < replay.decision_as_recorded.ts);
    previousJourneyEntry = before.length ? before[before.length - 1] : null;
  }

  const questions = _buildNarrative(replay, previousJourneyEntry);

  return {
    ok: true, engine_version: DECISION_NARRATIVE_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    decision_log_id: replay.decision_as_recorded.decision_log_id, symbol, decision_ts: replay.decision_as_recorded.ts,
    questions,
  };
}

async function handleDecisionNarrative(url, env) {
  const decisionLogId = url.searchParams.get("decision_log_id");
  try { return cors(await generateDecisionNarrative(env, decisionLogId, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Decision narrative generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DECISION EVOLUTION ANALYTICS — Phase 7.0 (v4.86)
//
// PURE STATISTICS LAYER over already-assembled evidence — every metric is a count,
// ratio, or simple delta computed from computePortfolioMemory()'s existing output
// (which itself already composes qe_decision_log, qe_holding_history, and
// _deriveThesisEvents). No new reconstruction, no new judgment/scoring system: a
// stability ratio, a frequency rate, and a volatility rate are descriptive
// statistics (counts divided by counts, deltas over time), not a new decision model.
//
// STRICT REUSE: consumes computePortfolioMemory() (Portfolio Memory Engine) via a
// direct call — journey, health_evolution, and thesis_events (already computed by
// the reused _deriveThesisEvents) are taken as-is, never recomputed. The only new
// query is a minimal (id, ts) lookup to attach decision_log_id references onto the
// timeline, purely so a caller can cross-reference into Decision Replay/Narrative
// for any specific point — a different, narrower purpose than Portfolio Memory's
// richer journey fetch, not a duplication of it.
//
// Every metric that needs at least 2 data points to be meaningful (frequencies,
// stability, volatility, health/conviction trend) explicitly reports
// HISTORICAL_DATA_NOT_AVAILABLE when fewer than 2 decisions/health rows exist,
// rather than computing a degenerate or misleading number.
// ═══════════════════════════════════════════════════════════════════════════════

const DECISION_EVOLUTION_ENGINE_VERSION = "evolution-v1.0";

async function _decisionIdsBySymbol(env, symbol) {
  const q = await env.QE_DB.prepare("SELECT id, ts FROM qe_decision_log WHERE symbol=?1 ORDER BY ts ASC").bind(symbol).all();
  const map = {}; for (const r of (q && q.results) || []) map[r.ts] = r.id;
  return map;
}

function _evolveOneSymbol(symbol, memoryRecord, idMap) {
  const journey = memoryRecord.decision_journey;
  const thesisEvents = memoryRecord.thesis_events;
  const n = journey.length;
  const timeline = journey.map(j => ({ decision_log_id: idMap[j.ts] || null, ts: j.ts, decision: j.decision, verdict: j.verdict,
    health_score: j.health_score, conviction_trend: j.conviction_trend, recommendation_confidence: j.recommendation_confidence }));

  const strengthening = thesisEvents.filter(e => e.event_type === "STRENGTHENING");
  const weakening = thesisEvents.filter(e => e.event_type === "WEAKENING");

  if (n < 2) {
    return {
      symbol, decision_progression_timeline: timeline,
      upgrade_frequency: HISTORICAL_DATA_NOT_AVAILABLE, downgrade_frequency: HISTORICAL_DATA_NOT_AVAILABLE,
      decision_stability_ratio: HISTORICAL_DATA_NOT_AVAILABLE, health_trend: HISTORICAL_DATA_NOT_AVAILABLE,
      conviction_distribution: HISTORICAL_DATA_NOT_AVAILABLE, decision_volatility: HISTORICAL_DATA_NOT_AVAILABLE,
      recommendation_persistence_days: HISTORICAL_DATA_NOT_AVAILABLE,
      thesis_strengthening_events: strengthening, thesis_weakening_events: weakening,
      reason: "INSUFFICIENT_HISTORICAL_EVIDENCE: fewer than 2 recorded decisions for this symbol — evolution metrics require at least 2 points.",
    };
  }

  const decisionEvents = thesisEvents.filter(e => e.trigger === "decision");
  const upgrades = decisionEvents.filter(e => e.event_type === "STRENGTHENING").length;
  const downgrades = decisionEvents.filter(e => e.event_type === "WEAKENING").length;
  const totalTransitions = n - 1;
  const unchanged = totalTransitions - (upgrades + downgrades); // pairs with no decision-rank-changing event — always >=0, since decisionEvents is a subset of the same totalTransitions pairs
  const decisionStabilityRatio = Math.round((unchanged / totalTransitions) * 10000) / 10000;

  const spanDays = (new Date(journey[n - 1].ts) - new Date(journey[0].ts)) / 86400000;
  const decisionVolatility = spanDays > 0 ? Math.round(((upgrades + downgrades) / spanDays) * 10000) / 10000 : null;

  const HEALTH_TREND_LOW_CONFIDENCE_THRESHOLD = 60; // ENGINEERING_ASSUMPTION — see comment above _evolveOneSymbol's health_trend block
  let healthTrend = HISTORICAL_DATA_NOT_AVAILABLE;
  const healthEvo = memoryRecord.health_evolution;
  if (healthEvo && healthEvo.length >= 2) {
    const firstRow = healthEvo[0], lastRow = healthEvo[healthEvo.length - 1];
    const first = firstRow.health_score, last = lastRow.health_score;
    const delta = (first != null && last != null) ? Math.round((last - first) * 100) / 100 : null;
    const firstConfidence = firstRow.data_confidence != null ? firstRow.data_confidence : null;
    const lowConfidenceStart = firstConfidence != null && firstConfidence < HEALTH_TREND_LOW_CONFIDENCE_THRESHOLD;
    healthTrend = { first_value: first, first_date: firstRow.snapshot_date, last_value: last, last_date: lastRow.snapshot_date,
      delta, direction: delta == null ? null : (delta > 0 ? "IMPROVING" : delta < 0 ? "DECLINING" : "FLAT"),
      period: { from: firstRow.snapshot_date, to: lastRow.snapshot_date },
      first_data_confidence: firstConfidence,
      period_label: lowConfidenceStart ? "EARLY_EVIDENCE" : null,
      period_note: lowConfidenceStart
        ? `First recorded score (${firstRow.snapshot_date}) had data_confidence=${firstConfidence} — technicals were still building for this newly-tracked holding, not a genuine deterioration. The delta below is measured from this baseline as-recorded; nothing has been excluded or altered.`
        : null,
      source: "qe_holding_history (daily granularity, reused from Portfolio Memory Engine)" };
  }

  const convCounts = { IMPROVING: 0, STABLE: 0, WEAKENING: 0, unrecorded: 0 };
  for (const j of journey) { const c = j.conviction_trend; if (c && convCounts[c] != null) convCounts[c]++; else convCounts.unrecorded++; }
  const convictionDistribution = { counts: convCounts, latest: journey[n - 1].conviction_trend || null, sample_size: n };

  let lastChangeIdx = 0;
  for (let i = 1; i < n; i++) { if (journey[i].decision !== journey[i - 1].decision) lastChangeIdx = i; }
  const persistenceDays = Math.round(((new Date(journey[n - 1].ts) - new Date(journey[lastChangeIdx].ts)) / 86400000) * 100) / 100;

  return {
    symbol, decision_progression_timeline: timeline,
    upgrade_frequency: { count: upgrades, rate_per_transition: Math.round((upgrades / totalTransitions) * 10000) / 10000 },
    downgrade_frequency: { count: downgrades, rate_per_transition: Math.round((downgrades / totalTransitions) * 10000) / 10000 },
    decision_stability_ratio: decisionStabilityRatio,
    decision_stability_method: "unchanged_transitions / total_transitions — a transition is any consecutive pair of recorded decisions for this symbol",
    health_trend: healthTrend,
    conviction_distribution: convictionDistribution,
    decision_volatility: decisionVolatility != null ? { value: decisionVolatility, method: "rank-changing transitions per day, over the full recorded span" } : HISTORICAL_DATA_NOT_AVAILABLE,
    recommendation_persistence_days: persistenceDays,
    recommendation_persistence_method: "days since the most recent recorded decision change (or full recorded span if the decision has never changed)",
    thesis_strengthening_events: strengthening, thesis_weakening_events: weakening,
  };
}

async function computeDecisionEvolution(env, symbolFilter, ctx) {
  const memory = symbolFilter ? await computePortfolioMemory(env, symbolFilter, ctx) : await ctx.portfolioMemoryAll();
  if (!memory.ok) return { ok: false, error: "Portfolio Memory unavailable: " + (memory.error || "unknown") };
  if (!memory.records || !memory.records.length) {
    return { ok: true, engine_version: DECISION_EVOLUTION_ENGINE_VERSION, generated_ts: new Date().toISOString(), symbols_covered: 0, records: [] };
  }

  const records = [];
  for (const rec of memory.records) {
    let idMap = {}; try { idMap = await _decisionIdsBySymbol(env, rec.symbol); } catch (_) {}
    records.push(_evolveOneSymbol(rec.symbol, rec, idMap));
  }

  return { ok: true, engine_version: DECISION_EVOLUTION_ENGINE_VERSION, generated_ts: new Date().toISOString(), symbols_covered: records.length, records };
}

async function handleDecisionEvolution(url, env) {
  const symbol = url.searchParams.get("symbol");
  try { return cors(await computeDecisionEvolution(env, symbol, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Decision evolution analytics failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO STORY ENGINE — Phase 7.0 (v4.87)
//
// PURE AGGREGATION over already-computed evidence — every field is either (a) a
// direct citation of an already-computed value, (b) a simple count/tally across
// symbols, or (c) a min/max/average over an already-computed per-symbol metric
// (Decision Evolution Analytics' health_trend.delta, decision_stability_ratio,
// upgrade/downgrade counts). Zero new scoring system: "strongest/weakest thesis"
// ranks by the single existing health_score field, not a new composite score;
// "biggest improvement/deterioration" is a plain argmax/argmin over an already-
// computed delta, not a new judgment.
//
// STRICT REUSE, ZERO DUPLICATION: calls computeDecisionEvolution() (all symbols),
// computePortfolioMemory() (all symbols), computePortfolioCapitalFoundation(),
// evaluateOptimizationConstraints(), computeProfitProtectionRecommendations(), and
// computePortfolioOptimization() directly — never re-implements any of their logic.
// The only new query is a minimal qe_portfolio_snapshot time-series read (date,
// portfolio_health, top3_pct across all dates) for the portfolio-level health
// trend — a different shape than any existing query (all existing snapshot reads
// fetch a single row, by id or "latest"; this is the only place that reads the
// full series).
//
// PER-FIELD INSUFFICIENT-EVIDENCE HANDLING: current-snapshot facts (concentration,
// diversification, capital, risk, strongest/weakest thesis) are reported whenever
// available, even with thin history — gating the ENTIRE story behind
// INSUFFICIENT_PORTFOLIO_HISTORY would suppress genuinely available facts.
// Trend-dependent fields (health trend, conviction trend, stability trend,
// momentum) require >=2 data points and report INSUFFICIENT_PORTFOLIO_HISTORY
// per-field when they don't have it, with the actual symbol count disclosed so
// coverage is transparent, not silently partial.
// ═══════════════════════════════════════════════════════════════════════════════

const PORTFOLIO_STORY_ENGINE_VERSION = "story-v1.0";
const INSUFFICIENT_PORTFOLIO_HISTORY = "INSUFFICIENT_PORTFOLIO_HISTORY";

async function _portfolioHealthSeries(env) {
  const q = await env.QE_DB.prepare("SELECT snapshot_date, portfolio_health, top3_pct, total_invested, total_value, total_pnl_pct, holdings_count FROM qe_portfolio_snapshot ORDER BY snapshot_date ASC").all();
  return (q && q.results) || [];
}

async function computePortfolioStory(env, ctx) {
  const evolution = await ctx.decisionEvolutionAll(); // reused unchanged, all symbols
  if (!evolution.ok) return { ok: false, error: "Decision Evolution Analytics unavailable: " + (evolution.error || "unknown") };
  const memory = await ctx.portfolioMemoryAll(); // reused unchanged, all symbols
  if (!memory.ok) return { ok: false, error: "Portfolio Memory unavailable: " + (memory.error || "unknown") };
  const foundation = await ctx.capitalFoundation();
  const constraints = await ctx.optimizationConstraints();
  const ppResults = await ctx.profitProtection();
  const optimizerResults = await ctx.portfolioOptimization();

  if (!memory.records.length) {
    return { ok: true, engine_version: PORTFOLIO_STORY_ENGINE_VERSION, generated_ts: new Date().toISOString(), status: INSUFFICIENT_PORTFOLIO_HISTORY, reason: "No decision history recorded for any symbol yet." };
  }

  const memBySymbol = {}; for (const r of memory.records) memBySymbol[r.symbol] = r;
  const evolvedRecords = (evolution.records || []).filter(r => r.reason !== "INSUFFICIENT_HISTORICAL_EVIDENCE");

  // ── Portfolio Health trend — new query (time series), distinct purpose from any existing single-row snapshot read ──
  let series = []; try { series = await _portfolioHealthSeries(env); } catch (_) {}
  let portfolioHealthTrend = INSUFFICIENT_PORTFOLIO_HISTORY;
  if (series.length >= 2) {
    const first = series[0], last = series[series.length - 1];
    const delta = (first.portfolio_health != null && last.portfolio_health != null) ? Math.round((last.portfolio_health - first.portfolio_health) * 100) / 100 : null;
    portfolioHealthTrend = { first_value: first.portfolio_health, first_date: first.snapshot_date, last_value: last.portfolio_health, last_date: last.snapshot_date,
      delta, direction: delta == null ? null : (delta > 0 ? "IMPROVING" : delta < 0 ? "DECLINING" : "FLAT"), data_points: series.length, source: "qe_portfolio_snapshot (full time series)" };
  } else {
    portfolioHealthTrend = { status: INSUFFICIENT_PORTFOLIO_HISTORY, data_points: series.length };
  }

  // ── Overall conviction trend — tally of current conviction_trend across symbols (Portfolio Memory, reused) ──
  const convTally = { IMPROVING: 0, STABLE: 0, WEAKENING: 0, unrecorded: 0 };
  for (const r of memory.records) { const c = r.current_thesis_snapshot && r.current_thesis_snapshot.conviction_trend; if (c && convTally[c] != null) convTally[c]++; else convTally.unrecorded++; }

  // ── Portfolio quality trend — tally of current decision tiers (a distinct, non-duplicate proxy from health trend) ──
  const tierTally = {};
  for (const r of memory.records) { const dec = (r.current_thesis_snapshot && r.current_thesis_snapshot.decision) || "unrecorded"; tierTally[dec] = (tierTally[dec] || 0) + 1; }

  // ── Decision stability trend / Portfolio consistency — average and range of already-computed per-symbol ratios ──
  const stabilityValues = evolvedRecords.filter(r => typeof r.decision_stability_ratio === "number").map(r => r.decision_stability_ratio);
  const avgStability = stabilityValues.length ? Math.round((stabilityValues.reduce((a, b) => a + b, 0) / stabilityValues.length) * 10000) / 10000 : null;
  const stabilityRange = stabilityValues.length ? { min: Math.min(...stabilityValues), max: Math.max(...stabilityValues) } : null;
  const decisionStabilityTrend = stabilityValues.length ? { average: avgStability, range: stabilityRange, symbols_covered: stabilityValues.length, symbols_total: memory.records.length } : { status: INSUFFICIENT_PORTFOLIO_HISTORY };

  // ── Biggest improvement / Biggest deterioration — argmax/argmin over already-computed health_trend.delta ──
  const withDelta = evolvedRecords.filter(r => r.health_trend && r.health_trend !== HISTORICAL_DATA_NOT_AVAILABLE && r.health_trend.delta != null);
  const bestMover  = withDelta.length ? withDelta.reduce((a, b) => b.health_trend.delta > a.health_trend.delta ? b : a) : null;
  const worstMover = withDelta.length ? withDelta.reduce((a, b) => b.health_trend.delta < a.health_trend.delta ? b : a) : null;
  // Trust Audit fix (13-Jul-2026): a delta of 0 (or positive) is not a deterioration, and a
  // delta of 0 (or negative) is not an improvement — populating these unconditionally from
  // reduce()'s min/max let the UI display "Health 0" for a holding that hadn't actually
  // gotten worse, reading exactly like an absolute health score of zero. Now gated on the
  // delta's actual sign; NO_MATERIAL_DETERIORATION/NO_MATERIAL_IMPROVEMENT is returned when
  // delta data exists but nothing crossed zero, distinct from INSUFFICIENT_PORTFOLIO_HISTORY
  // (no delta data exists at all).
  // Trust refinement (13-Jul-2026): expose sample-size context rather than hide it. With few
  // holdings, a symbol can "win" a comparative card by being the only eligible candidate (e.g.
  // only 2 of N holdings have >=2 recorded decisions) — the user should see that, not infer it.
  const dcTie = (mover) => withDelta.filter(r => r.symbol !== mover.symbol && r.health_trend.delta === mover.health_trend.delta).map(r => r.symbol);
  const deltaContext = (mover) => withDelta.length === 1 ? "ONLY_QUALIFYING_HOLDING"
    : dcTie(mover).length ? `TIED_WITH_${dcTie(mover).join("_")}` : `ONE_OF_${withDelta.length}_QUALIFYING_HOLDINGS`;
  const biggestImprovement = !bestMover ? INSUFFICIENT_PORTFOLIO_HISTORY
    : bestMover.health_trend.delta > 0 ? { symbol: bestMover.symbol, current_health_score: bestMover.health_trend.last_value, health_delta: bestMover.health_trend.delta,
        period: bestMover.health_trend.period, period_label: bestMover.health_trend.period_label, period_note: bestMover.health_trend.period_note, context: deltaContext(bestMover) }
    : { status: "NO_MATERIAL_IMPROVEMENT" };
  const biggestDeterioration = !worstMover ? INSUFFICIENT_PORTFOLIO_HISTORY
    : worstMover.health_trend.delta < 0 ? { symbol: worstMover.symbol, current_health_score: worstMover.health_trend.last_value, health_delta: worstMover.health_trend.delta,
        period: worstMover.health_trend.period, period_label: worstMover.health_trend.period_label, period_note: worstMover.health_trend.period_note, context: deltaContext(worstMover) }
    : { status: "NO_MATERIAL_DETERIORATION" };

  // ── Strongest / Weakest investment thesis — ranked by the single existing health_score field, not a new composite ──
  const withHealth = memory.records.filter(r => r.current_thesis_snapshot && r.current_thesis_snapshot.health_score != null);
  const strongestThesis = withHealth.length ? withHealth.reduce((a, b) => b.current_thesis_snapshot.health_score > a.current_thesis_snapshot.health_score ? b : a) : null;
  const weakestThesis = withHealth.length ? withHealth.reduce((a, b) => b.current_thesis_snapshot.health_score < a.current_thesis_snapshot.health_score ? b : a) : null;
  const thTie = (thesis) => withHealth.filter(r => r.symbol !== thesis.symbol && r.current_thesis_snapshot.health_score === thesis.current_thesis_snapshot.health_score).map(r => r.symbol);
  const thesisContext = (thesis) => withHealth.length === 1 ? "ONLY_HOLDING_WITH_RECORDED_HEALTH"
    : thTie(thesis).length ? `TIED_WITH_${thTie(thesis).join("_")}` : `ONE_OF_${withHealth.length}_HOLDINGS`;



  // ── Concentration / Diversification observations — direct reuse of Phase 3.5's constraint evaluation ──
  const concC = constraints.ok ? constraints.constraints.find(c => c.constraint === "max_portfolio_concentration_pct") : null;
  const sectorC = constraints.ok ? constraints.constraints.find(c => c.constraint === "max_sector_exposure_pct") : null;

  // ── Capital deployment observations — direct reuse of Phase 2.5's capital + Phase 4.0's cash recommendation ──
  const cashInfo = (foundation.ok && foundation.capital.ok) ? { available_cash: foundation.capital.available_cash, deployable_capital: foundation.capital.remaining_buying_capacity, status: "LIVE" } : { status: "CAPITAL_INFORMATION_UNAVAILABLE", reason: foundation.ok ? foundation.capital.reason : foundation.error };
  const optimizerCashRec = optimizerResults.ok ? optimizerResults.recommendations.find(r => r.type === "DEPLOY_CASH" || r.type === "HOLD_CASH") : null;

  // ── Risk observations — direct reuse of Phase 2.0's tally ──
  const ppTally = {}; if (ppResults.ok) for (const r of ppResults.recommendations) ppTally[r.recommendation] = (ppTally[r.recommendation] || 0) + 1;

  // ── Portfolio momentum — net(upgrades - downgrades), summed over already-computed per-symbol counts ──
  let totalUpgrades = 0, totalDowngrades = 0;
  for (const r of evolvedRecords) {
    if (r.upgrade_frequency && typeof r.upgrade_frequency.count === "number") totalUpgrades += r.upgrade_frequency.count;
    if (r.downgrade_frequency && typeof r.downgrade_frequency.count === "number") totalDowngrades += r.downgrade_frequency.count;
  }
  const portfolioMomentum = evolvedRecords.length ? { total_upgrades: totalUpgrades, total_downgrades: totalDowngrades, net: totalUpgrades - totalDowngrades, symbols_covered: evolvedRecords.length } : { status: INSUFFICIENT_PORTFOLIO_HISTORY };

  // ── Overall portfolio direction — factual tally of health_trend directions, not a new invented "score" ──
  const dirTally = { IMPROVING: 0, DECLINING: 0, FLAT: 0, unavailable: 0 };
  for (const r of evolvedRecords) { const dir = (r.health_trend && r.health_trend !== HISTORICAL_DATA_NOT_AVAILABLE) ? r.health_trend.direction : null; if (dir && dirTally[dir] != null) dirTally[dir]++; else dirTally.unavailable++; }

  // ── Deterministic templated summary — every value substituted from the aggregates above, no free-form text ──
  const summaryParts = [];
  summaryParts.push(`Portfolio covers ${memory.records.length} symbol(s) with recorded decisions.`);
  if (portfolioHealthTrend.direction) summaryParts.push(`Portfolio health is ${portfolioHealthTrend.direction} (${portfolioHealthTrend.first_value}→${portfolioHealthTrend.last_value} since ${portfolioHealthTrend.first_date}).`);
  if (biggestImprovement.symbol) summaryParts.push(`Biggest improvement: ${biggestImprovement.symbol} (health +${biggestImprovement.health_delta}).`);
  if (biggestDeterioration.symbol) summaryParts.push(`Biggest deterioration: ${biggestDeterioration.symbol} (health ${biggestDeterioration.health_delta}).`);
  if (strongestThesis) summaryParts.push(`Strongest recorded thesis: ${strongestThesis.symbol} (health_score=${strongestThesis.current_thesis_snapshot.health_score}).`);
  if (weakestThesis) summaryParts.push(`Weakest recorded thesis: ${weakestThesis.symbol} (health_score=${weakestThesis.current_thesis_snapshot.health_score}).`);
  if (concC) summaryParts.push(`Concentration: top3_pct=${concC.current_value}% vs threshold ${concC.threshold}% (${concC.status}).`);
  if (avgStability != null) summaryParts.push(`Average decision stability across ${stabilityValues.length} symbol(s) with sufficient history: ${avgStability}.`);
  if (totalUpgrades || totalDowngrades) summaryParts.push(`Portfolio-wide: ${totalUpgrades} upgrade(s), ${totalDowngrades} downgrade(s) recorded (net ${totalUpgrades - totalDowngrades >= 0 ? "+" : ""}${totalUpgrades - totalDowngrades}).`);

  return {
    ok: true, engine_version: PORTFOLIO_STORY_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    story_summary: summaryParts.join(" "),
    portfolio_health_trend: portfolioHealthTrend,
    overall_conviction_trend: convTally,
    portfolio_quality_trend: { decision_tier_tally: tierTally },
    decision_stability_trend: decisionStabilityTrend,
    biggest_improvement: biggestImprovement,
    biggest_deterioration: biggestDeterioration,
    strongest_investment_thesis: strongestThesis ? { symbol: strongestThesis.symbol, snapshot: strongestThesis.current_thesis_snapshot, context: thesisContext(strongestThesis) } : INSUFFICIENT_PORTFOLIO_HISTORY,
    weakest_investment_thesis: weakestThesis ? { symbol: weakestThesis.symbol, snapshot: weakestThesis.current_thesis_snapshot, context: thesisContext(weakestThesis) } : INSUFFICIENT_PORTFOLIO_HISTORY,
    concentration_observations: concC || INSUFFICIENT_PORTFOLIO_HISTORY,
    diversification_observations: sectorC || INSUFFICIENT_PORTFOLIO_HISTORY,
    capital_deployment_observations: { capital: cashInfo, optimizer_recommendation: optimizerCashRec || null },
    risk_observations: { profit_protection_tally: ppTally },
    portfolio_momentum: portfolioMomentum,
    portfolio_consistency: stabilityRange ? { stability_range: stabilityRange, interpretation_note: "Range of decision_stability_ratio across covered symbols — a tighter range means symbols are behaving more uniformly; this is a descriptive range, not a new score." } : INSUFFICIENT_PORTFOLIO_HISTORY,
    overall_portfolio_direction: { health_direction_tally: dirTally },
  };
}

async function handlePortfolioStory(env) {
  try { return cors(await computePortfolioStory(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Portfolio story generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXECUTIVE PORTFOLIO BRIEFING — Phase 8.0 (v4.88)
//
// PRESENTATION LAYER ONLY — the thinnest module in this build. Portfolio Story
// (Phase 7.0) already computed nearly every required section; most of this
// engine's job is direct citation/reformatting of that output, not new
// computation. Sections Portfolio Story doesn't directly cover (highest
// conviction, opportunities requiring attention, watch tomorrow) are answered by
// filtering already-existing fields from already-existing engines — never a new
// ranking dimension, never a new judgment.
//
// WHY DECISION NARRATIVE / DECISION REPLAY ARE NOT DIRECTLY CALLED HERE: both are
// per-single-decision tools (explain one decision, reconstruct one point in time).
// A 60-second portfolio-level briefing needs portfolio-level aggregates, which
// Portfolio Story already assembled from Decision Evolution Analytics and
// Portfolio Memory. Listing an engine as an allowed input is a whitelist of what
// may be reused, not a mandate to invoke every one regardless of whether a section
// needs it — forcing unnecessary calls here would not add real evidence, only
// redundant computation.
//
// "Today's Highest Conviction" reuses the SAME pattern Portfolio Story already
// established for strongest/weakest thesis (filter by an existing categorical
// field, then argmax over the existing health_score field) — filtered here to
// conviction_trend="IMPROVING" specifically, since "conviction" is literally that
// recorded field. No new scoring dimension introduced.
// ═══════════════════════════════════════════════════════════════════════════════

const EXECUTIVE_BRIEFING_ENGINE_VERSION = "briefing-v1.0";
const INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE";

async function computeExecutiveBriefing(env, ctx) {
  const story = await ctx.portfolioStory(); // Phase 7.0, reused unchanged — the primary source for most sections
  if (!story.ok) return { ok: false, error: "Portfolio Story unavailable: " + (story.error || "unknown") };
  if (story.status === INSUFFICIENT_PORTFOLIO_HISTORY) {
    return { ok: true, engine_version: EXECUTIVE_BRIEFING_ENGINE_VERSION, generated_ts: new Date().toISOString(), status: INSUFFICIENT_PORTFOLIO_HISTORY, reason: story.reason };
  }

  const memory = await ctx.portfolioMemoryAll(); // Portfolio Memory Engine, reused unchanged
  const ppResults = await ctx.profitProtection(); // Phase 2.0, reused unchanged
  const rotResults = await ctx.capitalRotation(); // Phase 3.0, reused unchanged
  const optResults = await ctx.portfolioOptimization(); // Phase 4.0, reused unchanged

  const memBySymbol = {}; if (memory.ok) for (const r of memory.records) memBySymbol[r.symbol] = r;

  // ── Today's Highest Conviction — filter by existing conviction_trend, argmax over existing health_score ──
  const improving = (memory.ok ? memory.records : []).filter(r => r.current_thesis_snapshot && r.current_thesis_snapshot.conviction_trend === "IMPROVING" && r.current_thesis_snapshot.health_score != null);
  const highestConviction = improving.length ? improving.reduce((a, b) => b.current_thesis_snapshot.health_score > a.current_thesis_snapshot.health_score ? b : a) : null;
  // Trust refinement (13-Jul-2026): disclose whether this holding won by being the only
  // IMPROVING-conviction holding, rather than letting the user infer a real contest happened.
  const convictionContext = highestConviction
    ? (improving.length === 1 ? "ONLY_QUALIFYING_HOLDING" : `ONE_OF_${improving.length}_QUALIFYING_HOLDINGS`)
    : null;

  // ── Opportunities Requiring Attention — filter over 3 already-existing engines' outputs, no new judgment ──
  // Trust refinement (13-Jul-2026), requirement #4: thread through each source engine's own
  // already-computed one-line explanation instead of dropping it — the user should never have
  // to leave this card to understand what a signal means or what to do about it.
  const attention = [];
  if (ppResults.ok) for (const r of ppResults.recommendations) if (r.recommendation === "PARTIAL_PROFIT_PROTECTION" || r.recommendation === "FULL_PROFIT_PROTECTION") attention.push({ symbol: r.symbol, source: "Profit Protection", signal: r.recommendation, reason: r.explanation || null });
  if (rotResults.ok) for (const r of rotResults.recommendations) if (!["RETAIN_POSITION", "NO_ROTATION", "DEFERRED_TO_DECISION_ENGINE"].includes(r.recommendation)) attention.push({ symbol: r.symbol, source: "Capital Rotation", signal: r.recommendation, reason: r.explanation || null });
  if (optResults.ok) for (const r of optResults.recommendations) if (!["MAINTAIN_POSITION", "NO_OPTIMIZATION_REQUIRED"].includes(r.type)) {
    const attrSymbol = (r.type === "REDUCE_CONCENTRATION" && r.supporting_evidence && r.supporting_evidence.top_contributors && r.supporting_evidence.top_contributors.length)
      ? r.supporting_evidence.top_contributors[0].symbol : null;
    attention.push({ symbol: attrSymbol, source: "Portfolio Optimizer", signal: r.type, scope: r.scope, reason: r.explanation || null });
  }

  // ── Watch Tomorrow — reversal_conditions for each attention-flagged symbol, reused directly from Portfolio Memory's decision_journey ──
  const watchTomorrow = []; const seen = new Set();
  for (const a of attention) {
    if (!a.symbol || seen.has(a.symbol)) continue; seen.add(a.symbol);
    const journey = memBySymbol[a.symbol] ? memBySymbol[a.symbol].decision_journey : null;
    const latest = journey && journey.length ? journey[journey.length - 1] : null;
    watchTomorrow.push({ symbol: a.symbol, reversal_conditions: latest && latest.reversal_conditions ? latest.reversal_conditions : HISTORICAL_DATA_NOT_AVAILABLE });
  }

  return {
    ok: true, engine_version: EXECUTIVE_BRIEFING_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    data_freshness: await ctx.holdingsFreshness(),
    executive_summary: story.story_summary,
    portfolio_health: story.portfolio_health_trend,
    todays_highest_conviction: highestConviction ? { symbol: highestConviction.symbol, snapshot: highestConviction.current_thesis_snapshot, context: convictionContext,
      evidence: { recorded_decisions: highestConviction.decision_journey.length,
        history_days: Math.max(0, Math.round((new Date(highestConviction.decision_journey[highestConviction.decision_journey.length - 1].ts) - new Date(highestConviction.decision_journey[0].ts)) / 86400000)) } } : INSUFFICIENT_EVIDENCE,
    todays_weakest_holding: story.weakest_investment_thesis,
    biggest_improvement: story.biggest_improvement,
    biggest_deterioration: story.biggest_deterioration,
    capital_deployment: story.capital_deployment_observations,
    portfolio_risks: { profit_protection: story.risk_observations, capital_rotation_tally: rotResults.ok ? rotResults.by_recommendation : INSUFFICIENT_EVIDENCE },
    concentration_summary: story.concentration_observations,
    opportunities_requiring_attention: attention.length ? attention : INSUFFICIENT_EVIDENCE,
    watch_tomorrow: watchTomorrow.length ? watchTomorrow : INSUFFICIENT_EVIDENCE,
  };
}

async function handleExecutiveBriefing(env) {
  try { return cors(await computeExecutiveBriefing(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Executive briefing generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXECUTIVE COCKPIT — Phase 9.0 (v4.89)
//
// PRESENTATION LAYER, ZERO NEW QUERIES: 8 of 12 sections are direct citations of
// Executive Briefing's already-computed output (Phase 8.0). "Portfolio Story" and
// "Today's Actions" are direct citations of Portfolio Story's already-computed
// output (Phase 7.0) — "Today's Actions" specifically reuses Portfolio Story's
// existing decision_tier_tally rather than recomputing anything, since a decision
// tally IS the set of currently-recommended actions in this system's vocabulary.
// Only "System Status" is genuinely new content, and it is purely OPERATIONAL
// metadata (engine versions, resolver config, calibration coverage, data
// freshness) — not investment intelligence, so it doesn't compete with the
// "no new scoring" guardrail; it's citation of existing version constants and
// existing config/status functions, never a new judgment about the portfolio.
// ═══════════════════════════════════════════════════════════════════════════════

const EXECUTIVE_COCKPIT_ENGINE_VERSION = "cockpit-v1.0";

async function computeExecutiveCockpit(env, ctx) {
  const briefing = await ctx.executiveBriefing(); // Phase 8.0, reused unchanged
  if (!briefing.ok) return { ok: false, error: "Executive Briefing unavailable: " + (briefing.error || "unknown") };
  if (briefing.status === INSUFFICIENT_PORTFOLIO_HISTORY) {
    return { ok: true, engine_version: EXECUTIVE_COCKPIT_ENGINE_VERSION, generated_ts: new Date().toISOString(), status: INSUFFICIENT_PORTFOLIO_HISTORY, reason: briefing.reason };
  }

  const story = await ctx.portfolioStory(); // Phase 7.0, reused unchanged — SAME memoized computation ctx.executiveBriefing() already triggered; no second run
  const calibration = await ctx.calibrationRecommendations(); // Phase 1.2, reused unchanged
  const resolverCfg = await _outcomeResolverConfig(env); // Phase 1.0, reused unchanged

  const systemStatus = {
    engine_versions: {
      decision_outcome_resolver: OUTCOME_RESOLVER_VERSION, decision_evaluation_policy: DECISION_EVAL_POLICY_VERSION,
      decision_calibration: CALIBRATION_ENGINE_VERSION, profit_protection: PROFIT_PROTECTION_ENGINE_VERSION,
      capital_foundation: CAPITAL_FOUNDATION_VERSION, capital_rotation: CAPITAL_ROTATION_ENGINE_VERSION,
      optimization_framework: OPTIMIZATION_FRAMEWORK_VERSION, portfolio_optimizer: PORTFOLIO_OPTIMIZER_VERSION,
      portfolio_memory: PORTFOLIO_MEMORY_ENGINE_VERSION, decision_replay: DECISION_REPLAY_ENGINE_VERSION,
      decision_narrative: DECISION_NARRATIVE_ENGINE_VERSION, decision_evolution: DECISION_EVOLUTION_ENGINE_VERSION,
      portfolio_story: PORTFOLIO_STORY_ENGINE_VERSION, executive_briefing: EXECUTIVE_BRIEFING_ENGINE_VERSION,
      executive_cockpit: EXECUTIVE_COCKPIT_ENGINE_VERSION,
    },
    outcome_resolver: { enabled: resolverCfg.enabled, windows: resolverCfg.windows },
    calibration_coverage: calibration.ok ? { proposed_count: calibration.proposed_count, no_calibration_count: calibration.no_calibration_count } : INSUFFICIENT_EVIDENCE,
    data_freshness: await ctx.holdingsFreshness(),
  };

  return {
    ok: true, engine_version: EXECUTIVE_COCKPIT_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    executive_summary: briefing.executive_summary,
    portfolio_health: briefing.portfolio_health,
    portfolio_story: story.ok ? story : INSUFFICIENT_EVIDENCE,
    todays_actions: story.ok ? story.portfolio_quality_trend : INSUFFICIENT_EVIDENCE,
    highest_conviction: briefing.todays_highest_conviction,
    weakest_holding: briefing.todays_weakest_holding,
    capital_allocation: briefing.capital_deployment,
    portfolio_risks: briefing.portfolio_risks,
    concentration_status: briefing.concentration_summary,
    opportunities: briefing.opportunities_requiring_attention,
    watch_tomorrow: briefing.watch_tomorrow,
    system_status: systemStatus,
  };
}

async function handleExecutiveCockpit(env) {
  try { return cors(await computeExecutiveCockpit(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Executive cockpit generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXECUTIVE INTELLIGENCE ENGINE (EIE) — Phase 1, Daily Operating Model (18-Jul-2026)
//
// Orchestration layer. Answers exactly four questions by synthesizing ALREADY-
// COMPUTED engine outputs — zero new scoring, zero new decision logic, zero
// duplicated computation. Every fact here is read from an existing engine via the
// SAME memoized Portfolio Intelligence Context every other route already uses.
//
//   What changed today?     — ctx.decisionEvolutionAll()'s existing decision_
//                              progression_timeline, filtered to entries dated
//                              today (IST) that differ from the entry before them,
//                              or show a health delta of 5+ points. The timeline
//                              itself is pre-existing, unmodified Decision
//                              Evolution output — the only new logic is the date
//                              filter and threshold.
//   Why did it change?      — each item carries the SAME explanation/reason field
//                              the source engine already produced. Nothing here is
//                              paraphrased, summarized, or invented.
//   What requires action?   — a lookup-table classification over each engine's
//                              OWN existing recommendation-type string (Profit
//                              Protection, Capital Rotation, Portfolio Optimizer,
//                              GTT Validation). No new severity scoring — this is
//                              a mapping over labels those engines already emit.
//   What can be ignored?    — every classified item that isn't action/opportunity/
//                              informational, listed explicitly (never silently
//                              dropped) so "nothing to do" is a visible, checked
//                              state, not an absence of information.
//
// THRESHOLD CLASSIFICATION: the 5-point same-day health-delta filter for "changed
// today" is an ENGINEERING_ASSUMPTION, no prior QuantEdge precedent — chosen to
// surface genuinely material single-day moves without flooding the section with
// routine 1-2 point noise on every holding every day.
//
// Explicitly OUT OF SCOPE for this phase (Phase 4, Portfolio Fit, per roadmap):
// raw discovery candidates are NOT included here. Only DEPLOY_CASH is surfaced as
// a "new opportunity," because it already passed through Capital Rotation's
// not-already-held filter and the Optimizer's position-sizing logic — a bare
// discovery signal has not been evaluated against this portfolio at all yet.
// ═══════════════════════════════════════════════════════════════════════════════

const EXECUTIVE_INTELLIGENCE_ENGINE_VERSION = "eie-v1.0";
const EIE_HEALTH_CHANGE_THRESHOLD = 5; // ENGINEERING_ASSUMPTION — see doc above

const EIE_ACTION_CLASSIFICATION = {
  // Profit Protection
  FULL_PROFIT_PROTECTION: "ACTION_REQUIRED", PARTIAL_PROFIT_PROTECTION: "ACTION_REQUIRED",
  LET_PROFITS_RUN: "IGNORE", HOLD: "IGNORE",
  // Capital Rotation
  FULL_ROTATION: "ACTION_REQUIRED", PARTIAL_ROTATION: "ACTION_REQUIRED",
  MONITOR_ROTATION: "INFORMATIONAL", RETAIN_POSITION: "IGNORE", NO_ROTATION: "IGNORE", DEFERRED_TO_DECISION_ENGINE: "IGNORE",
  // Portfolio Optimizer
  REDUCE_CONCENTRATION: "ACTION_REQUIRED", DEPLOY_CASH: "OPPORTUNITY", FRESH_DATA_REQUIRED: "INFORMATIONAL",
  IMPROVE_DIVERSIFICATION: "INFORMATIONAL", INCREASE_POSITION: "INFORMATIONAL", HOLD_CASH: "IGNORE",
  MAINTAIN_POSITION: "IGNORE", NO_OPTIMIZATION_REQUIRED: "IGNORE",
  // GTT Validation (18-Jul-2026 feature)
  SIGNAL_REVERSED: "ACTION_REQUIRED", ORPHANED_STOP: "ACTION_REQUIRED",
  STALE: "INFORMATIONAL", NEVER_CONFIRMED: "INFORMATIONAL", QTY_MISMATCH: "INFORMATIONAL",
  STOP_OUTDATED: "INFORMATIONAL", PRICE_DRIFTED: "INFORMATIONAL", NO_EVIDENCE: "INFORMATIONAL",
  VALID: "IGNORE", UNKNOWN_SYMBOL: "IGNORE",
};

// Shared by EIE (Phase 1, today-only) and Morning Briefing (Phase 2, most-recent
// regardless of date) — extracted so the "did the latest recorded decision differ
// from the one before it" logic exists in exactly one place.
function _recentDecisionChanges(evolution, healthDeltaThreshold, dateFilterIst) {
  const changes = [];
  if (!evolution || !evolution.ok) return changes;
  for (const rec of evolution.records) {
    const tl = rec.decision_progression_timeline;
    if (!tl || tl.length < 2) continue;
    const latest = tl[tl.length - 1], prev = tl[tl.length - 2];
    if (!latest.ts) continue;
    if (dateFilterIst) {
      const latestDateIst = new Date(new Date(latest.ts).getTime() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
      if (latestDateIst !== dateFilterIst) continue;
    }
    const decisionChanged = latest.decision !== prev.decision || latest.verdict !== prev.verdict;
    const healthDelta = (latest.health_score != null && prev.health_score != null) ? latest.health_score - prev.health_score : null;
    if (decisionChanged || (healthDelta != null && Math.abs(healthDelta) >= healthDeltaThreshold)) {
      changes.push({
        symbol: rec.symbol, as_of_ts: latest.ts,
        from: { decision: prev.decision, verdict: prev.verdict, health_score: prev.health_score },
        to: { decision: latest.decision, verdict: latest.verdict, health_score: latest.health_score },
        health_delta: healthDelta, decision_changed: decisionChanged,
      });
    }
  }
  return changes;
}

// Shared by EIE (Phase 1) and Morning Briefing (Phase 2) — builds the four action
// buckets from each engine's own already-existing recommendation-type labels via
// EIE_ACTION_CLASSIFICATION. Extracted so this exists in exactly one place.
function _classifyEngineRecommendations(profitProtection, capitalRotation, optimization, gttCheck) {
  const classify = function (type) { return EIE_ACTION_CLASSIFICATION[type] || "INFORMATIONAL"; };
  const buckets = { ACTION_REQUIRED: [], OPPORTUNITY: [], INFORMATIONAL: [], IGNORE: [] };
  if (profitProtection && profitProtection.ok) for (const r of (profitProtection.recommendations || [])) {
    buckets[classify(r.recommendation)].push({ source: "Profit Protection", symbol: r.symbol, type: r.recommendation, why: r.explanation || null });
  }
  if (capitalRotation && capitalRotation.ok) for (const r of (capitalRotation.recommendations || [])) {
    buckets[classify(r.recommendation)].push({ source: "Capital Rotation", symbol: r.symbol, type: r.recommendation, why: r.explanation || null });
  }
  if (optimization && optimization.ok) for (const r of (optimization.recommendations || [])) {
    const item = { source: "Portfolio Optimizer", symbol: (r.scope === "PORTFOLIO" ? (r.candidate_symbol || null) : r.scope), type: r.type, why: r.explanation || r.one_line_reason || null };
    if (r.type === "DEPLOY_CASH") { item.candidate_symbol = r.candidate_symbol; item.suggested_allocation = r.suggested_allocation; item.confidence = r.confidence; item.sizing_note = r.sizing_note || null; }
    buckets[classify(r.type)].push(item);
  }
  if (gttCheck && gttCheck.ok) for (const r of (gttCheck.results || [])) {
    buckets[classify(r.status_check)].push({ source: "GTT Validation", symbol: r.symbol, type: r.status_check, why: r.reason || null, trigger_price: r.trigger_price != null ? r.trigger_price : null });
  }
  return buckets;
}

async function computeExecutiveIntelligence(env, ctx) {
  const briefing = await ctx.executiveBriefing();
  if (!briefing.ok) return { ok: false, error: "Executive Briefing unavailable: " + (briefing.error || "unknown") };

  let profitProtection, capitalRotation, optimization, evolution, gttCheck;
  try {
    [profitProtection, capitalRotation, optimization, evolution] = await Promise.all([
      ctx.profitProtection(), ctx.capitalRotation(), ctx.portfolioOptimization(), ctx.decisionEvolutionAll(),
    ]);
  } catch (e) {
    return { ok: false, error: "Engine synthesis failed: " + (e && e.message) };
  }
  try { gttCheck = await validateGttOrdersEOD(env); }
  catch (e) { gttCheck = { ok: false, error: e && e.message }; } // GTT check failure must never block the rest of EIE

  const todayIst = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);

  // ── WHAT CHANGED TODAY — shared helper, date-filtered to today ──
  const whatChangedToday = _recentDecisionChanges(evolution, EIE_HEALTH_CHANGE_THRESHOLD, todayIst);

  // ── ACTION CLASSIFICATION — shared helper over each engine's own existing labels ──
  const buckets = _classifyEngineRecommendations(profitProtection, capitalRotation, optimization, gttCheck);

  return {
    ok: true, engine_version: EXECUTIVE_INTELLIGENCE_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    data_freshness: await ctx.holdingsFreshness(),
    what_changed_today: whatChangedToday,
    requires_action: buckets.ACTION_REQUIRED,
    new_opportunities: buckets.OPPORTUNITY,
    informational: buckets.INFORMATIONAL,
    can_be_ignored: buckets.IGNORE,
    executive_summary: briefing.executive_summary,
    portfolio_health: briefing.portfolio_health,
    gtt_check_status: gttCheck.ok ? "ok" : "unavailable",
  };
}

async function handleExecutiveIntelligence(env) {
  try { return cors(await computeExecutiveIntelligence(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Executive intelligence generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MORNING EXECUTIVE BRIEFING — Phase 2, Daily Operating Model (18-Jul-2026)
//
// Distinct from the EOD Executive Intelligence Engine (Phase 1): EIE asks "what
// changed TODAY" (requires today's 16:15 confirmed scan to have already run).
// Morning Briefing asks "what's the most current picture as of right now" — which,
// at login time before today's scan has run, is necessarily YESTERDAY's confirmed
// evaluation. Reuses the exact same _recentDecisionChanges/_classifyEngineRecommend
// -ations helpers Phase 1 already built, just without the today-only date filter.
// Zero new engines, zero new scoring — pure reuse + morning-specific framing.
// ═══════════════════════════════════════════════════════════════════════════════

const MORNING_BRIEFING_ENGINE_VERSION = "morning-v1.0";
const MORNING_PRIORITIES_CAP = 5; // ENGINEERING_ASSUMPTION — a "priorities" list capped to stay skimmable at login, not the full action list (that's still fully available separately)

async function computeMorningBriefing(env, ctx) {
  const briefing = await ctx.executiveBriefing();
  if (!briefing.ok) return { ok: false, error: "Executive Briefing unavailable: " + (briefing.error || "unknown") };

  let profitProtection, capitalRotation, optimization, evolution, gttCheck, regime;
  try {
    [profitProtection, capitalRotation, optimization, evolution, regime] = await Promise.all([
      ctx.profitProtection(), ctx.capitalRotation(), ctx.portfolioOptimization(), ctx.decisionEvolutionAll(), ctx.regime(),
    ]);
  } catch (e) { return { ok: false, error: "Engine synthesis failed: " + (e && e.message) }; }
  try { gttCheck = await validateGttOrdersEOD(env); }
  catch (e) { gttCheck = { ok: false, error: e && e.message }; }

  const sinceYesterday = _recentDecisionChanges(evolution, EIE_HEALTH_CHANGE_THRESHOLD, null); // no date filter — most recent, whatever day
  const buckets = _classifyEngineRecommendations(profitProtection, capitalRotation, optimization, gttCheck);
  const todaysPriorities = buckets.ACTION_REQUIRED.concat(buckets.OPPORTUNITY).slice(0, MORNING_PRIORITIES_CAP);

  return {
    ok: true, engine_version: MORNING_BRIEFING_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    data_freshness: await ctx.holdingsFreshness(),
    market_condition: { regime: regime || null },
    portfolio_health: briefing.portfolio_health,
    since_yesterday: sinceYesterday,
    urgent_actions: buckets.ACTION_REQUIRED,
    new_opportunities: buckets.OPPORTUNITY,
    todays_priorities: todaysPriorities,
    executive_summary: briefing.executive_summary,
  };
}

async function handleMorningBriefing(env) {
  try { return cors(await computeMorningBriefing(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Morning briefing generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO PERFORMANCE INTELLIGENCE — Phase 3, Daily Operating Model (18-Jul-2026)
//
// Almost entirely reuse: total value / capital deployed / available cash come
// directly from the already-live-verified ctx.portfolioCapital(). Today's P&L and
// historical trend come from the qe_portfolio_snapshot time series (extended
// additively in _portfolioHealthSeries above — same query Portfolio Story already
// uses, just selecting the value/invested columns it wasn't reading before).
// Best/worst performer and largest contributor are argmax/argmin over the
// already-computed per-holding pnl_pct and (ltp-avg_price)*qty fields — no new
// scoring. The ONE genuinely new query is an aggregate read over qe_trade_outcomes
// for realized profit, which honestly discloses (not silently drops) the closed
// trades that lack a recorded quantity, consistent with every prior trade-outcome
// backfill this session — never fabricates a quantity to make the total look complete.
// ═══════════════════════════════════════════════════════════════════════════════

const PORTFOLIO_PERFORMANCE_ENGINE_VERSION = "performance-v1.0";

async function computePortfolioPerformance(env, ctx) {
  let holdings, capital;
  try { holdings = await ctx.holdings(); } catch (e) { return { ok: false, error: "qe_holdings read failed: " + (e && e.message) }; }
  try { capital = await ctx.portfolioCapital(); } catch (e) { return { ok: false, error: "Capital read failed: " + (e && e.message) }; }

  const series = await _portfolioHealthSeries(env);
  const latestSnap = series.length ? series[series.length - 1] : null;
  const prevSnap = series.length >= 2 ? series[series.length - 2] : null;
  const todaysPnl = (latestSnap && prevSnap && latestSnap.total_value != null && prevSnap.total_value != null)
    ? Math.round((latestSnap.total_value - prevSnap.total_value) * 100) / 100 : null;

  let realizedRows = [];
  try {
    const q = await env.QE_DB.prepare("SELECT symbol, buy_price, sell_price, qty FROM qe_trade_outcomes WHERE status='CLOSED'").all();
    realizedRows = (q && q.results) || [];
  } catch (_) {}
  const knownProfitRows = realizedRows.filter(function (r) { return r.qty != null && r.buy_price != null && r.sell_price != null; });
  const totalRealizedProfit = knownProfitRows.length
    ? Math.round(knownProfitRows.reduce(function (s, r) { return s + (r.sell_price - r.buy_price) * r.qty; }, 0) * 100) / 100
    : null;
  const excludedCount = realizedRows.length - knownProfitRows.length;
  const realizedProfitNote = excludedCount > 0
    ? `${excludedCount} of ${realizedRows.length} closed trades lack recorded quantity and are excluded from this rupee total (return % only is on record for those).`
    : null;

  const withPnl = holdings.filter(function (h) { return h.pnl_pct != null; });
  const best = withPnl.length ? withPnl.reduce(function (a, b) { return b.pnl_pct > a.pnl_pct ? b : a; }) : null;
  const worst = withPnl.length ? withPnl.reduce(function (a, b) { return b.pnl_pct < a.pnl_pct ? b : a; }) : null;
  const withContribution = holdings
    .map(function (h) { return { symbol: h.symbol, contribution: (h.ltp != null && h.avg_price != null && h.qty != null) ? Math.round((h.ltp - h.avg_price) * h.qty * 100) / 100 : null }; })
    .filter(function (h) { return h.contribution != null; });
  const largestContributor = withContribution.length ? withContribution.reduce(function (a, b) { return b.contribution > a.contribution ? b : a; }) : null;

  const totalInvested = (capital && capital.ok) ? capital.current_invested_value : (latestSnap ? latestSnap.total_invested : null);
  const totalValue = (capital && capital.ok) ? capital.current_portfolio_value : (latestSnap ? latestSnap.total_value : null);
  const unrealizedProfit = (totalValue != null && totalInvested != null) ? Math.round((totalValue - totalInvested) * 100) / 100 : null;
  const portfolioReturnPct = (totalInvested && totalInvested > 0 && unrealizedProfit != null) ? Math.round((unrealizedProfit / totalInvested) * 10000) / 100 : null;

  return {
    ok: true, engine_version: PORTFOLIO_PERFORMANCE_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    data_freshness: await ctx.holdingsFreshness(),
    total_portfolio_value: totalValue,
    capital_deployed: totalInvested,
    available_cash: (capital && capital.ok) ? capital.available_cash : null,
    todays_pnl: todaysPnl,
    unrealized_profit: unrealizedProfit,
    realized_profit: totalRealizedProfit,
    realized_profit_note: realizedProfitNote,
    portfolio_return_pct: portfolioReturnPct,
    best_performer: best ? { symbol: best.symbol, pnl_pct: best.pnl_pct } : null,
    worst_performer: worst ? { symbol: worst.symbol, pnl_pct: worst.pnl_pct } : null,
    largest_contributor: largestContributor,
    historical_trend: series.map(function (s) { return { date: s.snapshot_date, total_value: s.total_value, total_invested: s.total_invested, pnl_pct: s.total_pnl_pct }; }),
  };
}

async function handlePortfolioPerformance(env) {
  try { return cors(await computePortfolioPerformance(env, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Portfolio performance generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO FIT INTELLIGENCE — Phase 4, Daily Operating Model (18-Jul-2026)
//
// "Discovery signals alone should never trigger recommendations" — this engine is
// the gate between a candidate qualifying technically (Discovery + QE gate) and it
// actually being presented as improving THIS portfolio. Reuses, does not
// duplicate: SECTOR_MAP + _sectorWeights (existing), max_position_weight_pct /
// max_sector_exposure_pct thresholds (existing, via ctx.optimizationConstraints()),
// and the exact same position-sizing formula already built for DEPLOY_CASH
// (X / (current_portfolio_value + X) <= maxWeight). The only new logic is
// projecting sector weight AFTER a hypothetical allocation, which nothing
// previously computed (existing sector-weight checks only evaluate current
// holdings, never a hypothetical addition).
//
// Verdicts:
//   IMPROVES_PORTFOLIO — fits within both position and sector limits, and either
//                         diversifies into a new sector or stays comfortably under
//                         both thresholds.
//   NEUTRAL             — fits within limits but concentrates further into an
//                          already-heavy sector; not harmful, not clearly beneficial.
//   WORSENS_CONCENTRATION — would push position or sector weight over its
//                           configured limit. This is the case where a
//                           technically-good Discovery signal should NOT become
//                           an active recommendation.
// ═══════════════════════════════════════════════════════════════════════════════

const PORTFOLIO_FIT_ENGINE_VERSION = "fit-v1.0";

async function computePortfolioFit(env, ctx, candidateSymbol, allocationAmount) {
  const foundation = await ctx.capitalFoundation();
  if (!foundation.ok) return { ok: false, error: "Capital Foundation unavailable: " + (foundation.error || "unknown") };
  const currentPortfolioValue = foundation.capital.ok ? foundation.capital.current_portfolio_value : null;
  if (currentPortfolioValue == null || allocationAmount == null || allocationAmount <= 0) {
    return { ok: true, symbol: candidateSymbol, verdict: "INSUFFICIENT_DATA", reason: "Missing current portfolio value or a positive allocation amount to evaluate." };
  }

  const constraints = await ctx.optimizationConstraints();
  if (!constraints.ok) return { ok: false, error: "Optimization constraints unavailable: " + (constraints.error || "unknown") };
  const cByType = {}; for (const c of constraints.constraints) cByType[c.constraint] = c;
  const maxPositionWeightPct = cByType["max_position_weight_pct"] ? cByType["max_position_weight_pct"].threshold : null;
  const maxSectorExposurePct = cByType["max_sector_exposure_pct"] ? cByType["max_sector_exposure_pct"].threshold : null;

  const alreadyHeld = (foundation.positions || []).some(function (p) { return p.symbol === candidateSymbol; });
  if (alreadyHeld) return { ok: true, symbol: candidateSymbol, verdict: "ALREADY_HELD", reason: "This symbol is already a current holding — Portfolio Fit evaluates NEW candidates, not additions to existing positions." };

  const candidateSector = SECTOR_MAP[candidateSymbol] || "OTHER";
  const currentSectorWeights = _sectorWeights(foundation.positions || []);
  const currentSectorPct = currentSectorWeights[candidateSector] || 0;
  const isNewSector = !(candidateSector in currentSectorWeights) || currentSectorPct === 0;

  const newTotalValue = currentPortfolioValue + allocationAmount;
  const newPositionWeightPct = Math.round((allocationAmount / newTotalValue) * 10000) / 100;
  const existingSectorRupeeValue = (currentSectorPct / 100) * currentPortfolioValue;
  const projectedSectorPct = Math.round(((existingSectorRupeeValue + allocationAmount) / newTotalValue) * 10000) / 100;

  const positionOverLimit = (maxPositionWeightPct != null) && (newPositionWeightPct > maxPositionWeightPct);
  const sectorOverLimit = (maxSectorExposurePct != null) && (projectedSectorPct > maxSectorExposurePct);

  let verdict, reason;
  if (positionOverLimit || sectorOverLimit) {
    verdict = "WORSENS_CONCENTRATION";
    const parts = [];
    if (positionOverLimit) parts.push(`projected position weight ${newPositionWeightPct}% exceeds the ${maxPositionWeightPct}% limit`);
    if (sectorOverLimit) parts.push(`projected ${candidateSector} sector weight ${projectedSectorPct}% exceeds the ${maxSectorExposurePct}% limit`);
    reason = "This allocation would push the portfolio over its own configured limits: " + parts.join("; ") + ".";
  } else if (isNewSector) {
    verdict = "IMPROVES_PORTFOLIO";
    reason = `${candidateSector} is not currently represented in the portfolio — this adds diversification within existing position (${newPositionWeightPct}%) and sector (${projectedSectorPct}%) limits.`;
  } else {
    verdict = "NEUTRAL";
    reason = `Fits within position (${newPositionWeightPct}%) and sector (${projectedSectorPct}%) limits, but concentrates further into an already-held sector (${candidateSector}, currently ${currentSectorPct}%) rather than diversifying.`;
  }

  return {
    ok: true, engine_version: PORTFOLIO_FIT_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    symbol: candidateSymbol, verdict, reason,
    sector: candidateSector, is_new_sector: isNewSector,
    projected_position_weight_pct: newPositionWeightPct, projected_sector_weight_pct: projectedSectorPct,
    max_position_weight_pct: maxPositionWeightPct, max_sector_exposure_pct: maxSectorExposurePct,
  };
}

async function handlePortfolioFit(request, env) {
  let body; try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON body", 400); }
  const symbol = String(body.symbol || "").toUpperCase().trim();
  const allocation = body.allocation != null ? parseFloat(body.allocation) : null;
  if (!symbol) return corsErr("symbol required", 400);
  try { return cors(await computePortfolioFit(env, createPortfolioContext(env), symbol, allocation)); }
  catch (e) { return corsErr(e.message || "Portfolio fit evaluation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MACRO & NEWS INTELLIGENCE — Phase 5, Daily Operating Model (18-Jul-2026)
//
// HONEST SCOPE STATEMENT, not a full implementation of the roadmap's ambition:
//
// SECTOR ROTATION — real, implemented below. Computed entirely from qe_forward_track,
// already-persisted data from the confirmed scan, zero new fetch. Aggregates
// today's candidates by SECTOR_MAP sector and average score, giving a genuine
// signal of which sectors are producing the strongest setups today — within the
// scanned universe, not the full exchange.
//
// MARKET BREADTH (full-exchange advance/decline, new highs/lows) — NOT built.
// Confirmed by inspection: this codebase fetches bhav-copy data transiently during
// the discovery pipeline's Stage 1 and never persists a market-wide summary.
// Building genuine breadth would mean modifying that pipeline stage to compute and
// store an advance/decline summary as a byproduct of the fetch it already makes —
// a real, scoped, achievable follow-up, but a pipeline change, not something to
// bolt on here without touching that stage directly.
//
// GLOBAL MACRO, COMPANY NEWS, ECONOMIC EVENTS — NOT built, and cannot be built
// from what exists in this codebase. Confirmed by direct search: zero references
// to any news API, economic calendar API, or global-markets data provider exist
// anywhere in kite.js. This requires a genuine external data subscription and API
// key that isn't configured — not a coding gap I can close, an infrastructure
// decision that needs to be made (which provider, at what cost) before any code
// can call it. Scaffolded below with an explicit NOT_CONFIGURED status so any
// future engine or dashboard reading this gets a clear, honest signal rather than
// silence or a fabricated number.
//
// THE EXISTING FRONTEND HEURISTIC IS NOT RETIRED. The roadmap asks for the
// browser-only macroScoreAdj to be replaced — I have not done this, deliberately.
// Removing it now, with no real backend macro signal to replace it, would be a
// regression (score would lose a signal it currently has), not an improvement.
// This is flagged as a decision for explicit sign-off once a real news/macro data
// source is actually connected, not something to unilaterally delete.
// ═══════════════════════════════════════════════════════════════════════════════

const MACRO_NEWS_INTELLIGENCE_ENGINE_VERSION = "macro-news-v0.1-partial";

async function computeSectorRotation(env) {
  let rows = [];
  try {
    const q = await env.QE_DB.prepare(
      "SELECT symbol, score, label FROM qe_forward_track WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM qe_forward_track)"
    ).all();
    rows = (q && q.results) || [];
  } catch (e) { return { ok: false, error: "qe_forward_track read failed: " + (e && e.message) }; }
  if (!rows.length) return { ok: true, engine_version: MACRO_NEWS_INTELLIGENCE_ENGINE_VERSION, generated_ts: new Date().toISOString(), sectors: [], note: "No candidates recorded for the most recent scan date." };

  const bySector = {};
  for (const r of rows) {
    const sector = SECTOR_MAP[r.symbol] || "OTHER";
    if (!bySector[sector]) bySector[sector] = { sector, candidate_count: 0, buy_count: 0, score_sum: 0 };
    bySector[sector].candidate_count++;
    if (r.label === "BUY") bySector[sector].buy_count++;
    bySector[sector].score_sum += (r.score || 0);
  }
  const sectors = Object.values(bySector)
    .map(function (s) { return { sector: s.sector, candidate_count: s.candidate_count, buy_count: s.buy_count, avg_score: Math.round((s.score_sum / s.candidate_count) * 10) / 10 }; })
    .sort(function (a, b) { return b.avg_score - a.avg_score; });

  return {
    ok: true, engine_version: MACRO_NEWS_INTELLIGENCE_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    scope_note: "Sector strength within today's scanned/qualifying universe only — not a full-exchange sector rotation measure.",
    sectors: sectors,
  };
}

async function computeMacroNewsIntelligence(env) {
  const sectorRotation = await computeSectorRotation(env);
  return {
    ok: true, engine_version: MACRO_NEWS_INTELLIGENCE_ENGINE_VERSION, generated_ts: new Date().toISOString(),
    sector_rotation: sectorRotation,
    market_breadth: { status: "NOT_BUILT", reason: "Requires persisting an advance/decline summary during the discovery pipeline's existing bhav-copy stage — a scoped pipeline change, not yet made." },
    global_macro: { status: "NOT_CONFIGURED", reason: "No global-macro data provider is connected. Requires a real external data subscription and API key before any code can be written against it." },
    company_news: { status: "NOT_CONFIGURED", reason: "No news data provider is connected. Same limitation as global_macro." },
    economic_events: { status: "NOT_CONFIGURED", reason: "No economic calendar provider is connected. Same limitation as global_macro." },
    frontend_heuristic_status: "NOT_RETIRED — the existing browser-only macro score adjustment remains in place, since no real backend replacement exists yet for that specific signal. Retiring it now would be a regression, not an improvement.",
  };
}

async function handleMacroNewsIntelligence(env) {
  try { return cors(await computeMacroNewsIntelligence(env)); }
  catch (e) { return corsErr(e.message || "Macro/News intelligence generation failed", 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// AUTOMATION LAYER — Productization Phase 1 (18-Jul-2026)
//
// One shared wrapper, used by BOTH briefing types, persisting to ONE table
// (qe_briefing_log) that serves THREE purposes at once: execution monitoring
// (did today's briefing run), failure recovery (manual retry route below reads
// the same table to know what needs retrying), and the Historical Briefing
// Archive (Phase 3 dashboard requirement) — one write, three consumers, no
// duplicated logging paths.
//
// RETRY: in-process only (up to 2 attempts, no delay between — a genuine
// transient error, e.g. a momentary D1 hiccup, is the realistic case this
// catches). This is NOT a distributed/delayed retry queue — Cloudflare Workers
// has no confirmed queue primitive wired into this project, and building one
// blind, untested against real infrastructure, would be worse than being honest
// about the limitation. The manual retry route is the deliberate fallback for
// anything that fails twice in a row.
//
// HISTORY POLICY: logging starts from deployment of this feature. No backfill —
// there is no prior data to reconstruct, and fabricating placeholder history rows
// would misrepresent what actually happened before this was built.
// ═══════════════════════════════════════════════════════════════════════════════

async function _generateAndLogBriefing(env, type) {
  const nowIso = new Date().toISOString();
  const briefingDate = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
  const t0 = Date.now();
  const ctx = createPortfolioContext(env);

  let payload = null, lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      if (type === "morning") {
        payload = await computeMorningBriefing(env, ctx);
      } else if (type === "eod") {
        const eie = await computeExecutiveIntelligence(env, ctx);
        const performance = await computePortfolioPerformance(env, ctx);
        payload = { ok: eie.ok && performance.ok, executive_intelligence: eie, portfolio_performance: performance };
      } else {
        return { ok: false, error: "Unknown briefing type: " + type };
      }
      if (payload && payload.ok) { lastError = null; break; }
      lastError = (payload && payload.error) || "Briefing computed but returned ok:false with no error detail";
    } catch (e) { lastError = e && e.message; }
  }

  const status = (payload && payload.ok) ? "SUCCESS" : "FAILED";
  const durationMs = Date.now() - t0;

  try {
    await env.QE_DB.prepare(
      "INSERT INTO qe_briefing_log (briefing_type,briefing_date,status,payload_json,error,generated_ts,duration_ms) VALUES (?1,?2,?3,?4,?5,?6,?7) " +
      "ON CONFLICT(briefing_type,briefing_date) DO UPDATE SET status=excluded.status,payload_json=excluded.payload_json,error=excluded.error,generated_ts=excluded.generated_ts,duration_ms=excluded.duration_ms"
    ).bind(type, briefingDate, status, payload ? JSON.stringify(payload) : null, lastError, nowIso, durationMs).run();
  } catch (e) { console.error("[briefing log] write failed:", e && e.message); } // logging failure must never mask the underlying result

  return { ok: status === "SUCCESS", type, briefing_date: briefingDate, status, payload, error: lastError, duration_ms: durationMs };
}

async function handleSystemHealth(env) {
  const todayIst = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
  let rows = [];
  try {
    const q = await env.QE_DB.prepare("SELECT briefing_type, briefing_date, status, error, generated_ts, duration_ms FROM qe_briefing_log WHERE briefing_date >= ?1 ORDER BY briefing_date DESC, briefing_type ASC").bind(
      new Date(Date.now() - 5 * 86400000 + 5.5 * 3600 * 1000).toISOString().slice(0, 10)
    ).all();
    rows = (q && q.results) || [];
  } catch (e) { return corsErr("Health check query failed: " + (e && e.message), 502); }

  const todayRows = rows.filter(function (r) { return r.briefing_date === todayIst; });
  const morningToday = todayRows.find(function (r) { return r.briefing_type === "morning"; });
  const eodToday = todayRows.find(function (r) { return r.briefing_type === "eod"; });
  const isTradingDay = _isIstTradingDay(todayIst);

  return cors({
    ok: true, checked_ts: new Date().toISOString(), today: todayIst, is_trading_day: isTradingDay,
    morning_briefing_today: morningToday ? { status: morningToday.status, generated_ts: morningToday.generated_ts, error: morningToday.error } : (isTradingDay ? { status: "MISSING" } : { status: "NOT_APPLICABLE_NON_TRADING_DAY" }),
    eod_briefing_today: eodToday ? { status: eodToday.status, generated_ts: eodToday.generated_ts, error: eodToday.error } : (isTradingDay ? { status: "MISSING" } : { status: "NOT_APPLICABLE_NON_TRADING_DAY" }),
    recent_history: rows,
  });
}

async function handleBriefingRetry(request, env) {
  let body; try { body = await request.json(); } catch (_) { body = {}; }
  const type = body.type === "eod" ? "eod" : (body.type === "morning" ? "morning" : null);
  if (!type) return corsErr("body.type must be 'morning' or 'eod'", 400);
  const result = await _generateAndLogBriefing(env, type);
  if (result.ok && type === "eod") { try { await _sendEodExecutiveBriefTelegram(env, result.payload); } catch (_) {} }
  if (result.ok && type === "morning") { try { await _sendMorningBriefingTelegram(env, result.payload); } catch (_) {} }
  return cors(result);
}

async function handleBriefingHistory(env, url) {
  const type = url.searchParams.get("type");
  const limit = Math.min(parseInt(url.searchParams.get("limit") || "30", 10) || 30, 100);
  let sql = "SELECT briefing_type, briefing_date, status, payload_json, error, generated_ts, duration_ms FROM qe_briefing_log";
  const binds = [];
  if (type === "morning" || type === "eod") { sql += " WHERE briefing_type=?1"; binds.push(type); }
  sql += " ORDER BY briefing_date DESC LIMIT " + limit;
  try {
    const q = await env.QE_DB.prepare(sql).bind(...binds).all();
    return cors({ ok: true, results: (q && q.results) || [] });
  } catch (e) { return corsErr("Briefing history query failed: " + (e && e.message), 502); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TELEGRAM DELIVERY LAYER — Productization Phase 2 (18-Jul-2026)
//
// Two distinct messages, matching the refined plan exactly:
//   Morning  — one message, "Executive Morning Brief"
//   EOD      — Message 1 (existing discovery scan, untouched) stays as-is;
//              Message 2 ("Executive Portfolio Brief") is new, sent separately,
//              never merged into Message 1.
// Pure formatting over already-computed payloads — no new scoring, no new
// synthesis. The one deterministic addition is the "conclusion" line, templated
// from the classified bucket COUNTS the Executive Intelligence Engine already
// produced, not free-form generated text.
// ═══════════════════════════════════════════════════════════════════════════════

function _fmtRupee(n) { return n == null ? "—" : "₹" + Math.round(n).toLocaleString("en-IN"); }
function _fmtPct(n) { return n == null ? "—" : (n >= 0 ? "+" : "") + n.toFixed(2) + "%"; }

async function _sendMorningBriefingTelegram(env, morning) {
  if (!morning || !morning.ok) { console.error("[Morning Telegram] payload not ok, skipping send"); return; }
  const dateStr = new Date().toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
  const regime = (morning.market_condition && morning.market_condition.regime) ? String(morning.market_condition.regime).toUpperCase() : "UNKNOWN";
  const ph = morning.portfolio_health;

  const sinceY = morning.since_yesterday || [];
  const sinceYLines = sinceY.length
    ? sinceY.slice(0, 8).map(function (c) { return `• ${c.symbol}: ${c.from.decision}→${c.to.decision}${c.health_delta != null ? ` (health ${c.health_delta >= 0 ? "+" : ""}${c.health_delta})` : ""}`; }).join("\n")
    : "No material changes since the last confirmed evaluation.";

  const priorities = morning.todays_priorities || [];
  const priorityLines = priorities.length
    ? priorities.map(function (p) { return `• [${p.source}] ${p.symbol || "Portfolio"} — ${_cxHumanizeServerSide(p.type)}`; }).join("\n")
    : "Nothing urgent flagged for today.";

  const healthLine = (ph && typeof ph === "object" && ph.last_value != null)
    ? `💼 <b>Portfolio Health:</b> ${ph.last_value}/100${ph.delta != null ? ` (${ph.delta >= 0 ? "+" : ""}${ph.delta} since ${ph.first_date})` : ""}\n`
    : `💼 <b>Portfolio Health:</b> insufficient history yet\n`;

  const msg = `🌅 <b>Executive Morning Brief</b> — ${dateStr}\n`
    + `⏰ ${new Date().toISOString().slice(11, 16)} UTC\n\n`
    + `📶 <b>Regime:</b> ${regime}\n`
    + healthLine
    + `\n<b>📋 Since Yesterday</b>\n${sinceYLines}\n\n`
    + `<b>⚡ Today's Priorities</b>\n${priorityLines}`;

  try { await sendTelegram(env, msg); } catch (e) { console.error("[Morning Telegram] send failed:", e && e.message); }
}

async function _sendEodExecutiveBriefTelegram(env, eodPayload) {
  if (!eodPayload || !eodPayload.ok) { console.error("[EOD Executive Telegram] payload not ok, skipping send"); return; }
  const eie = eodPayload.executive_intelligence, perf = eodPayload.portfolio_performance;
  const dateStr = new Date().toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });

  const changed = (eie && eie.what_changed_today) || [];
  const changedLines = changed.length
    ? changed.slice(0, 8).map(function (c) { return `• ${c.symbol}: ${c.from.decision}→${c.to.decision}${c.health_delta != null ? ` (health ${c.health_delta >= 0 ? "+" : ""}${c.health_delta})` : ""}`; }).join("\n")
    : "No material decision changes today.";

  const actions = (eie && eie.requires_action) || [];
  const actionLines = actions.length
    ? actions.slice(0, 8).map(function (a) { return `🔴 [${a.source}] ${a.symbol || "Portfolio"} — ${_cxHumanizeServerSide(a.type)}${a.why ? ": " + a.why.slice(0, 100) : ""}`; }).join("\n")
    : "Nothing requires action today.";

  const opps = (eie && eie.new_opportunities) || [];
  const oppLines = opps.length
    ? opps.map(function (o) { return `💡 ${o.candidate_symbol || o.symbol} — ${o.why || o.type}${o.suggested_allocation ? ` (₹${Math.round(o.suggested_allocation).toLocaleString("en-IN")})` : ""}`; }).join("\n")
    : "No new opportunities identified today.";

  const perfLine = perf && perf.ok
    ? `💼 Portfolio: ${_fmtRupee(perf.total_portfolio_value)} · Today: ${_fmtRupee(perf.todays_pnl)} · Return: ${_fmtPct(perf.portfolio_return_pct)}\n`
      + `Best: ${perf.best_performer ? perf.best_performer.symbol + " " + _fmtPct(perf.best_performer.pnl_pct) : "—"} · Worst: ${perf.worst_performer ? perf.worst_performer.symbol + " " + _fmtPct(perf.worst_performer.pnl_pct) : "—"}`
    : "Portfolio performance unavailable today.";

  const conclusion = actions.length > 0
    ? `${actions.length} item(s) need your attention today${opps.length ? `, ${opps.length} new opportunity(ies) identified` : ""}.`
    : (opps.length > 0 ? `Nothing urgent — ${opps.length} new opportunity(ies) worth a look.` : "Nothing urgent today. Portfolio is steady.");

  const msg = `📊 <b>Executive Portfolio Brief</b> — ${dateStr}\n\n`
    + `${perfLine}\n\n`
    + `<b>🧭 What Changed Today</b>\n${changedLines}\n\n`
    + `<b>🚨 Requires Action</b>\n${actionLines}\n\n`
    + `<b>💡 New Opportunities</b>\n${oppLines}\n\n`
    + `<b>✅ Conclusion:</b> ${conclusion}`;

  try { await sendTelegram(env, msg); } catch (e) { console.error("[EOD Executive Telegram] send failed:", e && e.message); }
}

// Pure display formatting (SCREAMING_SNAKE_CASE -> Title Case) — server-side twin
// of the SAME transform already applied on the frontend (_cxHumanize in index.html),
// so Telegram doesn't show raw enum strings either. Named distinctly (server-side)
// to avoid any confusion with the frontend function of the same purpose.
function _cxHumanizeServerSide(s) { return String(s == null ? "" : s).replace(/_/g, " ").replace(/\w\S*/g, function (t) { return t.charAt(0) + t.slice(1).toLowerCase(); }); }

// ═══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO INTELLIGENCE COPILOT — Phase 10.0 (v4.90)
//
// NOT AN LLM, NOT FREE-FORM NLU: this is deterministic pattern-matching against a
// fixed set of question shapes, each mapped to ONE specific reused engine call.
// There is no language model here, no fuzzy inference, no generated prose beyond
// what the matched engine already produced. If no pattern matches, or a required
// symbol can't be validated against real recorded symbols, the Copilot reports
// UNRECOGNIZED_QUESTION or HISTORICAL_DATA_NOT_AVAILABLE — it never guesses at
// intent or fabricates an answer.
//
// PURE ORCHESTRATION: every intent handler below calls an existing compute
// function directly (replayDecision, computePortfolioMemory,
// generateDecisionNarrative, computeExecutiveBriefing, computePortfolioStory,
// _latestDecisionsBySymbol) and returns ITS output verbatim, tagged with which
// field/engine answered the question. Zero new investment intelligence is
// computed anywhere in this file — the only new logic is the pattern-match
// itself and symbol-token validation against real recorded symbols.
//
// SYMBOL EXTRACTION: rather than trusting any uppercase-looking token in the
// question (which risks matching an unrelated word), every candidate token is
// checked against the actual distinct symbol list in qe_decision_log. Only an
// exact, case-insensitive match against a REAL recorded symbol is accepted —
// never a fuzzy or partial match, which would risk answering about the wrong
// holding.
// ═══════════════════════════════════════════════════════════════════════════════

const COPILOT_ENGINE_VERSION = "copilot-v1.0";

async function _allKnownSymbols(env) {
  const q = await env.QE_DB.prepare("SELECT DISTINCT symbol FROM qe_decision_log ORDER BY symbol ASC").all();
  return ((q && q.results) || []).map(r => r.symbol);
}

function _extractKnownSymbol(question, knownSymbols) {
  const tokens = (question.toUpperCase().match(/[A-Z]+/g)) || [];
  for (const t of tokens) if (knownSymbols.includes(t)) return t;
  return null;
}

function _copilotResult(intent, question, sourceEngine, result) {
  return { ok: true, engine_version: COPILOT_ENGINE_VERSION, generated_ts: new Date().toISOString(), question, intent, source_engine: sourceEngine, result };
}

async function answerCopilotQuestion(env, question, ctx) {
  if (!question || typeof question !== "string" || !question.trim()) return { ok: false, error: "A non-empty question string is required." };
  const q = question.trim();
  const knownSymbols = await _allKnownSymbols(env);

  // Order matters: check more specific patterns before generic ones.
  if (/replay/i.test(q)) {
    const idMatch = q.match(/(\d+)/);
    if (!idMatch) return _copilotResult("replay", q, "Decision Replay Engine", { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: "No decision_log_id found in the question — replay requires a specific decision identifier." });
    return _copilotResult("replay", q, "Decision Replay Engine", await replayDecision(env, idMatch[1])); // Phase 5.0, reused unchanged
  }
  if (/journey/i.test(q)) {
    const symbol = _extractKnownSymbol(q, knownSymbols);
    if (!symbol) return _copilotResult("journey", q, "Portfolio Memory Engine", { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: "No recognized symbol found in the question." });
    return _copilotResult("journey", q, "Portfolio Memory Engine", await computePortfolioMemory(env, symbol, ctx)); // reused unchanged
  }
  if (/why\s+(is|did|was)\b/i.test(q)) {
    const symbol = _extractKnownSymbol(q, knownSymbols);
    if (!symbol) return _copilotResult("why_decision", q, "Decision Narrative Engine", { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: "No recognized symbol found in the question." });
    const decisionsBySymbol = await ctx.decisionsBySymbol(); // Phase 2.0, reused unchanged
    const latest = decisionsBySymbol[symbol];
    if (!latest) return _copilotResult("why_decision", q, "Decision Narrative Engine", { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: `No decision found for ${symbol}.` });
    return _copilotResult("why_decision", q, "Decision Narrative Engine", await generateDecisionNarrative(env, latest.id, ctx)); // Phase 6.0, reused unchanged
  }
  if (/worr(y|ies)|concerning/i.test(q)) {
    const briefing = await ctx.executiveBriefing(); // Phase 8.0, reused unchanged
    return _copilotResult("worries_most", q, "Executive Portfolio Briefing (todays_weakest_holding)", briefing.ok ? briefing.todays_weakest_holding : briefing);
  }
  if (/strengthened/i.test(q)) {
    const story = await ctx.portfolioStory(); // Phase 7.0, reused unchanged
    return _copilotResult("strengthened_most", q, "Portfolio Story Engine (biggest_improvement)", story.ok ? story.biggest_improvement : story);
  }
  if (/weakened/i.test(q)) {
    const story = await ctx.portfolioStory();
    return _copilotResult("weakened_recently", q, "Portfolio Story Engine (biggest_deterioration)", story.ok ? story.biggest_deterioration : story);
  }
  if (/attention/i.test(q)) {
    const briefing = await ctx.executiveBriefing();
    return _copilotResult("attention_today", q, "Executive Portfolio Briefing (opportunities_requiring_attention)", briefing.ok ? briefing.opportunities_requiring_attention : briefing);
  }
  if (/highest\s+conviction/i.test(q)) {
    const briefing = await ctx.executiveBriefing();
    return _copilotResult("highest_conviction", q, "Executive Portfolio Briefing (todays_highest_conviction)", briefing.ok ? briefing.todays_highest_conviction : briefing);
  }
  if (/portfolio\s+story/i.test(q)) {
    return _copilotResult("portfolio_story", q, "Portfolio Story Engine", await ctx.portfolioStory());
  }
  if (/executive\s+briefing/i.test(q)) {
    return _copilotResult("executive_briefing", q, "Executive Portfolio Briefing", await ctx.executiveBriefing());
  }
  if (/\bholdings\b|current\s+portfolio\b|current\s+positions?\b|current\s+stocks\b|stocks?\s+do\s+i\s+own|what\s+do\s+i\s+own/i.test(q)) {
    const foundation = await ctx.capitalFoundation(); // Phase 2.5, reused unchanged
    return _copilotResult("current_holdings", q, "Portfolio Capital Intelligence (positions)", foundation.ok ? foundation.positions : foundation);
  }
  if (/changed\s+since\s+yesterday|what\s+changed/i.test(q)) {
    const memory = await ctx.portfolioMemoryAll(); // reused unchanged — reuses already-stored what_changed/why_changed, no new query
    const changes = memory.ok ? memory.records.map(r => {
      const j = r.decision_journey, latest = j[j.length - 1];
      return { symbol: r.symbol, ts: latest.ts, what_changed: latest.what_changed, why_changed: latest.why_changed };
    }).filter(c => c.what_changed || c.why_changed) : [];
    return _copilotResult("changed_since_yesterday", q, "Portfolio Memory Engine (decision_journey.what_changed/why_changed)", changes.length ? { changes } : { status: HISTORICAL_DATA_NOT_AVAILABLE, reason: "No recorded what_changed/why_changed evidence found." });
  }

  return _copilotResult("unrecognized", q, null, {
    status: "UNRECOGNIZED_QUESTION",
    reason: "This question did not match any supported deterministic pattern — the Copilot does not guess at intent.",
    supported_question_types: [
      "Why is {SYMBOL} {DECISION}? / Why did {SYMBOL} become {DECISION}?", "Show the complete journey of {SYMBOL}",
      "Which holding worries me most?", "Which thesis has strengthened the most?", "Which recommendation has weakened recently?",
      "Which positions require attention today?", "Which holdings have the highest conviction?", "Show today's portfolio story",
      "Explain today's executive briefing", "What changed since yesterday?", "Show replay for decision {decision_log_id}",
    ],
  });
}

async function handleCopilotQuestion(url, env) {
  const question = url.searchParams.get("q");
  try { return cors(await answerCopilotQuestion(env, question, createPortfolioContext(env))); }
  catch (e) { return corsErr(e.message || "Copilot question answering failed", 502); }
}

export default {

  // ── Scheduled cron handler ──────────────────────────────────────────────────
  async scheduled(event, env, ctx) {
    const cron = event.cron;

    // 03:15 UTC Mon–Fri = 08:45 IST — Auth reminder
    if (cron === "15 3 * * 2-6") {
      ctx.waitUntil(sendAuthReminder(env));
    }

    // 03:45 UTC Mon–Fri = 09:15 IST — Legacy discovery trigger
    // In v4.0 this now fires the server-side pipeline
    if (cron === "45 3 * * 2-6") {
      ctx.waitUntil(triggerDiscoveryScan(env));
    }

    // ── Commit 3: THREE scheduled production runs — all call the SAME
    //    pipeline path (runPipelineWithSummary → runFullPipeline). No duplicate
    //    code paths, no separate ranking. Each run pulls fresh market data and
    //    skips symbols already analysed earlier today (KV dedup).
    // 04:00 UTC Mon–Fri = 09:30 IST — Run 1 (market open)
    if (cron === "0 4 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "09:30 IST open scan"));
    }

    // 06:30 UTC Mon–Fri = 12:00 IST — Run 2 (midday)
    if (cron === "30 6 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "12:00 IST midday scan"));
    }

    // 09:00 UTC Mon–Fri = 14:30 IST — Run 3 (pre-close)
    if (cron === "0 9 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "14:30 IST pre-close scan"));
    }

    // 05:45 UTC Mon–Fri = 11:15 IST — STEP B (v4.56): INTRADAY FORMING scan #1.
    // Same pipeline; the QE gate scores on today's LIVE forming bar (gate re-fetches via
    // pipeFetch2yCandles, bypassing the completed-bar guard). Tagged "🟡 INTRADAY BREAKOUT · forming".
    // NOTE: partial intraday volume collapses the gate volume sub-score (Bug-A effect) — see v4.56
    // changelog OPEN DECISION on volume treatment. Dashboard trigger: "45 5 * * MON-FRI" (or "45 5 * * 2-6").
    if (cron === "45 5 * * MON-FRI" || cron === "45 5 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "11:15 IST intraday forming scan"));
    }

    // 08:30 UTC Mon–Fri = 14:00 IST — STEP B (v4.56): INTRADAY FORMING scan #2.
    // Same forming logic as the 11:15 run; later in the session more volume has accumulated.
    // Dashboard trigger: "30 8 * * MON-FRI" (or "30 8 * * 2-6"). 1-5 NOT used (= Sun–Thu, Cloudflare conv.).
    if (cron === "30 8 * * MON-FRI" || cron === "30 8 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "14:00 IST intraday forming scan"));
    }

    // 10:45 UTC Mon–Fri = 16:15 IST — STEP A (v4.55): POST-CLOSE CONFIRMED scan.
    // Same SSOT/D1-first discovery + QE-gate pipeline as the intraday runs, but fired AFTER
    // market close (16:15 IST > 15:45 IST) so pipeComputeIndicatorsFromCandles includes TODAY's
    // now-completed daily bar — QEGate.evaluate scores the CONFIRMED close, identical logic to the
    // manual /score gate. Output tagged "🟢 DAY-CLOSE BREAKOUT · confirmed" via the scanMode hook in
    // pipeDispatchTelegram. Cloudflare day-of-week is 1=Sun..7=Sat; Mon–Fri named=MON-FRI=numeric 2-6.
    // Match BOTH spellings so the branch cannot silently no-op; configure the dashboard trigger as
    // "45 10 * * MON-FRI" (or "45 10 * * 2-6"). Do NOT use 1-5 (= Sun–Thu under Cloudflare's convention).
    if (cron === "45 10 * * MON-FRI" || cron === "45 10 * * 2-6") {
      const discoveryChain = runPipelineWithSummary(env, "16:15 IST post-close confirmed scan")
        .then(function () { return sendGttValidationSummary(env); })
        .catch(function (e) { console.error("[EOD GTT chain]", e && e.message); });
      const portfolioChain = runPortfolioPipeline(env).catch(function (e) { console.error("[EOD portfolio pipeline]", e && e.message); });
      ctx.waitUntil(discoveryChain);
      ctx.waitUntil(portfolioChain);
      // Productization Phase 1+2 (18-Jul-2026): Executive Portfolio Brief (Telegram
      // Message 2) waits for BOTH chains above via Promise.all — it needs the
      // freshest data from both (today's candidates from the discovery chain,
      // today's health scores/decisions from the portfolio chain). Failure here is
      // fully isolated: it can never affect either chain above, which are already
      // running independently regardless of what happens here.
      ctx.waitUntil(
        Promise.all([discoveryChain, portfolioChain])
          .then(function () { return _generateAndLogBriefing(env, "eod"); })
          .then(function (result) { if (result && result.ok) return _sendEodExecutiveBriefTelegram(env, result.payload); })
          .catch(function (e) { console.error("[EOD Executive Brief chain]", e && e.message); })
      );
    }

    // Productization Phase 1 (18-Jul-2026): Morning Executive Briefing — 08:30 IST
    // Mon-Fri, before the first 09:15 discovery scan. NOTE, disclosed plainly: this
    // code path only runs once this exact cron pattern is added as a Cron Trigger in
    // the Cloudflare dashboard — the same manual step every other schedule in this
    // file already required. Writing this branch does not register the trigger.
    if (cron === "0 3 * * MON-FRI" || cron === "0 3 * * 2-6") {
      ctx.waitUntil(
        _generateAndLogBriefing(env, "morning")
          .then(function (result) { if (result && result.ok) return _sendMorningBriefingTelegram(env, result.payload); })
          .catch(function (e) { console.error("[Morning Brief chain]", e && e.message); })
      );
    }

    // Every 30 min 04:00–10:00 UTC Mon–Fri = 09:30–15:30 IST — Position monitor
    if (cron === "*/5 4-10 * * 2-6") {
      ctx.waitUntil(monitorPositions(env));
      // D1 cron-driven backfill: advances one chunk per tick WHEN ARMED (no-op
      // otherwise). Self-fetch chaining is blocked by Cloudflare, so the cron is
      // the driver. Fills the full universe over a market session once armed via
      // POST /d1/startbackfill. Independent of the monitor above.
      ctx.waitUntil(d1BackfillTick(env));
    }

    // Dedicated all-hours backfill cron (add "*/10 * * * *" in the dashboard while
    // backfilling, remove when done). Lets the backfill run OUTSIDE market hours so
    // a full-universe load finishes overnight. Only the backfill tick runs here; it
    // is a no-op unless armed. Harmless if left in place (just an idle tick).
    if (cron === "*/10 * * * *") {
      ctx.waitUntil(d1BackfillTick(env));
      // Commit S2 (v4.36): daily token-map age guard, piggybacked on this all-day cron and gated to
      // run once/day at 12:00 UTC (17:30 IST, after the 03:00 UTC Sunday rebuild → same-day catch).
      // Read-only health check; does not touch d1BackfillTick or any verdict/score/signal path.
      const _n = new Date();
      if (_n.getUTCHours() === 12 && _n.getUTCMinutes() < 10) ctx.waitUntil(tokenMapAgeGuard(env));
      // Fix-2 (v4.44): 20:00 IST (14:30 UTC) LOUD backstop — if the cache is still stale
      // hours after close, ping again. Gated to the single :30 tick. Stale check itself
      // suppresses weekends/already-current days, so no nag when data is fresh.
      if (_n.getUTCHours() === 14 && _n.getUTCMinutes() >= 30 && _n.getUTCMinutes() < 40) {
        ctx.waitUntil(checkStaleAndAlert(env, "loud"));
      }
    }

    // 10:30 UTC Mon–Fri = 16:00 IST — Daily summary
    if (cron === "30 10 * * 2-6") {
      ctx.waitUntil(sendDailySummary(env));
      // Fix-2 (v4.44): best-effort append (token is usually dead at 16:00 because the
      // Kite app re-auth killed it earlier) THEN a gentle "tap to refresh" nudge if the
      // cache isn't current. The reliable capture is the login-triggered refresh; this
      // is the 4 PM reminder + lucky-day catch. Result is no longer discarded.
      ctx.waitUntil((async () => {
        await runTokenFreshRefresh(env, "cron-1600");
        await checkStaleAndAlert(env, "gentle");
      })());
    }

    // 03:00 UTC Sunday = 08:30 IST Sunday — Weekly universe rebuild (Cloudflare 1=Sun).
    if (cron === "0 3 * * 1") {
      // Commit S1 (v4.35): the rebuild was fire-and-forget — buildUniverse returns {ok:false}
      // without throwing, so a silent failure showed "Success" while the token map rotted (≤1-day
      // margin → pipeline silently drops to 1y). Now we await the result and alert on failure, plus
      // a (muteable) success ping so SILENCE itself signals a missed cron. This is an OPERATIONAL
      // message via the existing sendTelegram helper — it touches NO verdict/score/signal-dispatch code.
      ctx.waitUntil((async () => {
        let r;
        try { r = await buildUniverse(env); }
        catch (e) { r = { ok: false, error: "exception: " + (((e && e.message) || e) + "").slice(0, 80) }; }
        if (!r || !r.ok) {
          await sendTelegram(env, "🔧 OPS ⚠️ Weekly universe rebuild FAILED: " + ((r && r.error) || "unknown") +
            " — token map expires in ≤1 day; pipeline will drop to 1y. Log in to Kite, then hit /universe/refresh.");
        } else {
          let pingOff = false;
          try { pingOff = (await env.KITE_STORE.get("qe_ops_ping")) === "off"; } catch (_) {}
          if (!pingOff) await sendTelegram(env, "🔧 OPS ✅ Universe rebuilt: " + r.count +
            " symbols · token map refreshed (8-day TTL).");
        }
      })());
    }
  },

  // ── HTTP fetch handler ──────────────────────────────────────────────────────
  async fetch(request, env, ctx) {
    const url    = new URL(request.url);
    const path   = url.pathname;
    const method = request.method;

    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    try {
      return await routeRequest(url, path, method, request, env, ctx);
    } catch (e) {
      // Safety net (Trust Audit follow-up, 13-Jul-2026): previously, any uncaught throw
      // here (e.g. getToken() on an expired Kite session) escaped fetch() entirely.
      // Cloudflare then returned its own generic error page instead of this Worker's
      // response — which carries NO CORS headers. A cross-origin browser fetch() call
      // (e.g. the GitHub Pages frontend calling this workers.dev origin) sees a
      // CORS-less error and reports "Failed to fetch", masking the real cause (e.g. a
      // 401 for an expired session) behind a generic network-looking failure. This
      // catch guarantees every response — including ones from bugs not yet found —
      // always carries the Worker's own CORS headers, so the browser can at least see
      // and report the real error instead of a misleading network failure.
      return corsErr((e && e.message) || "Unexpected server error", 500);
    }
  },
};

async function routeRequest(url, path, method, request, env, ctx) {
    // ══ NEW ROUTES (v3.0) ════════════════════════════════════════════════════

    // POST /telegram/callback — Telegram inline button handler (Priority 1)
    if (path === "/telegram/callback" && method === "POST") {
      return handleTelegramCallback(request, env);
    }

    // GET /kv/get — safe KV read for frontend (extended in v4.0)
    if (path === "/kv/get" && method === "GET") {
      return handleKvGet(url, env);
    }

    // POST /tg/register — store Telegram credentials from UI
    if (path === "/tg/register" && method === "POST") {
      return handleTgRegister(request, env);
    }

    // POST /signal/store — store signal for callback verification
    if (path === "/signal/store" && method === "POST") {
      return handleSignalStore(request, env);
    }

    // GET /tg/status — check if Telegram is configured
    if (path === "/tg/status" && method === "GET") {
      const { ok } = await getTgCreds(env);
      return cors({ status: "success", telegram_configured: ok });
    }

    // ══ UNIVERSE MANAGER ROUTES (v3.1) ═══════════════════════════════════════

    if (path === "/universe/refresh" && method === "GET") {
      return handleUniverseRefresh(env);
    }

    if (path === "/universe/status" && method === "GET") {
      return handleUniverseStatus(env);
    }

    // ══ PIPELINE ROUTES (v4.0) ════════════════════════════════════════════════

    // GET /pipe/trigger — manually run the full server-side discovery pipeline
    if (path === "/pipe/trigger" && method === "GET") {
      return handlePipeTrigger(env);
    }

    // GET /pipe/trigger-summary — DIAGNOSTIC: runs the same wrapper the
    // 0 6 / 0 9 crons use, so the heartbeat + summary path can be tested
    // on demand from the phone instead of waiting for the cron. Remove once
    // the re-scan summaries are confirmed working.
    if (path === "/pipe/trigger-summary" && method === "GET") {
      await runPipelineWithSummary(env, "MANUAL diagnostic test");
      return cors({ status: "done", note: "Heartbeat + summary should now be in Telegram." });
    }

    // GET /pipe/status — pipeline progress / last run summary
    if (path === "/pipe/status" && method === "GET") {
      return handlePipeStatus(env);
    }

    // GET /pipe/signals — completed signals for browser to render
    if (path === "/pipe/signals" && method === "GET") {
      return handlePipeSignals(env);
    }

    // GET /pipe/regime — single-source-of-truth regime snapshot (manual scan reads this)
    if (path === "/pipe/regime" && method === "GET") {
      return handlePipeRegime(env);
    }

    // GET /pipe/candidates — pre-deep-analysis candidates list
    if (path === "/pipe/candidates" && method === "GET") {
      return handlePipeCandidates(env);
    }

    // GET /pipe/streamb/debug — read-only Stream B execution proof (Commit 4.5)
    if (path === "/pipe/streamb/debug" && method === "GET") {
      return handlePipeStreamBDebug(env);
    }

    // GET /pipe/momentum/debug — TEMPORARY validation route (Commit 2).
    // Runs bhav → Stream A Fast → scores ALL survivors with MomentumScore +
    // freshness, returns the full distribution + ranked table. Read-only:
    // does NOT fetch history, does NOT write signals, does NOT touch production
    // selection or Telegram. Remove after validation review.
    if (path === "/pipe/momentum/debug" && method === "GET") {
      return handleMomentumDebug(env);
    }

    // GET /backtest/windows — Commit B: READ-ONLY 2y/3y/5y backtest comparison.
    // Does NOT change the production 2y window or any verdict/score/selection. Evidence tool only.
    if (path === "/backtest/windows" && method === "GET") {
      return handleBacktestWindows(request, env);
    }

    // GET /diff/layers — Phase 1: READ-ONLY layer differential (MC-veto / Pro Filter / Elite) on the
    // production engine over the Kite/D1 universe. NO writes, NO Telegram, NO verdict/score/ranking change.
    if (path === "/diff/layers" && method === "GET") {
      return handleDiffLayers(request, env);
    }

    // GET /breakout/debug — READ-ONLY criterion-level fresh-breakout visibility. No detector/verdict change.
    if (path === "/breakout/debug" && method === "GET") {
      return handleBreakoutDebug(request, env);
    }

    // GET /pipe/audit — full pipeline audit log
    // GET /pipe/qegate — full QE-gate decision audit (latest run). Captures every
    // candidate, both gate verdicts, all reasons. ?date=YYYY-MM-DD&run=xxxx for history.
    if (path === "/pipe/qegate" && method === "GET") {
      const u = new URL(request.url);
      const d = u.searchParams.get("date"), rn = u.searchParams.get("run");
      const key = (d && rn) ? ("qe_pipe_qegate_" + d + "_" + rn) : "qe_pipe_qegate";
      try {
        const raw = await env.KITE_STORE.get(key);
        return cors(raw ? JSON.parse(raw) : { status: "empty", note: "No QE-gate audit for " + key });
      } catch (e) { return corsErr("qegate read failed: " + e.message, 500); }
    }

    if (path === "/pipe/audit" && method === "GET") {
      return handlePipeAudit(env);
    }

    // C (17-Jun v4.40): read-only S5 rejection visibility
    if (path === "/pipe/rejects" && method === "GET") {
      return handlePipeRejects(env);
    }

    // v4.41: ntfy diagnostic — config check + live send test
    if (path === "/ntfy/test" && method === "GET") {
      return handleNtfyTest(env);
    }

    // ══ D1 HISTORY CACHE ROUTES (Option 2) ═══════════════════════════════════
    if (path === "/d1/init"     && method === "POST") { return handleD1Init(env); }
    if (path === "/d1/status"   && method === "GET")  { return handleD1Status(env); }
    if (path === "/d1/backfill" && method === "POST") { return handleD1Backfill(request, env); }
    if (path === "/d1/startbackfill" && method === "POST") { return handleD1StartBackfill(request, env); }
    if (path === "/d1/stopbackfill"  && method === "POST") { return handleD1StopBackfill(env); }
    if (path === "/d1/update"   && method === "POST") {
      const r = await handleD1Update(env);
      return cors({ status: r.ok ? "success" : "error", ...r });
    }
    if (path === "/d1/verify"   && method === "GET")  { return handleD1Verify(request, env); }
    if (path === "/prescan"     && method === "POST") { return handlePrescan(request, env); }  // Design A (v4.46) — bulk pre-scan gate (additive)
    if (path === "/forward-track/today" && method === "GET") { return handleForwardTrackToday(url, env); }  // Priority 2 (13-Jul-2026) — reconciliation source for RUN SCAN
    if (path === "/score"       && method === "POST") { return handleScore(request, env); }  // Phase 2 (v4.53) — single source of truth scorer (additive)

    if (path === "/outcome/add"     && method === "POST") { return handleOutcomeAdd(request, env); }
    if (path === "/outcome/resolve" && method === "POST") { return handleOutcomeResolve(request, env); }
    if (path === "/outcome/list"    && method === "GET")  { return handleOutcomeList(env); }
    if (path === "/outcome/delete"  && method === "POST") { return handleOutcomeDelete(request, env); }
    if (path.indexOf("/portfolio/")===0) { if(!(await pieAuthOk(request, env))) return corsErr("unauthorized", 401); }  // F6: gate all /portfolio routes
    if (path === "/portfolio/run"     && method === "POST") { return cors(await runPortfolioPipeline(env)); }  // v4.69: manual trigger for full pipeline (refresh→intel→digest→decision_report→audit) — same path as 16:15 IST cron
    if (path === "/gtt/validate"      && method === "GET")  { return cors(await validateGttOrdersEOD(env)); }  // 16-Jul-2026: read-only check, any time — same logic the 16:15 EOD cron uses
    if (path === "/gtt/validate"      && method === "POST") { await sendGttValidationSummary(env); return cors(await validateGttOrdersEOD(env)); }
    if (path === "/portfolio/status"  && method === "GET") { return handlePortfolioStatus(env); }
    if (path === "/portfolio/refresh" && method === "GET") { return handlePortfolioRefresh(env); }
    if (path === "/portfolio/holding" && method === "GET")  { return handlePortfolioHolding(url, env); }
    if (path === "/portfolio/memory"  && method === "GET")  { return handlePortfolioMemory(url, env); }
    if (path === "/portfolio/risk"    && method === "GET")  { return handlePortfolioRiskRoute(env); }
    if (path === "/portfolio/claude-note" && method === "POST") { return handleClaudeNote(request, env); }
    if (path === "/portfolio/calibration" && method === "GET") { return handleCalibration(env); }
    if (path === "/portfolio/outcomes/run"    && method === "GET") { return handleOutcomeResolverRun(env); }     // Phase 1.0 (v4.75) — manual trigger / backfill
    if (path === "/portfolio/outcomes/status" && method === "GET") { return handleOutcomeResolverStatus(env); }  // Phase 1.0 (v4.75) — observability only
    if (path === "/portfolio/analytics/decision-quality" && method === "GET") { return handleDecisionQualityAnalytics(url, env); }  // Phase 1.1 (v4.76) — read-only, computed on-demand
    if (path === "/portfolio/calibration/recommendations" && method === "GET") { return handleCalibrationRecommendations(env); }  // Phase 1.2 (v4.77) — read-only, RECOMMENDATIONS ONLY, never auto-applied
    if (path === "/portfolio/profit-protection" && method === "GET") { return handleProfitProtection(env); }  // Phase 2.0 (v4.78) — read-only, evaluates current profitable holdings
    if (path === "/portfolio/capital" && method === "GET") { return handleCapitalFoundation(env); }  // Phase 2.5 (v4.79) — pure infrastructure, no recommendations
    if (path === "/portfolio/capital/compare" && method === "GET") { return handleOpportunityComparison(url, env); }  // Phase 2.5 (v4.79) — comparison structure only, no ranking
    if (path === "/portfolio/capital-rotation" && method === "GET") { return handleCapitalRotation(env); }  // Phase 3.0 (v4.80) — evidence-backed rotation recommendations, prefers inactivity
    if (path === "/portfolio/optimization/objectives" && method === "GET") { return handleOptimizationObjectives(env); }  // Phase 3.5 (v4.81) — config catalog only
    if (path === "/portfolio/optimization/constraints" && method === "GET") { return handleOptimizationConstraints(env); }  // Phase 3.5 (v4.81) — status check, never enforcement
    if (path === "/portfolio/optimization/evidence" && method === "GET") { return handleOptimizationEvidence(env); }  // Phase 3.5 (v4.81) — pure aggregation of upstream phases
    if (path === "/portfolio/optimization/feasibility" && method === "GET") { return handleOptimizationFeasibility(url, env); }  // Phase 3.5 (v4.81) — feasibility check on a caller-supplied hypothetical only
    if (path === "/portfolio/optimize" && method === "GET") { return handlePortfolioOptimization(env); }  // Phase 4.0 (v4.82) — portfolio-level recommendations, never per-holding, never overrides upstream engines
    if (path === "/portfolio/institutional-memory" && method === "GET") { return handleInstitutionalMemory(url, env); }  // Institutional Memory foundation (v4.83, fixed) — pure memory layer, zero recommendations. Distinct path/name from the pre-existing /portfolio/memory (handlePortfolioMemory, holding-trend endpoint) — an earlier route/name collision was found and corrected here, see changelog.
    if (path === "/portfolio/replay" && method === "GET") { return handleDecisionReplay(url, env); }  // Decision Replay Engine (v4.84) — point-in-time reconstruction only, never live/current data
    if (path === "/portfolio/narrative" && method === "GET") { return handleDecisionNarrative(url, env); }  // Decision Narrative Engine (v4.85) — deterministic templated explanation, zero free-form generation
    if (path === "/portfolio/evolution" && method === "GET") { return handleDecisionEvolution(url, env); }  // Decision Evolution Analytics (v4.86) — pure statistics over Portfolio Memory's existing output
    if (path === "/portfolio/story" && method === "GET") { return handlePortfolioStory(env); }  // Portfolio Story Engine (v4.87) — pure aggregation over all upstream engines, zero new scoring
    if (path === "/portfolio/briefing" && method === "GET") { return handleExecutiveBriefing(env); }  // Executive Portfolio Briefing (v4.88) — presentation layer, cites Portfolio Story + targeted filters, zero new intelligence
    if (path === "/portfolio/cockpit" && method === "GET") { return handleExecutiveCockpit(env); }  // Executive Cockpit (v4.89) — presentation layer, cites Briefing + Story directly, zero new queries beyond System Status metadata
    if (path === "/portfolio/intelligence" && method === "GET") { return handleExecutiveIntelligence(env); }  // Executive Intelligence Engine — Phase 1, Daily Operating Model (18-Jul-2026)
    if (path === "/portfolio/morning" && method === "GET") { return handleMorningBriefing(env); }  // Morning Executive Briefing — Phase 2, Daily Operating Model (18-Jul-2026)
    if (path === "/portfolio/performance" && method === "GET") { return handlePortfolioPerformance(env); }  // Portfolio Performance Intelligence — Phase 3, Daily Operating Model (18-Jul-2026)
    if (path === "/portfolio/fit" && method === "POST") { return handlePortfolioFit(request, env); }  // Portfolio Fit Intelligence — Phase 4, Daily Operating Model (18-Jul-2026)
    if (path === "/portfolio/macro-news" && method === "GET") { return handleMacroNewsIntelligence(env); }  // Macro & News Intelligence — Phase 5, Daily Operating Model (18-Jul-2026) — partial, see engine doc
    if (path === "/system/health" && method === "GET") { return handleSystemHealth(env); }  // Productization Phase 1 — briefing execution/health monitoring (18-Jul-2026)
    if (path === "/system/briefing/retry" && method === "POST") { return handleBriefingRetry(request, env); }  // Productization Phase 1 — manual failure recovery (18-Jul-2026)
    if (path === "/portfolio/briefing-history" && method === "GET") { return handleBriefingHistory(env, url); }  // Productization Phase 3 — historical briefing archive (18-Jul-2026)
    if (path === "/portfolio/copilot" && method === "GET") { return handleCopilotQuestion(url, env); }  // Portfolio Intelligence Copilot (v4.90) — deterministic pattern-matched orchestration, never an LLM

    if (path.indexOf("/decision/")===0)   { if(!(await pieAuthOk(request, env))) return corsErr("unauthorized", 401); }  // reuse F6 auth pattern for Decision Intelligence Log routes
    if (path === "/decision/symbols" && method === "GET") { return handleDecisionSymbols(env); }
    if (path === "/decision/history" && method === "GET") { return handleDecisionHistory(url, env); }

    // GET /pipe/survivorship — eliminated stocks with rejection reasons
    if (path === "/pipe/survivorship" && method === "GET") {
      return handlePipeSurvivorship(env);
    }

    // POST /pipe/deep-result — browser posts deep analysis result per symbol
    if (path === "/pipe/deep-result" && method === "POST") {
      return handlePipeDeepResult(request, env);
    }

    // ══ ALL V2.5 ROUTES BELOW — PRESERVED EXACTLY ════════════════════════════

    // GET / — Root: status + OHLCV + fundamentals
    if ((path === "/" || path === "") && method === "GET") {
      const symbol   = url.searchParams.get("symbol");
      const interval = url.searchParams.get("interval");
      const range    = url.searchParams.get("range");
      const type     = url.searchParams.get("type");

      if (!symbol) {
        const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
        return cors({ kite: token ? "connected" : "disconnected",
                      status: "success", version: QE_VERSION });
      }

      if (type === "fundamentals") {
        // v4.4 FIX: Yahoo Finance v10/quoteSummary requires crumb auth since late 2024.
        // Both browser and Worker calls to v10 fail (crumb required / IP blocked).
        // Solution: Worker fetches Screener.in server-side — no CORS restriction from Worker.
        // Parses PE, ROE, Revenue Growth, Profit Growth, D/E from Screener HTML.
        // Returns structured JSON that browser renders directly — no browser parsing needed.
        try {
          const cleanSym = symbol.replace(/\.NS$|\.BO$/i, "").toUpperCase().trim();
          const screenerUrls = [
            "https://www.screener.in/company/" + cleanSym + "/consolidated/",
            "https://www.screener.in/company/" + cleanSym + "/",
          ];
          const headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
          };

          let fundData = null;

          for (let si = 0; si < screenerUrls.length; si++) {
            try {
              const ctrl  = new AbortController();
              const timer = setTimeout(function() { ctrl.abort(); }, 12000);
              let res;
              try {
                res = await fetch(screenerUrls[si], { signal: ctrl.signal, headers });
              } finally {
                clearTimeout(timer);
              }
              if (!res.ok) continue;
              const html = await res.text();
              if (!html || html.includes("Page not found") || html.includes("404")) continue;

              // Parse key ratios from Screener HTML server-side
              const parsed = parseScreenerFundamentals(html, cleanSym);
              if (parsed) { fundData = parsed; break; }
            } catch (_) { continue; }
          }

          if (!fundData) {
            return cors({ status: "error", source: "screener",
                          message: "Screener.in data unavailable for " + cleanSym,
                          fundamentals: null }, 200);
          }

          return cors({
            status: "success",
            source: "screener",
            symbol: cleanSym,
            fundamentals: fundData,
          });
        } catch (e) {
          return cors({ status: "error", source: "screener",
                        message: e.message, fundamentals: null }, 200);
        }
      }

      const decodedSym  = (function() { try { return decodeURIComponent(symbol); } catch(_) { return symbol; } })();
      const cleanSym    = decodedSym.replace(/\.NS$|\.BO$/, "").toUpperCase();
      const isGlobalSym = decodedSym.startsWith("^") || decodedSym.includes("=F") ||
                          decodedSym.includes("=X") || decodedSym.startsWith("%5E") ||
                          symbol.startsWith("%25") || symbol.includes("%3D");

      if (isGlobalSym) return await proxyYahooFinance(decodedSym, interval, range);

      // ── Fix 2: Parity — scoring fetch (src=d1) serves COMPLETED D1 bars so the
      // browser scores byte-identical to the cron (same d1ReadCandles window + same
      // finalDecision). Live fallback below on a D1 miss (e.g. new IPO) — that card
      // then returns source "kite" and the UI tags it KITE (live, not cache-backed).
      if (url.searchParams.get("src") === "d1" && (interval === "1d" || !interval)) {
        try {
          const d1c = await d1ReadCandles(env, cleanSym);
          if (d1c && d1c.length) {
            return cors({
              status: "success", source: "cache",
              chart: { result: [{
                meta: { symbol: cleanSym, currency: "INR", exchangeName: "NSE", dataSource: "cache" },
                timestamp: d1c.map(function(c) { return Math.floor(c[0]); }),
                indicators: { quote: [{
                  open:   d1c.map(function(c) { return c[1]; }),
                  high:   d1c.map(function(c) { return c[2]; }),
                  low:    d1c.map(function(c) { return c[3]; }),
                  close:  d1c.map(function(c) { return c[4]; }),
                  volume: d1c.map(function(c) { return c[5]; })
                }] }
              }], error: null }
            });
          }
          // D1 miss → fall through to the live path below.
        } catch (_) { /* fall through to live */ }
      }

      try {
        const token    = await getToken(env);
        const now      = new Date();
        const msDay    = 86400000;
        const rangeMap = { "5d":5, "1mo":30, "3mo":90, "6mo":180, "1y":365, "2y":730, "5y":1825 };
        const days     = rangeMap[range] || 365;
        const fromStr  = new Date(now - days * msDay).toISOString().slice(0, 10);
        const toStr    = now.toISOString().slice(0, 10);
        const intervalMap = { "1d":"day","1wk":"week","1mo":"month",
                              "5m":"5minute","15m":"15minute","60m":"60minute" };
        const kiteInterval = intervalMap[interval] || "day";

        const quoteRes = await kiteRequest("GET", `/quote?i=NSE:${encodeURIComponent(cleanSym)}`, null, token);
        if (!quoteRes.ok) throw new Error("Quote failed: " + (quoteRes.data.message || quoteRes.status));
        const instrToken = quoteRes.data.data["NSE:" + cleanSym].instrument_token;
        if (!instrToken) throw new Error("No instrument token for " + cleanSym);

        const histRes = await kiteRequest(
          "GET",
          `/instruments/historical/${instrToken}/${kiteInterval}?from=${fromStr}&to=${toStr}`,
          null, token
        );
        if (!histRes.ok) throw new Error("Historical fetch failed: " + (histRes.data.message || histRes.status));

        const candles = (histRes.data.data && histRes.data.data.candles) || [];
        if (!candles.length) throw new Error("No candles returned from Kite");

        return cors({
          status: "success", source: "kite",
          chart: { result: [{
            meta: { symbol: cleanSym, currency: "INR", exchangeName: "NSE", dataSource: "kite" },
            timestamp: candles.map(function(c) { return Math.floor(new Date(c[0]).getTime() / 1000); }),
            indicators: { quote: [{
              open:   candles.map(function(c) { return c[1]; }),
              high:   candles.map(function(c) { return c[2]; }),
              low:    candles.map(function(c) { return c[3]; }),
              close:  candles.map(function(c) { return c[4]; }),
              volume: candles.map(function(c) { return c[5]; })
            }] }
          }], error: null }
        });
      } catch (e) {
        return await proxyYahooFinance(decodedSym, interval, range);
      }
    }

    // GET /login
    if (path === "/login" && method === "GET") {
      const loginUrl = `https://kite.zerodha.com/connect/login?api_key=${API_KEY}&v=3`;
      return Response.redirect(loginUrl, 302);
    }

    // GET /callback + /auth
    if ((path === "/callback" || path === "/auth") && method === "GET") {
      const requestToken = url.searchParams.get("request_token");
      if (!requestToken) return corsErr("Missing request_token");
      const apiSecret = await env.KITE_STORE.get("api_secret");
      if (!apiSecret) return corsErr("API secret not configured in KV");
      const raw = `${API_KEY}${requestToken}${apiSecret}`;
      const hashBuffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(raw));
      const checksum = Array.from(new Uint8Array(hashBuffer))
        .map(function(b) { return b.toString(16).padStart(2, "0"); }).join("");
      const resp = await fetch(`${KITE_API_BASE}/session/token`, {
        method: "POST",
        headers: { "X-Kite-Version": "3", "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ api_key: API_KEY, request_token: requestToken, checksum: checksum }).toString(),
      });
      const data = await resp.json();
      if (!resp.ok) return corsErr(data.message || "Session generation failed", 401);
      const accessToken = data.data.access_token;
      await env.KITE_STORE.put(KV_TOKEN_KEY, accessToken);
      await env.KITE_STORE.put("kite_token_timestamp", String(Date.now()));
      // Fix-2 (v4.44): refresh on the HOT token. Token-store happened above, so the
      // login already succeeded even if this refresh fails. Captures today's bar and
      // records the "data as of" stamp; falls back to the last stamp if it throws.
      let _rf = null;
      try { _rf = await runTokenFreshRefresh(env, "login"); } catch (_) {}
      if (!_rf) {
        try { const _raw = await env.KITE_STORE.get("qe_last_refresh"); if (_raw) _rf = JSON.parse(_raw); } catch (_) {}
      }
      const _asOf  = (_rf && _rf.last_bar_date) || "n/a";
      const _syms  = (_rf && _rf.symbols) || 0;
      const _stamp = istStamp(Date.now());
      try {
        await sendTelegram(env, "✅ <b>QuantEdge refreshed</b> — data as of <b>" + _asOf +
          "</b> · " + _syms + " symbols · " + _stamp);
      } catch (_) {}
      return new Response(
        `<html><body style="font-family:monospace;padding:2rem">
          <h2>✅ Kite Login Successful</h2>
          <p>Access token stored &amp; market data refreshed. QuantEdge KITE badge will show ✓</p>
          <p style="padding:10px 12px;background:#0f1115;border:1px solid #2a2f3a;border-radius:8px;display:inline-block">
            <b>Data as of:</b> ${_asOf} &middot; ${_syms} symbols<br>
            <b>Refreshed:</b> ${_stamp}
          </p>
          <p><a href="${QE_URL}">→ Open QuantEdge</a></p>
        </body></html>`,
        { status: 200, headers: { "Content-Type": "text/html" } }
      );
    }

    // GET /token
    if (path === "/token" && method === "GET") {
      const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
      return cors({ status: "success", has_token: !!token });
    }

    // GET /refresh/status — Fix-2 (v4.44): freshness stamp for the app "Data as of" line.
    if (path === "/refresh/status" && method === "GET") {
      let rec = null;
      try { const raw = await env.KITE_STORE.get("qe_last_refresh"); if (raw) rec = JSON.parse(raw); } catch (_) {}
      if (!rec) {
        const nb = await d1NewestBar(env);
        rec = { last_bar_date: nb, refreshed_at: null, written: null, symbols: null, source: "fallback" };
      }
      // Fix 3: expose staleness so the in-app "Data as of" line can flag a behind cache.
      var _expBar  = lastExpectedTradingDate(Date.now());
      var _isStale = !(rec && rec.last_bar_date && rec.last_bar_date >= _expBar);
      return cors(Object.assign({ status: "success" }, rec, { stale: _isStale, expected_bar_date: _expBar }));
    }

    // GET /quote
    if (path === "/quote" && method === "GET") {
      const symbol = url.searchParams.get("symbol");
      if (!symbol) return corsErr("Missing symbol parameter");
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("GET", `/quote?i=NSE:${encodeURIComponent(symbol)}`, null, token);
        if (!ok) return corsErr(data.message || "Quote fetch failed", 502);
        return cors({ status: "success", data: data.data });
      } catch (e) { return corsErr(e.message, 401); }
    }

    // GET /fundamentals?symbol=XXX  (v4.74) — server-side Screener.in fetch,
    // replaces index.html's 4-proxy CORS chain. See handler comment block above.
    if (path === "/fundamentals" && method === "GET") {
      return handleFundamentals(url, env);
    }


    // GET /instruments/NSE
    if (path === "/instruments/NSE" && method === "GET") {
      try {
        const token = await getToken(env);
        const resp  = await fetch(`${KITE_API_BASE}/instruments/NSE`, {
          headers: { "X-Kite-Version": "3", Authorization: kiteAuthHeader(token) },
        });
        if (!resp.ok) return corsErr("Instruments fetch failed", 502);
        const csv = await resp.text();
        return new Response(csv, { headers: { "Content-Type": "text/csv", ...CORS } });
      } catch (e) { return corsErr(e.message, 401); }
    }

    // POST /gtt/create
    if (path === "/gtt/create" && method === "POST") {
      let body;
      try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON body"); }
      const { symbol, cmp, entry, quantity } = body;
      if (!symbol || !cmp || !entry || !quantity) return corsErr("Required: symbol, cmp, entry, quantity");
      // v4.28 SAFETY: a stop is mandatory. If sl is missing/invalid, REFUSE the entry rather
      // than place a BUY that can fill into a naked position (root cause of the 15-Jun ABDL
      // incident: the client dropped sl, the entry filled, armExitBracket had nothing to arm).
      if (body.sl == null || !(parseFloat(body.sl) > 0))
        return corsErr("Refused: no valid stop-loss (sl) — entry NOT placed to avoid a naked position");
      if (quantity <= 0)           return corsErr("Quantity must be > 0");
      if (entry <= 0 || cmp <= 0)  return corsErr("Price values must be > 0");
      const triggerPrice = parseFloat(entry).toFixed(2);
      const limitPrice   = parseFloat(entry).toFixed(2);
      const lastPrice    = parseFloat(cmp).toFixed(2);
      const condition = JSON.stringify({ exchange: "NSE", tradingsymbol: symbol.toUpperCase(),
                                         trigger_values: [parseFloat(triggerPrice)],
                                         last_price: parseFloat(lastPrice) });
      const orders = JSON.stringify([{ exchange: "NSE", tradingsymbol: symbol.toUpperCase(),
                                       transaction_type: "BUY", quantity: parseInt(quantity, 10),
                                       order_type: "LIMIT", product: "CNC",
                                       price: parseFloat(limitPrice) }]);
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("POST", "/gtt/triggers",
                                               { type: "single", condition, orders }, token);
        if (!ok) return corsErr(data.message || "GTT creation failed at Kite API",
                                data.status || 502);
        const triggerId = data.data.trigger_id;
        await appendGttLog(env, { timestamp: new Date().toISOString(),
                                   symbol: symbol.toUpperCase(), entry: parseFloat(entry),
                                   sl: body.sl || null, t1: body.t1 || null,
                                   quantity: parseInt(quantity, 10), trigger_id: triggerId,
                                   source: "ui" });
        const slStored = body.sl != null ? parseFloat(body.sl) : null;
        const t1Stored = body.t1 != null ? parseFloat(body.t1) : null;
        return cors({ status: "success", trigger_id: triggerId,
                      sl_stored: slStored, t1_stored: t1Stored,   // v4.29: proof of what was persisted
                      message: `GTT created for ${symbol.toUpperCase()} @ ₹${triggerPrice} | Qty: ${quantity} | SL ₹${slStored} · T1 ₹${t1Stored} (stored)`,
                      kite_url: "https://kite.zerodha.com/gtt" });
      } catch (e) { return corsErr(e.message, e.message.includes("token") ? 401 : 502); }
    }

    // GET /gtt/list
    if (path === "/gtt/list" && method === "GET") {
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
        if (!ok) return corsErr(data.message || "GTT list fetch failed", 502);
        const gtts = (data.data || []).filter(function(g) { return g.status === "active"; })
          .map(function(g) {
            return { trigger_id: g.id, symbol: g.condition && g.condition.tradingsymbol,
                     trigger_price: g.condition && g.condition.trigger_values && g.condition.trigger_values[0],
                     order_price: g.orders && g.orders[0] && g.orders[0].price,
                     quantity: g.orders && g.orders[0] && g.orders[0].quantity,
                     product: g.orders && g.orders[0] && g.orders[0].product,
                     type: g.orders && g.orders[0] && g.orders[0].transaction_type,
                     created_at: g.created_at, status: g.status };
          });
        return cors({ status: "success", count: gtts.length, gtts: gtts });
      } catch (e) { return corsErr(e.message, 401); }
    }

    // DELETE /gtt/delete/:id
    if (path.startsWith("/gtt/delete/") && method === "DELETE") {
      const triggerId = path.split("/gtt/delete/")[1];
      if (!triggerId || isNaN(triggerId)) return corsErr("Invalid trigger_id");
      try {
        const token = await getToken(env);
        const resp  = await fetch(`${KITE_API_BASE}/gtt/triggers/${triggerId}`, {
          method: "DELETE",
          headers: { "X-Kite-Version": "3", Authorization: kiteAuthHeader(token) },
        });
        const data = await resp.json();
        if (!resp.ok) return corsErr(data.message || "GTT delete failed", 502);
        return cors({ status: "success", message: `GTT #${triggerId} cancelled successfully` });
      } catch (e) { return corsErr(e.message, 401); }
    }

    return corsErr(`Unknown route: ${method} ${path}`, 404);
}
