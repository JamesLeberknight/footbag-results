1. Mission (North Star)

Transform an offline HTML mirror of footbag.org event pages into a deterministic, archive-quality canonical dataset and Excel workbook.

Primary rule:
Preserve truth, provenance, and uncertainty.
Never invent, assume, or optimize for completeness.

2. Core Operating Principles

These rules override all others.

Correctness > Coverage
Unknown or missing data is acceptable. Incorrect data is not.

No invention
Never fabricate:

divisions

player names

locations

dates

event types

Explicit provenance
Every value must be one of:

extracted

inferred (clearly flagged)

explicitly overridden

Determinism
Same input must always produce the same output.

3. What This System Is Not

Not a data-completion engine

Not a best-guess predictor

Not a metric-optimization pipeline

Not a historical correction tool

This system records what can be justified — nothing more.

4. Pipeline Model (Conceptual)
Stage 1 — Extraction

Extract raw facts from HTML only

No semantic interpretation

No normalization beyond safety cleaning

Stage 2 — Canonicalization

All interpretation, normalization, inference, and QC

Cross-validation happens here

Ambiguity is surfaced, not hidden

Stage 3 — Presentation

Formatting only (Excel, CSV)

No semantic changes allowed

5. Definition of “Done”

A run is acceptable only if:

No new ERROR-severity QC issues are introduced

All inferred values are clearly labeled

Ambiguous data remains ambiguous

Output is reproducible and explainable

Changes are minimal, localized, and documented

“Done” does not mean “every field filled”.

6. Trust Hierarchy (Conflict Resolution)

When signals conflict, prefer higher trust:

Structured HTML headers (e.g., division <h2> blocks)

Numbered result entries under headers

Preformatted results blocks

Event name patterns or known tournament formats

Manual overrides (explicit and event-specific)

Lower-trust evidence must never override higher-trust evidence.

7. Ambiguity & Inference Rules
Always stop and ask (or quarantine) if:

Multiple interpretations are plausible

A change affects many events

Domain judgment is required

A new category or rule would be introduced

May proceed autonomously if:

The correction is mechanically obvious

Only a few events are affected

No alternative interpretation exists

Meaning is preserved exactly

8. Inference Policy

Inference is allowed only when:

Direct evidence is absent

Strong domain conventions exist

The result is explicitly flagged as inferred

Inferred values must never be indistinguishable from extracted ones.

9. Overrides Policy

Overrides are permitted only when:

Source data is irreparably broken

The correction is historically certain

The scope is strictly event-specific

Overrides must be:

minimal

auditable

non-generalizing

10. Metrics Policy

Metrics are observational, not targets

Never change logic solely to improve a metric

Metric improvements must be a side effect of correctness

QC gates apply to severity, not counts

11. Iteration Protocol (Mandatory)

Follow this loop exactly:

Run canonicalization and QC

Select one highest-impact issue

Collect concrete examples (event_id based)

Inspect source HTML if needed

Ask at most one human question (if required)

Implement the smallest safe change

Re-run and compare QC deltas

Persist decisions in overrides if needed

Document reasoning

Proceed only after gates pass

12. Final Guiding Rule

When uncertain:

Preserve uncertainty.
Surface it clearly.
Move on.
