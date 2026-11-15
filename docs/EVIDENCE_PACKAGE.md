# Preparing the factual hand-in evidence

After the validated preparation, enrichment and streaming runs exist locally:

```bash
docker compose exec -T spark python -m jobs.reporting.prepare_evidence --output report-assets/evidence-20261115
```

An existing destination is never overwritten. For a later snapshot, choose a new output name. Run one Spark application at a time; this job uses Spark only to export existing per-record source metadata, not to recollect or rewrite text.

The bundle contains all seven table counts, all 78 column profiles, the global five highest raw missing percentages, recorded corrections/quarantines, IMDb source fingerprints, enrichment status/coverage progression and remaining cases, and the full metadata-only Wikipedia revision/license index. Streaming evidence includes the original real-session summary, configuration, progress metrics and committed batch markers, plus tabular exports.

`INDEX.md` links the files, `SOURCES.csv` identifies original paths and source URLs, and `MANIFEST.json` / `SHA256SUMS.txt` preserve checksums. `VALIDATION.json` records snapshot consistency, quality status, streaming marker/session agreement, record counts and protection of PROJECT.md and REPORT.md. Copies are checked against their source files; the complete bundle is published only after validation.

Report assets are intentionally small factual exports suitable for a separate documentation PR. Raw source archives, article bodies, Parquet data, checkpoints, Kafka data, credentials and `.env` are excluded. The source lineage index contains citations and identifiers, not synopsis or biography text.

These files provide factual evidence only. Contributors personally choose results and write all methodological explanations, alternatives, trade-offs and interpretations in REPORT.md. Add peer-reviewed exploration and open-question outputs separately; the package does not mark those deliverables complete. This exporter does not edit PROJECT.md or its user-written AI declaration.

## Exact bytes and Git checkouts

The `report-assets/** -text` Git attribute preserves evidence bytes, including
CSV record terminators. Original reports copied into a bundle must retain their
source SHA-256 hashes. Do not normalize their line endings or recalculate hashes
solely to hide a mismatch. Some original CSVs use CRLF and Spark exports use LF;
both are valid and must remain as recorded by the manifest. The scoped
`whitespace=cr-at-eol` attribute recognizes those CRLF record terminators during
Git whitespace checks without changing the files.

After applying the attribute to an existing checkout, explicitly stage the
restored original evidence with `git add --renormalize -- report-assets/evidence-20261115`.
Twelve CSVs in the first publication had been normalized to LF by Git; restoring
their original CRLF bytes makes them match the existing manifest and source-copy
hashes. The manifest, source data and numerical results do not change.

Verify the bundle after checkout, without Spark or network requests:

```bash
docker compose exec -T spark python -m jobs.reporting.verify_evidence
```

Host-side Python can also run `python -m jobs.reporting.verify_evidence` from the
repository. The verifier checks the complete file inventory, manifest sizes and
hashes, SHA256SUMS inventory and hashes, source-copy hashes and CSV readability.
Run it on a fresh clone of the corrected published commit as a separate final
check; a local verification does not establish that the correction was pushed.
