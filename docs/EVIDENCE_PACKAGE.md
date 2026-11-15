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
