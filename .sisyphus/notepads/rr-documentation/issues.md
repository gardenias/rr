# Issues — rr-documentation

## Known Concerns from Plan (Metis Review)
- Bilingual content doubles page length → Use visual separators (--- horizontal rules)
- Mermaid may not render CJK well → Keep diagram labels in English, CJK captions outside
- Docusaurus build may fail with CJK content → Verify build after T1 scaffold
- Benchmarks: no benchmark code exists → Placeholder page only
- Deployment: no Docker/K8s infra → Generic deployment guidance only

## F3 QA Issue: Broken links in intro.md (2026-03-04)
- `intro.md` line 22: `/architecture` → should be `/architecture/overview`
- `intro.md` line 39: `/architecture` → should be `/architecture/overview`
- `intro.md` line 22: `/guide` → should be `/guide/getting-started`
- `intro.md` line 39: `/guide` → should be `/guide/getting-started`
- These are NEW broken links (not pre-existing template artifacts)
- Non-blocking since onBrokenLinks is 'warn', but should be fixed
