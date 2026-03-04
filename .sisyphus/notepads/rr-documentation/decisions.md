# Decisions — rr-documentation

## 2026-03-04 Session Start

### Documentation Site Decisions
- **Framework**: Docusaurus with TypeScript config
- **Theme**: Mermaid via `@docusaurus/theme-mermaid` plugin
- **CJK**: Paragraph-level interleaving (NOT i18n plugin)
- **Style**: Default Docusaurus theme with CJK font stack in custom.css
- **Mode**: docs-only (routeBasePath: '/')

### Parallel Execution Strategy
- T1 + T2 parallel (Wave 1) — no content dependencies
- T3-T19 ALL parallel (Waves 2+3+4) — all just need T1 scaffold
- T20 sequential (Wave 5) — needs all content
- F1-F4 parallel final review

### Content Quality Standards
- No line-number references to source (go stale)
- No invented APIs — only document what actually exists
- No duplicate cargo doc content — link to it instead
- No Docker/K8s manifests
- No benchmark numbers (placeholder page only)
