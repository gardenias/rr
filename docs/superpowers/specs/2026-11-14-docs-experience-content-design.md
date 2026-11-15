# rr documentation experience and content design

## Intent

Make the 1.0 documentation site distinctive without sacrificing technical reading. The second pass covers brand lettering, typography, restrained motion, and a full bilingual content edit across all 61 documents.

## Goals

- Use **Rr** as the display wordmark, rendered as original geometric SVG lettering rather than a stock font. Code, crate names, package names, and the repository remain `rr`.
- Self-host variable Inter and Noto Sans SC fonts so Latin and Chinese text share consistent weights, metrics, and rendering across platforms.
- Add restrained pointer feedback, animated consensus connections, scroll reveals, and a documentation reading-progress indicator. Never hide content when JavaScript is unavailable and honor reduced-motion preferences.
- Standardize every document to a bilingual heading followed immediately by Chinese content and its English translation. Keep one shared copy of commands, code, and output.
- Remove duplicated introductions, language-only horizontal rules, filler, marketing claims, repetitive transitions, and translation drift.
- Check technical claims against current source where documents describe commands, behavior, versions, or operational procedures.

## Non-goals and anti-goals

- No animation library, custom cursor, parallax scrolling, autoplay sound, or continuous motion in document pages.
- No renaming of Rust crates or changing API examples for visual consistency.
- No mechanical deletion of English content or unverified changes to performance numbers and compatibility commitments.
- No split into separate Chinese and English sites.

## Constraints

- Preserve all document IDs, routes, sidebars, code fences, PlantUML diagrams, and existing redirects.
- The existing working tree contains the approved first-pass site changes; build on them without reverting user work.
- Use only local/self-hosted assets. Font packages must be loaded as needed and measured through the production build.
- Content edits must preserve technical meaning and flag uncertainty instead of inventing behavior.

## Approaches

1. **Selected: platform CSS and small React hooks plus domain-based content editing.** Use SVG wordmarks, variable fonts, IntersectionObserver, requestAnimationFrame, and CSS transforms. Edit documentation by architecture, guide, reference, deployment/operations, and overview/FAQ groups so each reviewer can check related claims together.
2. Add a motion framework and perform a whole-repository automated language rewrite. This adds runtime cost and produces unreliable technical translations, so it is rejected.
3. Polish only the homepage and leave documentation structure unchanged. This would not address the user's main concerns about embedded translations, AI-like prose, and accuracy.

## Content style contract

- Heading: `# 中文标题 / English Title`; use the same pattern for sections where both languages exist.
- Chinese explanation first, English translation immediately after it. Do not insert `---` between translations.
- Keep terminology consistent: 共识组 / Raft group, 分片 / shard or partition as defined by the document, 本地读 / local read, 线性一致读 / linearizable read.
- Prefer direct sentences: state behavior, constraints, commands, and failure handling. Remove “本指南将…”, “让我们…”, “无缝”, “强大”, and repeated summaries unless they carry information.
- Keep code identifiers unchanged. Code comments may be bilingual on adjacent lines only when useful.
- Use one shared table with bilingual headers or concise bilingual cells instead of duplicating large tables where practical.
- Preserve warnings, non-goals, version caveats, and fail-open/fail-closed distinctions.

## Verification

- Run a repository script over all documents to detect wholly separated Chinese/English halves, duplicate H1s, language-separating horizontal rules, and forbidden filler patterns.
- Run npm typecheck and production build with broken-link failures enabled.
- Serve the production build and inspect homepage motion, pointer interaction, reduced motion, mobile layout, long bilingual pages, code blocks, tables, and C4 diagrams using Playwright.
- Check font requests are local and no external font service is used; inspect bundle/build output for unexpected font weight duplication.
- Complete one independent final review covering visual quality, accessibility, performance, content consistency, and technical accuracy.
