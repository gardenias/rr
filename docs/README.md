# Rr documentation site

The Rr product homepage and technical documentation are built with Docusaurus 3.9 and React 19.

## Documentation authoring rules (required)

These rules are the first acceptance criteria for every documentation change. Do not bypass them when writing,
reviewing, building, or publishing documentation.

### 1. Support all three language modes

The navbar provides **中文 / English / 同时** modes. Chinese mode hides English blocks, English mode hides Chinese
blocks, and dual mode shows both. Hidden blocks use `display: none` and must not leave empty space.

- Write both languages in every user-facing document; the MDX build marks bilingual headings, paragraphs, list items,
  and table cells automatically.
- Keep commands, configuration, code, output, links to shared resources, and diagrams as one shared copy. Never paste
  a second copy into language-specific sections.
- Do not add custom language toggles, `visibility: hidden`, fixed-height placeholders, or per-language page routes.
- After changing the bilingual structure, run the production build and check all three modes at desktop and mobile
  widths. Check headings, paragraphs, lists, tables, anchor links, admonition titles, and the table of contents.
  The navbar, sidebars, breadcrumbs, and pagination must switch to the same language as the document body. Technical
  identifiers and code-only list items stay shared in every mode; do not misclassify them as untranslated prose.

### 2. Keep the bilingual structure complete and consistent

- Use exactly `中文标题 / English Title` for bilingual headings. Keep the slash separator and do not create separate
  Chinese and English heading sections.
- Place the Chinese explanation first and its complete English translation immediately after it. Separate standalone
  Chinese and English paragraphs with one blank line.
- Inside a list item or table cell, put the Chinese text first, then `<br />`, then the English translation. When a
  table cell already uses the heading-style `中文 / English` convention, keep that form.
- Keep one shared copy of commands, code, output, and diagrams. Preserve identifiers, crate names, API names, paths,
  and configuration keys verbatim; translate the explanation, not the identifier.
- Do not split a document into Chinese and English halves, insert horizontal rules between translations, or leave a
  new Chinese paragraph without its English counterpart.
- Prefer direct statements about behavior, prerequisites, limitations, and failure handling. Remove filler and repeated
  summaries. Check commands and technical claims against the current workspace before publishing.

### 3. Keep PlantUML diagrams local and reproducible

- Write diagrams in fenced blocks tagged `plantuml`. Do not hard-code plantuml.com URLs, generated SVG paths, manifest
  keys, or remote `<img>` elements in Markdown.
- Run `npm run diagrams` after adding or changing a diagram. The command scans all Markdown files, normalizes diagram
  source, generates content-hashed SVG files in `static/diagrams/`, and updates `src/generated/plantuml-manifest.json`.
- Commit the generated SVG files and manifest together with the Markdown change. Readers and normal cached builds must
  never contact plantuml.com.
- `npm start` and `npm run build` run the generator automatically. Existing valid SVG assets are reused without network
  access; missing or invalid assets make generation fail instead of silently falling back to a remote image.
- `PLANTUML_SERVER_URL` is only needed when generating a missing or changed diagram. It must expose the standard
  `/svg/<encoded-source>` endpoint. If the public service is unavailable, use a reachable self-hosted renderer.
- Remove obsolete diagram source when a diagram is no longer needed; the next generation removes orphaned SVG assets.
  Always run the production build and open the affected diagram before publishing.

## Install

```bash
npm ci
```

## Local development

```bash
npm start
```

Docusaurus serves the product homepage at `/` and documentation at `/docs`. Legacy documentation URLs redirect to
their `/docs` equivalents. Diagram assets and the language metadata are refreshed before the development server starts.
From the repository root, use `just docs-serve` for the same workflow and `just docs-build` for a release build. Both
recipes generate local SVG assets explicitly. Set `DOCS_PORT` when the default port 3000 is already in use.

## Fix one bilingual page

Pass one page relative to `docs/docs` (a `docs/docs/...` path is also accepted):

```bash
npm run fix-bilingual-page -- architecture/overview.md
npm run fix-bilingual-page -- architecture/overview.md --write
```

From the repository root, the equivalent commands are `just docs-fix-page architecture/overview.md` and
`just docs-fix-page architecture/overview.md --write`. The default mode only prints diagnostics and a diff. It converts
unambiguous bilingual list separators, link labels, and `strong` prefixes to the structure above; it never invents a
translation. Detected Chinese list items without an English translation, ambiguities in supported structures, and
Chinese-only diagram labels remain manual work and make the command exit nonzero. Arbitrary Chinese-only paragraphs are
not diagnosed. A dry run also exits nonzero when safe fixes are pending. `--write` applies only the safe
fixes, atomically and idempotently. Paths outside `docs/docs`, non-Markdown files, and symlinks are rejected. This
command does not generate SVG files or run a site build.

## Validate a release build

```bash
npm run typecheck
npm run build
npm run serve
```

The production build is written to `build/`. Broken links and missing or invalid PlantUML assets fail the build. A build
with all generated assets already present works without access to a PlantUML renderer. Verify the actual release output,
including language switching and local diagram loading, before publishing.
