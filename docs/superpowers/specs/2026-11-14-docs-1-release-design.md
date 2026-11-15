# Docusaurus 1.0 release design

## Intent

Prepare the rr documentation site for a 1.0 release: make C4/PlantUML diagrams visible, replace the default template with a credible engineering brand, and give the project a clear product name without renaming Rust crates or repository identifiers.

## Goals

- Render every existing `plantuml` fence as an accessible diagram with an optional source view.
- Add a dedicated product homepage and move documentation under `/docs`, with redirects for legacy document URLs.
- Establish an original visual identity: consensus-ring mark, graphite surfaces, warm Rust-orange accents, precise typography, responsive navigation, and readable long-form documentation in light and dark modes.
- Use **rr — Rust Raft Framework** as the site name and **Production-grade Multi-Raft for Rust** as the primary descriptor.
- Keep documentation source, crate names, and repository URLs stable.

## Non-goals

- No rewrite of technical content or backend APIs.
- No local Java/PlantUML toolchain requirement.
- No decorative animation that interferes with reading or reduced-motion preferences.

## Constraints

- Docusaurus 3.9.2, React 19, TypeScript, and npm are already in use.
- The site is bilingual; layout must accommodate Chinese and English without depending on a downloaded font.
- PlantUML source is public documentation. Diagram rendering defaults to the public PlantUML server and can be pointed at a self-hosted server through configuration.
- Existing absolute document links must continue to work after the `/docs` route migration.

## Approaches

1. **Selected: native Docusaurus pages plus a CodeBlock theme wrapper.** Add a React homepage, move docs to `/docs`, use the official client-redirects plugin, and render PlantUML through a small browser-compatible encoder. This preserves Markdown authoring and covers all existing diagrams without editing each file.
2. Build-time PlantUML rendering. This would produce fully self-contained SVG output, but requires a Java renderer or build-time network service plus a cache and failure pipeline. It is more infrastructure than this change needs.
3. Keep docs-only routing and restyle the introduction. This avoids URL changes but cannot provide a credible product landing page or clear separation between product narrative and reference documentation.

## Design decisions

- The icon is an original geometric mark: three nodes connected around an open consensus ring, with a forward path through the center. It must remain recognizable at favicon size and must not reuse Docusaurus artwork.
- The homepage leads with the value proposition, two clear actions (Get started, Architecture), a small code example, four engineering capabilities, and a final documentation call to action.
- Documentation typography and tables are the priority; decorative surfaces stay on the homepage and navigation.
- Motion is limited to short opacity/transform transitions and is disabled for reduced-motion users.
- Naming alternatives for release discussion: **rr / Rust Raft Framework** (recommended, compatible), **RaftForge** (distinct brand, larger rename), and **RRaft** (compact, but less consistent with existing crate names). “Rust Raft Application” incorrectly suggests an end-user application; “Rust Raft Startup” suggests a company or starter template.

## Verification

- Run npm typecheck and production build.
- Serve the production build and verify homepage, docs navigation, legacy redirects, light/dark modes, mobile layout, and keyboard-accessible diagram source controls in a real browser.
- Confirm PlantUML image URLs return valid SVG and the rendered C4 diagram is visible rather than a code-only block.
- Check browser console errors and horizontal overflow at desktop and mobile widths.
