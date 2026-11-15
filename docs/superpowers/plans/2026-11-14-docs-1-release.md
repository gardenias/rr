# Docusaurus 1.0 release implementation plan

1. Add the Docusaurus client-redirects plugin and PlantUML encoder at versions compatible with the existing 3.9.2 site.
2. Add a CodeBlock wrapper and PlantUML component. Render SVG images from a configurable server, provide loading/error states and a source disclosure, and preserve normal code blocks.
3. Move docs to `/docs`, create legacy redirects, update absolute links, and add a dedicated `src/pages/index.tsx` homepage with scoped styles.
4. Replace default navbar/footer metadata and global CSS with the rr design system. Add responsive, dark-mode, focus, and reduced-motion states.
5. Create original SVG logo/social artwork and generate favicon assets from the new mark using the locally available `rsvg-convert` toolchain.
6. Run typecheck/build, inspect the production site in Playwright at desktop/mobile sizes, test C4 rendering and legacy redirects, and resolve regressions.
7. Run GitNexus change analysis and one independent final review covering correctness, compatibility, accessibility, security, and scope.
