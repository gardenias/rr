import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import remarkBilingual from './src/remark/bilingual';

const config: Config = {
  title: 'Rr — Rust Raft Framework',
  tagline: 'Production-grade Multi-Raft for Rust',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://rr-framework.github.io',
  baseUrl: '/',

  organizationName: 'rr-framework',
  projectName: 'rr',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  headTags: [
    {
      tagName: 'script',
      attributes: {},
      innerHTML: `(function(){var mode='both';try{var saved=localStorage.getItem('rr-docs-language-mode');if(saved==='zh'||saved==='en'||saved==='both'){mode=saved;}}catch{}document.documentElement.dataset.languageMode=mode;})();`,
    },
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: 'docs',
          beforeDefaultRemarkPlugins: [remarkBilingual],
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        createRedirects(existingPath) {
          if (existingPath.startsWith('/docs/')) {
            const legacyPath = existingPath.replace(/^\/docs\//, '/');
            return legacyPath === '/' ? undefined : legacyPath;
          }
          return undefined;
        },
      },
    ],
  ],

  themeConfig: {
    image: 'img/rr-social-card.png',
    metadata: [
      {
        name: 'description',
        content:
          'Build production-grade distributed systems with multi-Raft consensus, sharding, client SDKs, and operational tooling for Rust.',
      },
    ],
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      logo: {
        alt: 'Rr — Rust Raft Framework',
        src: 'img/rr-wordmark-light.svg',
        srcDark: 'img/rr-wordmark-dark.svg',
      },
      items: [
        {
          to: '/docs',
          position: 'left',
          label: '文档 / Docs',
        },
        {
          to: '/docs/architecture/overview',
          position: 'left',
          label: '架构 / Architecture',
        },
        {
          to: '/docs/reference/api-overview',
          position: 'left',
          label: 'API 参考 / API Reference',
        },
        {
          type: 'custom-languageModeSwitcher',
          position: 'right',
        },
        {
          href: 'https://github.com/rr-framework/rr',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'rr on GitHub',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Learn',
          items: [
            {label: 'Getting started', to: '/docs/guide/getting-started'},
            {label: 'Architecture', to: '/docs/architecture/overview'},
            {label: 'C4 diagrams', to: '/docs/architecture/c4'},
          ],
        },
        {
          title: 'Build',
          items: [
            {label: 'Service trait', to: '/docs/guide/service-trait'},
            {label: 'Client SDK', to: '/docs/guide/client-sdk'},
            {label: 'API reference', to: '/docs/reference/api-overview'},
          ],
        },
        {
          title: 'Operate',
          items: [
            {label: 'Deployment', to: '/docs/deployment'},
            {label: 'Observability', to: '/docs/reference/metrics-reference'},
            {label: 'Runbooks', to: '/docs/operations/runbooks/leader-flapping'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/rr-framework/rr'},
            {label: 'FAQ', to: '/docs/faq'},
            {label: 'Compatibility', to: '/docs/reference/compatibility'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Rr contributors. Built for reliable distributed systems.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.oneDark,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
