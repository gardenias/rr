import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */
const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Architecture',
      items: [
        'architecture/overview',
        'architecture/crate-map',
        'architecture/data-flow',
        'architecture/multi-raft',
        'architecture/storage',
        'architecture/networking',
      ],
    },
    {
      type: 'category',
      label: 'Guide',
      items: [
        'guide/getting-started',
        'guide/service-trait',
        'guide/state-machine-trait',
        'guide/config-provider',
        'guide/storage-engines',
        'guide/client-sdk',
        'guide/consumer-guide',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/api-overview',
        'reference/proto-reference',
        'reference/config-reference',
        'reference/error-reference',
        'reference/benchmarks',
        'reference/admin-api',
        'reference/metrics-reference',
      ],
    },
    {
      type: 'category',
      label: 'Deployment',
      items: [
        'deployment/deployment',
      ],
    },
  ],
};

export default sidebars;
