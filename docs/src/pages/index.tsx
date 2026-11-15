import {useEffect, useRef, type CSSProperties, type PointerEvent, type ReactNode} from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import styles from './index.module.css';

type Feature = {
  number: string;
  title: string;
  description: string;
  icon: 'consensus' | 'scale' | 'operate' | 'rust';
};

const features: Feature[] = [
  {
    number: '01',
    title: 'Consensus, built in',
    description:
      'Run multiple independent Raft groups with strong consistency, leader election, snapshots, and membership safeguards.',
    icon: 'consensus',
  },
  {
    number: '02',
    title: 'Scale by shards',
    description:
      'Route partitions across Raft groups and grow throughput horizontally without turning replicas into a bottleneck.',
    icon: 'scale',
  },
  {
    number: '03',
    title: 'Ready to operate',
    description:
      'Ship with client retries, Prometheus metrics, structured tracing, TLS, admin tooling, and deployment templates.',
    icon: 'operate',
  },
  {
    number: '04',
    title: 'Rust end to end',
    description:
      'Embed the framework in your binary and connect business logic through explicit Service and StateMachine traits.',
    icon: 'rust',
  },
];

function useScrollReveal() {
  const pageRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const page = pageRef.current;
    if (!page || window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      return;
    }
    const elements = Array.from(page.querySelectorAll<HTMLElement>('[data-reveal]'));
    page.classList.add(styles.motionReady);
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            (entry.target as HTMLElement).dataset.visible = 'true';
            observer.unobserve(entry.target);
          }
        });
      },
      {rootMargin: '0px 0px -8% 0px', threshold: 0.08},
    );
    elements.forEach((element) => observer.observe(element));
    return () => {
      observer.disconnect();
      page.classList.remove(styles.motionReady);
    };
  }, []);

  return pageRef;
}

function FeatureIcon({name}: {name: Feature['icon']}): ReactNode {
  const paths = {
    consensus: (
      <>
        <circle cx="12" cy="12" r="3" />
        <circle cx="12" cy="3" r="2" />
        <circle cx="4" cy="18" r="2" />
        <circle cx="20" cy="18" r="2" />
        <path d="M10.8 10.3 5.5 16.4M13.2 10.3l5.3 6.1M12 9V5M6 18h12" />
      </>
    ),
    scale: (
      <>
        <rect x="3" y="3" width="7" height="7" rx="1" />
        <rect x="14" y="3" width="7" height="7" rx="1" />
        <rect x="3" y="14" width="7" height="7" rx="1" />
        <rect x="14" y="14" width="7" height="7" rx="1" />
        <path d="M10 6.5h4M6.5 10v4M17.5 10v4M10 17.5h4" />
      </>
    ),
    operate: (
      <>
        <path d="M4 17V7m5 10V4m6 13v-7m5 7V6" />
        <path d="M2 21h20" />
        <circle cx="4" cy="5" r="2" />
        <circle cx="15" cy="7" r="2" />
      </>
    ),
    rust: (
      <>
        <path d="m8 5-5 7 5 7M16 5l5 7-5 7M14 3l-4 18" />
      </>
    ),
  };

  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
      {paths[name]}
    </svg>
  );
}

function handleClusterPointer(event: PointerEvent<HTMLDivElement>): void {
  if (
    event.pointerType !== 'mouse' ||
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ) {
    return;
  }
  const card = event.currentTarget;
  const bounds = card.getBoundingClientRect();
  const x = (event.clientX - bounds.left) / bounds.width;
  const y = (event.clientY - bounds.top) / bounds.height;
  card.style.setProperty('--pointer-x', `${x * 100}%`);
  card.style.setProperty('--pointer-y', `${y * 100}%`);
  card.style.setProperty('--tilt-x', `${(0.5 - y) * 2.4}deg`);
  card.style.setProperty('--tilt-y', `${(x - 0.5) * 3.2}deg`);
}

function resetClusterPointer(event: PointerEvent<HTMLDivElement>): void {
  event.currentTarget.style.setProperty('--tilt-x', '0deg');
  event.currentTarget.style.setProperty('--tilt-y', '0deg');
}

function ClusterVisual(): ReactNode {
  return (
    <div
      className={styles.clusterCard}
      aria-label="A three-node Raft cluster with one leader and two followers"
      onPointerMove={handleClusterPointer}
      onPointerLeave={resetClusterPointer}>
      <div className={styles.clusterHeader}>
        <div>
          <span className={styles.clusterEyebrow}>Live topology</span>
          <div className={styles.clusterName}>production / group-01</div>
        </div>
        <span className={styles.health}><span />Healthy</span>
      </div>
      <div className={styles.topology}>
        <svg viewBox="0 0 520 240" role="img" aria-label="Connected Raft nodes">
          <path className={styles.linkLine} d="M260 112 112 184M260 112l148 72M112 184h296" />
          <path className={styles.activeLine} d="M260 112 112 184M260 112l148 72" />
        </svg>
        <div className={`${styles.node} ${styles.nodeLeader}`}>
          <span className={styles.nodeId}>01</span>
          <span className={styles.nodeRole}>Leader</span>
        </div>
        <div className={`${styles.node} ${styles.nodeLeft}`}>
          <span className={styles.nodeId}>02</span>
          <span className={styles.nodeRole}>Follower</span>
        </div>
        <div className={`${styles.node} ${styles.nodeRight}`}>
          <span className={styles.nodeId}>03</span>
          <span className={styles.nodeRole}>Follower</span>
        </div>
      </div>
      <div className={styles.logLine}>
        <span><i />AppendEntries</span>
        <span>term 42</span>
        <span>committed</span>
      </div>
    </div>
  );
}

export default function Home(): ReactNode {
  const pageRef = useScrollReveal();

  return (
    <Layout
      title="Production-grade Multi-Raft for Rust"
      description="rr is a Rust framework for building strongly consistent, horizontally sharded, production-ready distributed systems.">
      <main className={styles.page} ref={pageRef}>
        <section className={styles.hero}>
          <div className={`container ${styles.heroGrid}`}>
            <div className={styles.heroCopy}>
              <div className={styles.eyebrow}><span />Rust-native distributed systems</div>
              <h1>From Raft consensus<br /><em>to production.</em></h1>
              <p className={styles.heroDescription}>
                One framework for multi-Raft orchestration, shard routing, client SDKs, and operations—so you can build the service, not the infrastructure.
              </p>
              <div className={styles.actions}>
                <Link className={styles.primaryButton} to="/docs/guide/getting-started">
                  Start building <span aria-hidden="true">↗</span>
                </Link>
                <Link className={styles.secondaryButton} to="/docs/architecture/overview">
                  Explore architecture
                </Link>
              </div>
              <div className={styles.heroMeta}>
                <span>Strong consistency</span>
                <span>Horizontal sharding</span>
                <span>Failure-tested</span>
              </div>
            </div>
            <ClusterVisual />
          </div>
        </section>

        <section className={styles.proofStrip} aria-label="Project facts">
          <div className={`container ${styles.proofGrid}`} data-reveal>
            <div><strong>11</strong><span>modular crates</span></div>
            <div><strong>2</strong><span>traits to integrate</span></div>
            <div><strong>3</strong><span>nodes in the quickstart</span></div>
            <div><strong>1</strong><span>integrated framework</span></div>
          </div>
        </section>

        <section className={styles.capabilities}>
          <div className="container">
            <div className={styles.sectionHeading} data-reveal>
              <span className={styles.sectionKicker}>One integrated stack</span>
              <h2>The layers between your trait and a running cluster.</h2>
              <p>Consensus, routing, clients, and operations share the same contracts and integration tests.</p>
            </div>
            <div className={styles.featureGrid}>
              {features.map((feature, index) => (
                <article
                  className={styles.featureCard}
                  key={feature.number}
                  data-reveal
                  style={{'--reveal-delay': `${index * 70}ms`} as CSSProperties}>
                  <div className={styles.featureTop}>
                    <span className={styles.featureIcon}><FeatureIcon name={feature.icon} /></span>
                    <span className={styles.featureNumber}>{feature.number}</span>
                  </div>
                  <h3>{feature.title}</h3>
                  <p>{feature.description}</p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className={styles.architectureSection}>
          <div className={`container ${styles.architectureGrid}`}>
            <div className={styles.architectureCopy} data-reveal>
              <span className={styles.sectionKicker}>A complete foundation</span>
              <h2>Your business logic.<br />Our distributed backbone.</h2>
              <p>
                Stop stitching consensus, networking, routing, and operations together by hand. rr gives each concern a clear boundary and a tested integration path.
              </p>
              <Link to="/docs/architecture/c4" className={styles.textLink}>
                View the C4 architecture <span aria-hidden="true">→</span>
              </Link>
            </div>
            <div className={styles.stackCard} data-reveal>
              <div className={styles.stackRow}><span>01</span><strong>Your application</strong><code>Service / StateMachine</code></div>
              <div className={styles.stackConnector} />
              <div className={styles.stackRow}><span>02</span><strong>Client &amp; routing</strong><code>rr-client · rr-router</code></div>
              <div className={styles.stackConnector} />
              <div className={styles.stackRow}><span>03</span><strong>Consensus &amp; storage</strong><code>rr-raft · rr-storage</code></div>
              <div className={styles.stackConnector} />
              <div className={styles.stackRow}><span>04</span><strong>Network &amp; operations</strong><code>rr-network · rr-admin</code></div>
            </div>
          </div>
        </section>

        <section className={styles.finalCta}>
          <div className={`container ${styles.finalCtaInner}`} data-reveal>
            <div>
              <span className={styles.sectionKicker}>Run it yourself</span>
              <h2>Start with a three-node cluster.</h2>
            </div>
            <Link className={styles.primaryButton} to="/docs/guide/getting-started">
              Read the quickstart <span aria-hidden="true">↗</span>
            </Link>
          </div>
        </section>
      </main>
    </Layout>
  );
}
