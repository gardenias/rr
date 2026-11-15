import {useEffect, useRef, type CSSProperties, type PointerEvent, type ReactNode} from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import {useLanguageMode, type LanguageMode} from '@site/src/components/LanguageModeSwitcher/languageMode';
import styles from './index.module.css';

type Translation = {
  zh: string;
  en: string;
};

type Feature = {
  number: string;
  title: Translation;
  description: Translation;
  icon: 'consensus' | 'scale' | 'operate' | 'rust';
};

type StackLayer = {
  number: string;
  title: Translation;
  description: Translation;
  crates: string;
  icon: 'application' | 'routing' | 'storage' | 'network';
};

function text(zh: string, en: string): Translation {
  return {zh, en};
}

function translate(translation: Translation, mode: LanguageMode): string {
  return mode === 'both' ? `${translation.zh} / ${translation.en}` : translation[mode];
}

function Bilingual({zh, en}: Translation): ReactNode {
  return (
    <>
      <span data-lang="zh" className="language-zh">{zh}</span>
      <span data-language-separator className="language-separator"> / </span>
      <span data-lang="en" className="language-en">{en}</span>
    </>
  );
}

const features: Feature[] = [
  {
    number: '01',
    title: text('内置共识能力', 'Consensus, built in'),
    description: text(
      '运行多个相互独立的 Raft 组，获得强一致性、Leader 选举、快照与成员变更保障。',
      'Run multiple independent Raft groups with strong consistency, leader election, snapshots, and membership safeguards.',
    ),
    icon: 'consensus',
  },
  {
    number: '02',
    title: text('按分片扩展', 'Scale by shards'),
    description: text(
      '将分区路由到不同 Raft 组，横向提升吞吐量，避免副本成为系统瓶颈。',
      'Route partitions across Raft groups and grow throughput horizontally without turning replicas into a bottleneck.',
    ),
    icon: 'scale',
  },
  {
    number: '03',
    title: text('开箱即可运维', 'Ready to operate'),
    description: text(
      '内置客户端重试、Prometheus 指标、结构化追踪、TLS、管理工具与部署模板。',
      'Ship with client retries, Prometheus metrics, structured tracing, TLS, admin tooling, and deployment templates.',
    ),
    icon: 'operate',
  },
  {
    number: '04',
    title: text('端到端 Rust', 'Rust end to end'),
    description: text(
      '将框架嵌入你的二进制程序，通过明确的 Service 与 StateMachine trait 接入业务逻辑。',
      'Embed the framework in your binary and connect business logic through explicit Service and StateMachine traits.',
    ),
    icon: 'rust',
  },
];

const stackLayers: StackLayer[] = [
  {
    number: '01',
    title: text('你的应用', 'Your application'),
    description: text('定义业务命令与状态转换', 'Define business commands and state transitions'),
    crates: 'Service / StateMachine',
    icon: 'application',
  },
  {
    number: '02',
    title: text('客户端与路由', 'Client & routing'),
    description: text('发现分片，将请求送往正确的 Leader', 'Discover shards and direct requests to the right leader'),
    crates: 'rr-client · rr-router',
    icon: 'routing',
  },
  {
    number: '03',
    title: text('共识与存储', 'Consensus & storage'),
    description: text('复制日志、提交条目并持久化状态', 'Replicate logs, commit entries, and persist state'),
    crates: 'rr-raft · rr-storage',
    icon: 'storage',
  },
  {
    number: '04',
    title: text('网络与运维', 'Network & operations'),
    description: text('连接节点，并观测和管理整个集群', 'Connect nodes, then observe and manage the cluster'),
    crates: 'rr-network · rr-admin',
    icon: 'network',
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
    rust: <path d="m8 5-5 7 5 7M16 5l5 7-5 7M14 3l-4 18" />,
  };

  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
      {paths[name]}
    </svg>
  );
}

function StackIcon({name}: {name: StackLayer['icon']}): ReactNode {
  const paths = {
    application: (
      <>
        <path d="m8 6-5 6 5 6M16 6l5 6-5 6M14 3l-4 18" />
      </>
    ),
    routing: (
      <>
        <circle cx="5" cy="5" r="2" />
        <circle cx="19" cy="5" r="2" />
        <circle cx="12" cy="19" r="2" />
        <path d="M7 5h10M6 7l5 10M18 7l-5 10" />
      </>
    ),
    storage: (
      <>
        <ellipse cx="12" cy="5" rx="8" ry="3" />
        <path d="M4 5v7c0 1.65 3.6 3 8 3s8-1.35 8-3V5M4 12v7c0 1.65 3.6 3 8 3s8-1.35 8-3v-7" />
      </>
    ),
    network: (
      <>
        <rect x="3" y="3" width="7" height="6" rx="1" />
        <rect x="14" y="15" width="7" height="6" rx="1" />
        <path d="M10 6h5a3 3 0 0 1 3 3v6M14 18H9a3 3 0 0 1-3-3V9" />
      </>
    ),
  };

  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.45" aria-hidden="true">
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

function ClusterVisual({mode}: {mode: LanguageMode}): ReactNode {
  return (
    <div
      className={styles.clusterCard}
      aria-label={translate(text('三节点 Raft 集群：一个 Leader、两个 Follower', 'A three-node Raft cluster with one leader and two followers'), mode)}
      onPointerMove={handleClusterPointer}
      onPointerLeave={resetClusterPointer}>
      <div className={styles.clusterHeader}>
        <div>
          <span className={styles.clusterEyebrow}><Bilingual zh="实时拓扑" en="Live topology" /></span>
          <div className={styles.clusterName}>production / group-01</div>
        </div>
        <span className={styles.health}><i /><Bilingual zh="健康" en="Healthy" /></span>
      </div>
      <div className={styles.topology}>
        <svg viewBox="0 0 520 240" role="img" aria-label={translate(text('相互连接的 Raft 节点', 'Connected Raft nodes'), mode)}>
          <path className={styles.linkLine} d="M260 112 112 184M260 112l148 72M112 184h296" />
          <path className={styles.activeLine} d="M260 112 112 184M260 112l148 72" />
        </svg>
        <div className={`${styles.node} ${styles.nodeLeader}`}>
          <span className={styles.nodeId}>01</span>
          <span className={styles.nodeRole}><Bilingual zh="领导者" en="Leader" /></span>
        </div>
        <div className={`${styles.node} ${styles.nodeLeft}`}>
          <span className={styles.nodeId}>02</span>
          <span className={styles.nodeRole}><Bilingual zh="跟随者" en="Follower" /></span>
        </div>
        <div className={`${styles.node} ${styles.nodeRight}`}>
          <span className={styles.nodeId}>03</span>
          <span className={styles.nodeRole}><Bilingual zh="跟随者" en="Follower" /></span>
        </div>
      </div>
      <div className={styles.logLine}>
        <span><i />AppendEntries</span>
        <span>term 42</span>
        <span><Bilingual zh="已提交" en="committed" /></span>
      </div>
    </div>
  );
}

export default function Home(): ReactNode {
  const pageRef = useScrollReveal();
  const mode = useLanguageMode();
  const title = text('面向 Rust 的生产级 Multi-Raft 框架', 'Production-grade Multi-Raft for Rust');
  const description = text(
    'rr 是一个用于构建强一致、水平分片、生产可用分布式系统的 Rust 框架。',
    'rr is a Rust framework for building strongly consistent, horizontally sharded, production-ready distributed systems.',
  );

  return (
    <Layout title={translate(title, mode)} description={translate(description, mode)}>
      <main className={styles.page} ref={pageRef}>
        <section className={styles.hero}>
          <div className={`container ${styles.heroGrid}`}>
            <div className={styles.heroCopy}>
              <div className={styles.eyebrow}><span /><Bilingual zh="Rust 原生分布式系统" en="Rust-native distributed systems" /></div>
              <h1>
                <Bilingual zh="从 Raft 共识" en="From Raft consensus" /><br />
                <em><Bilingual zh="到生产级系统。" en="to production." /></em>
              </h1>
              <p className={styles.heroDescription}>
                <Bilingual
                  zh="一个框架整合 Multi-Raft 编排、分片路由、客户端 SDK 与运维能力，让你专注业务服务，而非重复搭建基础设施。"
                  en="One framework for multi-Raft orchestration, shard routing, client SDKs, and operations—so you can build the service, not the infrastructure."
                />
              </p>
              <div className={styles.actions}>
                <Link className={styles.primaryButton} to="/docs/guide/getting-started">
                  <Bilingual zh="开始构建" en="Start building" /> <span aria-hidden="true">↗</span>
                </Link>
                <Link className={styles.secondaryButton} to="/docs/architecture/overview">
                  <Bilingual zh="探索架构" en="Explore architecture" />
                </Link>
              </div>
              <div className={styles.heroMeta}>
                <span><Bilingual zh="强一致性" en="Strong consistency" /></span>
                <span><Bilingual zh="水平分片" en="Horizontal sharding" /></span>
                <span><Bilingual zh="故障验证" en="Failure-tested" /></span>
              </div>
            </div>
            <ClusterVisual mode={mode} />
          </div>
        </section>

        <section className={styles.proofStrip} aria-label={translate(text('项目事实', 'Project facts'), mode)}>
          <div className={`container ${styles.proofGrid}`} data-reveal>
            <div><strong>11</strong><span><Bilingual zh="模块化 crate" en="modular crates" /></span></div>
            <div><strong>2</strong><span><Bilingual zh="待集成 trait" en="traits to integrate" /></span></div>
            <div><strong>3</strong><span><Bilingual zh="快速入门节点" en="nodes in the quickstart" /></span></div>
            <div><strong>1</strong><span><Bilingual zh="一体化框架" en="integrated framework" /></span></div>
          </div>
        </section>

        <section className={styles.capabilities}>
          <div className="container">
            <div className={styles.sectionHeading} data-reveal>
              <span className={styles.sectionKicker}><Bilingual zh="一体化技术栈" en="One integrated stack" /></span>
              <h2><Bilingual zh="从你的 trait，到运行中的集群。" en="The layers between your trait and a running cluster." /></h2>
              <p><Bilingual zh="共识、路由、客户端与运维共享同一套契约和集成测试。" en="Consensus, routing, clients, and operations share the same contracts and integration tests." /></p>
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
                  <h3><Bilingual {...feature.title} /></h3>
                  <p><Bilingual {...feature.description} /></p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className={styles.architectureSection}>
          <div className={`container ${styles.architectureGrid}`}>
            <div className={styles.architectureCopy} data-reveal>
              <span className={styles.sectionKicker}><Bilingual zh="完整的分布式底座" en="A complete foundation" /></span>
              <h2><Bilingual zh="专注业务逻辑。" en="Your business logic." /><br /><Bilingual zh="分布式底座交给我们。" en="Our distributed backbone." /></h2>
              <p>
                <Bilingual
                  zh="无需再手工拼装共识、网络、路由与运维能力。rr 为每个关注点提供清晰边界和经过验证的集成路径。"
                  en="Stop stitching consensus, networking, routing, and operations together by hand. rr gives each concern a clear boundary and a tested integration path."
                />
              </p>
              <Link to="/docs/architecture/c4" className={styles.textLink}>
                <Bilingual zh="查看 C4 架构" en="View the C4 architecture" /> <span aria-hidden="true">→</span>
              </Link>
            </div>
            <div className={styles.stackCard} data-reveal>
              <div className={styles.stackHeader}>
                <span className={styles.stackHeaderLabel}><i /><Bilingual zh="端到端请求链路" en="End-to-end request path" /></span>
                <span className={styles.stackHeaderMeta}>01 — 04</span>
              </div>
              <div className={styles.stackRows}>
                {stackLayers.map((layer, index) => (
                  <article
                    className={styles.stackRow}
                    key={layer.number}
                    style={{'--stack-delay': `${index * 180}ms`} as CSSProperties}>
                    <div className={styles.stackRail}><span>{layer.number}</span></div>
                    <span className={styles.stackIcon}><StackIcon name={layer.icon} /></span>
                    <div className={styles.stackContent}>
                      <h3><Bilingual {...layer.title} /></h3>
                      <p><Bilingual {...layer.description} /></p>
                    </div>
                    <code>{layer.crates}</code>
                  </article>
                ))}
              </div>
              <div className={styles.stackFooter}>
                <span><Bilingual zh="你的 trait" en="Your traits" /></span>
                <i />
                <span><Bilingual zh="生产级集群" en="Production cluster" /></span>
              </div>
            </div>
          </div>
        </section>

        <section className={styles.finalCta}>
          <div className={`container ${styles.finalCtaInner}`} data-reveal>
            <div>
              <span className={styles.sectionKicker}><Bilingual zh="亲手运行" en="Run it yourself" /></span>
              <h2><Bilingual zh="从三节点集群开始。" en="Start with a three-node cluster." /></h2>
            </div>
            <Link className={styles.primaryButton} to="/docs/guide/getting-started">
              <Bilingual zh="阅读快速入门" en="Read the quickstart" /> <span aria-hidden="true">↗</span>
            </Link>
          </div>
        </section>
      </main>
    </Layout>
  );
}
