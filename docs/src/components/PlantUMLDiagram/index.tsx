import {useEffect, useRef, useState, type ReactNode} from 'react';
import useBaseUrl from '@docusaurus/useBaseUrl';
import manifest from '@site/src/generated/plantuml-manifest.json';
import styles from './styles.module.css';

type PlantUMLDiagramProps = {
  source: string;
};

const diagramManifest = manifest as Record<string, string>;

function diagramTitle(source: string): string {
  const title = source.match(/^title\s+(.+)$/im)?.[1]?.trim();
  return title || 'PlantUML architecture diagram';
}

export default function PlantUMLDiagram({source}: PlantUMLDiagramProps): ReactNode {
  const [status, setStatus] = useState<'loading' | 'loaded' | 'error'>('loaded');
  const imageRef = useRef<HTMLImageElement>(null);
  const normalizedSource = source.replace(/\r\n?/g, '\n').trimEnd();
  const diagramPath = diagramManifest[normalizedSource];
  const diagramUrl = useBaseUrl(diagramPath ? `/${diagramPath}` : '/diagrams/missing.svg');
  const title = diagramTitle(source);

  useEffect(() => {
    const image = imageRef.current;
    if (!image) {
      return;
    }
    if (image.complete) {
      setStatus(image.naturalWidth > 0 ? 'loaded' : 'error');
    } else {
      setStatus('loading');
    }
  }, [diagramUrl]);

  return (
    <figure className={styles.figure} aria-busy={status === 'loading'}>
      <div className={styles.toolbar}>
        <span className={styles.label}>架构图 / Architecture diagram</span>
        <a href={diagramUrl} target="_blank" rel="noreferrer">
          查看原图 / Open full size
        </a>
      </div>
      <div className={styles.canvas}>
        {status === 'loading' && <span className={styles.status}>加载图示… / Loading diagram…</span>}
        {status === 'error' && (
          <span className={styles.status} role="alert">
            图示资源缺失或无法加载，请运行 npm run diagrams。 / Diagram asset missing or unavailable; run npm run diagrams.
          </span>
        )}
        <img
          key={diagramUrl}
          ref={imageRef}
          className={styles.image}
          data-state={status}
          src={diagramUrl}
          alt={title}
          loading="lazy"
          onLoad={() => setStatus('loaded')}
          onError={() => setStatus('error')}
        />
      </div>
      <details className={styles.source}>
        <summary>查看 PlantUML 源码 / View PlantUML source</summary>
        <pre>
          <code>{source}</code>
        </pre>
      </details>
    </figure>
  );
}
