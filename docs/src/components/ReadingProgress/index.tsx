import {useEffect, useRef, type ReactNode} from 'react';
import styles from './styles.module.css';

export default function ReadingProgress(): ReactNode {
  const barRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let frame = 0;
    const update = () => {
      frame = 0;
      const root = document.documentElement;
      const available = root.scrollHeight - window.innerHeight;
      const progress = available > 0 ? Math.min(root.scrollTop / available, 1) : 0;
      barRef.current?.style.setProperty('--reading-progress', String(progress));
    };
    const onScroll = () => {
      if (frame === 0) {
        frame = window.requestAnimationFrame(update);
      }
    };
    update();
    window.addEventListener('scroll', onScroll, {passive: true});
    window.addEventListener('resize', onScroll, {passive: true});
    return () => {
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('resize', onScroll);
      if (frame !== 0) {
        window.cancelAnimationFrame(frame);
      }
    };
  }, []);

  return (
    <div className={styles.track} aria-hidden="true">
      <div className={styles.bar} ref={barRef} />
    </div>
  );
}
