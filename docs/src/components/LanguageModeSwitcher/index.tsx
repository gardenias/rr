import type {ReactNode} from 'react';
import {
  languageModeEvent,
  languageModeStorageKey,
  useLanguageMode,
  type LanguageMode,
} from './languageMode';

const modes: LanguageMode[] = ['zh', 'en', 'both'];
const labels: Record<LanguageMode, string> = {
  zh: '中文',
  en: 'English',
  both: '中文 + EN',
};
const nextLabels: Record<LanguageMode, string> = {
  zh: 'English',
  en: '中英双语',
  both: '中文',
};

export default function LanguageModeSwitcher(): ReactNode {
  const mode = useLanguageMode();
  const nextMode = modes[(modes.indexOf(mode) + 1) % modes.length];
  const label = `当前语言：${labels[mode]}。点击切换为${nextLabels[mode]} / Language: ${labels[mode]}. Switch to ${nextLabels[mode]}`;

  function cycleMode() {
    document.documentElement.dataset.languageMode = nextMode;
    try {
      window.localStorage.setItem(languageModeStorageKey, nextMode);
    } catch {
      // The selected mode still applies when storage is unavailable.
    }
    window.dispatchEvent(new CustomEvent(languageModeEvent, {detail: nextMode}));
  }

  return (
    <button
      type="button"
      className="clean-btn language-mode-switcher"
      onClick={cycleMode}
      aria-label={label}
      title={label}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true">
        <circle cx="12" cy="12" r="9" />
        <path d="M3 12h18M12 3c2.2 2.45 3.3 5.45 3.3 9S14.2 18.55 12 21c-2.2-2.45-3.3-5.45-3.3-9S9.8 5.45 12 3Z" />
      </svg>
      <span className="language-mode-switcher__label">{labels[mode]}</span>
      <svg className="language-mode-switcher__cycle" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
        <path d="M15.5 7A6 6 0 0 0 5 5.5L3 7.5M3 3.5v4h4M4.5 13A6 6 0 0 0 15 14.5l2-2M17 16.5v-4h-4" />
      </svg>
    </button>
  );
}
