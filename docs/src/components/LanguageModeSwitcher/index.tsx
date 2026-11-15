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
      <span className="language-mode-switcher__languages" data-mode={mode} aria-hidden="true">
        <span className="language-mode-switcher__chinese">文</span>
        <span className="language-mode-switcher__english">En</span>
      </span>
      <svg className="language-mode-switcher__cycle" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
        <path d="M15.5 7A6 6 0 0 0 5 5.5L3 7.5M3 3.5v4h4M4.5 13A6 6 0 0 0 15 14.5l2-2M17 16.5v-4h-4" />
      </svg>
    </button>
  );
}
