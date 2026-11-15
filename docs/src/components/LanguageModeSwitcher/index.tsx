import type {ChangeEvent, ReactNode} from 'react';
import {
  languageModeEvent,
  languageModeStorageKey,
  useLanguageMode,
  type LanguageMode,
} from './languageMode';

export default function LanguageModeSwitcher(): ReactNode {
  const mode = useLanguageMode();

  function changeMode(event: ChangeEvent<HTMLSelectElement>) {
    const nextMode = event.target.value as LanguageMode;
    document.documentElement.dataset.languageMode = nextMode;
    try {
      window.localStorage.setItem(languageModeStorageKey, nextMode);
    } catch {
      // The selected mode still applies when storage is unavailable.
    }
    window.dispatchEvent(new CustomEvent(languageModeEvent, {detail: nextMode}));
  }

  return (
    <select
      className="language-mode-switcher"
      value={mode}
      onChange={changeMode}
      aria-label="文档语言 / Documentation language"
      title="文档语言 / Documentation language">
      <option value="zh">中文</option>
      <option value="en">English</option>
      <option value="both">同时 / Both</option>
    </select>
  );
}
