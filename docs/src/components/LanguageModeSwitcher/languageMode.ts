import {useEffect, useState} from 'react';

export type LanguageMode = 'zh' | 'en' | 'both';

export const languageModeStorageKey = 'rr-docs-language-mode';
export const languageModeEvent = 'rr-language-mode-change';

export function isLanguageMode(value: string | undefined): value is LanguageMode {
  return value === 'zh' || value === 'en' || value === 'both';
}

export function selectBilingualText(text: string, mode: LanguageMode): string {
  if (mode === 'both') {
    return text;
  }
  for (const match of text.matchAll(/\s*\/\s*/g)) {
    if (match.index === undefined) {
      continue;
    }
    const left = text.slice(0, match.index).trim();
    const right = text.slice(match.index + match[0].length).trim();
    const leftChinese = /[\u3400-\u9fff]/.test(left);
    const rightChinese = /[\u3400-\u9fff]/.test(right);
    if (leftChinese !== rightChinese) {
      if (mode === 'zh') {
        return leftChinese ? left : right;
      }
      return leftChinese ? right : left;
    }
  }
  return text;
}

export function useLanguageMode(): LanguageMode {
  const [mode, setMode] = useState<LanguageMode>('both');

  useEffect(() => {
    const readMode = () => {
      let savedMode: string | undefined;
      try {
        savedMode = window.localStorage.getItem(languageModeStorageKey) ?? undefined;
      } catch {
        savedMode = undefined;
      }
      const documentMode = document.documentElement.dataset.languageMode;
      const nextMode = isLanguageMode(savedMode) ? savedMode : documentMode;
      setMode(isLanguageMode(nextMode) ? nextMode : 'both');
    };
    const handleModeChange = (event: Event) => {
      const nextMode = (event as CustomEvent<string>).detail;
      setMode(isLanguageMode(nextMode) ? nextMode : 'both');
    };
    readMode();
    window.addEventListener(languageModeEvent, handleModeChange);
    window.addEventListener('storage', readMode);
    return () => {
      window.removeEventListener(languageModeEvent, handleModeChange);
      window.removeEventListener('storage', readMode);
    };
  }, []);

  return mode;
}
