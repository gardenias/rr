import type {ReactNode} from 'react';
import OriginalAdmonition from '@theme-original/Admonition';
import type {Props} from '@theme/Admonition';

function bilingualTitle(title: string): ReactNode {
  for (const match of title.matchAll(/\s*\/\s*/g)) {
    if (match.index === undefined) {
      continue;
    }
    const slashIndex = match.index + match[0].indexOf('/');
    const before = title.slice(0, slashIndex).trimEnd().at(-1) ?? '';
    const after = title.slice(slashIndex + 1).trimStart()[0] ?? '';
    if (/[A-Za-z0-9]/.test(before) && /[A-Za-z0-9]/.test(after)) {
      continue;
    }
    const chinese = title.slice(0, match.index);
    const english = title.slice(match.index + match[0].length);
    if (/[\u3400-\u9fff]/.test(chinese) && !/[\u3400-\u9fff]/.test(english)) {
      return (
        <>
          <span data-lang="zh" className="language-zh">{chinese}</span>
          <span data-language-separator className="language-separator"> / </span>
          <span data-lang="en" className="language-en">{english}</span>
        </>
      );
    }
  }
  return title;
}

export default function Admonition(props: Props): ReactNode {
  if (typeof props.title !== 'string') {
    return <OriginalAdmonition {...props} />;
  }
  return <OriginalAdmonition {...props} title={bilingualTitle(props.title)} />;
}
