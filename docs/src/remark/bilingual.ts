import type {Root} from 'mdast';
import type {Plugin} from 'unified';

type Language = 'zh' | 'en';
type InlineLayout = 'inline' | 'block';
type MdastAttribute = {
  type: string;
  name: string;
  value: string;
};
type MdastNode = {
  type: string;
  name?: string;
  value?: string;
  alt?: string;
  attributes?: MdastAttribute[] | Record<string, string>;
  children?: MdastNode[];
};

const chinesePattern = /[\u3400-\u9fff]/;
const latinPattern = /[A-Za-z]/;
const inlineContainers = new Set(['strong', 'emphasis', 'delete', 'link']);

function textContent(node: MdastNode): string {
  return `${node.value ?? ''}${node.alt ?? ''}${(node.children ?? []).map(textContent).join('')}`;
}

function proseContent(node: MdastNode): string {
  if (node.type === 'inlineCode' || node.type === 'code') {
    return '';
  }
  return `${node.value ?? ''}${node.alt ?? ''}${(node.children ?? []).map(proseContent).join('')}`;
}

function languageOf(node: MdastNode): Language | undefined {
  const text = proseContent(node);
  if (chinesePattern.test(text)) {
    return 'zh';
  }
  return latinPattern.test(text) ? 'en' : undefined;
}

function jsxNode(
  name: 'div' | 'span' | 'br',
  children: MdastNode[],
  attributes: Record<string, string>,
): MdastNode {
  return {
    type: name === 'div' ? 'mdxJsxFlowElement' : 'mdxJsxTextElement',
    name,
    children,
    attributes: Object.entries(attributes).map(([attributeName, value]) => ({
      type: 'mdxJsxAttribute',
      name: attributeName,
      value,
    })),
  };
}

function languageSpan(language: Language, children: MdastNode[], layout: InlineLayout): MdastNode {
  return jsxNode('span', children, {
    'data-lang': language,
    'data-lang-inline': layout,
    className: `language-${language}`,
  });
}

function languageSeparator(children: MdastNode[]): MdastNode {
  return jsxNode('span', children, {
    'data-language-separator': '',
    className: 'language-separator',
  });
}

function isLineBreak(node: MdastNode): boolean {
  return (
    (node.type === 'mdxJsxTextElement' && node.name === 'br') ||
    (node.type === 'html' && /^<br\s*\/?\s*>$/i.test(node.value ?? ''))
  );
}

function attributeValue(node: MdastNode, name: string): string | undefined {
  const attributes = node.attributes;
  if (!attributes) {
    return undefined;
  }
  return Array.isArray(attributes)
    ? attributes.find((attribute) => attribute.name === name)?.value
    : attributes[name];
}

function markedLanguage(node: MdastNode): Language | undefined {
  const value = attributeValue(node, 'data-lang');
  return value === 'zh' || value === 'en' ? value : undefined;
}

function isLanguageSeparator(node: MdastNode): boolean {
  return attributeValue(node, 'data-language-separator') !== undefined;
}

function isTechnicalSlash(value: string, matchIndex: number, match: string): boolean {
  const slashIndex = matchIndex + match.indexOf('/');
  const before = value[slashIndex - 1] ?? '';
  const after = value[slashIndex + 1] ?? '';
  return /[A-Za-z0-9]/.test(before) && /[A-Za-z0-9]/.test(after);
}

type ExplicitInlineSplit = {
  groups: [MdastNode[], MdastNode[]];
  separator: 'slash' | 'break';
};

function explicitInlineSplit(
  children: MdastNode[],
  allowSlash = true,
  allowInlineCodeSlash = false,
): ExplicitInlineSplit | undefined {
  let groups: MdastNode[][] = [];
  let separator: ExplicitInlineSplit['separator'] = 'break';

  if (children.some(isLineBreak)) {
    groups = [[]];
    for (const child of children) {
      if (isLineBreak(child)) {
        groups.push([]);
      } else {
        groups.at(-1)?.push(child);
      }
    }
  } else if (allowSlash) {
    outer: for (const [childIndex, child] of children.entries()) {
      if (child.type !== 'text' || !child.value) {
        continue;
      }
      const betweenInlineCode = !allowInlineCodeSlash &&
        child.value.trim() === '/' &&
        children[childIndex - 1]?.type === 'inlineCode' &&
        children[childIndex + 1]?.type === 'inlineCode';
      for (const match of child.value.matchAll(/\s*\/\s*/g)) {
        if (
          match.index === undefined ||
          betweenInlineCode ||
          isTechnicalSlash(child.value, match.index, match[0])
        ) {
          continue;
        }
        const left = child.value.slice(0, match.index);
        const right = child.value.slice(match.index + match[0].length);
        const leftChildren = [...children.slice(0, childIndex), {...child, value: left}];
        const rightChildren = [{...child, value: right}, ...children.slice(childIndex + 1)];
        const leftLanguage = languageOf({type: 'group', children: leftChildren});
        const rightLanguage = languageOf({type: 'group', children: rightChildren});
        if (
          (leftLanguage === 'zh' && rightLanguage === 'en') ||
          (leftLanguage === 'en' && rightLanguage === 'zh')
        ) {
          groups = [leftChildren, rightChildren];
          separator = 'slash';
          break outer;
        }
      }
    }
  }

  if (groups.length !== 2) {
    return undefined;
  }
  const firstLanguage = languageOf({type: 'group', children: groups[0]});
  const secondLanguage = languageOf({type: 'group', children: groups[1]});
  if (
    !((firstLanguage === 'zh' && secondLanguage === 'en') ||
      (firstLanguage === 'en' && secondLanguage === 'zh'))
  ) {
    return undefined;
  }
  return {groups: groups as [MdastNode[], MdastNode[]], separator};
}

function takeSharedSuffix(children: MdastNode[]): {
  children: MdastNode[];
  suffix: MdastNode[];
} {
  let linkIndex = children.length - 1;
  const last = children[linkIndex];
  if (last?.type === 'text' && /^[\s,.;，。；]*$/.test(last.value ?? '')) {
    linkIndex -= 1;
  }

  if (children[linkIndex]?.type === 'link') {
    const suffix = children.slice(linkIndex);
    const remaining = children.slice(0, linkIndex);
    const before = remaining.at(-1);
    if (before?.type === 'text') {
      const value = before.value ?? '';
      const arrow = value.match(/(\s*(?:→|↔|->|=>)\s*)$/);
      if (arrow?.index !== undefined) {
        remaining[remaining.length - 1] = {...before, value: value.slice(0, arrow.index)};
        suffix.unshift({...before, value: arrow[0]});
      } else {
        const whitespace = value.match(/\s+$/);
        if (whitespace?.index !== undefined) {
          remaining[remaining.length - 1] = {...before, value: value.slice(0, whitespace.index)};
          suffix.unshift({...before, value: whitespace[0]});
        }
      }
    }
    return {children: remaining, suffix};
  }

  const before = children.at(-1);
  if (before?.type === 'text') {
    const value = before.value ?? '';
    const arrow = value.match(/(\s*(?:→|↔|->|=>)\s*)$/);
    if (arrow?.index !== undefined) {
      return {
        children: [...children.slice(0, -1), {...before, value: value.slice(0, arrow.index)}],
        suffix: [{...before, value: arrow[0]}],
      };
    }
  }
  return {children, suffix: []};
}

function takeSharedCodeSuffix(children: MdastNode[]): {
  children: MdastNode[];
  suffix: MdastNode[];
} {
  let start = children.length;
  while (start > 0 && languageOf({type: 'group', children: [children[start - 1]]}) === undefined) {
    start -= 1;
  }
  const suffix = children.slice(start);
  if (!suffix.some((child) => child.type === 'inlineCode')) {
    return {children, suffix: []};
  }
  return {children: children.slice(0, start), suffix};
}

function wrapInlineContainer(container: MdastNode, children: MdastNode[]): MdastNode[] {
  return children.map((child) => {
    if (!markedLanguage(child) || !child.children) {
      return child;
    }
    return {...child, children: [{...container, children: child.children}]};
  });
}

type LanguageBuckets = Record<Language, MdastNode[]>;

function appendTransformed(
  children: MdastNode[],
  buckets: LanguageBuckets,
  shared: MdastNode[],
): void {
  for (const child of children) {
    const language = markedLanguage(child);
    if (language) {
      buckets[language].push(...(child.children ?? []));
    } else if (!isLanguageSeparator(child)) {
      shared.push(child);
    }
  }
}

function appendOuter(children: MdastNode[], buckets: LanguageBuckets, shared: MdastNode[]): void {
  const language = languageOf({type: 'group', children});
  if (language) {
    buckets[language].push(...children);
  } else {
    shared.push(...children);
  }
}

function languagePair(
  firstLanguage: Language,
  secondLanguage: Language,
  buckets: LanguageBuckets,
  separator: MdastNode,
  layout: InlineLayout,
  sharedPrefix: MdastNode[],
  sharedSuffix: MdastNode[],
): MdastNode[] {
  return [
    ...sharedPrefix,
    languageSpan(firstLanguage, buckets[firstLanguage], layout),
    separator,
    languageSpan(secondLanguage, buckets[secondLanguage], layout),
    ...sharedSuffix,
  ];
}

function splitInline(
  children: MdastNode[],
  layout: InlineLayout,
  allowSlash = true,
  allowInlineCodeSlash = false,
  shareCodeSuffix = true,
): MdastNode[] {
  if (children.some(hasLanguageMarker)) {
    return children;
  }

  const explicit = explicitInlineSplit(children, allowSlash, allowInlineCodeSlash);
  if (explicit) {
    const firstLanguage = languageOf({type: 'group', children: explicit.groups[0]});
    const secondLanguage = languageOf({type: 'group', children: explicit.groups[1]});
    if (!firstLanguage || !secondLanguage) {
      return children;
    }
    const candidate = takeSharedSuffix(explicit.groups[1]);
    const suffixExtracted = languageOf({type: 'group', children: candidate.children}) === secondLanguage
      ? candidate
      : {children: explicit.groups[1], suffix: []};
    const codeExtracted = shareCodeSuffix
      ? takeSharedCodeSuffix(suffixExtracted.children)
      : {children: suffixExtracted.children, suffix: []};
    const extracted = {
      children: codeExtracted.children,
      suffix: [...codeExtracted.suffix, ...suffixExtracted.suffix],
    };
    const first = splitInline(explicit.groups[0], 'inline', false);
    const second = splitInline(extracted.children, 'inline', false);
    const buckets: LanguageBuckets = {zh: [], en: []};
    const sharedPrefix: MdastNode[] = [];
    const sharedSuffix: MdastNode[] = [...extracted.suffix];
    if (first === explicit.groups[0]) {
      buckets[firstLanguage].push(...first);
    } else {
      appendTransformed(first, buckets, sharedPrefix);
    }
    if (second === extracted.children) {
      buckets[secondLanguage].push(...second);
    } else {
      appendTransformed(second, buckets, sharedSuffix);
    }
    const separator = explicit.separator === 'slash'
      ? languageSeparator([{type: 'text', value: ' / '}])
      : languageSeparator([jsxNode('br', [], {})]);
    return languagePair(
      firstLanguage,
      secondLanguage,
      buckets,
      separator,
      explicit.separator === 'slash' ? 'inline' : layout,
      sharedPrefix,
      sharedSuffix,
    );
  }

  for (const [childIndex, child] of children.entries()) {
    if (!inlineContainers.has(child.type) || !child.children) {
      continue;
    }
    const nested = splitInline(
      child.children,
      'inline',
      true,
      child.type === 'link',
      child.type !== 'link',
    );
    if (nested === child.children) {
      continue;
    }
    const wrapped = wrapInlineContainer(child, nested);
    const languages = wrapped.map(markedLanguage).filter((language) => language !== undefined);
    const firstLanguage = languages[0];
    const secondLanguage = languages.find((language) => language !== firstLanguage);
    if (!firstLanguage || !secondLanguage) {
      continue;
    }
    const extracted = takeSharedSuffix(children.slice(childIndex + 1));
    const buckets: LanguageBuckets = {zh: [], en: []};
    const sharedPrefix: MdastNode[] = [];
    const sharedSuffix: MdastNode[] = [...extracted.suffix];
    appendOuter(children.slice(0, childIndex), buckets, sharedPrefix);
    appendTransformed(wrapped, buckets, sharedPrefix);
    const remaining = splitInline(extracted.children, 'inline');
    if (remaining === extracted.children) {
      appendOuter(remaining, buckets, sharedSuffix);
    } else {
      appendTransformed(remaining, buckets, sharedSuffix);
    }
    const separator = wrapped.find(isLanguageSeparator)
      ?? languageSeparator([{type: 'text', value: ' / '}]);
    return languagePair(
      firstLanguage,
      secondLanguage,
      buckets,
      separator,
      'inline',
      sharedPrefix,
      sharedSuffix,
    );
  }
  return children;
}

function splitParagraph(children: MdastNode[]): MdastNode[][] | undefined {
  for (const [childIndex, child] of children.entries()) {
    if (child.type !== 'text' || !child.value) {
      continue;
    }
    for (const match of child.value.matchAll(/\r?\n+/g)) {
      if (match.index === undefined) {
        continue;
      }
      const leftChildren = [
        ...children.slice(0, childIndex),
        {...child, value: child.value.slice(0, match.index).replace(/\s*\/\s*$/, '')},
      ];
      const rightChildren = [
        {...child, value: child.value.slice(match.index + match[0].length).replace(/^\s*\/\s*/, '')},
        ...children.slice(childIndex + 1),
      ];
      const leftLanguage = languageOf({type: 'group', children: leftChildren});
      const rightLanguage = languageOf({type: 'group', children: rightChildren});
      if (
        (leftLanguage === 'zh' && rightLanguage === 'en') ||
        (leftLanguage === 'en' && rightLanguage === 'zh')
      ) {
        if (explicitInlineSplit(leftChildren) || explicitInlineSplit(rightChildren)) {
          continue;
        }
        return [leftChildren, rightChildren];
      }
    }
  }
  return undefined;
}

function isSharedTechnicalListItem(paragraph: MdastNode): boolean {
  const text = proseContent(paragraph).trim();
  const children = paragraph.children ?? [];
  const hasLink = children.some((child) => child.type === 'link');
  const textOutsideLinks = children
    .filter((child) => child.type !== 'link')
    .map(textContent)
    .join('');
  return !/\s/.test(text) || (hasLink && !/[.!?]/.test(textOutsideLinks));
}

function hasLanguageMarker(node: MdastNode): boolean {
  return markedLanguage(node) !== undefined || (node.children?.some(hasLanguageMarker) ?? false);
}

function isPureLinkParagraph(paragraph: MdastNode): boolean {
  const meaningful = (paragraph.children ?? []).filter(
    (child) => child.type !== 'text' || (child.value ?? '').trim().length > 0,
  );
  if (meaningful.length !== 1) {
    return false;
  }
  let child = meaningful[0];
  while (child.type !== 'link' && ['strong', 'emphasis', 'delete'].includes(child.type)) {
    const nested = (child.children ?? []).filter(
      (node) => node.type !== 'text' || (node.value ?? '').trim().length > 0,
    );
    if (nested.length !== 1) {
      return false;
    }
    child = nested[0];
  }
  return child.type === 'link';
}

function transformChildren(parent: MdastNode): void {
  if (!parent.children) {
    return;
  }
  parent.children = parent.children.map((child) => {
    if (markedLanguage(child)) {
      return child;
    }
    if (child.type === 'heading') {
      return {...child, children: splitInline(child.children ?? [], 'inline')};
    }
    if (child.type === 'paragraph') {
      const children = child.children ?? [];
      const blockGroups = splitParagraph(children);
      if (blockGroups) {
        return jsxNode(
          'div',
          blockGroups.map((group) => {
            const language = languageOf({type: 'group', children: group}) ?? 'en';
            const paragraph = {...child, children: splitInline(group, 'block')};
            return jsxNode('div', [paragraph], {
              'data-lang': language,
              className: `language-${language}`,
            });
          }),
          {className: 'language-pair'},
        );
      }
      if (explicitInlineSplit(children)) {
        const explicitInline = splitInline(children, 'block');
        if (explicitInline.some(hasLanguageMarker)) {
          return {...child, children: explicitInline};
        }
      }
      const inline = splitInline(children, 'block');
      const updated = {...child, children: inline};
      if (inline.some(hasLanguageMarker) || isPureLinkParagraph(updated)) {
        return updated;
      }
      const language = languageOf(updated);
      if (language === 'en' && parent.type === 'listItem' && isSharedTechnicalListItem(updated)) {
        return updated;
      }
      return language ? jsxNode('div', [updated], {'data-lang': language}) : updated;
    }
    if (child.type === 'tableCell') {
      return {...child, children: splitInline(child.children ?? [], 'block', true, true)};
    }
    transformChildren(child);
    return child;
  });
}

const remarkBilingual: Plugin<[], Root> = () => (tree) => {
  transformChildren(tree as unknown as MdastNode);
};

export default remarkBilingual;
