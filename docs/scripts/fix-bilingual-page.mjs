import {randomBytes} from 'node:crypto';
import {lstat, open, readFile, realpath, rename, unlink} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const docsDir = path.join(siteDir, 'docs');
const chinesePattern = /[\u3400-\u9fff]/;
const latinPattern = /[A-Za-z]/;
const usage = 'Usage: fix-bilingual-page <page> [--write]';

function prose(text) {
  return text
    .replace(/`+[^`]*`+/g, ' ')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/<[^>]+>/g, ' ')
    .replace(/https?:\/\/\S+/g, ' ');
}

function language(text) {
  const value = prose(text);
  if (chinesePattern.test(value)) {
    return 'zh';
  }
  return latinPattern.test(value) ? 'en' : undefined;
}

function maskInlineCode(text) {
  return text.replace(/(`+)(?:\\.|(?!\1)[^\\])*\1/g, (match) => ' '.repeat(match.length));
}

function maskLinkTargets(text) {
  return text
    .replace(/(\[(?:\\.|[^\]\\])+\]\()((?:\\.|[^)\\])*)(\))/g, (whole, start, target, end) =>
      `${start}${' '.repeat(target.length)}${end}`,
    )
    .replace(/https?:\/\/\S+/g, (match) => ' '.repeat(match.length));
}

function languageSeparators(text) {
  const masked = maskLinkTargets(maskInlineCode(text));
  const matches = [];
  for (const match of masked.matchAll(/\s*\/\s*/g)) {
    const before = text[match.index - 1] ?? '';
    const after = text[match.index + match[0].length] ?? '';
    if (!/\s/.test(match[0]) && /[A-Za-z0-9]/.test(before) && /[A-Za-z0-9]/.test(after)) {
      continue;
    }
    matches.push(match);
  }
  return matches;
}

function bilingualSeparators(text) {
  const matches = [];
  for (const match of languageSeparators(text)) {
    const left = text.slice(0, match.index);
    const right = text.slice(match.index + match[0].length);
    if (language(left) === 'zh' && language(right) === 'en') {
      matches.push({index: match.index, length: match[0].length, left, right});
    }
  }
  return matches;
}

function splitBilingual(text) {
  const separators = bilingualSeparators(text);
  if (separators.length !== 1) {
    return undefined;
  }
  const [{left, right}] = separators;
  return {left: left.trim(), right: right.trim()};
}

function splitLinkLabel(label) {
  const wrapper = label.match(/^(\*\*|__)([\s\S]+)\1$/);
  const body = wrapper ? wrapper[2] : label;
  const pair = splitBilingual(body);
  if (!pair) {
    return undefined;
  }
  if (!wrapper) {
    return pair;
  }
  return {
    left: `${wrapper[1]}${pair.left}${wrapper[1]}`,
    right: `${wrapper[1]}${pair.right}${wrapper[1]}`,
  };
}

function fixLinks(line, multiline) {
  const pattern = /(?<!!)\[((?:\\.|[^\]\\])+)\]\(((?:\\.|[^)\\])*)\)/g;
  let issue;
  const output = line.replace(pattern, (whole, label, target) => {
    if (!/[\u3400-\u9fff]/.test(prose(label)) || languageSeparators(label).length === 0) {
      return whole;
    }
    const pair = splitLinkLabel(label);
    if (!pair) {
      issue = 'ambiguous bilingual link label';
      return whole;
    }
    const separator = multiline ? '<br />' : ' / ';
    return `[${pair.left}](${target})${separator}[${pair.right}](${target})`;
  });
  return {output, issue};
}

function matchingEnglishPrefix(lines, lineIndex, englishLabel) {
  for (let index = lineIndex + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line.trim()) {
      continue;
    }
    const match = line.match(/^\s*(?:>\s*)?(?:\*\*|__)([^*]+?)(?:\*\*|__)\s*:/);
    return match?.[1].trim() === englishLabel;
  }
  return false;
}

function fixPrefix(content, lines, lineIndex, multiline, paragraph) {
  const match = content.match(/^(\*\*|__)([^*\n]+?\s*\/\s*[^*\n]+?)\1([:：])(.*)$/);
  if (!match || languageSeparators(match[2]).length === 0) {
    return {output: content};
  }
  const pair = splitBilingual(match[2]);
  if (!pair) {
    return {output: content, issue: 'ambiguous bilingual strong prefix'};
  }

  const chinesePrefix = `${match[1]}${pair.left}${match[1]}`;
  const englishPrefix = `${match[1]}${pair.right}${match[1]}`;
  const rest = match[4];
  const breakMatch = rest.match(/<br\s*\/?\s*>/i);
  let bodies;
  if (breakMatch?.index !== undefined) {
    const left = rest.slice(0, breakMatch.index);
    const right = rest.slice(breakMatch.index + breakMatch[0].length);
    if (language(left) === 'zh' && language(right) === 'en') {
      bodies = {left, right};
    }
  } else {
    const separators = bilingualSeparators(rest);
    if (separators.length === 1) {
      bodies = separators[0];
    } else if (separators.length > 1) {
      return {output: content, issue: 'multiple bilingual separators after strong prefix'};
    }
  }

  if (bodies) {
    const chinese = `${chinesePrefix}：${bodies.left.trim()}`;
    const english = `${englishPrefix}: ${bodies.right.trim()}`;
    const separator = paragraph ? '\n\n' : '<br />';
    return {output: `${chinese}${separator}${english}`, changed: true};
  }

  if (language(rest) === 'zh' && paragraph && matchingEnglishPrefix(lines, lineIndex, pair.right)) {
    return {output: `${chinesePrefix}：${rest.trim()}`, changed: true};
  }
  return {output: content, issue: 'missing or ambiguous English body after strong prefix'};
}

function fixStrongLabels(line) {
  let issue;
  const output = line.replace(/(\*\*|__)([^*\n]+?\s*\/\s*[^*\n]+?)\1/g, (whole, marker, label) => {
    if (languageSeparators(label).length === 0) {
      return whole;
    }
    const pair = splitBilingual(label);
    if (!pair) {
      issue = 'ambiguous bilingual strong label';
      return whole;
    }
    return `${marker}${pair.left}${marker} / ${marker}${pair.right}${marker}`;
  });
  return {output, issue};
}

function hasEnglishContinuation(lines, lineIndex) {
  for (let index = lineIndex + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line.trim()) {
      break;
    }
    if (/^\s{2,}\S/.test(line)) {
      if (language(line) === 'en') {
        return true;
      }
      continue;
    }
    break;
  }
  return false;
}

function trailingListSeparator(line, nextLine) {
  const list = line.match(/^((?:\s*>\s*)*\s*(?:[-*+]|\d+[.)])\s+)(.*)$/);
  if (!list || !/^\s{2,}\S/.test(nextLine ?? '') || language(nextLine) !== 'en') {
    return undefined;
  }
  const content = list[2];
  const separators = languageSeparators(content);
  if (separators.length !== 1) {
    return undefined;
  }
  const [separator] = separators;
  if (separator.index + separator[0].length !== content.length) {
    return undefined;
  }
  const left = content.slice(0, separator.index).trimEnd();
  const slashIndex = separator.index + separator[0].indexOf('/');
  const before = content.slice(0, slashIndex).trimEnd().at(-1) ?? '';
  const after = nextLine.trimStart()[0] ?? '';
  if (/[A-Za-z0-9]/.test(before) && /[A-Za-z0-9]/.test(after)) {
    return undefined;
  }
  return language(left) === 'zh' ? `${list[1]}${left}<br />` : undefined;
}

function leadingContinuationSeparator(lines, lineIndex) {
  const match = lines[lineIndex].match(/^(\s{2,})\s*\/\s*(\S.*)$/);
  const previous = lines[lineIndex - 1]?.match(/^(?:\s*>\s*)*\s*(?:[-*+]|\d+[.)])\s+(.*)$/);
  if (!match || !previous || language(previous[1]) !== 'zh' || language(match[2]) !== 'en') {
    return undefined;
  }
  if (languageSeparators(lines[lineIndex]).length !== 1 || /<br\s*\/?\s*>/i.test(previous[1])) {
    return undefined;
  }
  const before = previous[1].trimEnd().at(-1) ?? '';
  const after = match[2][0] ?? '';
  if (/[A-Za-z0-9]/.test(before) && /[A-Za-z0-9]/.test(after)) {
    return undefined;
  }
  return `${match[1]}<br />${match[2].trimStart()}`;
}

function transformLine(line, lines, lineIndex) {
  const issues = [];
  const list = line.match(/^((?:\s*>\s*)*\s*(?:[-*+]|\d+[.)])\s+)(.*)$/);
  const table = /^\s*\|.*\|\s*$/.test(line);
  const multiline = Boolean(list || table);
  const prefix = list?.[1] ?? '';
  let content = list?.[2] ?? line;

  const strongPrefix = /^(?:\*\*|__)[^*\n]+?\s*\/\s*[^*\n]+?(?:\*\*|__)[:：]/.test(content);
  if (list && !strongPrefix && bilingualSeparators(content).length > 1) {
    return {output: line, issues: ['multiple bilingual list separators']};
  }

  const links = fixLinks(content, multiline);
  content = links.output;
  if (links.issue) {
    issues.push(links.issue);
  }

  const paragraph = !list && !table && /^\s*(?:>\s*)?(?:\*\*|__)/.test(content);
  const prefixed = fixPrefix(content, lines, lineIndex, multiline, paragraph);
  content = prefixed.output;
  if (prefixed.issue) {
    issues.push(prefixed.issue);
  }
  if (prefixed.changed) {
    return {output: `${prefix}${content}`, issues};
  }

  if (!prefixed.issue) {
    const strong = fixStrongLabels(content);
    content = strong.output;
    if (strong.issue) {
      issues.push(strong.issue);
    }
  }
  const crossLine = list && !content.includes('<br')
    ? trailingListSeparator(`${prefix}${content}`, lines[lineIndex + 1])
    : undefined;
  if (crossLine) {
    return {output: crossLine, issues};
  }
  if (list && !content.includes('<br')) {
    const separators = bilingualSeparators(content);
    if (separators.length === 1) {
      const [{left, right}] = separators;
      content = `${left.trimEnd()}<br />${right.trimStart()}`;
    } else if (separators.length > 1) {
      issues.push('multiple bilingual list separators');
    } else if (languageSeparators(content).length > 0 && chinesePattern.test(prose(content)) && latinPattern.test(prose(content))) {
      issues.push('ambiguous bilingual list separator');
    }
  }

  const output = `${prefix}${content}`;
  if (
    list &&
    language(content) === 'zh' &&
    !/<br\s*\/?\s*>/i.test(content) &&
    !hasEnglishContinuation(lines, lineIndex) &&
    !issues.length
  ) {
    issues.push('missing English translation for Chinese list item');
  }
  return {output, issues};
}

function diagramIssue(line, fence) {
  if (fence === 'plantuml') {
    for (const match of line.matchAll(/"([^"\n]*)"/g)) {
      if (chinesePattern.test(match[1]) && !latinPattern.test(match[1])) {
        return 'Chinese-only PlantUML label needs an English translation';
      }
    }
  } else if (chinesePattern.test(line) && (!latinPattern.test(line) || !line.includes('/'))) {
    return 'Chinese-only diagram line needs an English translation';
  }
  return undefined;
}

function transform(source) {
  const eol = source.includes('\r\n') ? '\r\n' : '\n';
  const normalized = source.replace(/\r\n?/g, '\n');
  const lines = normalized.split('\n');
  const output = [...lines];
  const changes = [];
  const issues = [];
  let fence;
  let frontmatter = lines[0] === '---';

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (frontmatter) {
      if (index > 0 && line === '---') {
        frontmatter = false;
      }
      continue;
    }
    const fenceMatch = line.match(/^\s{0,3}(`{3,}|~{3,})(.*)$/);
    if (fence) {
      if (fenceMatch && fenceMatch[1][0] === fence.marker && fenceMatch[1].length >= fence.length) {
        fence = undefined;
      } else if ((fence.language === 'plantuml' || /^(?:ascii|text|plaintext)$/i.test(fence.language)) && chinesePattern.test(line)) {
        const issue = diagramIssue(line, fence.language.toLowerCase());
        if (issue) {
          issues.push({line: index + 1, message: issue});
        }
      }
      continue;
    }
    if (fenceMatch) {
      fence = {
        marker: fenceMatch[1][0],
        length: fenceMatch[1].length,
        language: fenceMatch[2].trim().split(/\s+/)[0].toLowerCase(),
      };
      continue;
    }
    const continuation = leadingContinuationSeparator(lines, index);
    if (continuation) {
      output[index] = continuation;
      changes.push({line: index + 1, before: line, after: continuation});
      continue;
    }
    if (/^#{1,6}\s/.test(line) || /^\s{4,}\S/.test(line) || !line.trim()) {
      continue;
    }
    const result = transformLine(line, lines, index);
    if (result.output !== line) {
      output[index] = result.output;
      changes.push({line: index + 1, before: line, after: result.output});
    }
    for (const message of result.issues) {
      issues.push({line: index + 1, message});
    }
  }

  const changed = output.join('\n') !== normalized;
  return {
    content: changed ? output.join('\n').replace(/\n/g, eol) : source,
    changed,
    changes,
    issues,
  };
}

function printDiff(relativePath, changes) {
  console.log(`--- a/${relativePath}`);
  console.log(`+++ b/${relativePath}`);
  let lineDelta = 0;
  for (const change of changes) {
    const beforeCount = 1;
    const afterLines = change.after.split('\n');
    console.log(`@@ -${change.line},${beforeCount} +${change.line + lineDelta},${afterLines.length} @@`);
    console.log(`-${change.before}`);
    for (const line of afterLines) {
      console.log(`+${line}`);
    }
    lineDelta += afterLines.length - beforeCount;
  }
}

async function resolvePage(argument) {
  const portable = argument.replaceAll('\\', '/');
  const candidate = path.isAbsolute(argument)
    ? path.resolve(argument)
    : portable === 'docs/docs' || portable.startsWith('docs/docs/')
      ? path.resolve(siteDir, '..', argument)
      : portable.startsWith('docs/')
        ? path.resolve(siteDir, argument)
        : path.resolve(docsDir, argument);
  const relative = path.relative(docsDir, candidate);
  if (!relative || relative.startsWith(`..${path.sep}`) || relative === '..' || path.isAbsolute(relative)) {
    throw new Error('page must be one file inside docs/docs');
  }
  if (!/\.mdx?$/i.test(candidate)) {
    throw new Error('page must have a .md or .mdx extension');
  }
  const entry = await lstat(candidate);
  if (!entry.isFile() || entry.isSymbolicLink()) {
    throw new Error('page must be a regular, non-symlink file');
  }
  const [resolvedDocs, resolvedPage] = await Promise.all([realpath(docsDir), realpath(candidate)]);
  const realRelative = path.relative(resolvedDocs, resolvedPage);
  if (!realRelative || realRelative.startsWith(`..${path.sep}`) || realRelative === '..' || path.isAbsolute(realRelative)) {
    throw new Error('page resolves outside docs/docs');
  }
  return {filePath: candidate, mode: entry.mode & 0o777};
}

async function atomicWrite(filePath, content, mode) {
  const temporary = path.join(
    path.dirname(filePath),
    `.${path.basename(filePath)}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`,
  );
  let handle;
  try {
    handle = await open(temporary, 'wx', mode);
    await handle.writeFile(content, 'utf8');
    await handle.sync();
    await handle.close();
    handle = undefined;
    await rename(temporary, filePath);
  } finally {
    await handle?.close().catch(() => {});
    await unlink(temporary).catch((error) => {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    });
  }
}

const args = process.argv.slice(2);
if (args.includes('--help') || args.includes('-h')) {
  console.log(usage);
  process.exit(0);
}
const write = args.includes('--write');
const pages = args.filter((argument) => argument !== '--write');
if (pages.length !== 1 || args.some((argument) => argument.startsWith('--') && argument !== '--write')) {
  console.error(usage);
  process.exit(2);
}

try {
  const {filePath, mode} = await resolvePage(pages[0]);
  const source = await readFile(filePath, 'utf8');
  const result = transform(source);
  const relativePath = path.relative(path.dirname(siteDir), filePath).split(path.sep).join('/');
  for (const issue of result.issues) {
    console.error(`${relativePath}:${issue.line}: ${issue.message}`);
  }
  if (result.changed) {
    if (write) {
      await atomicWrite(filePath, result.content, mode);
      console.log(`${relativePath}: applied ${result.changes.length} safe fix(es) atomically`);
    } else {
      printDiff(relativePath, result.changes);
      console.log(`${relativePath}: ${result.changes.length} safe fix(es); rerun with --write to apply`);
    }
  } else {
    console.log(`${relativePath}: no safe formatting fixes`);
  }
  if (result.issues.length) {
    console.error(`${relativePath}: ${result.issues.length} item(s) require manual translation or disambiguation`);
    process.exitCode = 1;
  } else if (result.changed && !write) {
    process.exitCode = 1;
  }
} catch (error) {
  console.error(`fix-bilingual-page: ${error.message}`);
  process.exitCode = 2;
}
