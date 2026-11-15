import {createHash} from 'node:crypto';
import {mkdir, readFile, readdir, unlink, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import plantumlEncoder from 'plantuml-encoder';

const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
const siteDir = path.resolve(scriptsDir, '..');
const docsDir = path.join(siteDir, 'docs');
const outputDir = path.join(siteDir, 'static', 'diagrams');
const manifestPath = path.join(siteDir, 'src', 'generated', 'plantuml-manifest.json');
const serverUrl = (process.env.PLANTUML_SERVER_URL ?? 'https://www.plantuml.com/plantuml').replace(/\/+$/, '');

async function markdownFiles(directory) {
  const entries = await readdir(directory, {withFileTypes: true});
  const files = await Promise.all(entries.map(async (entry) => {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      return markdownFiles(entryPath);
    }
    return /\.mdx?$/.test(entry.name) ? [entryPath] : [];
  }));
  return files.flat().sort();
}

function normalizeSource(source) {
  return source.replace(/\r\n?/g, '\n').trimEnd();
}

function isValidSvg(svg) {
  const trimmed = svg.trim();
  return (trimmed.startsWith('<svg') || trimmed.startsWith('<?xml')) && trimmed.endsWith('</svg>');
}

async function cachedSvg(filePath) {
  try {
    const svg = await readFile(filePath, 'utf8');
    return isValidSvg(svg) ? svg : undefined;
  } catch {
    return undefined;
  }
}

const diagrams = new Map();
for (const filePath of await markdownFiles(docsDir)) {
  const markdown = await readFile(filePath, 'utf8');
  for (const match of markdown.matchAll(/^```plantuml[\t ]*\r?\n([\s\S]*?)^```[\t ]*$/gm)) {
    const source = normalizeSource(match[1]);
    const encoded = plantumlEncoder.encode(source);
    if (!diagrams.has(source)) {
      const hash = createHash('sha256').update(source).digest('hex');
      diagrams.set(source, {source, encoded, fileName: `${hash}.svg`, filePath});
    }
  }
}

await mkdir(outputDir, {recursive: true});
await mkdir(path.dirname(manifestPath), {recursive: true});

let generated = 0;
let cached = 0;
for (const [, diagram] of diagrams) {
  const outputPath = path.join(outputDir, diagram.fileName);
  if (await cachedSvg(outputPath)) {
    cached += 1;
    continue;
  }

  const relativeDoc = path.relative(siteDir, diagram.filePath);
  let response;
  try {
    response = await fetch(`${serverUrl}/svg/${diagram.encoded}`, {signal: AbortSignal.timeout(30_000)});
  } catch (error) {
    throw new Error(`PlantUML renderer unavailable for ${relativeDoc}: ${error.message}`);
  }
  if (!response.ok) {
    throw new Error(`PlantUML renderer returned ${response.status} for ${relativeDoc}`);
  }
  const svg = await response.text();
  if (!isValidSvg(svg)) {
    throw new Error(`PlantUML renderer returned malformed SVG for ${relativeDoc}`);
  }
  await writeFile(outputPath, svg);
  generated += 1;
}

const expectedFiles = new Set([...diagrams.values()].map((diagram) => diagram.fileName));
for (const fileName of await readdir(outputDir)) {
  if (fileName.endsWith('.svg') && !expectedFiles.has(fileName)) {
    await unlink(path.join(outputDir, fileName));
  }
}

const manifest = Object.fromEntries(
  [...diagrams.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([source, diagram]) => [source, `diagrams/${diagram.fileName}`]),
);
await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
console.log(`PlantUML diagrams: ${diagrams.size} total, ${cached} cached, ${generated} generated`);
