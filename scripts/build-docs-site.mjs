// Build a static GitHub Pages site from README.md.
// Renders the markdown with github-markdown-css and turns ```mermaid fences
// into live Mermaid diagrams. Relative .md links are rewritten to point back
// to the GitHub repo so they don't 404 on the Pages site.
import fs from 'node:fs';
import path from 'node:path';
import { marked } from 'marked';

const REPO_BLOB =
  'https://github.com/Aakash02A/E-Commerce-Customer-Segmentation-Dashboard/blob/main';
const OUT_DIR = 'site';

const decode = (s) =>
  s
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');

const readme = fs.readFileSync('README.md', 'utf8');
let html = marked.parse(readme);

// ```mermaid fenced blocks -> <div class="mermaid"> for mermaid.js to render.
html = html.replace(
  /<pre><code class="language-mermaid"[^>]*>([\s\S]*?)<\/code><\/pre>/g,
  (_m, body) => `<div class="mermaid">${decode(body)}</div>`,
);

// Relative .md links -> GitHub blob URLs (keep in-page #anchors working).
html = html.replace(
  /href="(?!https?:|#)([^"#]+?\.md)(#[^"]*)?"/g,
  (_m, file, anchor = '') => `href="${REPO_BLOB}/${file}${anchor}"`,
);

const page = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>E-Commerce Customer Segmentation Dashboard</title>
  <meta name="description" content="RFM customer segmentation analytics platform built with Flask, PySpark and Chart.js." />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/github-markdown-css@5.5.1/github-markdown-light.min.css" />
  <style>
    body { margin: 0; background: #f6f8fa; }
    .markdown-body { box-sizing: border-box; max-width: 980px; margin: 0 auto; padding: 40px 24px; background: #fff; }
    .markdown-body img { max-width: 100%; }
    .mermaid { text-align: center; margin: 20px 0; overflow-x: auto; }
    .mermaid svg { max-width: none; }
    @media (max-width: 767px) { .markdown-body { padding: 20px 14px; } }
  </style>
</head>
<body>
  <article class="markdown-body">
${html}
  </article>
  <script type="module">
    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    mermaid.initialize({ startOnLoad: true, theme: 'default', securityLevel: 'loose', flowchart: { useMaxWidth: false }, sequence: { useMaxWidth: false } });
  </script>
</body>
</html>
`;

fs.mkdirSync(path.join(OUT_DIR, 'assets'), { recursive: true });
fs.writeFileSync(path.join(OUT_DIR, 'index.html'), page);
fs.copyFileSync('assets/banner.png', path.join(OUT_DIR, 'assets', 'banner.png'));
console.log('Built ' + path.join(OUT_DIR, 'index.html'));
