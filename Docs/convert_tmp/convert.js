const fs = require('fs');
const path = require('path');
const { marked } = require('marked');

const mdPath = path.join(__dirname, '../Báo cáo Đồ án 2 Hoàn chỉnh.md');
const htmlPath = path.join(__dirname, '../Báo cáo Đồ án 2 Hoàn chỉnh.html');

const markdown = fs.readFileSync(mdPath, 'utf8');

// Convert Markdown to HTML
const contentHtml = marked.parse(markdown);

const styledHtml = `
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Báo cáo Đồ án 2 Hoàn chỉnh</title>
<style>
  body {
    font-family: "Times New Roman", Times, serif;
    font-size: 13pt;
    line-height: 1.5;
    margin: 20mm 20mm 20mm 20mm;
    color: #000;
  }
  h1, h2, h3, h4, h5, h6 {
    font-family: "Times New Roman", Times, serif;
    font-weight: bold;
    color: #000;
    margin-top: 18pt;
    margin-bottom: 6pt;
    page-break-after: avoid;
  }
  h1 {
    font-size: 16pt;
    text-align: center;
    text-transform: uppercase;
  }
  h2 {
    font-size: 14pt;
  }
  h3 {
    font-size: 13pt;
  }
  p {
    margin-top: 0;
    margin-bottom: 6pt;
    text-align: justify;
    text-indent: 10mm;
  }
  h1 + p, h2 + p, h3 + p, h4 + p, li p, blockquote p {
    text-indent: 0;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 12pt;
    margin-bottom: 12pt;
    page-break-inside: avoid;
  }
  th, td {
    border: 1px solid #000;
    padding: 6pt;
    text-align: left;
    font-size: 12pt;
  }
  th {
    background-color: #f2f2f2;
    font-weight: bold;
    text-align: center;
  }
  ul, ol {
    margin-top: 0;
    margin-bottom: 6pt;
    padding-left: 20pt;
  }
  li {
    margin-bottom: 3pt;
    text-align: justify;
  }
  pre, code {
    font-family: "Courier New", Courier, monospace;
    font-size: 11pt;
    background-color: #f9f9f9;
  }
  pre {
    border: 1px solid #ccc;
    padding: 6pt;
    white-space: pre-wrap;
    margin-top: 6pt;
    margin-bottom: 6pt;
  }
  code {
    padding: 1px 3px;
  }
  img {
    max-width: 100%;
    display: block;
    margin: 12pt auto;
    page-break-inside: avoid;
  }
  .page-break {
    page-break-before: always;
  }
  blockquote {
    margin: 6pt 20pt;
    padding-left: 10pt;
    border-left: 3px solid #ccc;
    font-style: italic;
  }
</style>
</head>
<body>
${contentHtml}
</body>
</html>
`;

fs.writeFileSync(htmlPath, styledHtml, 'utf8');
console.log("HTML file generated successfully!");
