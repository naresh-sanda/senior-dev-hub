const fs = require('fs');
const { marked } = require('marked');

let mdContent = fs.readFileSync('pyspark_handbook.md', 'utf-8');

// 1. Remove the manual Table of Contents block
mdContent = mdContent.replace(/## Table of Contents[\s\S]*?---\n+/, '');

// 2. Pre-process headings to extract TOC and set correct IDs
let toc = '';
mdContent = mdContent.replace(/^##\s+(.*?)(?:\s+\{#(.*?)\})?$/gm, (match, title, id) => {
    // If there's no id provided in {#id}, generate one from the title
    if (!id) {
        id = title.toLowerCase().replace(/[^\w]+/g, '-');
    }
    
    // Don't add 'Table of Contents' to TOC just in case
    if (title !== 'Table of Contents') {
        toc += `<li><a href="#${id}">${title}</a></li>\n`;
    }
    
    return `<h2 id="${id}">${title}</h2>`;
});

// Render markdown to HTML
const htmlBody = marked.parse(mdContent);

// Prepare HTML template
const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PySpark Handbook - Cheat Sheet</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="assets/styles.css">

    <!-- SEO & Metadata -->
    <meta name="description" content="Premium interactive cheat sheets for Senior Engineers, covering Java, Python, DSA, System Design, and DevOps. By Naresh Kumar Sanda.">
    <meta name="keywords" content="PySpark, Spark, Python, Data Engineering, Big Data, Cheat Sheet, Senior Developer">
    <meta property="og:title" content="PySpark Handbook - Cheat Sheet">
    <meta property="og:description" content="Premium interactive cheat sheets for Senior Engineers, covering Java, Python, DSA, System Design, and DevOps. By Naresh Kumar Sanda.">
    <meta property="og:image" content="https://raw.githubusercontent.com/username/repo/main/assets/ns_logo.png">
    <meta property="og:type" content="website">
    <link rel="icon" type="image/png" href="assets/ns_logo.png">
    <style>
        .markdown-body {
            font-family: 'Inter', sans-serif;
            line-height: 1.6;
        }
        .markdown-body h1, .markdown-body h2, .markdown-body h3 {
            margin-top: 24px;
            margin-bottom: 16px;
            font-weight: 600;
        }
        .markdown-body h2 {
            border-bottom: 1px solid var(--border-color, #ddd);
            padding-bottom: 8px;
        }
        .markdown-body pre {
            background-color: #1e1e1e;
            color: #d4d4d4;
            padding: 16px;
            overflow: auto;
            border-radius: 8px;
            font-family: 'Fira Code', monospace;
        }
        .markdown-body code {
            font-family: 'Fira Code', monospace;
            background-color: rgba(27,31,35,0.05);
            padding: 0.2em 0.4em;
            border-radius: 3px;
        }
        .markdown-body pre code {
            background-color: transparent;
            padding: 0;
        }
        .markdown-body table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 16px;
        }
        .markdown-body th, .markdown-body td {
            border: 1px solid var(--border-color, #ddd);
            padding: 8px 12px;
        }
        .markdown-body blockquote {
            margin: 0 0 16px 0;
            padding: 0 1em;
            color: #6a737d;
            border-left: 0.25em solid #dfe2e5;
        }
    </style>
</head>
<body>

    <header class="global-navbar">
        <a href="index.html" class="brand-logo">
            <img src="assets/ns_logo.png" alt="NS Logo" class="brand-image" >
            <div>Naresh Sanda</div>
        </a>
        <div class="header-subtitle">Senior Developer Hub</div>
    </header>

    <div class="app-container">
        
        <aside class="sidebar">
            <div class="search-container">
                <input type="text" id="searchInput" placeholder="Search topics...">
                <a href="index.html" style="display:block; margin-top:15px; text-decoration:none; color:var(--accent-mysql); font-weight:bold; font-size:13px;">← Back to Portal</a>
            </div>
            <nav class="toc-nav" id="tocNav">
                <h3>📋 Table of Contents</h3>
                <ul>
                    ${toc || '<li><a href="#">No sections found</a></li>'}
                </ul>
            </nav>
        </aside>

        <main class="main-content" id="mainContent">
            <div class="markdown-body">
                ${htmlBody}
            </div>
        </main>
    </div>

    <script src="assets/scripts.js"></script>

    <footer class="global-footer">
        <div class="footer-brand">
            <img src="assets/ns_logo.png" alt="NS Logo">
            Naresh Sanda
        </div>
        <div>&copy; 2026 Senior Developer Hub. All rights reserved.</div>
    </footer>
</body>
</html>`;

fs.writeFileSync('PySpark-Handbook.html', htmlTemplate, 'utf-8');
console.log("Successfully generated PySpark-Handbook.html");
