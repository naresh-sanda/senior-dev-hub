
        // Search functionality
        const searchInput = document.getElementById('searchInput');
        const tocLinks = document.querySelectorAll('#tocNav a');
        const sections = document.querySelectorAll('.section');

        searchInput.addEventListener('input', (e) => {
            const term = e.target.value.toLowerCase();
            
            // Filter TOC
            tocLinks.forEach(link => {
                const text = link.textContent.toLowerCase();
                const li = link.parentElement;
                if (text.includes(term)) {
                    li.style.display = 'block';
                } else {
                    li.style.display = 'none';
                }
            });
            
            // Filter Sections
            sections.forEach(sec => {
                const text = sec.textContent.toLowerCase();
                if (text.includes(term)) {
                    sec.style.display = 'block';
                } else {
                    sec.style.display = 'none';
                }
            });
        });

        // Copy Code Buttons
        const codeBlocks = document.querySelectorAll('.code-block pre');
        codeBlocks.forEach(block => {
            const btn = document.createElement('button');
            btn.className = 'copy-btn';
            btn.textContent = 'Copy';
            
            btn.addEventListener('click', () => {
                navigator.clipboard.writeText(block.textContent).then(() => {
                    btn.textContent = 'Copied!';
                    setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
                });
            });
            
            block.parentElement.appendChild(btn);
        });
    