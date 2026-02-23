// Extract pharmacy business names from Google Maps search results
// Run this in the browser console on a Google Maps pharmacy search page
(() => {
    const results = [];
    // Google Maps results are in article elements or divs with specific roles
    const articles = document.querySelectorAll('article, [role="article"]');
    articles.forEach(article => {
        const nameEl = article.querySelector('a[href*="/maps/place/"]');
        if (nameEl) {
            const name = nameEl.textContent.trim();
            // Get address from the article text
            const text = article.textContent;
            const addressMatch = text.match(/·\s*(\d+[\s\S]*?)(?:\n|Closed|Open|·)/);
            const address = addressMatch ? addressMatch[1].trim() : '';
            results.push({ name, address });
        }
    });
    
    // Fallback: look for feed results
    if (results.length === 0) {
        const feed = document.querySelector('[role="feed"]');
        if (feed) {
            const links = feed.querySelectorAll('a[href*="/maps/place/"]');
            links.forEach(link => {
                const name = link.textContent.trim();
                if (name && !name.includes('http') && name.length > 2) {
                    results.push({ name, address: '' });
                }
            });
        }
    }
    
    return JSON.stringify(results);
})()
