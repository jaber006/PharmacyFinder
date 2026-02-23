"""
Pharmacy Broker Scanner
========================
Scans 12 Australian pharmacy broker websites for current listings.
Stores results in SQLite, detects new/active/removed listings,
and generates markdown reports.

Usage:
    from scrapers.broker_scanner import BrokerScanner
    scanner = BrokerScanner()
    results = scanner.run_full_scan()
"""

import sqlite3
import re
import os
import sys
import io
import json
import logging
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
import time

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'encoding') and sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, TypeError, ValueError):
        pass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('broker_scanner')

# Project paths
PROJECT_DIR = Path(__file__).parent.parent
DB_PATH = PROJECT_DIR / 'pharmacy_finder.db'
OUTPUT_DIR = PROJECT_DIR / 'output'


# =============================================================================
# Web fetcher — uses requests + beautifulsoup
# =============================================================================

def _get_session():
    """Get a requests session with browser-like headers."""
    import requests
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-AU,en;q=0.9',
    })
    return session


def web_fetch(url: str, max_chars: int = 50000) -> Optional[str]:
    """Fetch URL and return text extracted from HTML."""
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("requests and beautifulsoup4 are required: pip install requests beautifulsoup4")
        return None

    try:
        session = _get_session()
        resp = session.get(url, timeout=20, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script', 'style']):
            tag.decompose()
        text = soup.get_text(separator='\n', strip=True)
        return text[:max_chars] if text else None
    except Exception as e:
        logger.error(f"Fetch failed for {url}: {e}")
        return None


def web_fetch_html(url: str, timeout: int = 20) -> Optional[str]:
    """Fetch raw HTML from a URL."""
    try:
        import requests
    except ImportError:
        return None

    try:
        session = _get_session()
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.error(f"HTML fetch failed for {url}: {e}")
        return None


def web_fetch_markdown(url: str, max_chars: int = 50000) -> Optional[str]:
    """Fetch URL and return readable markdown using html2text if available, else plain text."""
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    try:
        session = _get_session()
        resp = session.get(url, timeout=20, allow_redirects=True)
        resp.raise_for_status()

        try:
            import html2text
            h = html2text.HTML2Text()
            h.ignore_links = False
            h.ignore_images = True
            h.body_width = 0
            text = h.handle(resp.text)
        except ImportError:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for tag in soup(['script', 'style']):
                tag.decompose()
            # Preserve links
            for a in soup.find_all('a', href=True):
                a.string = f"[{a.get_text(strip=True)}]({a['href']})"
            text = soup.get_text(separator='\n', strip=True)

        return text[:max_chars] if text else None
    except Exception as e:
        logger.error(f"Markdown fetch failed for {url}: {e}")
        return None


# =============================================================================
# Australian state detection
# =============================================================================

AU_STATE_KEYWORDS = [
    ('TAS', ['tasmania', 'hobart', 'launceston', 'devonport', 'burnie',
             'north west coast tas', 'central highlands tas']),
    ('NT',  ['northern territory', 'darwin', 'alice springs']),
    ('ACT', ['australian capital territory', 'canberra']),
    ('NSW', ['new south wales', 'nsw', 'sydney', 'newcastle', 'hunter',
             'inner west', 'north western', 'northshore', 'wollongong',
             'central west', 'central tablelands', 'riverina', 'mid north coast',
             'eastern suburbs', 'north sydney', 'tambo western']),
    ('VIC', ['victoria', 'vic', 'melbourne', 'mornington', 'south-east vic',
             'gippsland', 'geelong', 'ballarat', 'bendigo', 'drysdale',
             'eltham', 'truganina', 'gisborne']),
    ('QLD', ['queensland', 'qld', 'brisbane', 'sunshine coast', 'gold coast',
             'noosa', 'bowen basin', 'north of brisbane', 'cairns', 'townsville',
             'outback qld', 'rural town qld']),
    ('WA',  ['western australia', 'perth', 'pilbara', 'south east western australia']),
    ('SA',  ['south australia', 'adelaide']),
]


def detect_au_state(text: str) -> str:
    """Detect Australian state from text. Returns state code or empty string."""
    if not text:
        return ''
    text_lower = text.lower()
    for state_code, keywords in AU_STATE_KEYWORDS:
        for kw in keywords:
            if kw in text_lower:
                return state_code
    return ''


# =============================================================================
# Listing dataclass
# =============================================================================

class Listing:
    """Represents a single pharmacy listing."""
    def __init__(self, source: str, title: str, price: str = '', location: str = '',
                 state: str = '', url: str = '', description: str = ''):
        self.source = source
        self.title = title.strip() if title else ''
        self.price = price.strip() if price else ''
        self.location = location.strip() if location else ''
        self.state = state.strip().upper() if state else ''
        self.url = url.strip() if url else ''
        self.description = description.strip() if description else ''

    def __repr__(self):
        return f"Listing({self.source}: {self.title[:50]})"

    @property
    def price_bucket(self) -> str:
        amount = self._parse_price()
        if amount is None:
            return 'Unknown'
        if amount < 500_000:
            return '<$500k'
        elif amount < 1_000_000:
            return '$500k-$1M'
        elif amount < 2_000_000:
            return '$1M-$2M'
        elif amount < 5_000_000:
            return '$2M-$5M'
        else:
            return '$5M+'

    def _parse_price(self) -> Optional[float]:
        if not self.price:
            return None
        text = self.price.lower().replace(',', '').replace('$', '')
        m = re.search(r'(\d+\.?\d*)\s*m(?:il|illion)?', text)
        if m:
            return float(m.group(1)) * 1_000_000
        m = re.search(r'(\d+\.?\d*)\s*k', text)
        if m:
            return float(m.group(1)) * 1_000
        m = re.search(r'(\d[\d.]+)', text)
        if m:
            return float(m.group(1))
        return None

    @property
    def tags(self) -> List[str]:
        tags = []
        combined = (self.title + ' ' + self.description).lower()
        if any(w in combined for w in ['freehold']):
            tags.append('freehold')
        if any(w in combined for w in ['regional', 'rural', 'country', 'single pharmacy town',
                                        'single town', 'one pharmacy town']):
            tags.append('regional')
        if any(w in combined for w in ['metro', 'metropolitan', 'suburban', 'shopping centre',
                                        'shopping center', 'cbd', 'inner city', 'inner west']):
            tags.append('metro')
        if 'under offer' in combined or 'under contract' in combined:
            tags.append('under_offer')
        if 'sold' in combined and 'settled' in combined:
            tags.append('sold')
        if any(w in combined for w in ['medical cent', 'doctor', 'gp']):
            tags.append('medical_nearby')
        if any(w in combined for w in ['compounding']):
            tags.append('compounding')
        return tags

    @property
    def unique_key(self) -> str:
        if self.url:
            return self.url
        return hashlib.md5(f"{self.source}:{self.title}".encode()).hexdigest()


# =============================================================================
# Individual broker scrapers
# =============================================================================

class BrokerScraper:
    """Base class for broker scrapers."""
    name = "Unknown"
    site_url = ""

    def scrape(self) -> List[Listing]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# 1. Practice4Sale — well-structured HTML, paginated, best source
# ---------------------------------------------------------------------------
class Practice4SaleScraper(BrokerScraper):
    name = "Practice4Sale"
    site_url = "https://www.practice4sale.com.au"

    def scrape(self) -> List[Listing]:
        from bs4 import BeautifulSoup
        listings = []
        base = self.site_url

        for page in range(1, 10):
            if page == 1:
                url = f"{base}/practice-for-sale/pharmacy/"
            else:
                url = f"{base}/pharmacy-practice-for-sale/{page}/"

            html = web_fetch_html(url)
            if not html:
                break

            soup = BeautifulSoup(html, 'html.parser')
            articles = soup.find_all('article')
            if not articles:
                break

            found_any = False
            for article in articles:
                # Find main listing link
                link = article.find('a', href=lambda h: h and '/practice/pharmacy/' in h)
                if not link:
                    continue
                title = link.get_text(strip=True)
                if not title or title == 'View Details':
                    continue

                found_any = True
                href = link['href']
                full_url = base + href if href.startswith('/') else href

                # Get article text for parsing
                article_text = article.get_text(separator=' | ', strip=True)

                # Extract description - find the paragraph that isn't the title or metadata
                desc = ''
                for p in article.find_all(['p', 'div']):
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 30 and p_text != title and 'View Details' not in p_text:
                        desc = p_text
                        break
                if not desc:
                    # Fallback: get text between title and metadata
                    desc = self._extract_desc_from_text(article_text, title)

                # Detect status
                status_text = ''
                if 'Under Offer' in article_text:
                    status_text = 'Under Offer'
                elif 'Under Contract' in article_text:
                    status_text = 'Under Contract'
                elif 'SUBMISSIONS CLOSED' in article_text.upper():
                    status_text = 'Submissions Closed'

                # Detect state
                combined = title + ' ' + desc + ' ' + article_text
                state = detect_au_state(combined)

                # Extract price
                price = ''
                pm = re.search(r'\$[\d,]+(?:\.\d+)?\s*(?:[mkMK](?:illion)?)?', combined)
                if pm:
                    price = pm.group(0)

                # Extract location from title
                location = title

                display_title = title
                if status_text:
                    display_title += f" [{status_text}]"

                listings.append(Listing(
                    source=self.name,
                    title=display_title,
                    price=price,
                    location=location,
                    state=state,
                    url=full_url,
                    description=desc[:500]
                ))

            if not found_any:
                break

            # Check for next page link
            if 'Next' not in html and f'/{page + 1}/' not in html:
                break

        logger.info(f"Practice4Sale: found {len(listings)} listings")
        return listings

    def _extract_desc_from_text(self, article_text: str, title: str) -> str:
        parts = article_text.split('|')
        desc_parts = []
        for part in parts:
            part = part.strip()
            if len(part) < 20:
                continue
            if part == title or 'View Details' in part:
                continue
            if re.match(r'^(New South Wales|Victoria|Queensland|Western Australia|South Australia|'
                        r'Tasmania|Northern Territory|ACT)\s*>', part):
                continue
            desc_parts.append(part)
        return ' '.join(desc_parts[:2])


# ---------------------------------------------------------------------------
# 2. SRPBS (Sue Raven) — Weebly site with structured headings
# ---------------------------------------------------------------------------
class SRPBSScraper(BrokerScraper):
    name = "SRPBS"
    site_url = "https://www.srpbs.com.au"

    def scrape(self) -> List[Listing]:
        from bs4 import BeautifulSoup
        listings = []
        url = f"{self.site_url}/pharmaciesforsale-902532.html"

        html = web_fetch_html(url)
        if not html:
            logger.warning("SRPBS: could not fetch listings page")
            return listings

        soup = BeautifulSoup(html, 'html.parser')

        # Find all headings with SR reference numbers
        for heading in soup.find_all(['h2', 'h3']):
            heading_text = heading.get_text(strip=True)
            ref_match = re.match(r'(SR\d+)\s+(.+)', heading_text)
            if not ref_match:
                continue

            ref = ref_match.group(1)
            location_part = ref_match.group(2).strip()

            # Check status
            status = ''
            is_sold = False
            if re.search(r'Sold\s*[&]\s*Settled|SOLD', location_part, re.IGNORECASE):
                is_sold = True
                status = 'Sold'
                # Clean location - remove "- Sold & Settled DATE"
                location_part = re.sub(r'\s*-?\s*Sold.*$', '', location_part, flags=re.IGNORECASE).strip()

            # Collect content after heading until next heading
            content_parts = []
            price = ''
            for sib in heading.find_next_siblings():
                if sib.name and sib.name in ['h2', 'h3']:
                    break
                text = sib.get_text(strip=True)
                if text:
                    content_parts.append(text)
                    # Look for price
                    pm = re.search(r'(?:Price\s*(?:Guide\s*)?)\$[\d,]+(?:\s*(?:to|[-])\s*\$[\d,]+)?', text)
                    if pm:
                        price = pm.group(0)
                    elif not price:
                        pm2 = re.search(r'\$[\d,]+(?:\s*(?:to|[-])\s*\$[\d,]+)?', text)
                        if pm2:
                            price = pm2.group(0)

            combined_text = heading_text + ' ' + ' '.join(content_parts)
            state = detect_au_state(combined_text)

            # Skip sold-and-settled listings in the "SOLD AND SETTLED" section
            # but include active ones even if they mention sold in context
            if is_sold:
                title = f"{ref} - {location_part} [SOLD]"
            else:
                title = f"{ref} - {location_part}"

            description = '; '.join(content_parts)

            listings.append(Listing(
                source=self.name,
                title=title,
                price=price,
                location=location_part,
                state=state,
                url=url + f"#{ref}",
                description=description[:500]
            ))

        logger.info(f"SRPBS: found {len(listings)} listings")
        return listings


# ---------------------------------------------------------------------------
# 3. Agile BB — Wix site with text-based listings
# ---------------------------------------------------------------------------
class AgileBBScraper(BrokerScraper):
    name = "Agile BB"
    site_url = "https://www.agilebb.com.au"

    # Valid ref codes are 2-3 uppercase letters (e.g. PU, PL, LL, LP, LN, PP, LT, AHH)
    VALID_REF_PATTERN = re.compile(r'^[A-Z]{2,4}$')

    # Nav/menu items to skip
    NAV_WORDS = {'ABOUT', 'BLOG', 'CONTACT', 'SERVICE', 'SERVICES', 'HOME',
                 'SELL', 'BUY', 'SOLD', 'TESTIMONIALS', 'PHARMACY', 'PHARMACIES',
                 'CONFIDENTIALITY', 'AGREEMENT', 'FORM', 'BELOW', 'VICTORIAN',
                 'SYDNEY', 'NEWCASTLE', 'REGIONAL', 'NEW', 'LISTINGS'}

    def scrape(self) -> List[Listing]:
        listings = []
        url = f"{self.site_url}/pharmacies-for-sale"

        # Fetch raw HTML and build clean text ourselves
        html = web_fetch_html(url)
        if not html:
            logger.warning("Agile BB: could not fetch listings page")
            return listings

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script', 'style']):
            tag.decompose()

        # Get text with newlines, then clean up zero-width chars
        raw_text = soup.get_text(separator='\n', strip=True)
        # Remove zero-width spaces and nbsp
        raw_text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', raw_text)
        raw_text = raw_text.replace('\xa0', ' ')

        # The Wix site sometimes splits "Ref XX:" and its description across lines.
        # Join lines: if a line starts with "Ref XX:" and has no description,
        # concatenate the next non-empty lines until another Ref or section header.
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
        joined_text = self._join_ref_lines(lines)

        # Get markdown for extracting links
        md_text = web_fetch_markdown(url, max_chars=30000) or ''

        seen_refs = set()

        # Extract linked listings from markdown: [Ref XX: Description](url)
        link_pattern = re.compile(
            r'\[(?:Ref\s+)?(\w{2,4}):\s*(.+?)\]\((https?://www\.agilebb\.com\.au/pharmacy[^\)]*)\)'
        )

        for match in link_pattern.finditer(md_text):
            ref = match.group(1).strip().upper()
            desc = match.group(2).strip()
            listing_url = match.group(3).strip()

            if not self.VALID_REF_PATTERN.match(ref):
                continue
            if ref in seen_refs or ref in self.NAV_WORDS:
                continue
            if len(desc) < 10:
                continue
            seen_refs.add(ref)

            state = detect_au_state(desc)
            status = self._detect_status(joined_text, ref)

            title = f"Ref {ref}: {desc}{status}"

            listings.append(Listing(
                source=self.name,
                title=title,
                price='',
                location=desc,
                state=state,
                url=listing_url,
                description=desc
            ))

        # Parse plain text refs from the joined text
        plain_pattern = re.compile(
            r'Ref\s+([A-Z]{2,4}):\s*(?:UNDER OFFER\s*)?(?:NEW LISTING:?\s*)?(.+)',
            re.IGNORECASE
        )

        for match in plain_pattern.finditer(joined_text):
            ref = match.group(1).strip().upper()
            if ref in seen_refs or ref in self.NAV_WORDS:
                continue
            if not self.VALID_REF_PATTERN.match(ref):
                continue

            desc = match.group(2).strip().rstrip('.')
            desc = re.sub(r'[\u200b\u200c\u200d\ufeff\xa0]', ' ', desc).strip()
            if not desc or len(desc) < 10:
                continue
            if any(nav in desc.upper() for nav in ['CLICK', 'FILL IN', 'PASSWORD',
                                                    'AGREEMENT', 'CONFIDENTIALITY',
                                                    'REFERENCE NUMBER']):
                continue

            seen_refs.add(ref)
            state = detect_au_state(desc)
            status = self._detect_status(joined_text, ref)

            title = f"Ref {ref}: {desc}{status}"

            listings.append(Listing(
                source=self.name,
                title=title,
                price='',
                location=desc,
                state=state,
                url=url,
                description=desc
            ))

        logger.info(f"Agile BB: found {len(listings)} listings")
        return listings

    def _join_ref_lines(self, lines: List[str]) -> str:
        """
        Join multi-line Ref entries from Wix.
        e.g. "Ref LN:", "Iconic:-", "In Sydney's..." -> "Ref LN: Iconic:- In Sydney's..."
        """
        result = []
        i = 0
        while i < len(lines):
            line = lines[i]
            # Check if this is a "Ref XX:" line with no/short description after the colon
            ref_match = re.match(r'^((?:ON HOLD\s+)?(?:UNDER OFFER\s+)?Ref\s+[A-Z]{2,4}:)\s*(.*)', 
                                 line, re.IGNORECASE)
            if ref_match:
                ref_prefix = ref_match.group(1)
                rest = ref_match.group(2).strip()

                # If description is short/empty, grab following lines
                if len(rest) < 15:
                    parts = [rest] if rest else []
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        # Stop at next Ref, section header, or "To find out more"
                        if re.match(r'^(?:ON HOLD\s+)?(?:UNDER OFFER\s+)?Ref\s+[A-Z]', next_line, re.IGNORECASE):
                            break
                        if re.match(r'^(VICTORIAN|SYDNEY|NSW|QUEENSLAND|WESTERN|SOUTH|NORTHERN|ACT)', 
                                    next_line, re.IGNORECASE):
                            break
                        if next_line.lower().startswith('to find out') or next_line.lower().startswith('you can'):
                            break
                        if not next_line:
                            break
                        parts.append(next_line)
                        j += 1
                    combined = ' '.join(parts).strip()
                    result.append(f"{ref_prefix} {combined}")
                    i = j
                    continue
                else:
                    result.append(line)
            else:
                result.append(line)
            i += 1

        return '\n'.join(result)

    def _detect_status(self, text: str, ref: str) -> str:
        """Detect listing status from surrounding text context."""
        idx = text.upper().find(f'REF {ref}')
        if idx < 0:
            idx = text.upper().find(ref)
        if idx < 0:
            return ''
        context = text[max(0, idx-80):idx+200].upper()
        if 'UNDER OFFER' in context:
            return ' [Under Offer]'
        if 'ON HOLD' in context:
            return ' [On Hold]'
        return ''


# ---------------------------------------------------------------------------
# 4. BusinessForSale.com.au — marketplace search
# ---------------------------------------------------------------------------
class BusinessForSaleScraper(BrokerScraper):
    name = "BusinessForSale"
    site_url = "https://www.businessforsale.com.au"

    def scrape(self) -> List[Listing]:
        """
        BusinessForSale.com.au pharmacy category (/for-sale/pharmacy) only contains
        "Wanted to buy" ads as of Feb 2026. The general search doesn't filter by keyword
        server-side. No pharmacy-for-sale listings are available via static scraping.
        """
        listings = []

        # Check the pharmacy category page
        html = web_fetch_html(f"{self.site_url}/for-sale/pharmacy?type=business")
        if html:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text(separator='\n', strip=True)

            # Parse listing blocks
            listing_urls = re.findall(
                r'(https://www\.businessforsale\.com\.au/australia/[^\s\n]+)', text
            )

            for listing_url in listing_urls:
                idx = text.find(listing_url.replace(self.site_url, ''))
                if idx < 0:
                    idx = text.find(listing_url)
                if idx < 0:
                    continue

                block = text[max(0, idx-100):idx+600]

                # Skip "Wanted" listings
                if 'Wanted' in block or 'SEEKING' in block.upper():
                    continue

                # Only include pharmacy-related
                if not re.search(r'pharmac|chemist', block, re.IGNORECASE):
                    continue

                # Parse title/price/location from block
                lines = [l.strip() for l in block.split('\n') if l.strip() and len(l.strip()) > 3]
                title = ''
                price = ''
                location = ''

                for line in lines:
                    if line.startswith('http') or line in ('FEATURED', 'NEW', 'EXCLUSIVE', 'Healthcare', 'Wanted'):
                        continue
                    loc_match = re.match(r'^([A-Za-z\s&,\'-]+?)\s+(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)$', line)
                    if loc_match and not location:
                        location = line
                        continue
                    if re.match(r'^\$[\d,]+', line) and not price:
                        price = line
                        continue
                    if not title and len(line) > 15:
                        title = line

                if title:
                    state = ''
                    sm = re.search(r'(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)', location)
                    if sm:
                        state = sm.group(1)
                    listings.append(Listing(
                        source=self.name,
                        title=title[:200],
                        price=price,
                        location=location,
                        state=state or detect_au_state(title + ' ' + location),
                        url=listing_url,
                        description=title[:500]
                    ))

        if not listings:
            logger.info("BusinessForSale: pharmacy category only has 'Wanted' ads; no for-sale listings found")

        return listings


# ---------------------------------------------------------------------------
# 5. AP Group — requires login, we can only note it
# ---------------------------------------------------------------------------
class APGroupScraper(BrokerScraper):
    name = "AP Group"
    site_url = "https://www.apgroup.com.au"

    def scrape(self) -> List[Listing]:
        # AP Group requires registration to view listings
        # Try to see if any public content reveals listing count
        text = web_fetch(f"{self.site_url}/", max_chars=10000)
        if text:
            # Look for any mention of how many listings they have
            count_match = re.search(r'(\d+)\s+(?:pharmacies?|listings?)\s+(?:for sale|available)', 
                                    text, re.IGNORECASE)
            if count_match:
                logger.info(f"AP Group: mentions {count_match.group(0)} (login required)")
            else:
                logger.info("AP Group: requires login/registration to view listings")
        else:
            logger.info("AP Group: site unreachable or requires login")
        return []


# ---------------------------------------------------------------------------
# 6. APB (Australian Pharmacy Brokers) — no public listings page
# ---------------------------------------------------------------------------
class APBScraper(BrokerScraper):
    name = "APB"
    site_url = "https://www.apb.net.au"

    def scrape(self) -> List[Listing]:
        listings = []

        # Try the buying page
        urls = [
            f"{self.site_url}/index.php?page=buying-a-pharmacy",
            f"{self.site_url}",
        ]

        for url in urls:
            text = web_fetch(url, max_chars=15000)
            if text:
                # APB doesn't list publicly — they ask buyers to register/contact
                if 'contact' in text.lower() or 'register' in text.lower():
                    logger.info("APB: no public listings (contact Chris Hodgkinson for listings)")
                    break

        return listings


# ---------------------------------------------------------------------------
# 7. Attain (iattain.com.au) — Aro CRM, JS-loaded listings
# ---------------------------------------------------------------------------
class AttainScraper(BrokerScraper):
    name = "Attain"
    site_url = "https://www.iattain.com.au"

    def scrape(self) -> List[Listing]:
        listings = []

        # Aro CRM loads listings via JavaScript API — static fetch shows templates
        # Try the main page which may have embedded data or listing counts
        html = web_fetch_html(f"{self.site_url}/pharmacies-for-sale/")
        if html:
            # Check for embedded JSON data
            json_match = re.search(r'(?:listingData|__INITIAL_STATE__|listings)\s*[=:]\s*(\[[\s\S]*?\])', html)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    for item in data:
                        if isinstance(item, dict):
                            listings.append(Listing(
                                source=self.name,
                                title=item.get('title', item.get('display_title', '')),
                                price=str(item.get('display_price', item.get('price', ''))),
                                location=item.get('suburb', ''),
                                state=item.get('state', ''),
                                url=item.get('link', item.get('url', '')),
                                description=str(item.get('desc_preview', ''))[:500]
                            ))
                except (json.JSONDecodeError, TypeError):
                    pass

            # Check if the template shows "No properties were found"
            if 'No properties were found' in (html or '') or not listings:
                # Try state-specific pages mentioned on their 404 page
                state_pages = [
                    '/pharmacies-for-sale-in-nsw/',
                    '/pharmacies-for-sale-in-vic/',
                    '/pharmacies-for-sale-in-qld/',
                    '/pharmacies-for-sale-in-wa/',
                    '/pharmacies-for-sale-in-sa/',
                    '/pharmacies-for-sale-in-tas/',
                ]
                for sp in state_pages:
                    state_html = web_fetch_html(f"{self.site_url}{sp}")
                    if state_html and 'property-card' in state_html:
                        # Parse property cards
                        self._parse_aro_html(state_html, listings)

        if not listings:
            logger.info("Attain: Aro CRM site — listings load via JS, not accessible via static fetch")

        return listings

    def _parse_aro_html(self, html: str, listings: list):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        cards = soup.select('.property-card, .listing-card, [class*="listing"]')
        for card in cards:
            title_el = card.select_one('h2, h3, .title')
            if title_el:
                listings.append(Listing(
                    source=self.name,
                    title=title_el.get_text(strip=True),
                    url=self.site_url
                ))


# ---------------------------------------------------------------------------
# 8. Wisharts — Aro CRM, currently no listings
# ---------------------------------------------------------------------------
class WishartsScraper(BrokerScraper):
    name = "Wisharts"
    site_url = "https://www.wisharts.com.au"

    def scrape(self) -> List[Listing]:
        listings = []
        html = web_fetch_html(f"{self.site_url}/pharmacy-business-for-sale/")
        if html:
            # Aro CRM — check for embedded data
            json_match = re.search(r'(?:listingData|properties)\s*[=:]\s*(\[[\s\S]*?\])', html)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    for item in data:
                        if isinstance(item, dict):
                            title = item.get('display_title', item.get('title', ''))
                            if title:
                                listings.append(Listing(
                                    source=self.name,
                                    title=title,
                                    price=str(item.get('display_price', '')),
                                    location=item.get('suburb', ''),
                                    state=item.get('state', ''),
                                    url=self.site_url + str(item.get('link', '')),
                                    description=str(item.get('desc_preview', ''))[:500]
                                ))
                except (json.JSONDecodeError, TypeError):
                    pass

            if 'No properties were found' in (html or ''):
                logger.info("Wisharts: Aro CRM site shows 'No properties found' (all listings may be confidential)")
        else:
            logger.info("Wisharts: could not reach site")

        return listings


# ---------------------------------------------------------------------------
# 9. VPB (Victorian Pharmacy Brokers) — connection issues
# ---------------------------------------------------------------------------
class VPBScraper(BrokerScraper):
    name = "VPB"
    site_url = "https://www.vpb.com.au"

    def scrape(self) -> List[Listing]:
        listings = []

        # Try with and without www
        for url in [self.site_url, "https://vpb.com.au"]:
            text = web_fetch(url, max_chars=15000)
            if text and len(text) > 100:
                # Parse for pharmacy listings
                blocks = re.split(r'\n\n+', text)
                for block in blocks:
                    if re.search(r'pharmacy|chemist|for sale', block, re.IGNORECASE) and len(block) > 30:
                        price = ''
                        pm = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                        if pm:
                            price = pm.group(0)
                        title = block.split('\n')[0].strip()[:200]
                        listings.append(Listing(
                            source=self.name,
                            title=title,
                            price=price,
                            location='Victoria',
                            state='VIC',
                            url=url,
                            description=block[:500]
                        ))
                break

        if not listings:
            logger.info("VPB: site unreachable (connection timeout)")

        return listings


# ---------------------------------------------------------------------------
# 10. Statewide Business Advisors — connection issues
# ---------------------------------------------------------------------------
class StatewideBAScraper(BrokerScraper):
    name = "Statewide BA"
    site_url = "https://www.statewideba.com.au"

    def scrape(self) -> List[Listing]:
        listings = []

        for url in [self.site_url, "https://statewideba.com.au"]:
            text = web_fetch(url, max_chars=15000)
            if text and len(text) > 100:
                blocks = re.split(r'\n\n+', text)
                for block in blocks:
                    if re.search(r'pharmacy|chemist', block, re.IGNORECASE) and len(block) > 30:
                        price = ''
                        pm = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                        if pm:
                            price = pm.group(0)
                        title = block.split('\n')[0].strip()[:200]
                        state = detect_au_state(title + ' ' + block)
                        listings.append(Listing(
                            source=self.name,
                            title=title,
                            price=price,
                            state=state,
                            url=url,
                            description=block[:500]
                        ))
                break

        if not listings:
            logger.info("Statewide BA: site unreachable (connection timeout)")

        return listings


# ---------------------------------------------------------------------------
# 11. Mint Business Brokers — connection issues
# ---------------------------------------------------------------------------
class MintBusinessBrokersScraper(BrokerScraper):
    name = "Mint Business Brokers"
    site_url = "https://www.mintbusinessbrokers.com.au"

    def scrape(self) -> List[Listing]:
        listings = []

        for url in [self.site_url, "https://mintbusinessbrokers.com.au"]:
            text = web_fetch(url, max_chars=15000)
            if text and len(text) > 100:
                blocks = re.split(r'\n\n+', text)
                for block in blocks:
                    if re.search(r'pharmacy|chemist', block, re.IGNORECASE) and len(block) > 30:
                        price = ''
                        pm = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                        if pm:
                            price = pm.group(0)
                        title = block.split('\n')[0].strip()[:200]
                        state = detect_au_state(title + ' ' + block)
                        listings.append(Listing(
                            source=self.name,
                            title=title,
                            price=price,
                            state=state,
                            url=url,
                            description=block[:500]
                        ))
                break

        if not listings:
            logger.info("Mint Business Brokers: site unreachable (connection timeout)")

        return listings


# ---------------------------------------------------------------------------
# 12. Rx Pharmacy Brokers — Aro CRM, currently no listings
# ---------------------------------------------------------------------------
class RxPharmacyScraper(BrokerScraper):
    name = "Rx Pharmacy"
    site_url = "https://www.rxpharmacybrokers.com.au"

    def scrape(self) -> List[Listing]:
        listings = []

        # Try various listing page paths
        for path in ['/pharmacies-for-sale', '/for-sale', '/buying', '']:
            html = web_fetch_html(f"{self.site_url}{path}")
            if html and '404' not in html[:500]:
                # Check for Aro CRM data
                json_match = re.search(r'(?:listingData|properties)\s*[=:]\s*(\[[\s\S]*?\])', html)
                if json_match:
                    try:
                        data = json.loads(json_match.group(1))
                        for item in data:
                            if isinstance(item, dict):
                                listings.append(Listing(
                                    source=self.name,
                                    title=item.get('display_title', item.get('title', '')),
                                    price=str(item.get('display_price', '')),
                                    url=self.site_url + str(item.get('link', ''))
                                ))
                    except (json.JSONDecodeError, TypeError):
                        pass

                if 'No properties were found' in html:
                    logger.info("Rx Pharmacy: Aro CRM site shows 'No properties found'")
                    break

        return listings


# =============================================================================
# Database manager
# =============================================================================

class ListingDatabase:
    """Manages the broker_listings SQLite table."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS broker_listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    price TEXT DEFAULT '',
                    location TEXT DEFAULT '',
                    state TEXT DEFAULT '',
                    url TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    price_bucket TEXT DEFAULT '',
                    tags TEXT DEFAULT '',
                    first_seen DATE NOT NULL,
                    last_seen DATE NOT NULL,
                    status TEXT DEFAULT 'NEW',
                    notified INTEGER DEFAULT 0
                )
            ''')
            # Create url index only if url is not empty (for uniqueness)
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_broker_listings_url
                ON broker_listings(url)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_broker_listings_status
                ON broker_listings(status)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_broker_listings_source
                ON broker_listings(source)
            ''')
            conn.commit()
        finally:
            conn.close()

    def process_listings(self, listings: List[Listing]) -> Dict:
        """Process scraped listings against the database."""
        today = date.today().isoformat()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            # Get all current active/new listings
            existing = {}
            for row in conn.execute(
                "SELECT id, url, title, source FROM broker_listings WHERE status IN ('NEW', 'ACTIVE')"
            ):
                key = row['url'] if row['url'] else f"{row['source']}:{row['title']}"
                existing[key] = dict(row)

            new_listings = []
            seen_keys = set()
            stats = {'new': 0, 'active': 0, 'removed': 0, 'total_scraped': len(listings)}

            for listing in listings:
                key = listing.unique_key
                seen_keys.add(key)

                if key in existing:
                    conn.execute(
                        "UPDATE broker_listings SET last_seen = ?, status = 'ACTIVE', "
                        "price = COALESCE(NULLIF(?, ''), price), "
                        "description = COALESCE(NULLIF(?, ''), description), "
                        "price_bucket = ?, tags = ? "
                        "WHERE id = ?",
                        (today, listing.price, listing.description,
                         listing.price_bucket, ','.join(listing.tags),
                         existing[key]['id'])
                    )
                    stats['active'] += 1
                else:
                    conn.execute(
                        "INSERT OR IGNORE INTO broker_listings "
                        "(source, title, price, location, state, url, description, "
                        "price_bucket, tags, first_seen, last_seen, status, notified) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW', 0)",
                        (listing.source, listing.title, listing.price,
                         listing.location, listing.state, listing.url,
                         listing.description, listing.price_bucket,
                         ','.join(listing.tags), today, today)
                    )
                    stats['new'] += 1
                    new_listings.append(listing)

            # Mark removed listings
            for key, row in existing.items():
                if key not in seen_keys:
                    conn.execute(
                        "UPDATE broker_listings SET status = 'REMOVED' WHERE id = ?",
                        (row['id'],)
                    )
                    stats['removed'] += 1

            conn.commit()

            # Get summary by source
            source_summary = {}
            for row in conn.execute(
                "SELECT source, status, COUNT(*) as cnt FROM broker_listings "
                "WHERE status IN ('NEW', 'ACTIVE') GROUP BY source, status"
            ):
                source = row[0]
                status = row[1]
                cnt = row[2]
                if source not in source_summary:
                    source_summary[source] = {'NEW': 0, 'ACTIVE': 0}
                source_summary[source][status] = cnt

            return {
                'stats': stats,
                'new_listings': new_listings,
                'source_summary': source_summary,
            }
        finally:
            conn.close()

    def get_all_active(self) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT * FROM broker_listings WHERE status IN ('NEW', 'ACTIVE') "
                "ORDER BY first_seen DESC, source"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


# =============================================================================
# Report generator
# =============================================================================

class ReportGenerator:
    def __init__(self, output_dir: Path = OUTPUT_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(self, results: Dict, scan_errors: Dict[str, str],
                        broker_notes: Dict[str, str] = None) -> str:
        today = date.today().isoformat()
        filename = f"broker_scan_{today}.md"
        filepath = self.output_dir / filename

        lines = []
        lines.append(f"# Pharmacy Broker Scan Report - {today}")
        lines.append(f"\n_Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S AEST')}_\n")

        stats = results['stats']
        lines.append("## Summary\n")
        lines.append(f"- **Total scraped**: {stats['total_scraped']} listings")
        lines.append(f"- **New listings**: {stats['new']}")
        lines.append(f"- **Still active**: {stats['active']}")
        lines.append(f"- **Removed since last scan**: {stats['removed']}")

        # Source breakdown
        lines.append("\n## Listings by Broker\n")
        lines.append("| Broker | New | Active | Total |")
        lines.append("|--------|-----|--------|-------|")

        source_summary = results.get('source_summary', {})
        for source in sorted(source_summary.keys()):
            counts = source_summary[source]
            new = counts.get('NEW', 0)
            active = counts.get('ACTIVE', 0)
            total = new + active
            lines.append(f"| {source} | {new} | {active} | {total} |")

        total_new = stats['new']
        total_active = stats['active']
        total_all = sum(s.get('NEW', 0) + s.get('ACTIVE', 0) for s in source_summary.values())
        lines.append(f"| **TOTAL** | **{total_new}** | **{total_active}** | **{total_all}** |")

        # New listings detail
        new_listings = results.get('new_listings', [])
        if new_listings:
            lines.append("\n## New Listings\n")

            # Highlight Tasmania listings
            tas_listings = [l for l in new_listings if l.state == 'TAS']
            if tas_listings:
                lines.append("### 🏝️ Tasmania Listings (Priority)\n")
                for listing in tas_listings:
                    self._format_listing(listing, lines)

            # Then all others by state
            other_listings = [l for l in new_listings if l.state != 'TAS']
            if other_listings:
                if tas_listings:
                    lines.append("### Other States\n")
                for listing in sorted(other_listings, key=lambda l: (l.state or 'ZZZ', l.source)):
                    self._format_listing(listing, lines)

        # Broker notes (login required, unreachable, etc.)
        if broker_notes:
            lines.append("\n## Broker Notes\n")
            for broker, note in sorted(broker_notes.items()):
                lines.append(f"- **{broker}**: {note}")

        # Scan errors
        if scan_errors:
            lines.append("\n## Scan Errors\n")
            for source, error in scan_errors.items():
                lines.append(f"- **{source}**: {error}")

        report_text = '\n'.join(lines)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_text)

        return str(filepath)

    def _format_listing(self, listing: Listing, lines: list):
        lines.append(f"#### {listing.title}")
        lines.append(f"- **Source**: {listing.source}")
        if listing.price:
            lines.append(f"- **Price**: {listing.price} ({listing.price_bucket})")
        if listing.location:
            lines.append(f"- **Location**: {listing.location}")
        if listing.state:
            lines.append(f"- **State**: {listing.state}")
        if listing.tags:
            lines.append(f"- **Tags**: {', '.join(listing.tags)}")
        if listing.url:
            lines.append(f"- **URL**: {listing.url}")
        if listing.description:
            lines.append(f"- {listing.description[:300]}")
        lines.append("")


# =============================================================================
# Main scanner orchestrator
# =============================================================================

class BrokerScanner:
    """Main scanner that orchestrates all broker scrapers."""

    SCRAPERS = [
        Practice4SaleScraper,
        SRPBSScraper,
        AgileBBScraper,
        BusinessForSaleScraper,
        APGroupScraper,
        APBScraper,
        AttainScraper,
        WishartsScraper,
        VPBScraper,
        StatewideBAScraper,
        MintBusinessBrokersScraper,
        RxPharmacyScraper,
    ]

    def __init__(self, db_path: Path = DB_PATH):
        self.db = ListingDatabase(db_path)
        self.report_gen = ReportGenerator()

    def run_full_scan(self, verbose: bool = True) -> Dict:
        if verbose:
            print("=" * 60)
            print("  PHARMACY BROKER SCANNER")
            print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            print()

        all_listings = []
        scan_errors = {}
        broker_notes = {}

        for scraper_cls in self.SCRAPERS:
            scraper = scraper_cls()
            source_name = scraper.name

            if verbose:
                print(f"  [{source_name:<25}] Scanning...", end=' ', flush=True)

            try:
                listings = scraper.scrape()
                all_listings.extend(listings)
                if verbose:
                    if listings:
                        print(f"OK ({len(listings)} listings)")
                    else:
                        print(f"OK (0 listings)")
            except Exception as e:
                error_msg = str(e)
                scan_errors[source_name] = error_msg
                logger.error(f"Error scanning {source_name}: {e}", exc_info=True)
                if verbose:
                    print(f"FAIL: {error_msg[:60]}")

        if verbose:
            print(f"\n  Total scraped: {len(all_listings)} listings")
            print("  Processing against database...", end=' ', flush=True)

        # Process against database
        results = self.db.process_listings(all_listings)
        results['errors'] = scan_errors

        if verbose:
            print("done")

        # Generate report
        report_path = self.report_gen.generate_report(results, scan_errors, broker_notes)
        results['report_path'] = report_path

        if verbose:
            self._print_summary(results)

        return results

    def _print_summary(self, results: Dict):
        stats = results['stats']
        print()
        print("=" * 60)
        print("  SCAN RESULTS")
        print("=" * 60)
        print(f"  New listings:     {stats['new']}")
        print(f"  Active listings:  {stats['active']}")
        print(f"  Removed listings: {stats['removed']}")
        print(f"  Total scraped:    {stats['total_scraped']}")
        print()

        source_summary = results.get('source_summary', {})
        if source_summary:
            print("  BY BROKER:")
            for source in sorted(source_summary.keys()):
                counts = source_summary[source]
                total = counts.get('NEW', 0) + counts.get('ACTIVE', 0)
                new = counts.get('NEW', 0)
                marker = " *NEW*" if new > 0 else ""
                print(f"    {source:<25} {total:>3} listings{marker}")
            print()

        new_listings = results.get('new_listings', [])
        if new_listings:
            print("  NEW LISTINGS:")
            print("  " + "-" * 56)

            # Tasmania first
            tas = [l for l in new_listings if l.state == 'TAS']
            if tas:
                print("  ** TASMANIA (PRIORITY) **")
                for listing in tas:
                    self._print_listing(listing)

            # Others
            for listing in sorted(new_listings, key=lambda l: (l.state or 'ZZZ', l.source)):
                if listing.state == 'TAS':
                    continue
                self._print_listing(listing)

        if results.get('errors'):
            print("\n  ERRORS:")
            for source, error in results['errors'].items():
                print(f"    {source}: {error[:60]}")

        print(f"\n  Report saved: {results.get('report_path', 'N/A')}")
        print("=" * 60)

    def _print_listing(self, listing: Listing):
        print(f"    [{listing.source}] {listing.title[:55]}")
        details = []
        if listing.price:
            details.append(f"Price: {listing.price}")
        if listing.state:
            details.append(f"State: {listing.state}")
        if listing.location and listing.location != listing.title:
            details.append(f"Loc: {listing.location[:40]}")
        if details:
            print(f"      {' | '.join(details)}")


if __name__ == '__main__':
    scanner = BrokerScanner()
    results = scanner.run_full_scan(verbose=True)
