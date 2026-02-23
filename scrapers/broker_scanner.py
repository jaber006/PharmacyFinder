"""
Pharmacy Broker Scanner
========================
Scans 12 Australian pharmacy broker websites for current listings.
Stores results in SQLite, detects new/active/removed listings,
and generates markdown alert reports.

Usage:
    from scrapers.broker_scanner import BrokerScanner
    scanner = BrokerScanner()
    results = scanner.run_full_scan()
"""

import sqlite3
import re
import os
import json
import logging
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
import subprocess
import sys

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
# Web fetcher — uses Node.js clawdbot web_fetch or falls back to requests
# =============================================================================

def web_fetch(url: str, max_chars: int = 30000) -> Optional[str]:
    """
    Fetch URL content as readable markdown text.
    Uses the 'requests' library with readability-like extraction,
    or falls back to raw HTML parsing.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("requests/bs4 not available, trying urllib")
        try:
            import urllib.request
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode('utf-8', errors='replace')
                # Basic HTML to text
                from html.parser import HTMLParser
                class TextExtractor(HTMLParser):
                    def __init__(self):
                        super().__init__()
                        self.text_parts = []
                    def handle_data(self, data):
                        self.text_parts.append(data)
                extractor = TextExtractor()
                extractor.feed(html)
                text = ' '.join(extractor.text_parts)
                return text[:max_chars] if text else None
        except Exception as e:
            logger.error(f"urllib fetch failed for {url}: {e}")
            return None

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-AU,en;q=0.9',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text

        soup = BeautifulSoup(html, 'html.parser')
        # Remove script/style
        for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        text = soup.get_text(separator='\n', strip=True)
        return text[:max_chars] if text else None

    except Exception as e:
        logger.error(f"Fetch failed for {url}: {e}")
        return None


def web_fetch_html(url: str) -> Optional[str]:
    """Fetch raw HTML from a URL."""
    try:
        import requests
    except ImportError:
        try:
            import urllib.request
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except Exception as e:
            logger.error(f"urllib HTML fetch failed for {url}: {e}")
            return None

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.error(f"HTML fetch failed for {url}: {e}")
        return None


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
        """Classify price into bucket."""
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
        """Extract numeric price value."""
        if not self.price:
            return None
        text = self.price.lower().replace(',', '').replace('$', '')
        # Handle "Xm" or "X.Xm"
        m = re.search(r'(\d+\.?\d*)\s*m(?:il|illion)?', text)
        if m:
            return float(m.group(1)) * 1_000_000
        # Handle "Xk"
        m = re.search(r'(\d+\.?\d*)\s*k', text)
        if m:
            return float(m.group(1)) * 1_000
        # Plain number
        m = re.search(r'(\d[\d.]+)', text)
        if m:
            val = float(m.group(1))
            # If looks like raw number > 1000, probably dollars
            return val
        return None

    @property
    def tags(self) -> List[str]:
        """Extract relevant tags from title + description."""
        tags = []
        combined = (self.title + ' ' + self.description).lower()
        if 'medical cent' in combined or 'medical practice' in combined or 'doctor' in combined:
            tags.append('medical_centre')
        if 'freehold' in combined:
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
        return tags

    @property
    def unique_key(self) -> str:
        """Generate a unique key for deduplication."""
        if self.url:
            return self.url
        # Fallback: hash of source + title
        return hashlib.md5(f"{self.source}:{self.title}".encode()).hexdigest()


# =============================================================================
# Individual broker scrapers
# =============================================================================

class BrokerScraper:
    """Base class for broker scrapers."""
    name = "Unknown"

    def scrape(self) -> List[Listing]:
        """Override in subclass. Returns list of Listing objects."""
        raise NotImplementedError


class Practice4SaleScraper(BrokerScraper):
    """practice4sale.com.au - pharmacy aggregator with paginated results."""
    name = "Practice4Sale"

    def scrape(self) -> List[Listing]:
        listings = []
        base_url = "https://www.practice4sale.com.au"

        for page in range(1, 10):  # Max 10 pages
            if page == 1:
                url = f"{base_url}/practice-for-sale/pharmacy/"
            else:
                url = f"{base_url}/pharmacy-practice-for-sale/{page}/"

            text = web_fetch(url, max_chars=50000)
            if not text:
                break

            # Check if page has listings
            if 'We found 0' in text or 'Next Last' not in text and page > 1:
                if page > 1:
                    break

            # Parse listings using regex on the text
            # Pattern: title links followed by broker link and description
            # Looking for ## [Title](URL) patterns
            lines = text.split('\n')
            current_title = None
            current_url = None
            current_desc = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Match ## [Title](/practice/pharmacy/...)
                title_match = re.match(r'##\s*\[(.+?)\]\((/practice/pharmacy/.+?)\)', line)
                if title_match:
                    # Save previous listing
                    if current_title:
                        self._add_listing(listings, current_title, current_url, ' '.join(current_desc))

                    current_title = title_match.group(1)
                    current_url = base_url + title_match.group(2)
                    current_desc = []
                    continue

                # Skip navigation, broker links, ads
                if current_title and not line.startswith('[') and not line.startswith('First') \
                        and not line.startswith('Advertisement') and not line.startswith('We found'):
                    # Check for "Under Offer" / "Under Contract" status markers
                    if line in ('Under Offer', 'Under Contract'):
                        current_desc.append(f'[{line}]')
                    elif not line.startswith('/') and len(line) > 10:
                        current_desc.append(line)

            # Save last listing
            if current_title:
                self._add_listing(listings, current_title, current_url, ' '.join(current_desc))

            # Check if there are more pages
            if f'[{page + 1}]' not in text and f'/{page + 1}/' not in text:
                break

        logger.info(f"Practice4Sale: found {len(listings)} listings")
        return listings

    def _add_listing(self, listings: list, title: str, url: str, desc: str):
        """Parse a listing and add it."""
        location, state = self._extract_location(title, desc)
        price = self._extract_price(title, desc)

        listings.append(Listing(
            source=self.name,
            title=title,
            price=price,
            location=location,
            state=state,
            url=url,
            description=desc[:500]
        ))

    def _extract_location(self, title: str, desc: str) -> Tuple[str, str]:
        """Extract location and state from title/description."""
        combined = title + ' ' + desc

        state_map = {
            'NSW': ['nsw', 'new south wales', 'sydney', 'newcastle', 'hunter', 'inner west'],
            'VIC': ['vic', 'victoria', 'melbourne', 'mornington'],
            'QLD': ['qld', 'queensland', 'brisbane', 'sunshine coast', 'gold coast', 'noosa', 'bowen basin'],
            'WA': ['wa', 'western australia', 'perth', 'pilbara'],
            'SA': ['sa', 'south australia', 'adelaide'],
            'TAS': ['tas', 'tasmania', 'hobart'],
            'NT': ['nt', 'northern territory', 'darwin'],
            'ACT': ['act', 'canberra'],
        }

        state = ''
        for st, keywords in state_map.items():
            for kw in keywords:
                if kw in combined.lower():
                    state = st
                    break
            if state:
                break

        # Location is roughly the title
        location = title
        return location, state

    def _extract_price(self, title: str, desc: str) -> str:
        """Extract price from text."""
        combined = title + ' ' + desc
        # Look for dollar amounts
        price_patterns = [
            r'\$[\d,]+\.?\d*\s*[mkMK]?(?:illion)?',
            r'Price[:\s]+\$[\d,]+',
            r'turnover[:\s]+\$[\d,]+\.?\d*\s*[mkMK]?',
        ]
        for pattern in price_patterns:
            m = re.search(pattern, combined, re.IGNORECASE)
            if m:
                return m.group(0)
        return ''


class SRPBSScraper(BrokerScraper):
    """srpbs.com.au - SR Pharmacy Business Sales (Weebly site)."""
    name = "SRPBS"

    def scrape(self) -> List[Listing]:
        listings = []
        url = "https://www.srpbs.com.au/pharmaciesforsale-902532.html"
        text = web_fetch(url, max_chars=30000)
        if not text:
            logger.warning("SRPBS: could not fetch listings page")
            return listings

        # Parse the structured listings
        # Format: ## SR73XXX Region State\n Key Details\n ...\n Price $XXX
        blocks = re.split(r'(?=##\s+SR\d+)', text)

        for block in blocks:
            if not block.strip():
                continue

            # Skip sold & settled entries
            if 'Sold & Settled' in block or 'SOLD AND SETTLED' in block:
                # Still record them but mark appropriately
                pass

            # Extract reference and title
            header_match = re.match(r'##\s+(SR\d+)\s+(.+?)(?:\s*-\s*(.+))?\n', block)
            if not header_match:
                continue

            ref = header_match.group(1)
            location_text = header_match.group(2).strip()
            status_text = header_match.group(3).strip() if header_match.group(3) else ''

            # Extract price
            price = ''
            price_match = re.search(r'Price\s+(?:Guide\s+)?\$[\d,]+(?:\s*(?:to|[-–])\s*\$[\d,]+)?(?:\s*000)?', block)
            if price_match:
                price = price_match.group(0)

            # Extract state
            state = ''
            state_patterns = {
                'QLD': ['qld', 'queensland'],
                'NSW': ['nsw', 'new south wales', 'hunter', 'riverina'],
                'VIC': ['vic', 'victoria'],
                'TAS': ['tas', 'tasmania', 'hobart'],
                'SA': ['sa', 'south australia'],
                'WA': ['wa', 'western australia'],
                'NT': ['nt', 'northern territory'],
            }
            for st, kws in state_patterns.items():
                for kw in kws:
                    if kw in location_text.lower() or kw in block.lower():
                        state = st
                        break
                if state:
                    break

            title = f"{ref} - {location_text}"
            if 'Sold' in status_text:
                title += f" [{status_text}]"

            # Get description (bullet points)
            desc_lines = []
            for line in block.split('\n'):
                line = line.strip()
                if line.startswith('-') or line.startswith('•'):
                    desc_lines.append(line.lstrip('-•').strip())
                elif 'Key Details' in line or 'key Details' in line:
                    continue

            listings.append(Listing(
                source=self.name,
                title=title,
                price=price,
                location=location_text,
                state=state,
                url=url + f"#{ref}",
                description='; '.join(desc_lines)[:500]
            ))

        logger.info(f"SRPBS: found {len(listings)} listings")
        return listings


class AgileBBScraper(BrokerScraper):
    """agilebb.com.au - Agile Business Brokers (Wix site)."""
    name = "Agile BB"

    def scrape(self) -> List[Listing]:
        listings = []
        url = "https://www.agilebb.com.au/pharmacies-for-sale"
        text = web_fetch(url, max_chars=30000)
        if not text:
            logger.warning("Agile BB: could not fetch listings page")
            return listings

        # Parse the text content
        # Links like [Ref XX: Description](URL)
        link_pattern = re.compile(
            r'\[(?:Ref\s+)?(\w+):\s*(.+?)\]\((https?://www\.agilebb\.com\.au/[^\)]+)\)'
        )

        for match in link_pattern.finditer(text):
            ref = match.group(1)
            desc = match.group(2).strip()
            listing_url = match.group(3)

            # Extract state from description
            state = ''
            location = desc
            state_keywords = {
                'NSW': ['nsw', 'sydney', 'newcastle', 'mid north coast', 'central west', 'eastern suburbs', 'north sydney'],
                'VIC': ['vic', 'victoria', 'melbourne'],
                'QLD': ['qld', 'queensland'],
            }
            for st, kws in state_keywords.items():
                for kw in kws:
                    if kw in desc.lower():
                        state = st
                        break
                if state:
                    break

            title = f"Ref {ref}: {desc}"

            listings.append(Listing(
                source=self.name,
                title=title,
                price='',
                location=location,
                state=state,
                url=listing_url,
                description=desc
            ))

        # Also parse plain text refs (Ref XX: Description without links)
        plain_pattern = re.compile(
            r'Ref\s+(\w+):\s*(?:UNDER OFFER\s+)?(?:NEW LISTING:\s+)?(.+?)(?:\n|$)'
        )
        existing_refs = {l.title.split(':')[0].replace('Ref ', '').strip() for l in listings}

        for match in plain_pattern.finditer(text):
            ref = match.group(1)
            if ref in existing_refs:
                continue
            desc = match.group(2).strip()
            if not desc or len(desc) < 5:
                continue

            state = ''
            state_keywords = {
                'NSW': ['nsw', 'sydney', 'newcastle', 'mid north coast', 'central west', 'eastern suburbs', 'north sydney'],
                'VIC': ['vic', 'victoria', 'melbourne'],
                'QLD': ['qld', 'queensland'],
            }
            for st, kws in state_keywords.items():
                for kw in kws:
                    if kw in desc.lower():
                        state = st
                        break
                if state:
                    break

            status = ''
            if 'UNDER OFFER' in text[max(0, match.start()-20):match.end()]:
                status = ' [Under Offer]'

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


class APGroupScraper(BrokerScraper):
    """apgroup.com.au - AP Group (requires registration for full listings)."""
    name = "AP Group"

    def scrape(self) -> List[Listing]:
        listings = []
        # AP Group requires login to see listings
        # Try the main buy page and any public content
        urls_to_try = [
            "https://www.apgroup.com.au/buy",
            "https://www.apgroup.com.au/pharmacies",
            "https://www.apgroup.com.au/listings",
            "https://www.apgroup.com.au/pharmacy-for-sale",
        ]

        for url in urls_to_try:
            text = web_fetch(url, max_chars=15000)
            if text and 'pharmacy' in text.lower() and 'Page not found' not in text:
                # Try to extract any listing info
                self._parse_text(text, url, listings)
                break

        if not listings:
            logger.info("AP Group: requires registration to view listings (no public listings found)")

        return listings

    def _parse_text(self, text: str, url: str, listings: list):
        """Try to extract listing info from page text."""
        # Look for patterns that might indicate listings
        pass


class APBScraper(BrokerScraper):
    """apb.net.au - Australian Pharmacy Brokers."""
    name = "APB"

    def scrape(self) -> List[Listing]:
        listings = []

        # Try various listing page URLs
        urls_to_try = [
            "https://www.apb.net.au/index.php?page=pharmacies-for-sale",
            "https://www.apb.net.au/index.php?page=listings",
            "https://www.apb.net.au/index.php?page=for-sale",
            "https://www.apb.net.au/pharmacies-for-sale",
        ]

        for url in urls_to_try:
            text = web_fetch(url, max_chars=15000)
            if text and '404' not in text and 'Not Found' not in text:
                self._parse_page(text, url, listings)
                if listings:
                    break

        if not listings:
            logger.info("APB: no public listings page found (contact broker directly)")

        return listings

    def _parse_page(self, text: str, url: str, listings: list):
        """Parse APB page for listings."""
        # Look for pharmacy listing patterns
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if re.search(r'pharmacy|chemist', line, re.IGNORECASE) and \
               re.search(r'\$[\d,]+|for sale|listing', line, re.IGNORECASE):
                price_match = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', line)
                price = price_match.group(0) if price_match else ''
                listings.append(Listing(
                    source=self.name,
                    title=line[:200],
                    price=price,
                    url=url,
                    description=line[:500]
                ))


class AroCRMScraper(BrokerScraper):
    """
    Base scraper for Aro Business Broker CRM sites.
    Used by: iattain.com.au, wisharts.com.au, rxpharmacybrokers.com.au
    These sites use a JS framework that loads listings via API.
    """
    name = "AroCRM"
    base_url = ""
    listings_path = ""

    def scrape(self) -> List[Listing]:
        listings = []

        # Aro CRM sites load data via JS API calls
        # Try the direct API endpoint
        api_urls = [
            f"{self.base_url}/api/v1/listings?type=sale&category=pharmacy",
            f"{self.base_url}/api/listings",
        ]

        # Also try scraping the HTML page which sometimes has data embedded
        page_url = f"{self.base_url}{self.listings_path}"
        html = web_fetch_html(page_url)

        if html:
            # Look for JSON data embedded in the page
            json_patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\});',
                r'var\s+listings\s*=\s*(\[.+?\]);',
                r'"properties"\s*:\s*(\[.+?\])',
            ]
            for pattern in json_patterns:
                m = re.search(pattern, html, re.DOTALL)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        self._parse_json_listings(data, listings)
                    except json.JSONDecodeError:
                        pass

            # Also try to extract from the HTML structure
            self._parse_html_listings(html, listings)

        # Try the text version too
        if not listings:
            text = web_fetch(page_url, max_chars=30000)
            if text:
                self._parse_text_listings(text, listings)

        logger.info(f"{self.name}: found {len(listings)} listings")
        return listings

    def _parse_json_listings(self, data: any, listings: list):
        """Parse JSON listing data from Aro CRM."""
        items = data if isinstance(data, list) else data.get('items', data.get('listings', []))
        for item in items:
            if isinstance(item, dict):
                title = item.get('title', item.get('address', item.get('name', '')))
                price = item.get('display_price', item.get('price', ''))
                location = item.get('suburb', item.get('location', ''))
                state = item.get('state', '')
                url = item.get('link', item.get('url', ''))
                desc = item.get('desc_preview', item.get('description', ''))

                if url and not url.startswith('http'):
                    url = self.base_url + url

                listings.append(Listing(
                    source=self.name,
                    title=str(title),
                    price=str(price),
                    location=str(location),
                    state=str(state),
                    url=str(url),
                    description=str(desc)[:500]
                ))

    def _parse_html_listings(self, html: str, listings: list):
        """Parse HTML for listing content."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')

            # Aro CRM typically uses property-card or listing-card classes
            cards = soup.select('.property-card, .listing-card, .property-item, [class*="listing"]')
            for card in cards:
                title_el = card.select_one('h2, h3, .title, .property-title')
                price_el = card.select_one('.price, .display-price, [class*="price"]')
                link_el = card.select_one('a[href*="/property/"], a[href*="/listing/"]')
                desc_el = card.select_one('.description, .desc, p')

                if title_el:
                    title = title_el.get_text(strip=True)
                    price = price_el.get_text(strip=True) if price_el else ''
                    url = link_el['href'] if link_el and link_el.has_attr('href') else ''
                    desc = desc_el.get_text(strip=True) if desc_el else ''

                    if url and not url.startswith('http'):
                        url = self.base_url + url

                    listings.append(Listing(
                        source=self.name,
                        title=title,
                        price=price,
                        url=url,
                        description=desc[:500]
                    ))
        except ImportError:
            pass

    def _parse_text_listings(self, text: str, listings: list):
        """Fallback: parse text content for any listing patterns."""
        # Aro sites often show "No properties were found" when empty
        if 'No properties were found' in text:
            logger.info(f"{self.name}: no current listings")
            return

        # Look for price + location patterns
        blocks = re.split(r'\n\n+', text)
        for block in blocks:
            if re.search(r'\$[\d,]+|for sale|pharmacy', block, re.IGNORECASE):
                price_match = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                price = price_match.group(0) if price_match else ''
                first_line = block.split('\n')[0].strip()
                if len(first_line) > 5 and len(first_line) < 200:
                    listings.append(Listing(
                        source=self.name,
                        title=first_line,
                        price=price,
                        url=self.base_url + self.listings_path,
                        description=block[:500]
                    ))


class IAttainScraper(AroCRMScraper):
    """iattain.com.au - Attain (Aro CRM platform)."""
    name = "Attain"
    base_url = "https://www.iattain.com.au"
    listings_path = "/pharmacies-for-sale/"


class WishartsScraper(AroCRMScraper):
    """wisharts.com.au - Wisharts Pharmacy Brokers (Aro CRM platform)."""
    name = "Wisharts"
    base_url = "https://www.wisharts.com.au"
    listings_path = "/pharmacy-business-for-sale/"


class RxPharmacyScraper(AroCRMScraper):
    """rxpharmacybrokers.com.au - Rx Pharmacy Brokers (Aro CRM platform)."""
    name = "Rx Pharmacy Brokers"
    base_url = "https://www.rxpharmacybrokers.com.au"
    listings_path = "/buying/"


class BusinessForSaleScraper(BrokerScraper):
    """businessforsale.com.au - Business For Sale marketplace."""
    name = "BusinessForSale"

    def scrape(self) -> List[Listing]:
        listings = []

        # Try multiple URL patterns
        urls_to_try = [
            "https://www.businessforsale.com.au/businesses-for-sale/search/pharmacies-in-australia",
            "https://www.businessforsale.com.au/businesses-for-sale/pharmacies",
            "https://www.businessforsale.com.au/businesses/pharmacy",
            "https://www.businessforsale.com.au/search?q=pharmacy&category=all&location=australia",
        ]

        for url in urls_to_try:
            text = web_fetch(url, max_chars=30000)
            if text and 'Sorry, this page has been removed' not in text and '404' not in text:
                self._parse_listings(text, url, listings)
                if listings:
                    break

        if not listings:
            logger.info("BusinessForSale: could not find correct pharmacy listings URL (site may have changed structure)")

        return listings

    def _parse_listings(self, text: str, base_url: str, listings: list):
        """Parse businessforsale.com.au listing format."""
        # Look for listing blocks
        blocks = re.split(r'\n\n+', text)
        for block in blocks:
            if 'pharmacy' in block.lower() and len(block) > 20:
                price_match = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                price = price_match.group(0) if price_match else ''
                first_line = block.split('\n')[0].strip()

                if len(first_line) > 5:
                    listings.append(Listing(
                        source=self.name,
                        title=first_line,
                        price=price,
                        url=base_url,
                        description=block[:500]
                    ))


class VPBScraper(BrokerScraper):
    """vpb.com.au - Victorian Pharmacy Brokers."""
    name = "VPB"

    def scrape(self) -> List[Listing]:
        listings = []

        urls_to_try = [
            "https://www.vpb.com.au",
            "https://vpb.com.au",
            "https://www.vpb.com.au/pharmacies-for-sale",
            "https://www.vpb.com.au/listings",
        ]

        for url in urls_to_try:
            text = web_fetch(url, max_chars=15000)
            if text and len(text) > 100:
                self._parse_page(text, url, listings)
                break

        if not listings:
            logger.info("VPB: site unreachable or no public listings")

        return listings

    def _parse_page(self, text: str, url: str, listings: list):
        """Parse VPB page."""
        # Look for pharmacy listing patterns
        blocks = re.split(r'\n\n+', text)
        for block in blocks:
            if re.search(r'pharmacy|chemist|for sale', block, re.IGNORECASE) and len(block) > 20:
                price_match = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                price = price_match.group(0) if price_match else ''
                first_line = block.split('\n')[0].strip()
                if len(first_line) > 5 and len(first_line) < 200:
                    listings.append(Listing(
                        source=self.name,
                        title=first_line,
                        price=price,
                        location='Victoria',
                        state='VIC',
                        url=url,
                        description=block[:500]
                    ))


class StatewideBAScraper(BrokerScraper):
    """statewideba.com.au - Statewide Business Advisors."""
    name = "Statewide BA"

    def scrape(self) -> List[Listing]:
        listings = []

        urls_to_try = [
            "https://www.statewideba.com.au",
            "https://statewideba.com.au",
            "https://www.statewideba.com.au/pharmacies-for-sale",
            "https://www.statewideba.com.au/listings",
        ]

        for url in urls_to_try:
            text = web_fetch(url, max_chars=15000)
            if text and len(text) > 100:
                self._parse_page(text, url, listings)
                break

        if not listings:
            logger.info("Statewide BA: site unreachable or no public listings")

        return listings

    def _parse_page(self, text: str, url: str, listings: list):
        """Parse Statewide page for pharmacy listings."""
        blocks = re.split(r'\n\n+', text)
        for block in blocks:
            if re.search(r'pharmacy|chemist', block, re.IGNORECASE) and len(block) > 20:
                price_match = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                price = price_match.group(0) if price_match else ''
                first_line = block.split('\n')[0].strip()
                if len(first_line) > 5 and len(first_line) < 200:
                    listings.append(Listing(
                        source=self.name,
                        title=first_line,
                        price=price,
                        url=url,
                        description=block[:500]
                    ))


class MintBusinessBrokersScraper(BrokerScraper):
    """mintbusinessbrokers.com.au - Mint Business Brokers."""
    name = "Mint Business Brokers"

    def scrape(self) -> List[Listing]:
        listings = []

        urls_to_try = [
            "https://www.mintbusinessbrokers.com.au",
            "https://mintbusinessbrokers.com.au",
            "https://www.mintbusinessbrokers.com.au/pharmacies-for-sale",
            "https://www.mintbusinessbrokers.com.au/listings",
        ]

        for url in urls_to_try:
            text = web_fetch(url, max_chars=15000)
            if text and len(text) > 100:
                self._parse_page(text, url, listings)
                break

        if not listings:
            logger.info("Mint Business Brokers: site unreachable or no public listings")

        return listings

    def _parse_page(self, text: str, url: str, listings: list):
        """Parse Mint page for pharmacy listings."""
        blocks = re.split(r'\n\n+', text)
        for block in blocks:
            if re.search(r'pharmacy|chemist', block, re.IGNORECASE) and len(block) > 20:
                price_match = re.search(r'\$[\d,]+\.?\d*\s*[mkMK]?', block)
                price = price_match.group(0) if price_match else ''
                first_line = block.split('\n')[0].strip()
                if len(first_line) > 5 and len(first_line) < 200:
                    listings.append(Listing(
                        source=self.name,
                        title=first_line,
                        price=price,
                        url=url,
                        description=block[:500]
                    ))


# =============================================================================
# Database manager
# =============================================================================

class ListingDatabase:
    """Manages the broker_listings SQLite table."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create the broker_listings table if it doesn't exist."""
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
        """
        Process scraped listings against the database.
        Returns dict with 'new', 'active', 'removed' counts and new listing details.
        """
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
                    # Existing listing — update last_seen, mark ACTIVE
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
                    # New listing
                    conn.execute(
                        "INSERT INTO broker_listings "
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

            # Mark removed listings (were active but not in current scan)
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
                source = row['source'] if isinstance(row, dict) else row[0]
                status = row['status'] if isinstance(row, dict) else row[1]
                cnt = row['cnt'] if isinstance(row, dict) else row[2]
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
        """Get all active/new listings."""
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

    def mark_notified(self, listing_ids: List[int]):
        """Mark listings as notified."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executemany(
                "UPDATE broker_listings SET notified = 1 WHERE id = ?",
                [(lid,) for lid in listing_ids]
            )
            conn.commit()
        finally:
            conn.close()


# =============================================================================
# Report generator
# =============================================================================

class ReportGenerator:
    """Generates markdown reports for scan results."""

    def __init__(self, output_dir: Path = OUTPUT_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(self, results: Dict, scan_errors: Dict[str, str]) -> str:
        """Generate a markdown report and save to file. Returns file path."""
        today = date.today().isoformat()
        filename = f"broker_scan_{today}.md"
        filepath = self.output_dir / filename

        lines = []
        lines.append(f"# Pharmacy Broker Scan Report — {today}")
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

        total_all = sum(s.get('NEW', 0) + s.get('ACTIVE', 0) for s in source_summary.values())
        lines.append(f"| **TOTAL** | **{stats['new']}** | **{stats['active']}** | **{total_all}** |")

        # New listings detail
        new_listings = results.get('new_listings', [])
        if new_listings:
            lines.append("\n## 🆕 New Listings\n")

            for listing in new_listings:
                lines.append(f"### {listing.title}")
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
                    lines.append(f"- **Description**: {listing.description[:300]}")
                lines.append("")
        else:
            lines.append("\n## New Listings\n")
            lines.append("_No new listings found since last scan._\n")

        # Errors
        if scan_errors:
            lines.append("\n## Scan Errors\n")
            for source, error in scan_errors.items():
                lines.append(f"- **{source}**: {error}")

        report_text = '\n'.join(lines)

        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_text)

        return str(filepath)


# =============================================================================
# Main scanner orchestrator
# =============================================================================

class BrokerScanner:
    """
    Main scanner that orchestrates all broker scrapers.

    Usage:
        scanner = BrokerScanner()
        results = scanner.run_full_scan()
    """

    # All broker scrapers
    SCRAPERS = [
        Practice4SaleScraper,
        SRPBSScraper,
        AgileBBScraper,
        IAttainScraper,
        WishartsScraper,
        RxPharmacyScraper,
        APGroupScraper,
        APBScraper,
        BusinessForSaleScraper,
        VPBScraper,
        StatewideBAScraper,
        MintBusinessBrokersScraper,
    ]

    def __init__(self, db_path: Path = DB_PATH):
        self.db = ListingDatabase(db_path)
        self.report_gen = ReportGenerator()

    def run_full_scan(self, verbose: bool = True) -> Dict:
        """
        Run a full scan of all broker sites.

        Returns dict with:
            - stats: {new, active, removed, total_scraped}
            - new_listings: list of new Listing objects
            - source_summary: dict of counts by source
            - report_path: path to generated report
            - errors: dict of source -> error message
        """
        if verbose:
            print("=" * 60)
            print("  PHARMACY BROKER SCANNER")
            print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            print()

        all_listings = []
        scan_errors = {}

        for scraper_cls in self.SCRAPERS:
            scraper = scraper_cls()
            source_name = scraper.name

            if verbose:
                print(f"  Scanning {source_name}...", end=' ', flush=True)

            try:
                listings = scraper.scrape()
                all_listings.extend(listings)
                if verbose:
                    print(f"OK ({len(listings)} listings)")
            except Exception as e:
                error_msg = str(e)
                scan_errors[source_name] = error_msg
                logger.error(f"Error scanning {source_name}: {e}", exc_info=True)
                if verbose:
                    print(f"FAIL: {error_msg[:80]}")

        if verbose:
            print(f"\n  Total scraped: {len(all_listings)} listings from {len(self.SCRAPERS)} brokers")
            print("  Processing against database...", end=' ', flush=True)

        # Process against database
        results = self.db.process_listings(all_listings)
        results['errors'] = scan_errors

        if verbose:
            print("done")

        # Generate report
        report_path = self.report_gen.generate_report(results, scan_errors)
        results['report_path'] = report_path

        if verbose:
            self._print_summary(results)

        return results

    def _print_summary(self, results: Dict):
        """Print a nice summary to console."""
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

        # Source breakdown
        source_summary = results.get('source_summary', {})
        if source_summary:
            print("  BY BROKER:")
            for source in sorted(source_summary.keys()):
                counts = source_summary[source]
                total = counts.get('NEW', 0) + counts.get('ACTIVE', 0)
                new = counts.get('NEW', 0)
                marker = " [NEW]" if new > 0 else ""
                print(f"    {source:<25} {total:>3} listings{marker}")
            print()

        # New listings
        new_listings = results.get('new_listings', [])
        if new_listings:
            print("  ** NEW LISTINGS **")
            print("  " + "-" * 56)
            for listing in new_listings:
                print(f"    [{listing.source}] {listing.title[:50]}")
                if listing.price:
                    print(f"      Price: {listing.price} ({listing.price_bucket})")
                if listing.state:
                    print(f"      State: {listing.state}")
                if listing.tags:
                    print(f"      Tags:  {', '.join(listing.tags)}")
                print()

        # Errors
        if results.get('errors'):
            print("  ERRORS:")
            for source, error in results['errors'].items():
                print(f"    {source}: {error[:60]}")
            print()

        print(f"  Report saved: {results.get('report_path', 'N/A')}")
        print("=" * 60)


# =============================================================================
# CLI entry point
# =============================================================================

if __name__ == '__main__':
    scanner = BrokerScanner()
    results = scanner.run_full_scan(verbose=True)
