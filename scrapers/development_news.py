"""
Development News Scanner

Monitors for growth signals that indicate emerging pharmacy opportunities:
- New supermarket announcements (Woolworths, Coles, Aldi)
- Shopping center developments
- Housing estate approvals
- Hospital/health precinct expansions
- Aged care facility developments
- Major infrastructure projects

When a development is announced, the area becomes a priority zone for:
1. Medical centers without on-site pharmacies
2. Commercial tenancies in new developments
3. Gaps in pharmacy coverage
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional
import json
import time
import re


@dataclass
class DevelopmentSignal:
    """Represents a development announcement that signals growth."""
    title: str
    location: str  # Suburb/region
    state: str
    development_type: str  # supermarket, shopping_center, housing, hospital, aged_care, infrastructure
    developer: Optional[str]
    estimated_completion: Optional[str]
    source_url: str
    date_found: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    priority_score: int = 0  # Higher = more interesting for pharmacy opportunities
    notes: str = ""


class DevelopmentNewsScanner:
    """Scans news sources for development announcements across Australia."""
    
    # Keywords that signal pharmacy-relevant developments
    SUPERMARKET_KEYWORDS = ['woolworths', 'coles', 'aldi', 'iga', 'supermarket']
    SHOPPING_KEYWORDS = ['shopping centre', 'shopping center', 'retail precinct', 'town centre']
    HOUSING_KEYWORDS = ['housing estate', 'residential development', 'subdivision', 'new homes', 'masterplanned']
    HEALTH_KEYWORDS = ['hospital', 'medical centre', 'medical center', 'health precinct', 'aged care', 'nursing home']
    INFRASTRUCTURE_KEYWORDS = ['new road', 'bypass', 'train station', 'transport hub']
    
    # News sources to scan
    NEWS_SOURCES = [
        {
            'name': 'Urban Developer',
            'url': 'https://www.theurbandeveloper.com/',
            'type': 'property_news'
        },
        {
            'name': 'Shopping Centre News',
            'url': 'https://www.shoppingcentrenews.com.au/',
            'type': 'retail_news'
        },
        {
            'name': 'Inside Retail',
            'url': 'https://insideretail.com.au/',
            'type': 'retail_news'
        },
        {
            'name': 'Woolworths Newsroom',
            'url': 'https://www.woolworthsgroup.com.au/au/en/media/latest-news.html',
            'type': 'company_news'
        },
        {
            'name': 'Coles Newsroom',
            'url': 'https://www.colesgroup.com.au/media-releases/',
            'type': 'company_news'
        },
    ]
    
    # Regional news sources by state
    REGIONAL_SOURCES = {
        'TAS': [
            {'name': 'Pulse Tasmania', 'url': 'https://pulsetasmania.com.au/'},
            {'name': 'The Examiner', 'url': 'https://www.examiner.com.au/'},
            {'name': 'The Mercury', 'url': 'https://www.themercury.com.au/'},
        ],
        'VIC': [
            {'name': 'Herald Sun', 'url': 'https://www.heraldsun.com.au/'},
        ],
        'NSW': [
            {'name': 'Daily Telegraph', 'url': 'https://www.dailytelegraph.com.au/'},
        ],
        'QLD': [
            {'name': 'Courier Mail', 'url': 'https://www.couriermail.com.au/'},
        ],
        'WA': [
            {'name': 'The West Australian', 'url': 'https://thewest.com.au/'},
        ],
        'SA': [
            {'name': 'The Advertiser', 'url': 'https://www.adelaidenow.com.au/'},
        ],
    }
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.signals: List[DevelopmentSignal] = []
    
    def scan_all_sources(self, days_back: int = 30) -> List[DevelopmentSignal]:
        """Scan all news sources for development announcements."""
        print(f"Scanning development news (last {days_back} days)...")
        
        # Scan national sources
        for source in self.NEWS_SOURCES:
            try:
                self._scan_source(source)
                time.sleep(2)  # Rate limiting
            except Exception as e:
                print(f"  Error scanning {source['name']}: {e}")
        
        # Scan regional sources
        for state, sources in self.REGIONAL_SOURCES.items():
            for source in sources:
                try:
                    self._scan_source(source, state=state)
                    time.sleep(2)
                except Exception as e:
                    print(f"  Error scanning {source['name']}: {e}")
        
        # Score and sort signals
        self._score_signals()
        self.signals.sort(key=lambda x: x.priority_score, reverse=True)
        
        return self.signals
    
    def _scan_source(self, source: dict, state: str = None):
        """Scan a single news source for relevant articles."""
        print(f"  Scanning {source['name']}...")
        
        try:
            response = requests.get(source['url'], headers=self.headers, timeout=15)
            if response.status_code != 200:
                return
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find article links and headlines
            articles = soup.find_all(['article', 'div'], class_=re.compile(r'article|post|news|story'))
            
            for article in articles[:20]:  # Limit to first 20 articles
                title_elem = article.find(['h1', 'h2', 'h3', 'a'])
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True).lower()
                
                # Check if article is relevant
                dev_type = self._classify_development(title)
                if dev_type:
                    link = title_elem.get('href') or article.find('a', href=True)
                    if isinstance(link, dict):
                        link = link.get('href', '')
                    elif hasattr(link, 'get'):
                        link = link.get('href', '')
                    
                    # Make absolute URL
                    if link and not link.startswith('http'):
                        link = source['url'].rstrip('/') + '/' + link.lstrip('/')
                    
                    signal = DevelopmentSignal(
                        title=title_elem.get_text(strip=True),
                        location=self._extract_location(title),
                        state=state or self._extract_state(title),
                        development_type=dev_type,
                        developer=self._extract_developer(title),
                        estimated_completion=None,
                        source_url=link or source['url'],
                        date_found=datetime.now().isoformat(),
                    )
                    self.signals.append(signal)
        
        except requests.RequestException as e:
            print(f"    Request failed: {e}")
    
    def _classify_development(self, text: str) -> Optional[str]:
        """Classify the type of development from headline text."""
        text = text.lower()
        
        if any(kw in text for kw in self.SUPERMARKET_KEYWORDS):
            if any(word in text for word in ['new', 'open', 'build', 'construction', 'announce', 'approved']):
                return 'supermarket'
        
        if any(kw in text for kw in self.SHOPPING_KEYWORDS):
            if any(word in text for word in ['new', 'develop', 'build', 'construction', 'announce', 'approved']):
                return 'shopping_center'
        
        if any(kw in text for kw in self.HOUSING_KEYWORDS):
            return 'housing'
        
        if any(kw in text for kw in self.HEALTH_KEYWORDS):
            if any(word in text for word in ['new', 'expand', 'build', 'construction', 'approved']):
                return 'health_precinct'
        
        if any(kw in text for kw in self.INFRASTRUCTURE_KEYWORDS):
            return 'infrastructure'
        
        return None
    
    def _extract_location(self, text: str) -> str:
        """Extract suburb/location from headline."""
        # Common patterns: "in [Location]", "[Location]'s new", "at [Location]"
        patterns = [
            r"in ([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)",
            r"at ([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)",
            r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)'s",
            r"for ([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return "Unknown"
    
    def _extract_state(self, text: str) -> str:
        """Extract state from headline."""
        states = {
            'tasmania': 'TAS', 'tas': 'TAS',
            'victoria': 'VIC', 'vic': 'VIC', 'melbourne': 'VIC',
            'new south wales': 'NSW', 'nsw': 'NSW', 'sydney': 'NSW',
            'queensland': 'QLD', 'qld': 'QLD', 'brisbane': 'QLD',
            'western australia': 'WA', 'wa': 'WA', 'perth': 'WA',
            'south australia': 'SA', 'sa': 'SA', 'adelaide': 'SA',
            'northern territory': 'NT', 'nt': 'NT', 'darwin': 'NT',
            'act': 'ACT', 'canberra': 'ACT',
        }
        
        text_lower = text.lower()
        for key, state in states.items():
            if key in text_lower:
                return state
        
        return "Unknown"
    
    def _extract_developer(self, text: str) -> Optional[str]:
        """Extract developer/company name from headline."""
        developers = ['woolworths', 'coles', 'aldi', 'scentre', 'vicinity', 'stockland', 
                     'mirvac', 'lendlease', 'qic', 'charter hall', 'gpf', 'dexus']
        
        text_lower = text.lower()
        for dev in developers:
            if dev in text_lower:
                return dev.title()
        
        return None
    
    def _score_signals(self):
        """Score signals by pharmacy opportunity potential."""
        for signal in self.signals:
            score = 0
            
            # Development type scoring
            if signal.development_type == 'supermarket':
                score += 50  # High value - supermarket = pharmacy anchor
            elif signal.development_type == 'shopping_center':
                score += 40
            elif signal.development_type == 'health_precinct':
                score += 60  # Highest - medical = scripts
            elif signal.development_type == 'housing':
                score += 20
            elif signal.development_type == 'infrastructure':
                score += 15
            
            # Developer bonus
            if signal.developer in ['Woolworths', 'Coles']:
                score += 20  # Major retailers = anchor tenants
            
            # State preferences (adjust based on your focus)
            if signal.state == 'TAS':
                score += 10  # Home turf bonus
            elif signal.state in ['VIC', 'NSW', 'QLD']:
                score += 5  # Major markets
            
            signal.priority_score = score
    
    def search_google_news(self, query: str, days_back: int = 30) -> List[DevelopmentSignal]:
        """Search Google News for specific development queries."""
        # This would use a news API or Google Custom Search
        # For now, placeholder for manual implementation
        pass
    
    def get_priority_areas(self) -> List[dict]:
        """Get list of areas with development signals, ranked by opportunity."""
        areas = {}
        
        for signal in self.signals:
            key = f"{signal.location}, {signal.state}"
            if key not in areas:
                areas[key] = {
                    'location': signal.location,
                    'state': signal.state,
                    'signals': [],
                    'total_score': 0
                }
            
            areas[key]['signals'].append(signal)
            areas[key]['total_score'] += signal.priority_score
        
        # Sort by total score
        ranked = sorted(areas.values(), key=lambda x: x['total_score'], reverse=True)
        
        return ranked
    
    def export_signals(self, filepath: str):
        """Export signals to JSON file."""
        data = [
            {
                'title': s.title,
                'location': s.location,
                'state': s.state,
                'development_type': s.development_type,
                'developer': s.developer,
                'estimated_completion': s.estimated_completion,
                'source_url': s.source_url,
                'date_found': s.date_found,
                'priority_score': s.priority_score,
                'notes': s.notes,
            }
            for s in self.signals
        ]
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Exported {len(data)} signals to {filepath}")


if __name__ == '__main__':
    # Test the scanner
    scanner = DevelopmentNewsScanner()
    signals = scanner.scan_all_sources(days_back=30)
    
    print(f"\nFound {len(signals)} development signals:")
    for signal in signals[:10]:
        print(f"  [{signal.priority_score}] {signal.development_type}: {signal.title[:60]}...")
        print(f"      Location: {signal.location}, {signal.state}")
        print(f"      Source: {signal.source_url[:50]}...")
        print()
    
    # Export to file
    scanner.export_signals('development_signals.json')
    
    # Show priority areas
    print("\nPriority Areas:")
    for area in scanner.get_priority_areas()[:5]:
        print(f"  {area['location']}, {area['state']} (score: {area['total_score']})")
        for sig in area['signals']:
            print(f"    - {sig.development_type}: {sig.title[:40]}...")
