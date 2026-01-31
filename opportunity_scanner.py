"""
Opportunity Scanner

The full playbook MJ taught me:
1. Monitor development news (new Woolworths, Coles, shopping centers, housing, hospitals)
2. When major development announced → that area is growing
3. Scan that area for pharmacy opportunities (medical centers without pharmacies)
4. Move before others notice

This combines:
- Development news monitoring (growth signals)
- Medical center scanning (opportunity identification)
- Priority scoring (where to focus)
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from scrapers.development_news import DevelopmentNewsScanner, DevelopmentSignal
from scrapers.medical_centers import MedicalCenterScanner, MedicalCenter
from typing import List, Dict


class OpportunityScanner:
    """
    Scans for greenfield pharmacy opportunities by combining:
    1. Development news signals (growth areas)
    2. Medical centers without on-site pharmacies
    """
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.dev_scanner = DevelopmentNewsScanner()
        self.med_scanner = MedicalCenterScanner()
        
        self.signals: List[DevelopmentSignal] = []
        self.opportunities: List[MedicalCenter] = []
        self.priority_areas: List[Dict] = []
    
    def scan_development_news(self, days_back: int = 30) -> List[DevelopmentSignal]:
        """Step 1: Find areas with development activity."""
        print("\n" + "="*60)
        print("STEP 1: SCANNING DEVELOPMENT NEWS")
        print("="*60 + "\n")
        
        self.signals = self.dev_scanner.scan_all_sources(days_back=days_back)
        self.priority_areas = self.dev_scanner.get_priority_areas()
        
        print(f"\nFound {len(self.signals)} development signals")
        print(f"Identified {len(self.priority_areas)} priority areas\n")
        
        # Show top areas
        print("Top 10 Priority Areas:")
        print("-" * 40)
        for area in self.priority_areas[:10]:
            print(f"  {area['location']}, {area['state']} (score: {area['total_score']})")
            for sig in area['signals'][:2]:
                print(f"    └─ {sig.development_type}: {sig.title[:50]}...")
        
        return self.signals
    
    def scan_priority_areas(self, limit: int = 10) -> List[MedicalCenter]:
        """Step 2: Scan priority areas for medical centers without pharmacies."""
        print("\n" + "="*60)
        print("STEP 2: SCANNING PRIORITY AREAS FOR OPPORTUNITIES")
        print("="*60 + "\n")
        
        for area in self.priority_areas[:limit]:
            print(f"\nScanning {area['location']}, {area['state']}...")
            centers = self.med_scanner.scan_area(area['location'], area['state'])
            print(f"  Found {len(centers)} medical centers")
        
        # Score opportunities
        self.med_scanner.score_opportunities(self.priority_areas)
        self.opportunities = self.med_scanner.get_opportunities()
        
        print(f"\nTotal opportunities found: {len(self.opportunities)}")
        
        return self.opportunities
    
    def scan_specific_area(self, suburb: str, state: str):
        """Scan a specific area (not from development news)."""
        print(f"\nScanning specific area: {suburb}, {state}")
        
        centers = self.med_scanner.scan_area(suburb, state)
        self.med_scanner.score_opportunities(self.priority_areas)
        self.opportunities = self.med_scanner.get_opportunities()
        
        return self.opportunities
    
    def generate_report(self) -> Dict:
        """Generate a full opportunity report."""
        print("\n" + "="*60)
        print("GENERATING OPPORTUNITY REPORT")
        print("="*60 + "\n")
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'development_signals': len(self.signals),
                'priority_areas': len(self.priority_areas),
                'opportunities': len(self.opportunities),
            },
            'priority_areas': [
                {
                    'location': a['location'],
                    'state': a['state'],
                    'score': a['total_score'],
                    'signals': [
                        {
                            'type': s.development_type,
                            'title': s.title,
                            'source': s.source_url,
                        }
                        for s in a['signals']
                    ]
                }
                for a in self.priority_areas[:20]
            ],
            'opportunities': [
                {
                    'name': o.name,
                    'address': o.address,
                    'suburb': o.suburb,
                    'state': o.state,
                    'num_gps': o.num_gps,
                    'has_specialists': o.has_specialists,
                    'specialist_types': o.specialist_types,
                    'phone': o.phone,
                    'website': o.website,
                    'score': o.opportunity_score,
                    'notes': o.notes,
                }
                for o in sorted(self.opportunities, key=lambda x: x.opportunity_score, reverse=True)[:50]
            ],
        }
        
        # Save report
        report_path = self.output_dir / f"opportunity_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Report saved to: {report_path}")
        
        # Print summary
        print("\n" + "-"*40)
        print("SUMMARY")
        print("-"*40)
        print(f"Development signals found: {report['summary']['development_signals']}")
        print(f"Priority areas identified: {report['summary']['priority_areas']}")
        print(f"Opportunities found: {report['summary']['opportunities']}")
        
        if self.opportunities:
            print("\nTop 5 Opportunities:")
            for opp in sorted(self.opportunities, key=lambda x: x.opportunity_score, reverse=True)[:5]:
                print(f"  [{opp.opportunity_score}] {opp.name}")
                print(f"      {opp.suburb}, {opp.state}")
                if opp.has_specialists:
                    print(f"      Specialists: {', '.join(opp.specialist_types)}")
        
        return report
    
    def run_full_scan(self, days_back: int = 30, area_limit: int = 10):
        """Run the complete opportunity scan."""
        print("\n" + "#"*60)
        print("# PHARMACY OPPORTUNITY SCANNER")
        print("# " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("#"*60)
        
        # Step 1: Find growth areas
        self.scan_development_news(days_back=days_back)
        
        # Step 2: Scan those areas for opportunities
        self.scan_priority_areas(limit=area_limit)
        
        # Step 3: Generate report
        report = self.generate_report()
        
        print("\n" + "#"*60)
        print("# SCAN COMPLETE")
        print("#"*60 + "\n")
        
        return report


def main():
    parser = argparse.ArgumentParser(
        description="Scan for greenfield pharmacy opportunities"
    )
    parser.add_argument(
        '--mode', 
        choices=['full', 'news', 'area'], 
        default='full',
        help="Scan mode: full=news+areas, news=development news only, area=specific area"
    )
    parser.add_argument(
        '--days', 
        type=int, 
        default=30,
        help="Days of news to scan (default: 30)"
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=10,
        help="Number of priority areas to scan (default: 10)"
    )
    parser.add_argument(
        '--suburb',
        type=str,
        help="Specific suburb to scan (requires --state)"
    )
    parser.add_argument(
        '--state',
        type=str,
        help="State for specific suburb scan"
    )
    parser.add_argument(
        '--output',
        type=str,
        default='output',
        help="Output directory (default: output)"
    )
    
    args = parser.parse_args()
    
    scanner = OpportunityScanner(output_dir=args.output)
    
    if args.mode == 'full':
        scanner.run_full_scan(days_back=args.days, area_limit=args.limit)
    
    elif args.mode == 'news':
        scanner.scan_development_news(days_back=args.days)
        scanner.generate_report()
    
    elif args.mode == 'area':
        if not args.suburb or not args.state:
            print("Error: --suburb and --state required for area mode")
            return
        scanner.scan_specific_area(args.suburb, args.state)
        scanner.generate_report()


if __name__ == '__main__':
    main()
