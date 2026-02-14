"""Run Item 136 scan for all Australian states and report results."""
import subprocess
import sys
import re

STATES = ['TAS', 'NSW', 'VIC', 'QLD', 'SA', 'WA', 'NT', 'ACT']

results = {}

for state in STATES:
    print(f"\n{'='*60}")
    print(f"  SCANNING {state}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, '-u', 'main.py', 'scan', '--region', state, '--skip-collect'],
            capture_output=True, text=True, timeout=300
        )
        output = result.stdout + result.stderr
        
        # Extract Item 136 candidate count
        m136 = re.search(r'Scanning Item 136\.\.\.\s*->\s*(\d+)\s*candidates', output)
        item136_count = int(m136.group(1)) if m136 else 0
        
        # Extract total unique
        m_total = re.search(r'After de-duplication:\s*(\d+)\s*unique', output)
        total = int(m_total.group(1)) if m_total else 0
        
        # Extract medical centres loaded count
        m_mc = re.search(r'Medical centres:\s*(\d+)', output)
        mc_count = int(m_mc.group(1)) if m_mc else 0
        
        results[state] = {
            'item136': item136_count,
            'total': total,
            'medical_centres': mc_count,
        }
        
        # Print Item 136 specific results
        lines = output.split('\n')
        in_item136 = False
        for line in lines:
            if 'Item 136' in line and ('candidate' in line.lower() or '->' in line):
                in_item136 = True
                print(f"  {line.strip()}")
            elif in_item136 and line.strip() and not line.startswith('='):
                if 'Item 13' in line and 'Item 136' not in line:
                    in_item136 = False
                elif 'candidate' in line.lower() or 'unique' in line.lower():
                    in_item136 = False
                    print(f"  {line.strip()}")
        
        print(f"  Item 136 candidates: {item136_count}")
        print(f"  Medical centres loaded: {mc_count}")
        
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT for {state}")
        results[state] = {'item136': -1, 'total': -1, 'medical_centres': 0}
    except Exception as e:
        print(f"  ERROR for {state}: {e}")
        results[state] = {'item136': -1, 'total': -1, 'medical_centres': 0}

print(f"\n{'='*60}")
print(f"  SUMMARY - Item 136 Opportunities by State")
print(f"{'='*60}")
total_136 = 0
for state in STATES:
    r = results.get(state, {})
    count = r.get('item136', 0)
    mc = r.get('medical_centres', 0)
    total_136 += max(count, 0)
    print(f"  {state}: {count} Item 136 opportunities ({mc} med centres)")
print(f"  TOTAL: {total_136} Item 136 opportunities nationwide")
