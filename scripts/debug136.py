import json, re

data = json.load(open('output/item136_opportunities.json','r',encoding='utf-8'))
for d in data[:5]:
    reason = d.get('reason','')
    gp_match = re.search(r'(\d+(?:\.\d+)?)\s*FTE\s*GPs?', reason)
    gp_match2 = re.search(r'GPs?[=:]\s*(\d+)', reason)
    fte_match = re.search(r'FTE[=]\s*(\d+(?:\.\d+)?)', reason)
    print(f"Name: {d['name']}")
    print(f"  FTE regex: {gp_match.group(1) if gp_match else 'NO MATCH'}")
    print(f"  GP= regex: {gp_match2.group(1) if gp_match2 else 'NO MATCH'}")
    print(f"  FTE= regex: {fte_match.group(1) if fte_match else 'NO MATCH'}")
    print(f"  Reason: {reason[:150]}")
    print()
