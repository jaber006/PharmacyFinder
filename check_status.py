import json
r = json.load(open('output/verification_results.json','r',encoding='utf-8'))
print(f'Results: {len(r)}')
invalid = [x for x in r if not x.get('still_valid', True)]
print(f'Invalid: {len(invalid)}')
for x in invalid[:10]:
    name = x.get('name', '?')
    state = x.get('state', '?')
    vc = x.get('verdict_change', '?')
    print(f'  {name} ({state}): {vc}')
