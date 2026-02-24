import json

with open('output/scored_v2.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

likely = [x for x in data if x.get('verdict') == 'LIKELY']
print(f'Total LIKELY: {len(likely)}')
for i, item in enumerate(likely):
    print(f"{i}: id={item['id']} | {item['name']} | {item.get('poi_type','?')} | {item.get('state','?')} | ({item.get('lat')}, {item.get('lng')})")

# Save compact version
compact = []
for item in likely:
    compact.append({
        'id': item['id'],
        'name': item['name'],
        'poi_type': item.get('poi_type', ''),
        'state': item.get('state', ''),
        'lat': item.get('lat'),
        'lng': item.get('lng'),
        'address': item.get('address', ''),
        'evidence': item.get('evidence', ''),
        'nearest_pharmacy': item.get('nearest_pharmacy', ''),
        'nearest_pharmacy_km': item.get('nearest_pharmacy_km'),
        'pharmacy_5km': item.get('pharmacy_5km', 0),
        'pharmacy_10km': item.get('pharmacy_10km', 0),
        'pop_5km': item.get('pop_5km', 0),
        'pop_10km': item.get('pop_10km', 0),
        'best_rule': item.get('best_rule', ''),
        'best_rule_display': item.get('best_rule_display', ''),
        'rules_checked': item.get('rules_checked', {}),
        'manual_checks': item.get('manual_checks', []),
        'manual_check_list': item.get('manual_check_list', []),
    })

with open('output/likely_compact.json', 'w', encoding='utf-8') as f:
    json.dump(compact, f, indent=2, ensure_ascii=False)
print(f"\nSaved {len(compact)} items to output/likely_compact.json")
