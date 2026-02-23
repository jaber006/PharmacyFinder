import json
data = json.load(open('output/verify_batch.json'))
for d in data:
    print(f"{d['index']} {d['state']} {d['lat']},{d['lng']} {d['poi_name'][:50]}")
