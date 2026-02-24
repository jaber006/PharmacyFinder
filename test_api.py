import urllib.request, json
key = 'AIzaSyBM2QXFoTaVL_XYnumkW8fE5MUhYNzzZGM'
url = f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=-30.9553,148.3936&radius=15000&type=pharmacy&key={key}'
resp = urllib.request.urlopen(url)
data = json.loads(resp.read())
print('Status:', data.get('status'))
print('Results:', len(data.get('results', [])))
for r in data.get('results', []):
    loc = r['geometry']['location']
    print(f"  {r['name']} @ {loc['lat']},{loc['lng']}")
