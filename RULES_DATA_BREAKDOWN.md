# Rules → Data → Code Breakdown

Every rule is a set of data checks. This document maps each rule requirement to:
1. What data is needed
2. Where to get it (scrape/download/API)
3. What the Python check looks like

---

## Shared Data (used by multiple rules)

### Pharmacies (ALL rules need this)
| Field | Description | Source |
|-------|-------------|--------|
| name | Pharmacy name | PBS Approved Suppliers (findapharmacy.com.au) |
| address | Full street address | PBS Approved Suppliers |
| suburb | Suburb/town name | PBS Approved Suppliers |
| postcode | Postcode | PBS Approved Suppliers |
| state | State code | PBS Approved Suppliers |
| lat, lon | Coordinates | Geocode from address (Nominatim) |

**Primary source:** PBS Approved Suppliers — https://www.findapharmacy.com.au  
**Backup source:** OpenStreetMap (Overpass API, `amenity=pharmacy`)  
**Update frequency:** Monthly  

```python
# Core check used by almost every rule
def nearest_pharmacy(lat, lon, pharmacies):
    """Returns (pharmacy_dict, distance_km)"""
    return min(pharmacies, key=lambda p: geodesic((lat, lon), (p['lat'], p['lon'])).km)
```

---

## Item 130: New pharmacy (1.5km)

### Data needed

| Requirement | Data | Fields | Source |
|-------------|------|--------|--------|
| 130(a) | All pharmacy locations | lat, lon | PBS Approved Suppliers |
| 130(b)(i) | Supermarkets with GLA | name, lat, lon, gla_sqm, brand | OpenStreetMap + brand defaults |
| 130(b)(i) | GP practices | name, lat, lon, fte | OpenStreetMap + Healthdirect |
| 130(b)(ii) | Supermarkets with GLA | name, lat, lon, gla_sqm, brand | OpenStreetMap + brand defaults |

### GLA defaults by brand (when actual GLA unknown)
| Brand | Default GLA (m²) | Qualifies for |
|-------|-------------------|---------------|
| Woolworths | 3,500 | 130(b)(ii) alone |
| Coles | 3,500 | 130(b)(ii) alone |
| ALDI | 1,700 | 130(b)(i) with GP |
| IGA | 800 | 130(b)(i) with GP |
| Woolworths Metro | 1,200 | 130(b)(i) with GP |
| Coles Local | 1,000 | 130(b)(i) with GP |
| Harris Farm | 2,500 | 130(b)(ii) alone |
| Costco | 14,000 | 130(b)(ii) alone |

### Python logic
```python
def check_item_130(candidate_lat, candidate_lon, pharmacies, supermarkets, gps):
    # (a) Distance check
    nearest, dist_km = nearest_pharmacy(candidate_lat, candidate_lon, pharmacies)
    if dist_km < 1.5:
        return FAIL, f"Nearest pharmacy {dist_km:.3f}km (need ≥1.5km)"
    
    # (b)(ii) Large supermarket within 500m — no GP needed
    supers_500m = within_radius(candidate_lat, candidate_lon, supermarkets, 0.5)
    for s in supers_500m:
        gla = s['gla_sqm'] or DEFAULT_GLA[s['brand']]
        if gla >= 2500:
            return PASS, f"130(b)(ii): {s['name']} GLA={gla}m² within 500m"
    
    # (b)(i) Smaller supermarket + GP within 500m
    gps_500m = within_radius(candidate_lat, candidate_lon, gps, 0.5)
    for s in supers_500m:
        gla = s['gla_sqm'] or DEFAULT_GLA[s['brand']]
        if gla >= 1000 and len(gps_500m) > 0:
            return PASS, f"130(b)(i): {s['name']} GLA={gla}m² + GP within 500m"
    
    return FAIL, "No qualifying supermarket/GP combo within 500m"
```

---

## Item 131: New pharmacy (10km by road)

### Data needed

| Requirement | Data | Fields | Source |
|-------------|------|--------|--------|
| Distance | All pharmacy locations | lat, lon | PBS Approved Suppliers |
| Road distance | Driving route | route_km | OSRM API (self-hosted or public) |

### Python logic
```python
def check_item_131(candidate_lat, candidate_lon, pharmacies):
    nearest, geodesic_km = nearest_pharmacy(candidate_lat, candidate_lon, pharmacies)
    
    # Quick pre-filter
    if geodesic_km > 8.0:  # 8km straight ≈ 11km+ by road
        return PASS, f"Clearly passes — geodesic {geodesic_km:.1f}km"
    if geodesic_km < 5.0:  # 5km straight ≈ 7km by road
        return FAIL, f"Clearly fails — geodesic {geodesic_km:.1f}km"
    
    # Borderline — get actual road distance
    road_km = osrm_route_distance(candidate_lat, candidate_lon, nearest['lat'], nearest['lon'])
    if road_km >= 10.0:
        return PASS, f"Road distance {road_km:.1f}km (≥10km)"
    return FAIL, f"Road distance {road_km:.1f}km (<10km)"
```

---

## Item 132: New additional pharmacy in same town

### Data needed

| Requirement | Data | Fields | Source |
|-------------|------|--------|--------|
| 132(a)(i) | Pharmacy town/postcode | suburb, postcode | PBS Approved Suppliers |
| 132(a)(ii) | Pharmacy locations | lat, lon | PBS Approved Suppliers |
| 132(a)(iii) | Road distances to ALL other pharmacies | route_km | OSRM |
| 132(b)(i) | GP practices in same town | name, suburb, postcode, fte | Healthdirect + OpenStreetMap |
| 132(b)(ii) | Supermarkets in same town | name, suburb, postcode, gla_sqm | OpenStreetMap + brand defaults |

### Critical: "same town" = same suburb name + same postcode

### Python logic
```python
def check_item_132(candidate_lat, candidate_lon, candidate_suburb, candidate_postcode,
                   pharmacies, gps, supermarkets):
    # (a)(i) Must be in same town as an existing pharmacy
    same_town_pharmacies = [p for p in pharmacies 
                            if p['suburb'] == candidate_suburb 
                            and p['postcode'] == candidate_postcode]
    if not same_town_pharmacies:
        return FAIL, "No pharmacy in the same town+postcode"
    
    # (a)(ii) ≥200m straight line from nearest pharmacy
    nearest = min(same_town_pharmacies, key=lambda p: geodesic_m(candidate, p))
    dist_m = geodesic_m(candidate_lat, candidate_lon, nearest['lat'], nearest['lon'])
    if dist_m < 200:
        return FAIL, f"Nearest pharmacy {dist_m:.0f}m (need ≥200m)"
    
    # (a)(iii) ≥10km by road from ALL OTHER pharmacies (not the nearest)
    other_pharmacies = [p for p in pharmacies if p['id'] != nearest['id']]
    for p in other_pharmacies:
        road_km = osrm_route_distance(candidate_lat, candidate_lon, p['lat'], p['lon'])
        if road_km is not None and road_km < 10.0:
            return FAIL, f"Other pharmacy {p['name']} only {road_km:.1f}km by road"
    
    # (b)(i) ≥4 FTE GPs in same town
    same_town_gps = [g for g in gps 
                     if g['suburb'] == candidate_suburb 
                     and g['postcode'] == candidate_postcode]
    total_fte = sum(g.get('fte', 1.0) for g in same_town_gps)
    if total_fte < 4.0:
        return FAIL, f"Only {total_fte:.1f} FTE GPs in town (need ≥4)"
    
    # (b)(ii) 1-2 supermarkets in same town with combined GLA ≥2,500m²
    same_town_supers = [s for s in supermarkets 
                        if s['suburb'] == candidate_suburb 
                        and s['postcode'] == candidate_postcode]
    # Sort by GLA descending, take top 2 only
    same_town_supers.sort(key=lambda s: s.get('gla_sqm') or DEFAULT_GLA.get(s['brand'], 0), reverse=True)
    top_2 = same_town_supers[:2]
    combined_gla = sum(s.get('gla_sqm') or DEFAULT_GLA.get(s['brand'], 0) for s in top_2)
    if combined_gla < 2500:
        return FAIL, f"Combined supermarket GLA {combined_gla:.0f}m² (need ≥2,500m²)"
    
    return PASS, "All Item 132 requirements met"
```

---

## Item 133: New pharmacy in small shopping centre

### Data needed — "Small Shopping Centre" definition

| Field | Requirement | Source |
|-------|-------------|--------|
| name | Centre name | OpenStreetMap (`shop=mall`) + curated list |
| lat, lon | Coordinates | OpenStreetMap / geocode |
| gla_sqm | ≥5,000 m² | Council records, centre websites, property databases |
| tenant_count | ≥15 (and <50) commercial establishments | Centre directories, Google Maps listing counts |
| has_supermarket_2500 | Contains supermarket ≥2,500m² GLA | OSM + brand defaults |
| has_parking | Customer parking | OSM (`amenity=parking` nearby) or assumed true for centres |
| single_management | Under single management | Manual verification / centre websites |

### What counts as a "commercial establishment" (from glossary)
**YES:** shops, cafes, restaurants, takeaways, service businesses (hairdresser, bank, etc.)  
**NO:** offices (accountant/lawyer unless shopfront), govt offices, car wash, library, kindergarten, childcare (unless for shoppers), storage, ATMs, temp selling points  
**Special:** 2+ shops by same business = 1 establishment. Max 1 professional shopfront (accountant etc.) counts for small centres.

### Python logic
```python
def check_item_133(candidate_lat, candidate_lon, shopping_centres, pharmacies, 
                   large_shopping_centres, private_hospitals):
    # Must be in a small shopping centre
    nearby_centres = within_radius(candidate_lat, candidate_lon, shopping_centres, 0.3)
    if not nearby_centres:
        return FAIL, "No shopping centre within 300m"
    
    centre = nearby_centres[0]  # nearest
    gla = centre.get('gla_sqm', 0)
    tenants = centre.get('tenant_count', 0)
    
    # Must be SMALL (not large — <50 tenants)
    if tenants >= 50:
        return FAIL, f"Centre has {tenants} tenants — use Item 134/134A instead"
    if gla > 0 and gla < 5000:
        return FAIL, f"Centre GLA {gla}m² (need ≥5,000m²)"
    if tenants > 0 and tenants < 15:
        return FAIL, f"Centre has {tenants} tenants (need ≥15)"
    
    # (c) No pharmacy already in the centre
    pharmacies_in_centre = within_radius(centre['lat'], centre['lon'], pharmacies, 0.1)
    if pharmacies_in_centre:
        return FAIL, "Pharmacy already in this centre"
    
    # (b) ≥500m from nearest pharmacy EXCLUDING those in large SCs or private hospitals
    excluded_pharmacy_ids = set()
    for p in pharmacies:
        # Is this pharmacy inside a large shopping centre?
        for lsc in large_shopping_centres:
            if geodesic_km(p['lat'], p['lon'], lsc['lat'], lsc['lon']) < 0.3:
                excluded_pharmacy_ids.add(p['id'])
        # Is this pharmacy inside a private hospital?
        for h in private_hospitals:
            if geodesic_km(p['lat'], p['lon'], h['lat'], h['lon']) < 0.15:
                excluded_pharmacy_ids.add(p['id'])
    
    non_excluded = [p for p in pharmacies if p['id'] not in excluded_pharmacy_ids]
    nearest, dist_m = nearest_from(candidate_lat, candidate_lon, non_excluded)
    if dist_m < 500:
        return FAIL, f"Nearest non-excluded pharmacy {dist_m:.0f}m (need ≥500m)"
    
    return PASS, "All Item 133 requirements met"
```

---

## Item 134: Large shopping centre (no existing pharmacy)

### Data needed — "Large Shopping Centre" definition

| Field | Requirement | Source |
|-------|-------------|--------|
| name | Centre name | OpenStreetMap + curated list |
| lat, lon | Coordinates | OpenStreetMap / geocode |
| gla_sqm | ≥5,000 m² | Council records, centre websites |
| tenant_count | ≥50 commercial establishments | Centre directories |
| has_supermarket_2500 | Contains supermarket ≥2,500m² GLA | OSM + brand defaults |
| has_parking | Customer parking | Assumed true for large centres |
| single_management | Under single management | Centre websites |

### Python logic
```python
def check_item_134(candidate_lat, candidate_lon, shopping_centres, pharmacies):
    nearby_centres = within_radius(candidate_lat, candidate_lon, shopping_centres, 0.3)
    if not nearby_centres:
        return FAIL, "No shopping centre within 300m"
    
    centre = nearby_centres[0]
    tenants = centre.get('tenant_count', 0)
    gla = centre.get('gla_sqm', 0)
    
    if tenants > 0 and tenants < 50:
        return FAIL, f"Centre has {tenants} tenants (need ≥50 for large)"
    if gla > 0 and gla < 5000:
        return FAIL, f"Centre GLA {gla}m² (need ≥5,000m²)"
    
    # No existing pharmacy in the centre
    pharmacies_in_centre = within_radius(centre['lat'], centre['lon'], pharmacies, 0.3)
    if pharmacies_in_centre:
        return FAIL, f"Existing pharmacy in centre: {pharmacies_in_centre[0]['name']}"
    
    # No distance requirement from pharmacies outside the centre
    return PASS, "Large shopping centre with no pharmacy"
```

---

## Item 134A: Large shopping centre (additional pharmacy)

### Data needed — same as Item 134 plus:

| Field | Requirement | Source |
|-------|-------------|--------|
| existing_pharmacy_count | How many pharmacies already in centre | PBS Suppliers + geocoding |
| tenant_count | Determines tier: 100-199 or ≥200 | Centre directories |

### Python logic
```python
def check_item_134a(candidate_lat, candidate_lon, shopping_centres, pharmacies):
    nearby_centres = within_radius(candidate_lat, candidate_lon, shopping_centres, 0.3)
    if not nearby_centres:
        return FAIL, "No shopping centre within 300m"
    
    centre = nearby_centres[0]
    tenants = centre.get('tenant_count', 0)
    
    # Must have at least 100 tenants for 134A to apply
    if tenants > 0 and tenants < 100:
        return FAIL, f"Centre has {tenants} tenants (need ≥100 for Item 134A)"
    
    # Count existing pharmacies in the centre
    pharmacies_in_centre = within_radius(centre['lat'], centre['lon'], pharmacies, 0.3)
    existing_count = len(pharmacies_in_centre)
    
    if existing_count == 0:
        return FAIL, "No existing pharmacy — use Item 134 instead"
    
    # Determine max allowed
    if tenants >= 200:
        max_existing = 2  # allows 3rd
    elif tenants >= 100:
        max_existing = 1  # allows 2nd
    else:
        max_existing = 0  # can't determine
    
    if existing_count > max_existing:
        return FAIL, f"Already {existing_count} pharmacies (max {max_existing} for {tenants} tenants)"
    
    return PASS, f"Room for additional pharmacy ({existing_count}/{max_existing} existing)"
```

---

## Item 135: Large private hospital

### Data needed — "Large Private Hospital" definition

| Field | Requirement | Source |
|-------|-------------|--------|
| name | Hospital name | AIHW MyHospitals + OpenStreetMap |
| lat, lon | Coordinates | AIHW / OSM / geocode |
| hospital_type | Must be "private" (not public) | AIHW MyHospitals data |
| bed_count | ≥150 (proxy for admission capacity) | AIHW MyHospitals |
| admission_capacity | Can admit ≥150 patients at one time | State health dept registers |

### Where to get hospital data
- **AIHW MyHospitals:** https://www.myhospitals.gov.au — has bed counts and public/private classification
- **State registers:** Each state publishes licensed private hospital registers with admission capacity
- **OpenStreetMap:** `amenity=hospital` — has name and location but rarely bed count or type

### Python logic
```python
def check_item_135(candidate_lat, candidate_lon, hospitals, pharmacies):
    nearby = within_radius(candidate_lat, candidate_lon, hospitals, 0.3)
    if not nearby:
        return FAIL, "No hospital within 300m"
    
    hospital = nearby[0]
    
    # Must be private
    if 'public' in (hospital.get('hospital_type', '') or '').lower():
        return FAIL, f"{hospital['name']} is public — need private"
    
    # Must have ≥150 admission capacity (bed_count as proxy)
    beds = hospital.get('bed_count', 0) or 0
    if beds > 0 and beds < 150:
        return FAIL, f"{hospital['name']} has {beds} beds (need ≥150)"
    
    # No pharmacy in the hospital
    pharmacies_in_hospital = within_radius(hospital['lat'], hospital['lon'], pharmacies, 0.15)
    if pharmacies_in_hospital:
        return FAIL, f"Existing pharmacy in hospital: {pharmacies_in_hospital[0]['name']}"
    
    return PASS, f"Large private hospital with no pharmacy"
```

---

## Item 136: Large medical centre

### Data needed — "Large Medical Centre" definition

| Field | Requirement | Source |
|-------|-------------|--------|
| name | Centre name | Healthdirect + Hotdoc + OpenStreetMap |
| lat, lon | Coordinates | Geocode / OSM |
| single_management | Under single management | Manual / centre websites |
| operating_hours | ≥70 hours/week | Hotdoc, Google Maps, centre websites |
| gp_available_hours | GP available ≥70 hours/week | Hotdoc (shows bookable hours per GP) |
| num_gps | Headcount of GPs | Hotdoc (lists individual practitioners) |
| total_fte | ≥8 FTE PBS prescribers | Calculated: sum(each GP's hours / 38) |
| medical_fte | ≥7 of the 8 FTE must be medical practitioners | Hotdoc (can see GP vs dentist/optom) |
| hours_per_week | Centre operating hours | Google Maps / Hotdoc |

### Where to get medical centre data
- **Hotdoc:** https://www.hotdoc.com.au — lists individual GPs per practice, their available hours, and appointment availability. Best source for GP headcount and hours.
- **Healthdirect:** https://www.healthdirect.gov.au/australian-health-services — national directory of health services with practice details.
- **OpenStreetMap:** `amenity=doctors` or `healthcare=doctor` — has location but rarely GP count or hours.
- **Google Maps:** Operating hours, sometimes lists practitioners.

### FTE calculation
```python
def calculate_fte(gps_at_centre):
    """
    Full-time = 38 hours/week.
    Each GP's hours are capped at 38 (even if they work 57hrs, that's 1.5 FTE not 1.5).
    Only count hours physically at THIS centre.
    Telehealth only counts if GP is physically at the centre.
    """
    total_fte = 0
    medical_fte = 0
    for gp in gps_at_centre:
        hours = min(gp['hours_per_week_at_centre'], 38)  # cap at 38
        fte = hours / 38.0
        total_fte += fte
        if gp['type'] == 'medical_practitioner':  # not dentist/optom/midwife/nurse prac
            medical_fte += fte
    return total_fte, medical_fte
```

### Python logic
```python
def check_item_136(candidate_lat, candidate_lon, medical_centres, pharmacies,
                   shopping_centres, hospitals):
    # Must be at a medical centre
    nearby_mcs = within_radius(candidate_lat, candidate_lon, medical_centres, 0.3)
    if not nearby_mcs:
        return FAIL, "No medical centre within 300m"
    
    mc = nearby_mcs[0]
    
    # (b) No pharmacy already in the centre
    pharmacies_in_mc = within_radius(mc['lat'], mc['lon'], pharmacies, 0.15)
    if pharmacies_in_mc:
        return FAIL, f"Existing pharmacy in centre: {pharmacies_in_mc[0]['name']}"
    
    # (a) Large medical centre definition
    hours = mc.get('operating_hours', 0) or 0
    gp_hours = mc.get('gp_available_hours', 0) or 0
    if hours > 0 and hours < 70:
        return FAIL, f"Centre operates {hours} hrs/week (need ≥70)"
    if gp_hours > 0 and gp_hours < 70:
        return FAIL, f"GP available {gp_hours} hrs/week (need ≥70)"
    
    # (c) Distance check — TWO BRANCHES
    mc_in_complex = (
        any(geodesic_km(mc['lat'], mc['lon'], sc['lat'], sc['lon']) < 0.05 for sc in shopping_centres) or
        any(geodesic_km(mc['lat'], mc['lon'], h['lat'], h['lon']) < 0.05 for h in hospitals)
    )
    
    if mc_in_complex:
        # (c)(i) MC is in a SC/hospital: 300m from ANY pharmacy except those in a DIFFERENT SC/hospital
        pharmacies_300m = within_radius(candidate_lat, candidate_lon, pharmacies, 0.3)
        for p in pharmacies_300m:
            p_in_different_complex = (
                any(geodesic_km(p['lat'], p['lon'], sc['lat'], sc['lon']) < 0.3 
                    and geodesic_km(mc['lat'], mc['lon'], sc['lat'], sc['lon']) > 0.3
                    for sc in shopping_centres) or
                any(geodesic_km(p['lat'], p['lon'], h['lat'], h['lon']) < 0.15
                    and geodesic_km(mc['lat'], mc['lon'], h['lat'], h['lon']) > 0.15
                    for h in hospitals)
            )
            if not p_in_different_complex:
                return FAIL, f"Pharmacy {p['name']} within 300m and not in a different complex"
    else:
        # (c)(ii) MC not in SC/hospital: 300m from nearest pharmacy EXCLUDING those in SC/hospital
        non_complex_pharmacies = exclude_complex_pharmacies(pharmacies, shopping_centres, hospitals)
        nearest, dist_m = nearest_from(candidate_lat, candidate_lon, non_complex_pharmacies)
        if nearest and dist_m < 300:
            return FAIL, f"Nearest non-excluded pharmacy {dist_m:.0f}m (need ≥300m)"
    
    # (d) FTE check
    total_fte = mc.get('total_fte', 0) or 0
    medical_fte = mc.get('medical_fte', 0) or 0
    num_gps = mc.get('num_gps', 0) or 0
    
    # Estimate FTE from headcount if FTE data missing
    if total_fte == 0 and num_gps > 0:
        total_fte = num_gps * 0.8
        medical_fte = total_fte  # assume all are medical practitioners if no breakdown
    
    if total_fte > 0 and total_fte < 8:
        return FAIL, f"Only {total_fte:.1f} FTE prescribers (need ≥8)"
    if medical_fte > 0 and medical_fte < 7:
        return FAIL, f"Only {medical_fte:.1f} medical FTE (need ≥7 of the 8)"
    
    return PASS, "All Item 136 requirements met"
```

---

## Master Data Sources Summary

| Data | Primary Source | Backup Source | Fields Needed |
|------|--------------|--------------|---------------|
| **Pharmacies** | PBS Approved Suppliers (findapharmacy.com.au) | OpenStreetMap | name, address, suburb, postcode, state, lat, lon |
| **Supermarkets** | OpenStreetMap (Overpass API) | Google Maps | name, brand, lat, lon, gla_sqm |
| **GPs / Medical centres** | Hotdoc + Healthdirect | OpenStreetMap | name, lat, lon, num_gps, fte, hours_per_week |
| **Hospitals** | AIHW MyHospitals | OpenStreetMap | name, lat, lon, bed_count, hospital_type (public/private) |
| **Shopping centres** | Curated list + OpenStreetMap | Centre directories | name, lat, lon, gla_sqm, tenant_count |
| **Road distances** | OSRM (self-hosted Docker) | OSRM public server (rate limited) | route_km |
| **Geocoding** | Nominatim (self-hosted or public) | Google Maps API | lat, lon from address |
| **Population** | ABS Census (SA2 level) | — | population by area |
| **Town boundaries** | ABS SAL/POA boundaries | — | suburb name, postcode, geometry |
| **Council DAs** | PlanningAlerts.org.au API | Council websites | DA number, address, development type |

---

## Helper functions needed

```python
from geopy.distance import geodesic

def geodesic_km(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers

def geodesic_m(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters

def within_radius(lat, lon, items, radius_km):
    """Return items within radius_km, sorted by distance."""
    results = []
    for item in items:
        d = geodesic_km(lat, lon, item['lat'], item['lon'])
        if d <= radius_km:
            results.append((item, d))
    results.sort(key=lambda x: x[1])
    return results

def nearest_from(lat, lon, items):
    """Return (nearest_item, distance_m) or (None, inf)."""
    best, best_d = None, float('inf')
    for item in items:
        d = geodesic_m(lat, lon, item['lat'], item['lon'])
        if d < best_d:
            best, best_d = item, d
    return best, best_d

def osrm_route_distance(lat1, lon1, lat2, lon2):
    """Get driving distance in km via OSRM. Returns None on failure."""
    url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
    resp = requests.get(url, timeout=10)
    if resp.ok:
        data = resp.json()
        if data['code'] == 'Ok':
            return data['routes'][0]['distance'] / 1000.0
    return None

def exclude_complex_pharmacies(pharmacies, large_shopping_centres, private_hospitals):
    """Return pharmacies NOT inside a large shopping centre or private hospital."""
    excluded_ids = set()
    for p in pharmacies:
        for sc in large_shopping_centres:
            if geodesic_km(p['lat'], p['lon'], sc['lat'], sc['lon']) < 0.3:
                excluded_ids.add(p['id'])
                break
        for h in private_hospitals:
            if geodesic_km(p['lat'], p['lon'], h['lat'], h['lon']) < 0.15:
                excluded_ids.add(p['id'])
                break
    return [p for p in pharmacies if p['id'] not in excluded_ids]

# Brand GLA defaults
DEFAULT_GLA = {
    'woolworths': 3500, 'coles': 3500, 'aldi': 1700, 'iga': 800,
    'woolworths metro': 1200, 'coles local': 1000, 'harris farm': 2500,
    'costco': 14000, 'foodworks': 600, 'drakes': 2500, 'spar': 700,
}
```
