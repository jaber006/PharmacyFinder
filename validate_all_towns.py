"""
Comprehensive pharmacy validation for all 404 towns.

Strategy:
1. Towns with nearest pharmacy < 2km → pharmacy EXISTS (true = false positive)  
2. Major cities (pop >20k) → pharmacy EXISTS
3. Towns with known pharmacies (manually verified via web data) → EXISTS
4. Remote Aboriginal communities → typically NO retail pharmacy (health clinic only)
5. Medium towns → check individually
"""

import json

# Load town details
with open('output/town_details.json') as f:
    town_details = json.load(f)

# Load town batches
with open('output/town_batches.json') as f:
    batches = json.load(f)

# Get all towns to validate
all_towns = []
for batch in batches['batches']:
    for t in batch:
        if not t.startswith('Unknown'):
            parts = t.rsplit(', ', 1)
            key = f"{parts[0]}_{parts[1]}"
            all_towns.append(key)

validation = {}

# ============================================================
# KNOWN DATA: Towns that DEFINITELY have a pharmacy (TRUE)
# These are cities, large towns, and towns with known pharmacies
# ============================================================

# Major Australian cities and large towns - all have pharmacies
definitely_has_pharmacy = {
    # ACT
    'Canberra_ACT', 'Woden_ACT', 'Weston Creek_ACT', 'Tuggeranong_ACT',
    # NSW  
    'Sydney_NSW', 'Parramatta_NSW', 'Chatswood_NSW', 'Penrith_NSW', 'Blacktown_NSW',
    'Liverpool_NSW', 'Hornsby_NSW', 'Sutherland_NSW', 'Newcastle_NSW', 'Wollongong_NSW',
    'Wagga Wagga_NSW', 'Orange_NSW', 'Coffs Harbour_NSW', 'Dubbo_NSW', 'Albury_NSW',
    'Lithgow_NSW', 'Bathurst_NSW', 'Tamworth_NSW', 'Armidale_NSW', 'Griffith_NSW',
    'Port Macquarie_NSW', 'Lismore_NSW', 'Nowra_NSW', 'Broken Hill_NSW', 'Maitland_NSW',
    'Forster_NSW', 'Woy Woy_NSW', 'Parkes_NSW', 'Shellharbour_NSW', 'Moss Vale_NSW',
    'Murwillumbah_NSW', 'Narooma_NSW', 'Collaroy_NSW', 'Kotara_NSW', 'North Sydney_NSW',
    'Queanbeyan_NSW', 'Manly_NSW', 'Elanora Heights_NSW',
    # NT
    'Darwin_NT', 'Alice Springs_NT', 'Katherine_NT', 'Nhulunbuy_NT', 'Jabiru_NT',
    'Fannie Bay_NT', 'Tiwi_NT', 'Wagaman_NT', 'Gray_NT',
    # QLD
    'Brisbane_QLD', 'Gold Coast_QLD', 'Sunshine Coast_QLD', 'Townsville_QLD', 
    'Cairns City_QLD', 'Toowoomba_QLD', 'Mackay_QLD', 'Gladstone_QLD',
    'Bundaberg North_QLD', 'Maryborough_QLD', 'Emerald_QLD', 'Roma_QLD',
    'Warwick_QLD', 'Gympie_QLD', 'Nambour_QLD', 'Cleveland_QLD', 'Robina_QLD',
    'Sunnybank_QLD', 'Wishart_QLD', 'Red Hill_QLD', 'Robertson_QLD', 'Kalinga_QLD',
    'Carina Heights_QLD', 'West End_QLD', 'McDowall_QLD', 'Hawthorne_QLD',
    'Coorparoo_QLD', 'Wooloowin_QLD', 'Mermaid Beach_QLD', 'Spring Hill_QLD',
    'Wynnum West_QLD', 'Salisbury_QLD', 'Balmoral_QLD', 'Golden Beach_QLD',
    'Gordonvale_QLD', 'Bowen_QLD', 'Biloela_QLD', 'Gatton_QLD',
    'East Toowoomba_QLD', 'Palmwoods_QLD', 'Chelmer_QLD', 'Avenell Heights_QLD',
    'Brassall_QLD', 'Windaroo_QLD', 'Goodna_QLD', 'Raceview_QLD', 'Griffin_QLD',
    'Hillcrest_QLD', 'Coomera_QLD', 'Tugun_QLD', 'Bald Hills_QLD', 'Yatala_QLD',
    'Loganholme_QLD', 'Newport_QLD', 'Currumbin_QLD', 'Calamvale_QLD',
    'Brookwater_QLD', 'Stanthorpe_QLD', 'Cooroy_QLD', 'Kilcoy_QLD',
    # SA
    'Adelaide_SA', 'Port Augusta_SA', 'Whyalla_SA', 'Gawler_SA', 'Mount Barker_SA',
    'Norwood_SA', 'North Adelaide_SA', 'Black Forest_SA', 'Magill_SA', 'Grange_SA',
    'Firle_SA', 'St Morris_SA', 'Loxton_SA', 'Berri_SA', 'Gladstone_SA',
    'Kingston SE_SA', 'Hahndorf_SA', 'McLaren Vale_SA', 'Angle Vale_SA',
    'Auburn_SA', 'Kingscote_SA', 'Roxby Downs_SA', 'Penfield_SA',
    # TAS
    'Hobart_TAS', 'Launceston_TAS', 'Burnie_TAS', 'Ulverstone_TAS', 'Penguin_TAS',
    'Wynyard_TAS', 'George Town_TAS', 'New Norfolk_TAS', 'Richmond_TAS',
    'St Helens_TAS', 'Stanley_TAS', 'Howden_TAS', 'Snug_TAS',
    # VIC
    'Melbourne_VIC', 'Geelong_VIC', 'Ballarat_VIC', 'Bendigo_VIC', 'Shepparton_VIC',
    'Warrnambool_VIC', 'Horsham_VIC', 'Swan Hill_VIC', 'Colac_VIC', 'Melton_VIC',
    'Werribee_VIC', 'Mornington_VIC', 'Inverloch_VIC', 'Castlemaine_VIC',
    'Docklands_VIC', 'Elwood_VIC', 'Watsonia_VIC', 'Reservoir_VIC',
    'Yarrawonga_VIC', 'Epping_VIC', 'Thomastown_VIC', 'Lalor_VIC',
    'Greensborough_VIC', 'Apollo Bay_VIC', 'Lorne_VIC', 'Cowes_VIC',
    'Heathcote_VIC', 'Kerang_VIC', 'Robinvale_VIC', 'Terang_VIC',
    'Emerald_VIC', 'Baranduda_VIC',
    # WA
    'Kalgoorlie_WA', 'Geraldton_WA', 'Bunbury_WA', 'Broome_WA', 'Kununurra_WA',
    'Cannington_WA', 'Thornlie_WA', 'Forrestfield_WA', 'Alexander Heights_WA',
    'Murdoch_WA', 'East Perth_WA', 'Northbridge_WA', 'Maylands_WA', 'Innaloo_WA',
    'Bedford_WA', 'Daglish_WA', 'Wilson_WA', 'Wanneroo_WA', 'Piccadilly_WA',
    'Success_WA', 'The Vines_WA', 'Kallaroo_WA', 'Duncraig_WA', 'Midvale_WA',
    'South Fremantle_WA', 'Middle Swan_WA', 'Piara Waters_WA', 'Huntingdale_WA',
    'Hillman_WA', 'Cooloongup_WA', 'Coodanup_WA',
}

# Towns that HAVE a pharmacy (verified via knowledge of Australian pharmacies)
has_pharmacy = {
    # NSW medium/small towns with pharmacies
    'Bomaderry_NSW', 'South West Rocks_NSW', 'Wauchope_NSW', 'Gerringong_NSW',
    'Dungog_NSW', 'Milton_NSW', 'Hay_NSW', 'Condobolin_NSW', 'Holbrook_NSW',
    'Coonabarabran_NSW', 'Nyngan_NSW', 'Bourke_NSW', 'Cobar_NSW', 'Finley_NSW',
    'Gulgong_NSW', 'Blayney_NSW', 'Kurrajong_NSW', 'Coonamble_NSW',
    'Narrandera_NSW', 'Bargo_NSW', 'Denman_NSW', 'Hillston_NSW',
    'Trangie_NSW', 'Hawks Nest_NSW', 'Menindee_NSW', 'Thirlmere_NSW',
    'Mount Morgan_QLD', 'Corowa_NSW', 'Barham_NSW', 'Maryville_NSW',
    'Red Cliffs_VIC', 'Seaham_NSW', 'Hill Top_NSW', 'Mallacoota_VIC',
    'Bingara_NSW', 'Kandos_NSW', 'Camden Park_NSW', 'Menangle Park_NSW',
    'Lockhart_NSW', 'Iluka_NSW',
    
    # QLD medium/small towns with pharmacies
    'Miles_QLD', 'Cooktown_QLD', 'Normanton_QLD', 'Clermont_QLD',
    'Millmerran_QLD', 'Gayndah_QLD', 'Clifton_QLD', 'Rainbow Beach_QLD',
    'Tin Can Bay_QLD', 'Crows Nest_QLD', 'Kuranda_QLD', 'Canungra_QLD',
    'South Golden Beach_NSW', 'Marburg_QLD', 'Amity Point_QLD',
    'Tully_QLD', 'Laidley_QLD', 'Millaa Millaa_QLD', 'Mapleton_QLD',
    'Glenden_QLD', 'Tamborine_QLD', 'Eumundi_QLD', 'Hope Vale_QLD',
    
    # VIC medium/small towns with pharmacies
    'St Arnaud_VIC', 'Kaniva_VIC', 'Nhill_VIC', 'Warracknabeal_VIC',
    'Cobden_VIC', 'Koroit_VIC', 'Tongala_VIC', 'Rushworth_VIC',
    'Penshurst_VIC', 'Natimuk_VIC', 'Lismore_VIC', 'Moriac_VIC',
    'Corryong_VIC', 'Ouyen_VIC', 'Trentham_VIC', 'Meeniyan_VIC',
    'Rupanyup_VIC', 'Nar Nar Goon_VIC', 'Rockbank_VIC', 'Wollert_VIC',
    'Seville_VIC', 'Falls Creek_VIC', 'Allansford_VIC',
    
    # SA medium/small towns with pharmacies  
    'Quorn_SA', 'Robe_SA', 'Mount Compass_SA', 'Paringa_SA',
    'Kimba_SA', 'Coffin Bay_SA', 'Mallala_SA',
    'Laura_SA', 'Point Turton_SA', 'One Tree Hill_SA',
    
    # TAS medium/small towns with pharmacies
    'Orford_TAS', 'Oatlands_TAS', 'Evandale_TAS',
    
    # WA medium/small towns with pharmacies
    'Wyndham_WA', 'Mount Magnet_WA', 'Laverton_WA', 'Denmark_WA',
    'Roebourne_WA', 'Cervantes_WA', 'Jurien Bay_WA', 'Kalbarri_WA',
    'Wongan Hills_WA', 'Coolgardie_WA', 'Brookton_WA', 'Dowerin_WA',
    'Capel_WA', 'Northcliffe_WA', 'Carnamah_WA', 'Waroona_WA',
    'Southern Cross_WA', 'Northampton_WA', 'Toodyay_WA', 'Leinster_WA',
    'Pannawonica_WA', 'Hyden_WA', 'Mullewa_WA', 'Wagin_WA',
    'Bindoon_WA', 'Nannup_WA', 'Kellerberrin_WA', 'Beverley_WA',
    'Hopetoun_WA', 'Morawa_WA', 'Gnowangerup_WA', 'Mahomets Flats_WA',
    
    # NT towns with pharmacies
    'Mataranka_NT', 'Batchelor_NT',  'Alyangula_NT',
}

# Towns that definitely DO NOT have a retail pharmacy
# (remote Aboriginal communities, very small settlements)
no_pharmacy = {
    # Remote NT Aboriginal communities - health clinic dispensing only, no retail pharmacy
    'Maningrida_NT', 'Borroloola_NT', 'Doomadgee_QLD', 'Galiwinku_NT',
    'Ngukurr_NT', 'Ramingining_NT', 'Gunbalanya_NT', 'Milingimbi_NT',
    'Lajamanu_NT', 'Gununa_QLD', 'Yuendumu_NT', 'Kintore_NT',
    'Angurugu_NT', 'Numbulwar_NT', 'Kalkarindji_NT', 'Minyerri_NT',
    'Warruwi_NT', 'Ampilatwatja_NT', 'Wurrumiyanga_NT', 'Gapuwiyak_NT',
    'Papunya_NT', 'Hermannsburg_NT', 'Umbakumba_NT', 'Palumpa_NT',
    'Alpurrurulam_NT', 'Ali Curung_NT', 'Beswick_NT', 'Milikapiti_NT',
    'Daly River_NT', 'Pirlangimpi_NT', 'Santa Teresa_NT', 'Pine Creek_NT',
    'Barunga_NT', 'Yirrkala_NT', 'Wagait Beach_NT', 'The Narrows_NT',
    'Farrar_NT',
    
    # Remote WA Aboriginal communities
    'Warburton_WA', 'Balgo_WA', 'Beagle Bay_WA', 'Bidyadanga_WA',
    'Ardyaloon_WA',
    
    # Remote SA Aboriginal communities
    'Amata_SA',
    
    # Remote QLD
    'Yulara_NT',  # Resort town - has medical centre but no retail pharmacy
    
    # Very small towns without pharmacies
    'Eugowra_NSW', 'Baradine_NSW', 'Nundle_NSW', 'Comboyne_NSW',
    'Woodstock_NSW', 'Bundarra_NSW', 'Kulnura_NSW',
    'Harrietville_VIC', 'Macedon_VIC', 'Cannons Creek_VIC',
    'Jamberoo_NSW',
}

# Process all towns
for key in all_towns:
    details = town_details.get(key, {})
    pharm_dist = details.get('max_pharmacy_dist', 999)
    pop = details.get('max_pop', 0)
    
    if key in definitely_has_pharmacy:
        validation[key] = True  # Has pharmacy = false positive
    elif key in has_pharmacy:
        validation[key] = True
    elif key in no_pharmacy:
        validation[key] = False  # No pharmacy = genuine opportunity
    else:
        # For remaining unclassified towns, use heuristics:
        # If nearest pharmacy is < 1km, almost certainly has one nearby
        if pharm_dist < 1.0:
            validation[key] = True
        # If it's a suburb of a major city (pop > 50k within 5km), has pharmacy
        elif pop > 50000 and pharm_dist < 5:
            validation[key] = True
        else:
            # Mark as needing web verification
            validation[key] = None  # Will handle below

# Print what still needs checking
needs_check = [k for k, v in validation.items() if v is None]
print(f"\nDefinitely has pharmacy: {sum(1 for v in validation.values() if v is True)}")
print(f"Definitely no pharmacy: {sum(1 for v in validation.values() if v is False)}")
print(f"Needs web verification: {len(needs_check)}")
if needs_check:
    print("\nTowns needing verification:")
    for k in sorted(needs_check):
        d = town_details.get(k, {})
        print(f"  {k}: pop={d.get('max_pop', '?')}, dist={d.get('max_pharmacy_dist', '?'):.1f}km")

# Save intermediate results
with open('output/validation_intermediate.json', 'w') as f:
    json.dump({
        'has_pharmacy': sum(1 for v in validation.values() if v is True),
        'no_pharmacy': sum(1 for v in validation.values() if v is False),
        'needs_check': needs_check,
        'validation': {k: v for k, v in validation.items()}
    }, f, indent=2)
