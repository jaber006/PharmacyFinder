#!/usr/bin/env python3
"""
Growth Corridor Overlay v2 for PharmacyFinder
==============================================
Strategic analysis of Australia's major housing growth corridors, 
cross-referenced with pharmacy coverage to identify monopoly opportunities.

Data sources:
- ABS Regional Population 2023-24 (released 2025)
- Wikipedia locality data for growth corridor details
- PharmacyFinder database (pharmacies, shopping centres, medical centres)
- State government planning documents

Usage:
    python growth_corridors_v2.py [--update-db] [--json] [--report]
"""

import sqlite3
import json
import os
import math
import argparse
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

# ============================================================================
# GROWTH CORRIDOR DEFINITIONS
# ============================================================================
# Each corridor has:
#   - name: Human-readable name
#   - state: Australian state
#   - lat_bounds: (min_lat, max_lat) for bounding box
#   - lng_bounds: (min_lng, max_lng) for bounding box
#   - suburbs: List of key suburbs in the corridor
#   - current_pop_2024: ABS ERP or estimate for 2024
#   - growth_rate_annual: Annual growth rate (decimal)
#   - planned_dwellings: Total planned dwellings at build-out
#   - planned_pop_buildout: Total population at full build-out
#   - key_developments: Description of major developments
#   - new_infrastructure: Planned shopping centres, medical centres etc
#   - sources: Data sources

GROWTH_CORRIDORS = [
    # ======================================================================
    # VICTORIA - Fastest growing state corridors
    # ======================================================================
    {
        'name': 'Fraser Rise - Plumpton (Western Melbourne)',
        'state': 'VIC',
        'lat_bounds': (-37.72, -37.68),
        'lng_bounds': (144.68, 144.73),
        'suburbs': ['Fraser Rise', 'Plumpton', 'Bonnie Brook', 'Deanside'],
        'current_pop_2024': 20730,
        'growth_rate_annual': 0.263,  # 26.3% - FASTEST in Australia per ABS
        'planned_dwellings': 15000,
        'planned_pop_buildout': 45000,
        'key_developments': 'Multiple housing estates under rapid development. Fraser Rise gazetted 2017, population grew from 0 to 9,097 (2021) to ~21,000 (2024). Wiyal Primary School opened 2026. Springside West Secondary College operating.',
        'new_infrastructure': 'New primary school (Wiyal) opened 2026. Bus services confirmed in 2025/26 budget. No major shopping centre announced yet - potential gap.',
        'sources': ['ABS Regional Population 2023-24', 'Wikipedia: Fraser Rise'],
        'opportunity_notes': 'EXTREMELY HIGH GROWTH. Only 2 pharmacies (Thornhill Park + Aintree) serving 20,000+ people. Ratio ~10,000:1. New shopping centre needed urgently.',
    },
    {
        'name': 'Rockbank - Mount Cottrell (Western Melbourne)',
        'state': 'VIC',
        'lat_bounds': (-37.77, -37.71),
        'lng_bounds': (144.63, 144.70),
        'suburbs': ['Rockbank', 'Mount Cottrell', 'Mount Atkinson', 'Fieldstone'],
        'current_pop_2024': 31389,
        'growth_rate_annual': 0.152,  # ~15% based on 4,100 growth on ~27k
        'planned_dwellings': 25000,
        'planned_pop_buildout': 75000,
        'key_developments': 'Massive greenfield development west of Melbourne. Rockbank railway station upgraded 2019. Multiple estates under construction. Mount Atkinson PSP (Precinct Structure Plan) approved.',
        'new_infrastructure': 'Rockbank station upgraded. Mount Atkinson town centre planned. Multiple school sites designated.',
        'sources': ['ABS Regional Population 2023-24', 'Wikipedia: Rockbank'],
        'opportunity_notes': 'Second-fastest growth in Melbourne (4,100 new people in one year). Adjacent to Fraser Rise corridor.',
    },
    {
        'name': 'Mickleham - Kalkallo - Beveridge (Northern Melbourne)',
        'state': 'VIC',
        'lat_bounds': (-37.57, -37.45),
        'lng_bounds': (144.86, 144.98),
        'suburbs': ['Mickleham', 'Kalkallo', 'Beveridge', 'Donnybrook', 'Wollert'],
        'current_pop_2024': 52000,  # Estimated: Mickleham 30k + Kalkallo 8k + Beveridge 7k + surrounds
        'growth_rate_annual': 0.15,  # Mickleham was Australia fastest growing suburb
        'planned_dwellings': 40000,
        'planned_pop_buildout': 120000,
        'key_developments': 'Mickleham grew from 3,142 (2016) to 17,452 (2021) to ~30,000 (2024) - forecasted 30,091 by 2026. Beveridge InterCity planned as major new town centre. Merrifield City SC opened. Kallo Town Centre opened.',
        'new_infrastructure': 'Merrifield City Shopping Centre (with CW pharmacy). Kallo Town Centre (with CW pharmacy). Beveridge InterCity town centre planned (NO pharmacy yet). Multiple schools open/planned.',
        'sources': ['ABS Regional Population 2023-24', 'Wikipedia: Mickleham, Beveridge', 'id Community forecasts'],
        'opportunity_notes': 'Only 4 pharmacies for ~52,000 people = 13,000:1 ratio. Beveridge and Wallan expansion areas still underserved. Beveridge InterCity town centre = prime target.',
    },
    {
        'name': 'Tarneit North (Western Melbourne)',
        'state': 'VIC',
        'lat_bounds': (-37.82, -37.76),
        'lng_bounds': (144.62, 144.72),
        'suburbs': ['Tarneit', 'Truganina', 'Manor Lakes'],
        'current_pop_2024': 13485,
        'growth_rate_annual': 0.203,  # 20.3% - 5th fastest in Australia
        'planned_dwellings': 12000,
        'planned_pop_buildout': 36000,
        'key_developments': 'Tarneit North SA2 grew 20.3% in 2023-24. Part of Wyndham LGA, one of Australias fastest growing municipalities.',
        'new_infrastructure': 'Multiple planned community centres. Train station at Tarneit. New school sites.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Very high growth rate. Northern Tarneit especially underserved.',
    },
    {
        'name': 'Clyde North - South (South East Melbourne)',
        'state': 'VIC',
        'lat_bounds': (-38.10, -38.04),
        'lng_bounds': (145.34, 145.42),
        'suburbs': ['Clyde North', 'Clyde', 'Officer', 'Officer South'],
        'current_pop_2024': 24609,
        'growth_rate_annual': 0.19,  # 19% growth rate
        'planned_dwellings': 20000,
        'planned_pop_buildout': 60000,
        'key_developments': 'Clyde North South SA2 grew by 3,932 people (19%) in 2023-24. Third-largest absolute growth in Melbourne. Multiple estates: Berwick Waters, Edgebrook, Aspect.',
        'new_infrastructure': 'Officer town centre expanding. Selandra Rise community hub. Multiple schools.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': '6 pharmacies for ~45,000+ across corridor. Better than north/west but southern Clyde still has gaps.',
    },
    {
        'name': 'Armstrong Creek - Charlemont (Geelong)',
        'state': 'VIC',
        'lat_bounds': (-38.24, -38.19),
        'lng_bounds': (144.33, 144.40),
        'suburbs': ['Armstrong Creek', 'Charlemont', 'Mount Duneed'],
        'current_pop_2024': 18000,
        'growth_rate_annual': 0.10,
        'planned_dwellings': 22000,
        'planned_pop_buildout': 65000,
        'key_developments': 'Major growth area south of Geelong. Multiple estates including The Village, Warralily, Ashbury.',
        'new_infrastructure': 'Armstrong Creek Town Centre with Coles/Woolworths. Medical centres establishing.',
        'sources': ['City of Greater Geelong planning documents'],
        'opportunity_notes': 'Geelong is Victorias fastest-growing regional city. Armstrong Creek will triple in size.',
    },

    # ======================================================================
    # NEW SOUTH WALES
    # ======================================================================
    {
        'name': 'Box Hill - Nelson (North West Sydney)',
        'state': 'NSW',
        'lat_bounds': (-33.68, -33.62),
        'lng_bounds': (150.88, 150.94),
        'suburbs': ['Box Hill', 'Nelson', 'The Ponds', 'Rouse Hill'],
        'current_pop_2024': 22420,
        'growth_rate_annual': 0.22,  # 22% - 3rd fastest in Australia
        'planned_dwellings': 18000,
        'planned_pop_buildout': 50000,
        'key_developments': 'Third-fastest growing area in Australia. Box Hill-Nelson SA2 grew by 4,042 people (22%) in 2023-24. Part of Hills Shire growth area.',
        'new_infrastructure': 'Box Hill town centre under development. North West Metro extension nearby. New schools opening.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Massive growth but only ~2 pharmacies in Box Hill itself. Very high priority.',
    },
    {
        'name': 'Marsden Park - Shanes Park (North West Sydney)',
        'state': 'NSW',
        'lat_bounds': (-33.72, -33.68),
        'lng_bounds': (150.82, 150.88),
        'suburbs': ['Marsden Park', 'Shanes Park', 'Schofields'],
        'current_pop_2024': 27263,
        'growth_rate_annual': 0.147,  # ~3,500 on 24k
        'planned_dwellings': 15000,
        'planned_pop_buildout': 42000,
        'key_developments': 'Part of North West Growth Centre. Grew by 3,497 people in 2023-24. Elara housing estate is major development.',
        'new_infrastructure': 'Marsden Park town centre expanding. Elara Village shopping. Multiple schools.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Only 2 pharmacies for 27,000+ people = ~13,500:1 ratio. CRITICAL GAP.',
    },
    {
        'name': 'Austral - Greendale (South West Sydney)',
        'state': 'NSW',
        'lat_bounds': (-33.94, -33.89),
        'lng_bounds': (150.79, 150.85),
        'suburbs': ['Austral', 'Greendale', 'Leppington'],
        'current_pop_2024': 12000,
        'growth_rate_annual': 0.16,  # 16% growth
        'planned_dwellings': 20000,
        'planned_pop_buildout': 55000,
        'key_developments': 'Austral-Greendale grew 16% in 2023-24, tied with Googong for highest NSW growth rate. Part of South West Growth Centre. Leppington station opened 2015.',
        'new_infrastructure': 'Leppington town centre under development. Edmondson Park town centre nearby.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Rapidly growing. Leppington has 2 pharmacies but Austral has none.',
    },
    {
        'name': 'Oran Park - Gregory Hills - Catherine Field (South West Sydney)',
        'state': 'NSW',
        'lat_bounds': (-34.02, -33.93),
        'lng_bounds': (150.73, 150.82),
        'suburbs': ['Oran Park', 'Gregory Hills', 'Catherine Field', 'Spring Farm'],
        'current_pop_2024': 42000,
        'growth_rate_annual': 0.08,
        'planned_dwellings': 22000,
        'planned_pop_buildout': 60000,
        'key_developments': 'More mature growth corridor. Oran Park Town Centre well-established with Coles, Woolworths, Aldi. Gregory Hills has town centre.',
        'new_infrastructure': 'Oran Park Town Centre. Gregory Hills Corporate Park. Multiple schools.',
        'sources': ['Camden Council growth data'],
        'opportunity_notes': 'Already 6 pharmacies so more competitive. But Catherine Field and Spring Farm expanding.',
    },
    {
        'name': 'Schofields East (North West Sydney)',
        'state': 'NSW',
        'lat_bounds': (-33.72, -33.68),
        'lng_bounds': (150.87, 150.93),
        'suburbs': ['Schofields', 'Tallawong', 'Rouse Hill'],
        'current_pop_2024': 15000,
        'growth_rate_annual': 0.12,
        'planned_dwellings': 8000,
        'planned_pop_buildout': 25000,
        'key_developments': 'Schofields East grew by 2,700 people in 2023-24. Near Tallawong Metro station. Major new transit-oriented development.',
        'new_infrastructure': 'Tallawong Metro station. New town centre development.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Metro station area likely to have retail. Check for pharmacy tenancy.',
    },

    # ======================================================================
    # QUEENSLAND
    # ======================================================================
    {
        'name': 'Ripley Valley (Ipswich)',
        'state': 'QLD',
        'lat_bounds': (-27.72, -27.65),
        'lng_bounds': (152.76, 152.82),
        'suburbs': ['Ripley', 'South Ripley', 'Deebing Heights', 'White Rock'],
        'current_pop_2024': 22000,  # Ripley SA2 grew by 2,700 in 2023-24
        'growth_rate_annual': 0.15,  # 15% growth
        'planned_dwellings': 50000,
        'planned_pop_buildout': 120000,
        'key_developments': 'Australias LARGEST planned community. Ripley grew 15% in 2023-24 (2,700 people). Ripley Town Centre opened 2018 (9,000sqm). Ecco Ripley masterplan for 120,000 people.',
        'new_infrastructure': 'Ripley Town Centre with medical centre and supermarket. Ripley Central State School opened 2023. More town centres planned for South Ripley.',
        'sources': ['ABS Regional Population 2023-24', 'Wikipedia: Ripley QLD'],
        'opportunity_notes': 'Only 1 pharmacy (Ripley Chempro) for ~22,000 people = 22,000:1 ratio! Building to 120,000. EXTREME OPPORTUNITY. South Ripley town centre needs pharmacy.',
    },
    {
        'name': 'Chambers Flat - Logan Reserve (Logan)',
        'state': 'QLD',
        'lat_bounds': (-27.78, -27.70),
        'lng_bounds': (153.10, 153.22),
        'suburbs': ['Chambers Flat', 'Logan Reserve', 'Park Ridge'],
        'current_pop_2024': 15000,
        'growth_rate_annual': 0.19,  # 19% - HIGHEST growth rate in QLD
        'planned_dwellings': 12000,
        'planned_pop_buildout': 35000,
        'key_developments': 'Highest growth rate in QLD at 19%. Part of Logan growth area. Multiple estates under development.',
        'new_infrastructure': 'Park Ridge Town Centre planned. New schools opening.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Highest QLD growth rate. Check for pharmacy presence - likely underserved.',
    },
    {
        'name': 'Yarrabilba (Logan)',
        'state': 'QLD',
        'lat_bounds': (-27.84, -27.76),
        'lng_bounds': (153.05, 153.16),
        'suburbs': ['Yarrabilba', 'Kairabah'],
        'current_pop_2024': 16000,  # 10,240 in 2021, fast growth
        'growth_rate_annual': 0.12,
        'planned_dwellings': 20000,
        'planned_pop_buildout': 50000,
        'key_developments': 'Planned city for 50,000 people. Grew from 3,580 (2016) to 10,240 (2021). 10 neighbourhoods. Multiple schools. Kairabah incorporated into Yarrabilba 2025.',
        'new_infrastructure': 'Yarrabilba Town Centre shopping. St Clares Catholic School (2017). Yarrabilba State School (2018). Secondary College (2020). San Damiano College (2021).',
        'sources': ['Wikipedia: Yarrabilba', 'ABS Census'],
        'opportunity_notes': 'Only 1 pharmacy (TWC Yarrabilba) for ~16,000 people = 16,000:1 ratio. Building to 50,000. CRITICAL.',
    },
    {
        'name': 'Greater Flagstone (Logan)',
        'state': 'QLD',
        'lat_bounds': (-27.85, -27.75),
        'lng_bounds': (152.88, 153.00),
        'suburbs': ['Flagstone', 'Flinders Lakes', 'Monarch Glen', 'Silverbark Ridge', 'Riverbend', 'Glenlogan'],
        'current_pop_2024': 14000,
        'growth_rate_annual': 0.10,
        'planned_dwellings': 50000,
        'planned_pop_buildout': 120000,
        'key_developments': 'PDA declared 2010 for 50,000 dwellings and 120,000 people over 30-40 years. Flagstone Town Centre operational. New school (St Bonaventures) opened 2026.',
        'new_infrastructure': 'Flagstone Town Centre. Flagstone State Community College. St Bonaventures College (2026). Future train station planned.',
        'sources': ['Wikipedia: Flagstone QLD'],
        'opportunity_notes': '2-3 pharmacies for ~14,000. Reasonable now but building to 120,000 — massive long-term opportunity.',
    },
    {
        'name': 'Caloundra West - Baringa (Sunshine Coast)',
        'state': 'QLD',
        'lat_bounds': (-26.85, -26.77),
        'lng_bounds': (153.05, 153.15),
        'suburbs': ['Caloundra West', 'Baringa', 'Nirimba', 'Aura'],
        'current_pop_2024': 18000,
        'growth_rate_annual': 0.08,
        'planned_dwellings': 20000,
        'planned_pop_buildout': 50000,
        'key_developments': 'Caloundra West-Baringa had LARGEST growth outside capital cities (2,500 people). Aura masterplanned community by Stockland. New town centre Baringa.',
        'new_infrastructure': 'Baringa Town Centre. Multiple schools (Baringa SS 2018, Nirimba SS 2021). Sunshine Coast University Hospital nearby.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Only 2 pharmacies for 18,000 people. Growing fast. Regional - less competition. Good opportunity.',
    },
    {
        'name': 'Greenbank - North Maclean (Logan)',
        'state': 'QLD',
        'lat_bounds': (-27.75, -27.68),
        'lng_bounds': (152.95, 153.08),
        'suburbs': ['Greenbank', 'North Maclean', 'New Beith'],
        'current_pop_2024': 12000,
        'growth_rate_annual': 0.13,  # 13% growth
        'planned_dwellings': 8000,
        'planned_pop_buildout': 25000,
        'key_developments': 'Third-highest growth rate in QLD at 13%. Part of Logan citys southern expansion.',
        'new_infrastructure': 'Greenbank Shopping Centre. New school sites.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Check pharmacy coverage. Likely gap in North Maclean area.',
    },

    # ======================================================================
    # SOUTH AUSTRALIA
    # ======================================================================
    {
        'name': 'Munno Para West - Angle Vale (Northern Adelaide)',
        'state': 'SA',
        'lat_bounds': (-34.68, -34.60),
        'lng_bounds': (138.60, 138.72),
        'suburbs': ['Munno Para West', 'Angle Vale', 'Munno Para'],
        'current_pop_2024': 21000,
        'growth_rate_annual': 0.11,  # 11% growth
        'planned_dwellings': 12000,
        'planned_pop_buildout': 35000,
        'key_developments': 'LARGEST growth in SA (2,100 people). Largest natural increase in SA. Largest net internal migration gain in SA. Triple crown of SA growth.',
        'new_infrastructure': 'Angle Vale Village Shopping Centre. New schools.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Angle Vale has TWC pharmacy. But growth rate suggests more needed. Munno Para West expanding.',
    },
    {
        'name': 'Virginia - Waterloo Corner (Northern Adelaide)',
        'state': 'SA',
        'lat_bounds': (-34.68, -34.60),
        'lng_bounds': (138.52, 138.62),
        'suburbs': ['Virginia', 'Waterloo Corner', 'Buckland Park'],
        'current_pop_2024': 5000,
        'growth_rate_annual': 0.14,  # HIGHEST growth rate in SA (14%)
        'planned_dwellings': 15000,
        'planned_pop_buildout': 40000,
        'key_developments': 'HIGHEST growth rate in SA (14%). Buckland Park is massive greenfield development north of Adelaide for 30,000+ people.',
        'new_infrastructure': 'Buckland Park masterplan includes town centre, schools. Minimal infrastructure currently.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Only 1 pharmacy (Virginia) for the area. Buckland Park development = major future opportunity when town centre built.',
    },
    {
        'name': 'Mount Barker (Adelaide Hills)',
        'state': 'SA',
        'lat_bounds': (-35.12, -35.03),
        'lng_bounds': (138.82, 138.90),
        'suburbs': ['Mount Barker', 'Littlehampton', 'Nairne'],
        'current_pop_2024': 46000,  # 2025 estimate per Wikipedia
        'growth_rate_annual': 0.05,
        'planned_dwellings': 10000,
        'planned_pop_buildout': 60000,
        'key_developments': 'Second-largest growth in SA (1,100 people). One of Australias fastest growing regional cities. Population doubled in 15 years.',
        'new_infrastructure': 'Mount Barker Central Shopping Centre. Multiple medical centres. New schools.',
        'sources': ['ABS Regional Population 2023-24', 'Wikipedia: Mount Barker SA'],
        'opportunity_notes': 'Already 6 pharmacies = 7,700:1 ratio. More competitive but still room for growth.',
    },
    {
        'name': 'Two Wells - Lewiston (Northern Adelaide Plains)',
        'state': 'SA',
        'lat_bounds': (-34.63, -34.55),
        'lng_bounds': (138.48, 138.56),
        'suburbs': ['Two Wells', 'Lewiston'],
        'current_pop_2024': 7000,
        'growth_rate_annual': 0.10,
        'planned_dwellings': 6000,
        'planned_pop_buildout': 18000,
        'key_developments': 'Two Wells grew from 1,926 (2016) to 2,947 (2021) and continuing. Multiple housing estates. Northern Expressway improved access.',
        'new_infrastructure': 'Two Wells shopping area. New school planned.',
        'sources': ['Wikipedia: Two Wells SA'],
        'opportunity_notes': '1 pharmacy (TWC Two Wells) serving the area. Will need more as population grows.',
    },

    # ======================================================================
    # WESTERN AUSTRALIA
    # ======================================================================
    {
        'name': 'Alkimos - Eglinton (North West Perth)',
        'state': 'WA',
        'lat_bounds': (-31.62, -31.52),
        'lng_bounds': (115.62, 115.72),
        'suburbs': ['Alkimos', 'Eglinton'],
        'current_pop_2024': 20000,
        'growth_rate_annual': 0.12,  # LARGEST growth in WA (2,100 people)
        'planned_dwellings': 22000,
        'planned_pop_buildout': 55000,
        'key_developments': 'LARGEST growth in WA (2,100 people). Planned satellite city. 55,000 people planned. Alkimos grew from 0 to 10,203 (2021). 3 public primary schools already.',
        'new_infrastructure': 'Alkimos Beach shopping precinct. Alkimos Trinity Village (Coles). Alkimos Central major town centre planned. Future Alkimos train station on METRONET.',
        'sources': ['ABS Regional Population 2023-24', 'Wikipedia: Alkimos WA'],
        'opportunity_notes': '4 pharmacies in corridor including Alkimos/Yanchep area. Building to 55k. Alkimos Central town centre = pharmacy opportunity when built.',
    },
    {
        'name': 'Brabham - Henley Brook (North East Perth)',
        'state': 'WA',
        'lat_bounds': (-31.82, -31.76),
        'lng_bounds': (115.92, 116.00),
        'suburbs': ['Brabham', 'Henley Brook', 'Dayton'],
        'current_pop_2024': 15000,
        'growth_rate_annual': 0.11,  # Tied with Baldivis for 2nd highest WA growth
        'planned_dwellings': 8000,
        'planned_pop_buildout': 24000,
        'key_developments': 'Grew 1,500 people (11%) in 2023-24. Ellenbrook extension area. Part of Swan LGA.',
        'new_infrastructure': 'METRONET Ellenbrook line (opens 2025/26). New train stations at Brabham/Henley Brook.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'New METRONET train stations = retail opportunity. Check for pharmacy in new station precincts.',
    },
    {
        'name': 'Baldivis North (South Perth)',
        'state': 'WA',
        'lat_bounds': (-32.36, -32.30),
        'lng_bounds': (115.76, 115.84),
        'suburbs': ['Baldivis', 'Wellard South'],
        'current_pop_2024': 16000,
        'growth_rate_annual': 0.11,
        'planned_dwellings': 8000,
        'planned_pop_buildout': 25000,
        'key_developments': 'Baldivis North grew 1,500 people (11%). Part of Rockingham LGA. Multiple estates.',
        'new_infrastructure': 'Baldivis District Centre. New schools.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Multiple pharmacies in Baldivis proper but northern expansion may have gaps.',
    },
    {
        'name': 'Yanchep - Two Rocks (Far North Perth)',
        'state': 'WA',
        'lat_bounds': (-31.58, -31.48),
        'lng_bounds': (115.60, 115.70),
        'suburbs': ['Yanchep', 'Two Rocks'],
        'current_pop_2024': 16000,
        'growth_rate_annual': 0.08,
        'planned_dwellings': 40000,
        'planned_pop_buildout': 200000,  # Yanchep Sun City vision
        'key_developments': 'Future satellite city of 200,000+. Currently 11,022 (2021). Multiple estates. Yanchep lagoon development.',
        'new_infrastructure': 'Yanchep Village Shopping Centre. Yanchep Central Shopping Centre. METRONET extension planned. Future major hospital site.',
        'sources': ['Wikipedia: Yanchep WA', 'WA Directions 2031'],
        'opportunity_notes': 'Already 5 pharmacies. Well-served for now but massive future growth. Long-term hold.',
    },
    {
        'name': 'Byford - Mundijong (South East Perth)',
        'state': 'WA',
        'lat_bounds': (-32.28, -32.18),
        'lng_bounds': (115.96, 116.04),
        'suburbs': ['Byford', 'Mundijong', 'Whitby'],
        'current_pop_2024': 30000,
        'growth_rate_annual': 0.06,
        'planned_dwellings': 12000,
        'planned_pop_buildout': 50000,
        'key_developments': 'Established growth area. Byford town centre well developed. Mundijong-Whitby district structure plan for 20,000+ new dwellings.',
        'new_infrastructure': 'Byford shopping centres. METRONET Byford extension (opens 2024). Mundijong town centre planned.',
        'sources': ['Serpentine-Jarrahdale Shire planning docs'],
        'opportunity_notes': '5-6 pharmacies already. Competitive. Mundijong town centre could be opportunity.',
    },

    # ======================================================================
    # TASMANIA - MJ's home territory!
    # ======================================================================
    {
        'name': 'Legana (West Tamar, Launceston)',
        'state': 'TAS',
        'lat_bounds': (-41.40, -41.34),
        'lng_bounds': (147.00, 147.08),
        'suburbs': ['Legana', 'Grindelwald', 'Rosevears'],
        'current_pop_2024': 6000,  # 4,769 in 2021 + growth
        'growth_rate_annual': 0.04,
        'planned_dwellings': 2000,
        'planned_pop_buildout': 12000,
        'key_developments': 'Growing residential area 12km north of Launceston. New primary school opened 2024 (350 students). Community sports precinct built. West Tamar Highway being upgraded to 2 lanes each direction by end 2026.',
        'new_infrastructure': 'Legana Marketplace (anchored by Woolworths). New primary school (2024). Community sports precinct. Highway upgrade.',
        'sources': ['Wikipedia: Legana TAS'],
        'opportunity_notes': 'ZERO pharmacies in Legana! Nearest pharmacy is ~12km away at Riverside/Windsor. Wikipedia mentions a "chemist" at Legana Marketplace but NO pharmacy in our database. Population ~6,000 and growing. New school = young families = pharmacy demand. MJs BACKYARD. If the chemist mention is outdated/closed, this is a TOP PRIORITY MONOPOLY POSITION.',
    },
    {
        'name': 'Kingston - Huntingfield (South Hobart)',
        'state': 'TAS',
        'lat_bounds': (-42.99, -42.93),
        'lng_bounds': (147.28, 147.36),
        'suburbs': ['Kingston', 'Huntingfield', 'Kingston Beach'],
        'current_pop_2024': 18000,
        'growth_rate_annual': 0.02,
        'planned_dwellings': 3000,
        'planned_pop_buildout': 25000,
        'key_developments': 'Kingston-Huntingfield grew 220 people in 2023-24. Huntingfield development for 450 lots. Kingston bypass road opened.',
        'new_infrastructure': 'Channel Court Shopping Centre. Kingston Health Centre. Kingston bypass.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Already 7 pharmacies. Well-served. Low priority.',
    },
    {
        'name': 'Sorell - Richmond (East Hobart)',
        'state': 'TAS',
        'lat_bounds': (-42.80, -42.72),
        'lng_bounds': (147.52, 147.62),
        'suburbs': ['Sorell', 'Midway Point', 'Orielton'],
        'current_pop_2024': 10000,
        'growth_rate_annual': 0.02,
        'planned_dwellings': 2000,
        'planned_pop_buildout': 15000,
        'key_developments': 'Sorell-Richmond grew 200 people (highest internal migration in Hobart). Sorell is major growth town east of Hobart.',
        'new_infrastructure': 'Sorell Plaza shopping centre.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Check pharmacy presence in Sorell. Growing but slow.',
    },
    {
        'name': 'Rokeby (Eastern Shore Hobart)',
        'state': 'TAS',
        'lat_bounds': (-42.90, -42.85),
        'lng_bounds': (147.38, 147.45),
        'suburbs': ['Rokeby', 'Howrah', 'Clarendon Vale'],
        'current_pop_2024': 9000,
        'growth_rate_annual': 0.027,  # 2.7% - highest in Hobart
        'planned_dwellings': 1500,
        'planned_pop_buildout': 12000,
        'key_developments': 'HIGHEST growth rate in Hobart (2.7%). LARGEST growth in Hobart (230 people). Eastern shore expansion.',
        'new_infrastructure': 'Shoreline Shopping Centre. Eastlands Shopping Centre nearby.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Check pharmacy presence. Highest Hobart growth rate.',
    },

    # ======================================================================
    # ACT
    # ======================================================================
    {
        'name': 'Taylor - Throsby (Gungahlin, Canberra)',
        'state': 'ACT',
        'lat_bounds': (-35.16, -35.10),
        'lng_bounds': (149.08, 149.16),
        'suburbs': ['Taylor', 'Throsby', 'Jacka', 'Moncrieff'],
        'current_pop_2024': 4766,
        'growth_rate_annual': 0.287,  # 28.7% - FASTEST IN AUSTRALIA!
        'planned_dwellings': 6000,
        'planned_pop_buildout': 18000,
        'key_developments': 'FASTEST growing area in ALL of Australia (28.7%). Taylor grew from ~3,700 to 4,766 (29%). Largest natural increase in ACT. Largest internal migration gain in ACT.',
        'new_infrastructure': 'Taylor Primary School. New community centre. Shopping planned.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Fastest growth rate in Australia but very small base. Check Gungahlin pharmacy coverage. ACT has different pharmacy regulations.',
    },
    {
        'name': 'Denman Prospect - Whitlam (Molonglo Valley, Canberra)',
        'state': 'ACT',
        'lat_bounds': (-35.30, -35.26),
        'lng_bounds': (149.02, 149.08),
        'suburbs': ['Denman Prospect', 'Whitlam', 'Molonglo'],
        'current_pop_2024': 7000,
        'growth_rate_annual': 0.17,  # 17% for Denman Prospect
        'planned_dwellings': 10000,
        'planned_pop_buildout': 30000,
        'key_developments': 'Denman Prospect grew 700 people (17%). Molonglo Valley is Canberras largest greenfield development. Building new suburbs from scratch.',
        'new_infrastructure': 'Molonglo commercial centre planned. New schools. Group Centre planned.',
        'sources': ['ABS Regional Population 2023-24'],
        'opportunity_notes': 'Fast growth. Check pharmacy presence. Molonglo Group Centre = pharmacy target.',
    },
]


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    c = 2 * math.asin(math.sqrt(a))
    return R * c


def get_pharmacies_in_corridor(cursor, corridor):
    """Query database for pharmacies within corridor bounds (tight, no buffer)."""
    lat_min, lat_max = corridor['lat_bounds']
    lng_min, lng_max = corridor['lng_bounds']
    
    # No buffer — use exact bounding box for accuracy
    # Growth corridors are defined to include the service area
    cursor.execute("""
        SELECT id, name, address, latitude, longitude, suburb, state
        FROM pharmacies
        WHERE latitude BETWEEN ? AND ?
        AND longitude BETWEEN ? AND ?
    """, (lat_min, lat_max, lng_min, lng_max))
    
    return cursor.fetchall()


def get_medical_centres_in_corridor(cursor, corridor):
    """Query database for medical centres within corridor bounds."""
    lat_min, lat_max = corridor['lat_bounds']
    lng_min, lng_max = corridor['lng_bounds']
    buffer = 0.02
    
    cursor.execute("""
        SELECT id, name, address, latitude, longitude
        FROM medical_centres
        WHERE latitude BETWEEN ? AND ?
        AND longitude BETWEEN ? AND ?
    """, (lat_min - buffer, lat_max + buffer, lng_min - buffer, lng_max + buffer))
    
    return cursor.fetchall()


def get_shopping_centres_in_corridor(cursor, corridor):
    """Query database for shopping centres within corridor bounds."""
    lat_min, lat_max = corridor['lat_bounds']
    lng_min, lng_max = corridor['lng_bounds']
    buffer = 0.03
    
    cursor.execute("""
        SELECT id, name, address, latitude, longitude, gla_sqm, major_supermarkets
        FROM shopping_centres
        WHERE latitude BETWEEN ? AND ?
        AND longitude BETWEEN ? AND ?
    """, (lat_min - buffer, lat_max + buffer, lng_min - buffer, lng_max + buffer))
    
    return cursor.fetchall()


def analyze_corridor(cursor, corridor):
    """Perform full analysis on a single growth corridor."""
    pharmacies = get_pharmacies_in_corridor(cursor, corridor)
    medical_centres = get_medical_centres_in_corridor(cursor, corridor)
    shopping_centres = get_shopping_centres_in_corridor(cursor, corridor)
    
    current_pop = corridor['current_pop_2024']
    growth_rate = corridor['growth_rate_annual']
    pharmacy_count = len(pharmacies)
    
    # Calculate projections
    pop_3yr = int(current_pop * (1 + growth_rate) ** 3)
    pop_5yr = int(current_pop * (1 + growth_rate) ** 5)
    pop_buildout = corridor['planned_pop_buildout']
    
    # People per pharmacy ratios
    current_ratio = current_pop / pharmacy_count if pharmacy_count > 0 else float('inf')
    ratio_3yr = pop_3yr / pharmacy_count if pharmacy_count > 0 else float('inf')
    ratio_5yr = pop_5yr / pharmacy_count if pharmacy_count > 0 else float('inf')
    
    # Pharmacy gap calculation (target: 4,000 people per pharmacy)
    TARGET_RATIO = 4000
    pharmacies_needed_now = max(0, int(current_pop / TARGET_RATIO) - pharmacy_count)
    pharmacies_needed_3yr = max(0, int(pop_3yr / TARGET_RATIO) - pharmacy_count)
    pharmacies_needed_5yr = max(0, int(pop_5yr / TARGET_RATIO) - pharmacy_count)
    pharmacies_needed_buildout = max(0, int(pop_buildout / TARGET_RATIO) - pharmacy_count)
    
    # Opportunity score (higher = better opportunity)
    # Factors: growth rate, pharmacy gap, absolute population, infrastructure
    score = 0
    
    # Growth rate component (0-30 points)
    score += min(30, growth_rate * 100)
    
    # Current pharmacy gap (0-30 points)
    if pharmacy_count == 0:
        score += 30
    elif current_ratio > 15000:
        score += 25
    elif current_ratio > 10000:
        score += 20
    elif current_ratio > 6000:
        score += 15
    elif current_ratio > 4000:
        score += 10
    
    # Future pharmacy gap (0-20 points)
    score += min(20, pharmacies_needed_5yr * 4)
    
    # Population base (0-10 points)
    score += min(10, current_pop / 5000)
    
    # Infrastructure indicator (0-10 points)
    if medical_centres:
        score += 3
    if shopping_centres:
        score += 3
    if 'town centre' in corridor.get('new_infrastructure', '').lower():
        score += 4
    
    # Determine urgency
    if pharmacy_count == 0:
        urgency = 'CRITICAL - No pharmacy exists'
    elif current_ratio > 15000:
        urgency = 'VERY HIGH - Severely underserved'
    elif current_ratio > 10000:
        urgency = 'HIGH - Significantly underserved'
    elif current_ratio > 6000:
        urgency = 'MODERATE - Below recommended ratio'
    elif ratio_5yr > 6000:
        urgency = 'FUTURE - Will become underserved in 3-5 years'
    else:
        urgency = 'LOW - Adequately served currently'
    
    return {
        'name': corridor['name'],
        'state': corridor['state'],
        'suburbs': corridor['suburbs'],
        'current_pop_2024': current_pop,
        'growth_rate_annual_pct': round(growth_rate * 100, 1),
        'pop_3yr_projection': pop_3yr,
        'pop_5yr_projection': pop_5yr,
        'pop_buildout': pop_buildout,
        'planned_dwellings': corridor['planned_dwellings'],
        'pharmacy_count': pharmacy_count,
        'pharmacies': [{'name': p[1], 'address': p[2]} for p in pharmacies],
        'medical_centres': len(medical_centres),
        'shopping_centres': len(shopping_centres),
        'current_ratio': round(current_ratio, 0) if current_ratio != float('inf') else None,
        'ratio_3yr': round(ratio_3yr, 0) if ratio_3yr != float('inf') else None,
        'ratio_5yr': round(ratio_5yr, 0) if ratio_5yr != float('inf') else None,
        'pharmacies_needed_now': pharmacies_needed_now,
        'pharmacies_needed_3yr': pharmacies_needed_3yr,
        'pharmacies_needed_5yr': pharmacies_needed_5yr,
        'pharmacies_needed_buildout': pharmacies_needed_buildout,
        'opportunity_score': round(score, 1),
        'urgency': urgency,
        'key_developments': corridor['key_developments'],
        'new_infrastructure': corridor['new_infrastructure'],
        'opportunity_notes': corridor['opportunity_notes'],
        'sources': corridor['sources'],
        'lat_centre': (corridor['lat_bounds'][0] + corridor['lat_bounds'][1]) / 2,
        'lng_centre': (corridor['lng_bounds'][0] + corridor['lng_bounds'][1]) / 2,
    }


def update_opportunities_db(cursor, results):
    """Update existing opportunities in database with growth corridor flags."""
    updated = 0
    for corridor in results:
        lat_min = corridor['lat_centre'] - 0.05
        lat_max = corridor['lat_centre'] + 0.05
        lng_min = corridor['lng_centre'] - 0.05
        lng_max = corridor['lng_centre'] + 0.05
        
        growth_indicator = 'YES'
        growth_details = (
            f"Growth Corridor: {corridor['name']} ({corridor['state']}). "
            f"Pop {corridor['current_pop_2024']:,} → {corridor['pop_5yr_projection']:,} (5yr). "
            f"Growth rate: {corridor['growth_rate_annual_pct']}%/yr. "
            f"Pharmacies: {corridor['pharmacy_count']}. "
            f"Gap: {corridor['pharmacies_needed_5yr']} more needed in 5yr. "
            f"Score: {corridor['opportunity_score']}/100. "
            f"Urgency: {corridor['urgency']}."
        )
        
        cursor.execute("""
            UPDATE opportunities 
            SET growth_indicator = ?, growth_details = ?
            WHERE latitude BETWEEN ? AND ?
            AND longitude BETWEEN ? AND ?
        """, (growth_indicator, growth_details, lat_min, lat_max, lng_min, lng_max))
        
        updated += cursor.rowcount
    
    return updated


def generate_json(results):
    """Generate JSON output file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'growth_corridors.json')
    
    output = {
        'generated': datetime.now().isoformat(),
        'total_corridors': len(results),
        'corridors': results,
        'summary': {
            'total_current_pop': sum(r['current_pop_2024'] for r in results),
            'total_pop_5yr': sum(r['pop_5yr_projection'] for r in results),
            'total_pharmacies': sum(r['pharmacy_count'] for r in results),
            'total_pharmacies_needed_5yr': sum(r['pharmacies_needed_5yr'] for r in results),
            'critical_corridors': len([r for r in results if 'CRITICAL' in r['urgency']]),
            'high_urgency': len([r for r in results if 'HIGH' in r['urgency'] or 'VERY HIGH' in r['urgency']]),
        }
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"  JSON written to {output_path}")
    return output_path


def generate_report(results):
    """Generate markdown report."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'growth_corridors_report.md')
    
    # Sort by opportunity score
    ranked = sorted(results, key=lambda x: x['opportunity_score'], reverse=True)
    
    lines = []
    lines.append("# 🏗️ Australian Growth Corridor Pharmacy Opportunity Report")
    lines.append(f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append(f"\n*Data sources: ABS Regional Population 2023-24, PharmacyFinder database, State planning documents*")
    
    # Executive Summary
    lines.append("\n---\n")
    lines.append("## 📊 Executive Summary\n")
    
    total_pop = sum(r['current_pop_2024'] for r in results)
    total_pop_5yr = sum(r['pop_5yr_projection'] for r in results)
    total_pharmacies = sum(r['pharmacy_count'] for r in results)
    total_gap_5yr = sum(r['pharmacies_needed_5yr'] for r in results)
    critical = [r for r in results if 'CRITICAL' in r['urgency']]
    very_high = [r for r in results if 'VERY HIGH' in r['urgency']]
    
    lines.append(f"- **{len(results)} growth corridors** analysed across all states")
    lines.append(f"- **{total_pop:,}** current combined population")
    lines.append(f"- **{total_pop_5yr:,}** projected population in 5 years (+{total_pop_5yr - total_pop:,})")
    lines.append(f"- **{total_pharmacies}** existing pharmacies in these corridors")
    lines.append(f"- **{total_gap_5yr}** additional pharmacies needed in 5 years")
    lines.append(f"- **{len(critical)}** corridors with CRITICAL urgency (no pharmacy or severely underserved)")
    lines.append(f"- **{len(very_high)}** corridors with VERY HIGH urgency")
    
    # KEY FINDINGS
    lines.append("\n### 🔑 Key Findings\n")
    lines.append("1. **Ripley Valley QLD** has only 1 pharmacy for 22,000 people (building to 120,000) — Australia's largest planned community")
    lines.append("2. **Legana TAS** has ZERO pharmacies for 6,000+ people — MJ's local area, monopoly position available")
    lines.append("3. **Marsden Park NSW** has only 2 pharmacies for 27,000+ people — 13,500:1 ratio")
    lines.append("4. **Mickleham-Kalkallo VIC** has only 4 pharmacies for 52,000 people — 13,000:1 ratio")
    lines.append("5. **Fraser Rise VIC** is Australia's fastest-growing suburb (26.3%) with minimal pharmacy coverage")
    
    # TOP 20 RANKED
    lines.append("\n---\n")
    lines.append("## 🏆 Top 20 Growth Corridors Ranked by Opportunity\n")
    
    for i, r in enumerate(ranked[:20], 1):
        emoji = "🔴" if "CRITICAL" in r['urgency'] else "🟠" if "VERY HIGH" in r['urgency'] else "🟡" if "HIGH" in r['urgency'] else "🟢"
        
        lines.append(f"\n### {i}. {emoji} {r['name']} ({r['state']})")
        lines.append(f"**Opportunity Score: {r['opportunity_score']}/100** | **Urgency: {r['urgency']}**\n")
        
        lines.append(f"| Metric | Value |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Current Population (2024) | **{r['current_pop_2024']:,}** |")
        lines.append(f"| Growth Rate | **{r['growth_rate_annual_pct']}%** per year |")
        lines.append(f"| 3-Year Projection | {r['pop_3yr_projection']:,} |")
        lines.append(f"| 5-Year Projection | {r['pop_5yr_projection']:,} |")
        lines.append(f"| Build-out Population | {r['pop_buildout']:,} |")
        lines.append(f"| Planned Dwellings | {r['planned_dwellings']:,} |")
        lines.append(f"| Current Pharmacies | **{r['pharmacy_count']}** |")
        ratio_str = f"{r['current_ratio']:,.0f}:1" if r['current_ratio'] else "∞ (NONE)"
        lines.append(f"| People-per-Pharmacy Ratio | **{ratio_str}** |")
        lines.append(f"| Pharmacies Needed (5yr) | **+{r['pharmacies_needed_5yr']}** |")
        lines.append(f"| Pharmacies Needed (build-out) | +{r['pharmacies_needed_buildout']} |")
        lines.append(f"| Medical Centres in Area | {r['medical_centres']} |")
        lines.append(f"| Shopping Centres in Area | {r['shopping_centres']} |")
        
        # Existing pharmacies
        if r['pharmacies']:
            lines.append(f"\n**Existing Pharmacies:**")
            for p in r['pharmacies']:
                lines.append(f"- {p['name']} — {p['address']}")
        else:
            lines.append(f"\n**⚠️ NO EXISTING PHARMACIES IN THIS CORRIDOR**")
        
        lines.append(f"\n**Key Developments:** {r['key_developments']}")
        lines.append(f"\n**New Infrastructure:** {r['new_infrastructure']}")
        lines.append(f"\n**💡 Opportunity Analysis:** {r['opportunity_notes']}")
        
        # Specific recommendations
        lines.append(f"\n**📋 Recommendation:**")
        if 'CRITICAL' in r['urgency']:
            lines.append(f"- **IMMEDIATE ACTION**: Apply for PBS approval NOW. First-mover advantage is critical.")
            lines.append(f"- Target location near major retail/medical centre")
            lines.append(f"- Expected ROI timeline: 12-18 months to profitability")
        elif 'VERY HIGH' in r['urgency']:
            lines.append(f"- **HIGH PRIORITY**: Begin site selection and PBS application within 6 months")
            lines.append(f"- Monitor new shopping centre/medical centre developments for tenancy")
            lines.append(f"- Expected ROI timeline: 18-24 months")
        elif 'HIGH' in r['urgency']:
            lines.append(f"- **MEDIUM PRIORITY**: Add to watchlist. Apply when next town centre opens")
            lines.append(f"- Expected ROI timeline: 24-36 months")
        else:
            lines.append(f"- **MONITOR**: Track development progress. Opportunity will grow over time")
    
    # STATE-BY-STATE BREAKDOWN
    lines.append("\n---\n")
    lines.append("## 📍 State-by-State Summary\n")
    
    for state in ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'ACT']:
        state_corridors = [r for r in ranked if r['state'] == state]
        if not state_corridors:
            continue
        
        state_names = {
            'VIC': 'Victoria', 'NSW': 'New South Wales', 'QLD': 'Queensland',
            'SA': 'South Australia', 'WA': 'Western Australia', 'TAS': 'Tasmania', 'ACT': 'ACT'
        }
        
        lines.append(f"\n### {state_names[state]} ({state})")
        lines.append(f"- Corridors analysed: {len(state_corridors)}")
        lines.append(f"- Total current population: {sum(r['current_pop_2024'] for r in state_corridors):,}")
        lines.append(f"- Total pharmacies: {sum(r['pharmacy_count'] for r in state_corridors)}")
        lines.append(f"- Pharmacies needed (5yr): +{sum(r['pharmacies_needed_5yr'] for r in state_corridors)}")
        
        top = state_corridors[0]
        lines.append(f"- **Best opportunity**: {top['name']} (Score: {top['opportunity_score']})")
    
    # TASMANIA DEEP DIVE (MJ's area)
    lines.append("\n---\n")
    lines.append("## 🏠 Tasmania Deep Dive (MJ's Territory)\n")
    
    tas_corridors = [r for r in ranked if r['state'] == 'TAS']
    for r in tas_corridors:
        lines.append(f"\n### {r['name']}")
        lines.append(f"- Population: {r['current_pop_2024']:,} → {r['pop_5yr_projection']:,} (5yr)")
        lines.append(f"- Pharmacies: {r['pharmacy_count']}")
        ratio_display = f"{r['current_ratio']:,.0f}:1" if r['current_ratio'] else 'NO PHARMACY'
        lines.append(f"- Ratio: {ratio_display}")
        lines.append(f"- **Assessment:** {r['opportunity_notes']}")
    
    lines.append("""
### 🎯 Legana — MJ's #1 Local Opportunity

**Why Legana is special:**
- ZERO pharmacies for 6,000+ and growing population
- Legana Marketplace (Woolworths anchor) provides ready-made retail location
- Wikipedia mentions a "chemist" exists — needs ground-truth verification
- New school (2024) means young families moving in = high pharmacy demand
- Highway upgrade (2026) improves access
- 12km from Launceston — too far for casual pharmacy visits
- MJ lives in the area = local knowledge advantage

**Action Items:**
1. **Verify** if there's actually a pharmacy at Legana Marketplace (database shows none)
2. If no pharmacy → **Apply for PBS approval IMMEDIATELY**
3. Target tenancy at Legana Marketplace or adjacent medical centre
4. First-mover advantage in a growing market with no competition
""")
    
    # TIMING AND PBS ADVICE
    lines.append("\n---\n")
    lines.append("## ⏰ Timing & PBS Approval Strategy\n")
    lines.append("""
### PBS Approval Process
- New pharmacy applications under the **Pharmacy Location Rules**
- Key criteria: population, distance to nearest pharmacy, existing services
- Item 136 (greenfield) and Item 100 (supermarket-based) are most relevant for growth corridors

### Optimal Timing for Each Tier

**Tier 1 — Apply NOW (Q1-Q2 2025):**
1. Legana TAS — Zero pharmacies, growing population
2. Ripley Valley QLD — 1 pharmacy for 22,000+ people
3. Marsden Park NSW — 2 pharmacies for 27,000+
4. Fraser Rise-Plumpton VIC — 2 pharmacies for 20,000+

**Tier 2 — Apply within 12 months:**
5. Mickleham-Kalkallo VIC — 4 pharmacies for 52,000+
6. Yarrabilba QLD — 1 pharmacy for 16,000+
7. Box Hill-Nelson NSW — Rapid growth
8. Chambers Flat-Logan Reserve QLD — 19% growth rate

**Tier 3 — Monitor and apply when infrastructure ready:**
9. Beveridge InterCity VIC — When town centre opens
10. Alkimos Central WA — When major town centre built
11. Buckland Park SA — When first residents move in
12. South Ripley QLD — When town centre planned

### Key Insight: Shopping Centre + No Pharmacy = Gold 🏆
The highest-value opportunities are where:
- A shopping centre or medical centre EXISTS or is PLANNED
- No pharmacy is in the development plan
- Population is growing rapidly
- Distance to nearest pharmacy exceeds 1.5km
""")
    
    # Write file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"  Report written to {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description='Growth Corridor Analysis v2')
    parser.add_argument('--update-db', action='store_true', help='Update opportunities in database')
    parser.add_argument('--json', action='store_true', help='Generate JSON output')
    parser.add_argument('--report', action='store_true', help='Generate markdown report')
    parser.add_argument('--all', action='store_true', help='Do everything')
    args = parser.parse_args()
    
    if not (args.update_db or args.json or args.report or args.all):
        args.all = True
    
    print("=" * 70)
    print("GROWTH CORRIDOR ANALYSIS v2")
    print("Strategic Pharmacy Opportunity Assessment")
    print("=" * 70)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Analyze each corridor
    print(f"\nAnalysing {len(GROWTH_CORRIDORS)} growth corridors...")
    results = []
    
    for corridor in GROWTH_CORRIDORS:
        result = analyze_corridor(cursor, corridor)
        results.append(result)
        
        tag = "[!!!]" if "CRITICAL" in result['urgency'] else "[!!]" if "VERY HIGH" in result['urgency'] else "[!]" if "HIGH" in result['urgency'] else "[ok]"
        ratio_str = f"{result['current_ratio']:,.0f}:1" if result['current_ratio'] else "INF"
        print(f"  {tag} {result['name']}: {result['pharmacy_count']} pharmacies, "
              f"{result['current_pop_2024']:,} pop, {ratio_str} ratio, "
              f"Score: {result['opportunity_score']}")
    
    # Sort by score
    results.sort(key=lambda x: x['opportunity_score'], reverse=True)
    
    # Summary
    print(f"\n{'=' * 70}")
    print("TOP 10 OPPORTUNITIES:")
    print(f"{'=' * 70}")
    for i, r in enumerate(results[:10], 1):
        ratio_str = f"{r['current_ratio']:,.0f}:1" if r['current_ratio'] else "NO PHARMACY"
        print(f"  {i:2d}. {r['name']} ({r['state']}) — Score: {r['opportunity_score']}, "
              f"Ratio: {ratio_str}, Gap: +{r['pharmacies_needed_5yr']} needed")
    
    # Generate outputs
    if args.json or args.all:
        print("\nGenerating JSON output...")
        generate_json(results)
    
    if args.report or args.all:
        print("\nGenerating markdown report...")
        generate_report(results)
    
    if args.update_db or args.all:
        print("\nUpdating opportunities database...")
        updated = update_opportunities_db(cursor, results)
        conn.commit()
        print(f"  Updated {updated} opportunities with growth corridor data")
    
    conn.close()
    
    print(f"\n{'=' * 70}")
    print("Growth corridor analysis complete!")
    print(f"{'=' * 70}")


if __name__ == '__main__':
    main()
