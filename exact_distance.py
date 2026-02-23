from math import radians, cos, sin, asin, sqrt

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

# Google Maps exact pin coordinates (from URL)
# 22 Norwood Avenue (Norwood Post Office)
post_lat = -41.4555988
post_lon = 147.1727903

# Slade Pharmacy Kings Meadows (86 Hobart Rd)
slade_lat = -41.465197
slade_lon = 147.1589568

d = haversine(post_lat, post_lon, slade_lat, slade_lon)
print(f"22 Norwood Avenue: ({post_lat}, {post_lon})")
print(f"Slade Pharmacy:    ({slade_lat}, {slade_lon})")
print(f"")
print(f"Straight-line distance: {d:.1f}m = {d/1000:.4f}km")
print(f"")
print(f"Item 130 threshold: 1500m")
print(f"Margin: {d - 1500:.1f}m")
print(f"Result: {'PASS' if d >= 1500 else 'FAIL'}")
