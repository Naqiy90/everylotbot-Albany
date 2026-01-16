#!/usr/bin/env python3
import os
import sys
import requests
import sqlite3
import argparse
from dotenv import load_dotenv

# Albany "TaxParcelsWithRPS" FeatureServer
ALBANY_FEATURE_SERVER = "https://services6.arcgis.com/JJzptGyn7EDStgyp/ArcGIS/rest/services/TaxParcelsWithRPS/FeatureServer/0"

def fetch_albany_parcels(city='City of Albany', batch_size=1000):
    """
    Fetch parcel data from Albany ArcGIS FeatureServer.
    Uses pagination.
    """
    url = f"{ALBANY_FEATURE_SERVER}/query"
    
    all_features = []
    offset = 0
    
    print(f"Fetching records for {city}...")
    
    while True:
        params = {
            'where': f"Parcel_Muni = '{city}'",
            'outFields': 'Parcel_TaxID,Street_Address,City_State_Zip,Easting_Coordinate,Northing_Coordinate,Parcel_Muni',
            'returnGeometry': 'true',  # We can get lat/lon from geometry or convert coordinates
            'outSR': '4326',  # Request WGS84 for lat/lon
            'resultOffset': offset,
            'resultRecordCount': batch_size,
            'f': 'json'
        }
        
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            
            if 'error' in data:
                raise ValueError(f"API Error: {data['error']}")
                
            features = data.get('features', [])
            if not features:
                break
                
            all_features.extend(features)
            offset += len(features)
            print(f"Fetched {len(features)} records (Total: {offset})...")
            
            if len(features) < batch_size:
                break
                
        except Exception as e:
            print(f"Error fetching batch at offset {offset}: {e}")
            raise

    return all_features

def create_local_db(features, db_path="albany_lots.db"):
    """
    Creates or overwrites the local SQLite DB with a 'lots' table.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS lots;")
    c.execute("""
    CREATE TABLE lots (
        id TEXT PRIMARY KEY,
        address TEXT,
        lat REAL,
        lon REAL,
        posted_twitter TEXT DEFAULT '0',
        posted_bluesky TEXT DEFAULT '0'
    )
    """)

    insert_sql = "INSERT OR IGNORE INTO lots (id, address, lat, lon) VALUES (?, ?, ?, ?)"
    
    count = 0
    for feat in features:
        attrs = feat.get('attributes', {})
        geom = feat.get('geometry', {})
        
        # ID: Parcel_TaxID (e.g., 87.39-1-33)
        pid = attrs.get('Parcel_TaxID')
        if not pid:
            continue
            
        # Address: Street_Address + City_State_Zip
        # Example: "812 Old S Pearl St ", "Albany, NY  12202"
        street = (attrs.get('Street_Address') or '').strip()
        city_zip = (attrs.get('City_State_Zip') or '').strip()
        
        if not street:
            # Fallback to lat/lon if no address? Or skip.
            # Let's try to construct it.
            if not city_zip:
                city_zip = "Albany, NY"
            address = f"{street}, {city_zip}".strip(", ")
        else:
             address = f"{street}, {city_zip}"

        # STRICT FILTER: Only include if address contains "Albany"
        # This removes Guilderland, Delmar, Loudonville, etc.
        if "albany" not in address.lower():
            continue

        # Lat/Lon
        # Geometry usually comes as {x: ..., y: ...} for points or rings for polygons
        lat, lon = 0.0, 0.0
        if geom:
            if 'rings' in geom:
                # Simple centroid of first ring
                ring = geom['rings'][0]
                lats = [pt[1] for pt in ring]
                lons = [pt[0] for pt in ring]
                lat = sum(lats) / len(lats)
                lon = sum(lons) / len(lons)
            elif 'x' in geom:
                lat = geom['y']
                lon = geom['x']
            
        c.execute(insert_sql, (pid, address, lat, lon))
        count += 1

    # Handle start pin
    start_pin = os.getenv('START_PIN')
    if start_pin:
        print(f"\nMarking all pins up to {start_pin} as posted...")
        c.execute("""
            UPDATE lots 
            SET posted_bluesky = '1' 
            WHERE id <= ?
        """, (start_pin,))
        rows_marked = c.execute("SELECT COUNT(*) FROM lots WHERE posted_bluesky = '1'").fetchone()[0]
        print(f"Marked {rows_marked:,d} pins as posted")

    conn.commit()
    conn.close()
    print(f"Saved {count} lots to database.")

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description='Fetch and process Albany property data')
    parser.add_argument('--city', type=str, default='City of Albany', help='Municipality to filter by')
    parser.add_argument('--db', type=str, default='albany_lots.db', help='Output database path')
    args = parser.parse_args()

    try:
        features = fetch_albany_parcels(args.city)
        print(f"\nFetched {len(features):,d} total records")

        print(f"\nCreating local database at {args.db}...")
        create_local_db(features, args.db)
        print("Database created successfully!")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
