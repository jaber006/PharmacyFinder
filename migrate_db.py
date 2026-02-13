"""Migrate the pharmacies table to the new schema."""
import sqlite3
import sys

DB_PATH = "pharmacy_finder.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Check if old table exists with old schema
    c.execute("SELECT sql FROM sqlite_master WHERE name='pharmacies'")
    row = c.fetchone()
    if not row:
        print("No pharmacies table found, nothing to migrate.")
        conn.close()
        return
    
    old_schema = row[0]
    print(f"Current schema: {old_schema}")
    
    # If using old UNIQUE(address) constraint, we need to recreate
    if 'UNIQUE(address)' in old_schema or 'UNIQUE(name, latitude, longitude)' not in old_schema:
        print("Migrating to new schema...")
        
        # Rename old table
        c.execute("ALTER TABLE pharmacies RENAME TO pharmacies_old")
        
        # Create new table
        c.execute("""
            CREATE TABLE pharmacies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                source TEXT,
                date_scraped TEXT,
                suburb TEXT,
                state TEXT,
                postcode TEXT,
                opening_hours TEXT,
                UNIQUE(name, latitude, longitude)
            )
        """)
        
        # Copy old data (best effort)
        try:
            c.execute("""
                INSERT OR IGNORE INTO pharmacies 
                (name, address, latitude, longitude, source, date_scraped)
                SELECT name, address, latitude, longitude, source, date_scraped
                FROM pharmacies_old
            """)
            print(f"Migrated {c.rowcount} rows from old table")
        except Exception as e:
            print(f"Error copying data: {e}")
        
        # Drop old table
        c.execute("DROP TABLE pharmacies_old")
        
        conn.commit()
        print("Migration complete!")
    else:
        print("Schema already up to date, no migration needed.")
    
    conn.close()

if __name__ == "__main__":
    migrate()
