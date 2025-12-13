#!/usr/bin/env python3
"""
Migrate SQLite client databases to PostgreSQL.
Each supermarket gets its own schema in PostgreSQL.
"""

import sqlite3
import psycopg2
from pathlib import Path
import sys
import re

# Configuration
SQLITE_DB_DIR = Path("LamApp/databases")
PG_HOST = "localhost"
PG_DATABASE = "lamarestock_products"
PG_USER = "lamauser"
PG_PASSWORD = "20031995Ruru@"  # Change this!


def sanitize_schema_name(name):
    """Convert supermarket name to valid PostgreSQL schema name."""
    # Remove special characters, replace spaces with underscores
    clean = re.sub(r'[^\w\s-]', '', name.lower())
    clean = re.sub(r'[-\s]+', '_', clean)
    return clean


def get_sqlite_schema(sqlite_path):
    """Extract schema from SQLite database."""
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # Get all table creation statements
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = cursor.fetchall()
    
    conn.close()
    return [table[0] for table in tables if table[0]]


def convert_sqlite_to_postgres_sql(sqlite_sql):
    """Convert SQLite CREATE TABLE to PostgreSQL."""
    # Basic conversions
    pg_sql = sqlite_sql
    
    # Remove SQLite-specific syntax
    pg_sql = pg_sql.replace('AUTOINCREMENT', 'SERIAL')
    pg_sql = pg_sql.replace('BOOLEAN DEFAULT 0', 'INTEGER DEFAULT 0')

    # Fix INTEGER PRIMARY KEY to SERIAL PRIMARY KEY
    pg_sql = re.sub(
        r'(\w+)\s+INTEGER\s+NOT\s+NULL\s+PRIMARY\s+KEY\s+AUTOINCREMENT',
        r'\1 SERIAL PRIMARY KEY',
        pg_sql,
        flags=re.IGNORECASE
    )
    # SQLite JSON TEXT + CHECK → PostgreSQL JSON
    pg_sql = re.sub(
        r'(\w+)\s+TEXT\s+CHECK\s*\(\s*json_valid\(\s*\1\s*\)\s*\)',
        r'\1 JSON',
        pg_sql,
        flags=re.IGNORECASE
    )
    return pg_sql


def migrate_supermarket_db(sqlite_path, schema_name, pg_conn):
    """Migrate one supermarket database to PostgreSQL schema."""
    print(f"\n{'='*60}")
    print(f"Migrating: {sqlite_path.name} → schema '{schema_name}'")
    print(f"{'='*60}")
    
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cur = sqlite_conn.cursor()
    
    pg_cur = pg_conn.cursor()
    
    try:
        # Create schema
        print(f"Creating schema '{schema_name}'...")
        pg_cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        pg_cur.execute(f"CREATE SCHEMA {schema_name}")
        
        # Get all tables
        sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in sqlite_cur.fetchall()]
        
        print(f"Found {len(tables)} tables: {', '.join(tables)}")
        
        # Migrate each table
        for table in tables:
            print(f"\n  Migrating table: {table}")
            
            # Get table schema
            sqlite_cur.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
            create_sql = sqlite_cur.fetchone()[0]
            
            # Convert to PostgreSQL
            pg_create_sql = convert_sqlite_to_postgres_sql(create_sql)
            pg_create_sql = pg_create_sql.replace(f"CREATE TABLE {table}", 
                                                   f"CREATE TABLE {schema_name}.{table}")
            
            # Create table in PostgreSQL
            pg_cur.execute(pg_create_sql)
            
            # Get column names
            sqlite_cur.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in sqlite_cur.fetchall()]
            
            # Copy data
            sqlite_cur.execute(f"SELECT * FROM {table}")
            rows = sqlite_cur.fetchall()
            
            if rows:
                # Prepare insert statement
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                insert_sql = f"INSERT INTO {schema_name}.{table} ({columns_str}) VALUES ({placeholders})"
                
                # Insert data
                data = [tuple(row) for row in rows]
                pg_cur.executemany(insert_sql, data)
                
                print(f"    ✓ Copied {len(rows)} rows")
            else:
                print(f"    ⚠ Table is empty")
        
        pg_conn.commit()
        print(f"\n✅ Successfully migrated {sqlite_path.name}")
        return True
        
    except Exception as e:
        pg_conn.rollback()
        print(f"\n❌ Error migrating {sqlite_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        sqlite_conn.close()


def main():
    """Main migration process."""
    print("="*60)
    print("SQLite to PostgreSQL Migration Tool")
    print("="*60)
    
    # Check if databases directory exists
    if not SQLITE_DB_DIR.exists():
        print(f"❌ Error: {SQLITE_DB_DIR} directory not found!")
        sys.exit(1)
    
    # Find all SQLite databases
    sqlite_dbs = list(SQLITE_DB_DIR.glob("*.db"))
    
    if not sqlite_dbs:
        print(f"❌ Error: No .db files found in {SQLITE_DB_DIR}")
        sys.exit(1)
    
    print(f"\nFound {len(sqlite_dbs)} databases to migrate:")
    for db in sqlite_dbs:
        print(f"  - {db.name}")
    
    # Confirm
    print(f"\nTarget PostgreSQL:")
    print(f"  Host: {PG_HOST}")
    print(f"  Database: {PG_DATABASE}")
    print(f"  User: {PG_USER}")
    
    response = input("\nProceed with migration? (yes/no): ")
    if response.lower() != 'yes':
        print("Migration cancelled.")
        sys.exit(0)
    
    # Connect to PostgreSQL
    print("\nConnecting to PostgreSQL...")
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)
    
    # Migrate each database
    success_count = 0
    for sqlite_path in sqlite_dbs:
        # Create schema name from filename
        schema_name = sanitize_schema_name(sqlite_path.stem)
        
        if migrate_supermarket_db(sqlite_path, schema_name, pg_conn):
            success_count += 1
    
    pg_conn.close()
    
    # Summary
    print("\n" + "="*60)
    print("Migration Complete!")
    print("="*60)
    print(f"Successfully migrated: {success_count}/{len(sqlite_dbs)} databases")
    
    if success_count < len(sqlite_dbs):
        print("\n⚠ Some migrations failed. Check errors above.")
        sys.exit(1)
    else:
        print("\n✅ All databases migrated successfully!")
        print("\nNext steps:")
        print("1. Update DatabaseManager.py to use PostgreSQL")
        print("2. Test the application")
        print("3. Keep SQLite backups for 1 week")


if __name__ == "__main__":
    main()