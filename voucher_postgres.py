import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import csv
import os
import time
import datetime
from dotenv import load_dotenv

# Load environment variables (DB credentials)
load_dotenv()

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "voucher_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

# Connection Pool
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
    print("PostgreSQL connection pool created successfully.")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    print("Ensure PostgreSQL is running and credentials in .env are correct.")
    db_pool = None

def get_db_connection():
    if db_pool:
        return db_pool.getconn()
    else:
        raise Exception("No database connection available.")

def release_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)

def init_db_partitions():
    """
    Creates partitions for the next few days.
    In production, this should be a scheduled maintenance job.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Create partitions for today and next 3 days
        base_date = datetime.date.today()
        for i in range(4):
            day_start = base_date + datetime.timedelta(days=i)
            day_end = day_start + datetime.timedelta(days=1)
            
            # Format: raw_pool_y2025m12d01
            part_name = f"raw_pool_y{day_start.strftime('%Y')}m{day_start.strftime('%m')}d{day_start.strftime('%d')}"
            
            # Check existence first (Postgres doesn't have IF NOT EXISTS for partitions in simple syntax)
            cur.execute(f"SELECT to_regclass('{part_name}');")
            if not cur.fetchone()[0]:
                print(f"Creating partition: {part_name}")
                sql = f"""
                    CREATE TABLE {part_name} PARTITION OF raw_pool
                    FOR VALUES FROM ('{day_start}') TO ('{day_end}');
                """
                cur.execute(sql)
        
        conn.commit()
        print("Partitions initialized.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating partitions: {e}")
    finally:
        cur.close()
        release_db_connection(conn)

def ingest_file_postgres(filename):
    """
    High-performance ingestion using COPY or execute_values.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Prepare data in memory (chunking is recommended for 6GB files)
        # For huge files, we should stream this. For <1GB chunks, list is fine.
        records = []
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                phone = row['phone']
                # Determine provider
                if phone.startswith('0812'): provider = 'tsel'
                elif phone.startswith('0857'): provider = 'isat'
                elif phone.startswith('0819'): provider = 'xl'
                else: provider = 'other'
                
                records.append((
                    phone, 
                    provider, 
                    row['url'], 
                    row['timestamp'], 
                    filename
                ))

        print(f"Ingesting {len(records)} records to Postgres...")
        start = time.time()
        
        # Method 1: execute_values (Fast)
        query = """
            INSERT INTO raw_pool (phone_number, provider, url_source, imported_at, file_source)
            VALUES %s
        """
        execute_values(cur, query, records)
        
        conn.commit()
        duration = time.time() - start
        print(f"Ingested in {duration:.2f}s ({len(records)/duration:.0f} rows/sec)")
        
    except Exception as e:
        conn.rollback()
        print(f"Error ingestion: {e}")
    finally:
        cur.close()
        release_db_connection(conn)

def process_order_postgres(customer_name, requirements):
    """
    Order processing with Anti-Join on PostgreSQL
    """
    conn = get_db_connection()
    results = {}
    
    try:
        cur = conn.cursor()
        
        # Logic parameters
        cutoff_days = 30 # Don't reuse numbers sold in last 30 days
        freshness_days = 3 # Only take numbers imported in last 3 days
        
        print(f"\nProcessing Postgres order for {customer_name}...")
        
        for req in requirements:
            provider = req['provider']
            qty = req['qty']
            
            print(f"  - Requesting {qty} {provider} numbers...")
            
            # OPTIMIZED POSTGRES QUERY
            # Uses CTEs and standard Anti-Join
            # Postgres Partition pruning kicks in because of 'imported_at' filter
            query = """
                WITH Candidates AS (
                    SELECT phone_number
                    FROM raw_pool
                    WHERE provider = %s
                      AND imported_at >= NOW() - INTERVAL '%s days'
                ),
                Blacklist AS (
                    SELECT phone_number
                    FROM sales_history
                    WHERE sold_at >= NOW() - INTERVAL '%s days'
                )
                SELECT c.phone_number
                FROM Candidates c
                LEFT JOIN Blacklist b ON c.phone_number = b.phone_number
                WHERE b.phone_number IS NULL
                LIMIT %s;
            """
            
            start = time.time()
            cur.execute(query, (provider, freshness_days, cutoff_days, qty))
            found_numbers = [row[0] for row in cur.fetchall()]
            duration = time.time() - start
            
            if len(found_numbers) < qty:
                print(f"    WARNING: Shortage! Found {len(found_numbers)}/{qty}")
            else:
                print(f"    Success. Found {len(found_numbers)} in {duration:.4f}s")
            
            # Record Sale
            if found_numbers:
                insert_query = """
                    INSERT INTO sales_history (phone_number, customer_name, sold_at)
                    VALUES %s
                """
                sales_data = [(num, customer_name, datetime.datetime.now()) for num in found_numbers]
                execute_values(cur, insert_query, sales_data)
                results[provider] = found_numbers
        
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        print(f"Error processing order: {e}")
    finally:
        cur.close()
        release_db_connection(conn)
        
    return results

import random

# ... (Previous imports remain)

# --- HELPER: Dummy Data Generator (Copied from voucher_system.py for standalone usage) ---
def generate_dummy_file(filename, count=10000, date_str=None):
    """Generates a dummy CSV file mimicking the input format."""
    providers = ['tsel', 'isat', 'xl', 'three']
    urls = ['game_a.com', 'slot_b.com', 'news_c.com']
    
    if not date_str:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"Generating {count} records into {filename}...")
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['phone', 'url', 'timestamp'])
        for _ in range(count):
            prov = random.choice(providers)
            prefix = "0812" if prov == 'tsel' else "0857" if prov == 'isat' else "0819"
            phone = f"{prefix}{random.randint(10000000, 99999999)}"
            writer.writerow([phone, random.choice(urls), date_str])
    print("Done.")
# ---------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Check connection before running demo
    if db_pool:
        print("--- initializing partitions ---")
        init_db_partitions()
        
        # 1. Generate Dummy Data
        print("\n1. Generating Dummy Data...")
        today_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dummy_file = "data_postgres_test.csv"
        generate_dummy_file(dummy_file, count=3_000_000, date_str=today_str)

        # 2. Ingest Data
        print("\n2. Ingesting Data to Postgres...")
        ingest_file_postgres(dummy_file)
        
        # 3. Process Order
        print("\n3. Processing Order...")
        process_order_postgres("Anto_Postgres_Test", [
            {'provider': 'tsel', 'qty': 1_000_000},
            {'provider': 'isat', 'qty': 1_000_000}
        ])

        # Cleanup
        try:
            os.remove(dummy_file)
            print("\nTest file cleaned up.")
        except:
            pass
    else:
        print("\nSkipping demo because database connection failed.")
        print("Please set up .env and ensure PostgreSQL is running.")

