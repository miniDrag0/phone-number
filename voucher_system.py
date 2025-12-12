import sqlite3
import csv
import random
import datetime
import os
import time

DB_FILE = 'voucher.db'

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database with optimized schema for high volume."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Raw Pool: Stores incoming data. 
    # In a real scenario (Postgres/ClickHouse), this would be partitioned by 'imported_at'.
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_pool (
            phone_number TEXT,
            provider TEXT,
            url_source TEXT,
            imported_at TIMESTAMP,
            file_source TEXT
        )
    ''')
    
    # Indices are crucial for performance
    # Index for fast filtering by provider and date (Order Step 1)
    c.execute('CREATE INDEX IF NOT EXISTS idx_raw_provider_date ON raw_pool(provider, imported_at)')
    # Index for preventing duplicates (Order Step 2)
    c.execute('CREATE INDEX IF NOT EXISTS idx_raw_phone ON raw_pool(phone_number)')

    # Sales History: Tracks sold numbers to prevent duplicates within N days
    c.execute('''
        CREATE TABLE IF NOT EXISTS sales_history (
            phone_number TEXT,
            customer_name TEXT,
            sold_at TIMESTAMP
        )
    ''')
    
    # Index for the Anti-Join query
    c.execute('CREATE INDEX IF NOT EXISTS idx_sales_check ON sales_history(phone_number, sold_at)')
    
    conn.commit()
    conn.close()
    print("Database initialized.")

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
            # Generate random phone number (reuse some to test deduplication)
            # Prefix determines provider roughly for simulation
            prov = random.choice(providers)
            prefix = "0812" if prov == 'tsel' else "0857" if prov == 'isat' else "0819"
            phone = f"{prefix}{random.randint(10000000, 99999999)}"
            
            writer.writerow([phone, random.choice(urls), date_str])
    print("Done.")

def ingest_file(filename):
    """
    Simulates high-speed ingestion.
    In production: Use 'COPY' command in Postgres or ClickHouse for max speed.
    Here: We use SQLite transaction.
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    # Determine provider from phone number logic (simplified for demo)
    # The user said they separate files by URL/Day, but here we ingest mixed.
    
    records = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            phone = row['phone']
            # Simple provider detection
            if phone.startswith('0812'): provider = 'tsel'
            elif phone.startswith('0857'): provider = 'isat'
            elif phone.startswith('0819'): provider = 'xl'
            else: provider = 'other'
            
            records.append((phone, provider, row['url'], row['timestamp'], filename))

    print(f"Ingesting {len(records)} records...")
    start = time.time()
    c.executemany('''
        INSERT INTO raw_pool (phone_number, provider, url_source, imported_at, file_source)
        VALUES (?, ?, ?, ?, ?)
    ''', records)
    conn.commit()
    conn.close()
    print(f"Ingested in {time.time() - start:.2f}s")

def process_order(customer_name, requirements):
    """
    Complex Order Processing.
    requirements: list of dicts [{'provider': 'tsel', 'qty': 1000}, {'provider': 'xl', 'qty': 500}]
    
    CRITICAL: Ensures numbers sold in last 30 days are NOT reused.
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    results = {}
    
    # 30-day window for deduplication
    cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    
    # Data freshness (e.g., from last 3 days files)
    data_freshness_date = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')

    print(f"\nProcessing order for {customer_name}...")
    
    for req in requirements:
        wanted_provider = req['provider']
        qty = req['qty']
        
        print(f"  - Looking for {qty} {wanted_provider} numbers (Fresh since {data_freshness_date})...")
        
        # THE CORE LOGIC: ANTI-JOIN
        # Select from Raw Pool WHERE NOT EXISTS in Sales History (last 30 days)
        # Using specific provider and freshness constraints
        query = f'''
            SELECT r.phone_number 
            FROM raw_pool r
            WHERE r.provider = ? 
              AND r.imported_at >= ?
              AND r.phone_number NOT IN (
                  SELECT s.phone_number 
                  FROM sales_history s 
                  WHERE s.sold_at >= ?
              )
            GROUP BY r.phone_number -- Ensure unique numbers in this batch
            LIMIT ?
        '''
        
        start = time.time()
        c.execute(query, (wanted_provider, data_freshness_date, cutoff_date, qty))
        found_numbers = [row[0] for row in c.fetchall()]
        duration = time.time() - start
        
        if len(found_numbers) < qty:
            print(f"    WARNING: Only found {len(found_numbers)} available numbers!")
        else:
            print(f"    Success. Found {len(found_numbers)} numbers in {duration:.4f}s")
            
        # Record the sale (Locking these numbers)
        if found_numbers:
            sales_records = [(num, customer_name, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) for num in found_numbers]
            c.executemany('INSERT INTO sales_history (phone_number, customer_name, sold_at) VALUES (?, ?, ?)', sales_records)
            results[wanted_provider] = found_numbers

    conn.commit()
    conn.close()
    return results

if __name__ == "__main__":
    if not os.path.exists(DB_FILE):
        init_db()
        
        # Simulate Data Setup
        # Create data for 3 days ago, 2 days ago, today
        dates = [
            (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S'),
            (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ]
        
        for d in dates:
            fname = f"data_{d.replace(':','-')}.csv"
            generate_dummy_file(fname, count=50000, date_str=d) # 50k rows per file
            ingest_file(fname)
            try:
                os.remove(fname) # Clean up
            except:
                pass

    # TEST SCENARIO
    print("\n--- TEST SCENARIO ---")
    
    # 1. Anto SMS (Mix)
    process_order("Anto", [
        {'provider': 'tsel', 'qty': 5000},
        {'provider': 'isat', 'qty': 2000}
    ])
    
    # 2. Try to buy same Tsel numbers again immediately (Should get DIFFERENT numbers or Fail if out of stock)
    print("\n--- Re-ordering immediately (Should not get duplicates) ---")
    process_order("Anto_Repeat", [
        {'provider': 'tsel', 'qty': 5000}
    ])

