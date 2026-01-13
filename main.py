import sys
import os
import time
from datetime import datetime, timedelta

# 1. SETUP: Ensure Python can see your 'gtrends' folder
# This adds the current directory to Python's search path
sys.path.append(os.getcwd())

# 2. IMPORTS: Bring in your classes
from gtrends.db.keywords import GoogleTrendsRepository
from gtrends.chunk.fetch import SerpApiTrendsFetcher
from gtrends.process.sync import GoogleTrendSyncer


def run_sync_pipeline():
    print("--- 🚀 Starting Google Trends Sync ---")

    # --- INITIALIZATION ---
    # 1. Connect to Database (Repository)
    repo = GoogleTrendsRepository()
    
    # 2. Setup Math Logic (Syncer)
    syncer = GoogleTrendSyncer()

    # 3. Setup API (Fetcher)
    fetcher = SerpApiTrendsFetcher()
    try:
        fetcher.initialize()
        print("✅ API Initialized")
    except Exception as e:
        print(f"❌ API Error: {e}")
        return

    # --- STEP 1: GET TASKS ---
    # Ask DB: "Which keywords need updating?"
    searches = repo.searches_to_update()
    
    if not searches:
        print("zzz No keywords pending update.")
        return

    print(f"📋 Found {len(searches)} keywords to process.")

    # --- STEP 2: PROCESS EACH KEYWORD ---
    for search in searches:
        # Extract details from the DB row
        search_id = search['id']
        keyword = search['search_keyword']
        geo = search.get('search_geo', 'IN')  # Default to India if missing
        gprop = search.get('gprop', 'youtube') # Default to youtube if missing

        print(f"\nProcessing: {keyword} ({geo})")

        # --- STEP 3: DETERMINE START DATE ---
        # Check if we have old data in the DB
        past_data = repo.get_past_data(search_id)
        
        end_date = datetime.now().date()
        
        if past_data:
            # HISTORY EXISTS: Start 30 days back from the last known date
            # (We need 30 days overlap to calculate the scaling factor)
            last_entry = past_data[-1]
            
            # Handle if 'timestamp' is stored as integer or datetime object
            if isinstance(last_entry['timestamp'], (int, float)):
                 last_date = datetime.fromtimestamp(last_entry['timestamp']).date()
            else:
                 last_date = last_entry['timestamp'] # Assuming it's already a date object

            start_date = last_date - timedelta(days=30)
            
            # Load existing data into our "stitched" list so we can append to it
            stitched_data = []
            for row in past_data:
                # Convert DB format to Syncer format: {'date': 'YYYY-MM-DD', 'value': 50}
                d_str = row['date'] if 'date' in row else datetime.fromtimestamp(row['timestamp']).strftime("%Y-%m-%d")
                stitched_data.append({"date": d_str, "value": row["value"]})
                
            print(f"   Refining existing data starting from: {start_date}")
        else:
            # NO HISTORY: Start fresh from 4 years ago
            start_date = end_date - timedelta(days=365*4)
            stitched_data = []
            print(f"   New keyword. Fetching fresh from: {start_date}")

        # --- STEP 4: GENERATE TIME WINDOWS ---
        # Break the long timeline into manageable 8-month chunks
        windows = syncer.generate_windows(start_date, end_date)
        print(f"   Created {len(windows)} time windows.")

        # --- STEP 5: FETCH & STITCH LOOP ---
        for start_str, end_str in windows:
            print(f"   Fetching {start_str} -> {end_str}...", end=" ")
            
            # A. FETCH from SerpApi
            try:
                chunk = fetcher.fetch_chunk(
                    keyword=keyword,
                    start=start_str,
                    end=end_str,
                    gprop=gprop
                )
            except Exception as e:
                print(f"[Error: {e}]")
                continue # Skip this chunk if API fails

            if not chunk:
                print("[Empty Data]")
                continue

            # B. STITCH (Merge)
            if not stitched_data:
                # If this is the very first piece of data, just save it
                stitched_data = chunk
                print("[First Chunk Saved]")
            else:
                # If we already have data, we must GLUE them together
                
                # 1. Find overlapping dates
                overlap = syncer._find_overlap(stitched_data, chunk)
                
                # 2. Calculate Math Ratio (Scaling Factor)
                scale = syncer.compute_scaling_factor(overlap)
                
                # 3. Adjust the new chunk to match the old scale
                normalized_chunk = syncer.normalize_new_data(chunk, scale)
                
                # 4. Merge them
                stitched_data = syncer.merge_timeseries(stitched_data, normalized_chunk)
                
                print(f"[Merged with scale: {scale:.2f}]")
            
            # Be polite to the API (wait 1 second)
            time.sleep(1)

        # --- STEP 6: SAVE TO DB ---
        if stitched_data:
            print(f"   💾 Saving {len(stitched_data)} rows to database...")
            syncer.upsert_timeseries(keyword, geo, stitched_data)
            print("   ✅ Done.")
        else:
            print("   ⚠️ No data found to save.")

    print("\n--- All jobs finished ---")

if __name__ == "__main__":
    run_sync_pipeline()