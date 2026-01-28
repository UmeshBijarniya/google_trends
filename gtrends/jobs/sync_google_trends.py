import time
from datetime import datetime, timedelta

from gtrends.chunk.fetch import SerpApiTrendsFetcher
from gtrends.db.keywords import GoogleTrendsRepository
from gtrends.process.sync import GoogleTrendSyncer

fetcher = SerpApiTrendsFetcher()
gtr = GoogleTrendsRepository()
syncer = GoogleTrendSyncer()

window_days = 240
overlap_days = 30

try:
    fetcher.initialize()
    print("Api initialised!")
except Exception as e:
    print(f"Api error : {e}")
    exit()
    
# get data 
if gtr.sync_search_ids():
    print("ensured entries exists for each exam")

search_list = gtr.searches_to_update()

if not search_list:
    print(f"no keywords to fetch !")
    exit()

print(f"keywords to process: {len(search_list)}")

#  fetch data in search_list
for data in search_list:
    try:
        # `id`,`search_keyword`,`search_platform`,`search_geo` 
        search_id = data["id"]
        keyword = data["search_keyword"]
        gprop = data["search_platform"]
        geo = data["search_geo"]

        past_raw = gtr.get_past_data(search_id)
        past = syncer.normalise_past_data(past_raw) if past_raw else []

        if past_raw:
            stitched = past
            start_dt = datetime.fromtimestamp(past_raw[-1]["timestamp"]) - timedelta(days = 30)
        
        else:
            stitched = []
            start_dt = datetime.now() - timedelta(days=4*365)
        end_dt = datetime.now()

        window_list = syncer.generate_windows(start_dt, end_dt, window_days, overlap_days)
        print(f"Created {len(window_list)} time windows.")

        for win_start, win_end in window_list:
            print(f"fetching data from {win_start} to {win_end}")
            try:
                chunk = fetcher.fetch_chunk(keyword, win_start, win_end, gprop=gprop)

                if not chunk:
                    print(f"empty chunk")
                    continue
                if not stitched:
                    stitched = sorted(chunk, key=lambda x:x["timestamp"])
                    continue
            except Exception as e:
                print(f"error fetching {e}")
                continue

            print(f"finding overlap!")
            overlap = syncer.find_overlap(stitched, chunk)
            if overlap:
                scale = syncer.compute_scaling_factor(overlap)
            else:
                print(f"No overlap for {keyword} ({win_start}-{win_end}) | set scaling factor to 1")
                scale = 1

            print(f"syncing with scaling factor: {scale}")
            normalized_chunk = syncer.normalize_new_data(chunk, scale)
            stitche_d = syncer.merge_timeseries(stitched, normalized_chunk)

            time.sleep(1)
        if stitche_d:
            print(f"upserting {len(stitche_d)} data rows")
            syncer.upsert_timeseries(keyword, geo, stitche_d, search_id)
            print(f"Done")
        else:
            print(f"No data to save!")
    except Exception as e:
        print(f"failed syncing {data.get('search_keyword')} : {e}")
        continue
#  data saved in db
            

    