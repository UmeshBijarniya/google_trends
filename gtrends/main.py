import os,time
from datetime import datetime
import csv
from chunk.fetch import SerpApiTrendsFetcher
from process.sync import GoogleTrendSyncer

fetcher = SerpApiTrendsFetcher()
syncer = GoogleTrendSyncer()

Api_key = "dd91f8ff20268440571b3410cec679caacbaa390ef89e238e20d5d649829f35d"

kw_to_search = "Rajasthan"
window_days = 240
overlap_days = 30

start = datetime.strptime("2021-04-02", "%Y-%m-%d").strftime("%Y-%m-%d")
end = datetime.today().strftime("%Y-%m-%d")

try:
    windows = syncer.generate_windows(start,end,window_days,overlap_days)
    print(f"Windows generated of length:{len(windows)}")
except Exception as e:
    print(f"error generating window: {e}")


for window in windows:

    w_start, w_end = window
    print(f"fetching window of length:{len(window)}")

    try:
        chunk = fetcher.fetch_chunk(kw_to_search,w_start,w_end,"youtube")
        print(f"chunk fetched sucessfully")
    except Exception as e:
        print(f"error fetching {e}")

    print(f"length of chunk: {len(chunk)}")

    time.sleep(3)
    import json

    csv_file = "google_trends.csv"
    file_exists = os.path.isfile(csv_file)

    with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # write header only once
        if not file_exists:
            writer.writerow(["date", "value"])

        for row in chunk:
            writer.writerow([
                datetime.fromtimestamp(row["timestamp"]).strftime("%Y-%m-%d"),
                row["value"]
            ])
        print(f"data saved to file successfully")




