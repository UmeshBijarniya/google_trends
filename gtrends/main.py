import os,time
from datetime import datetime, timedelta
import csv
from chunk.fetch import SerpApiTrendsFetcher
from process.sync import GoogleTrendSyncer


syncer = GoogleTrendSyncer()

Api_key = "dd91f8ff20268440571b3410cec679caacbaa390ef89e238e20d5d649829f35d"
fetcher = SerpApiTrendsFetcher(Api_key)

# kw_to_search = "Rajasthan"
search_list = ["Jaipur"]
window_days = 240
overlap_days = 30

start = datetime.strptime("2021-04-02", "%Y-%m-%d").strftime("%Y-%m-%d")
end = datetime.today().strftime("%Y-%m-%d")

for search in search_list:

        safe_keyword = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in search)
        csv_file = f"{safe_keyword}.csv"

        file_exists = os.path.isfile(csv_file)

        # get last date from csv
        last_date = syncer.get_last_date(csv_file,"date")
        if last_date:
            print(f"fetching from the date:{last_date}")

            start = (last_date - timedelta(days=overlap_days)).strftime("%Y-%m-%d")

        # generate windows
        try:
            windows = syncer.generate_windows(start,end,window_days,overlap_days)
            print(f"Windows generated of length:{len(windows)}")
        except Exception as e:
            print(f"error generating window: {e}")


        for window in windows:

            w_start, w_end = window
            print(f"fetching window :{w_start}->{w_end}")

            # fetch data
            try:
                chunk = fetcher.fetch_chunk(search,w_start,w_end,"youtube")
                print(f"chunk fetched sucessfully")
            except Exception as e:
                print(f"error fetching {e}")

            
            # get last 30 days data from the csv file to find overlap
            if file_exists:
                with open(csv_file, mode="r",newline="", encoding="utf-8") as f:
                    reader = list(csv.reader(f))

                    rows = reader[1:]           # actual data rows

                    past = rows[-30:] 
                    if not past:
                        past = chunk

                overlap = syncer.find_overlap(past, chunk)
                scale = syncer.compute_scaling_factor(overlap)
                chunk = syncer.normalize_new_data(chunk, scale)


                if rows:
                    last_saved_date = rows[-1][0] 
                    # Filter chunk to keep only dates after the last saved date
                    chunk = [row for row in chunk if row["date"] > last_saved_date]

            # append the data in file
            with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                # write header only once
                if not file_exists:
                    writer.writerow(["date", search])
                    file_exists = True

                for row in chunk:
                    writer.writerow([
                        row["date"],
                        row["value"]
                    ])

            print(f"Data saved to {csv_file}")




