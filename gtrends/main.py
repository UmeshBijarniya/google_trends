import os, time
from datetime import datetime, timedelta
import csv
from chunk.fetch import SerpApiTrendsFetcher
from process.sync import GoogleTrendSyncer


syncer = GoogleTrendSyncer()

Api_key = "dd91f8ff20268440571b3410cec679caacbaa390ef89e238e20d5d649829f35d"
fetcher = SerpApiTrendsFetcher(Api_key)

# kw_to_search = "Rajasthan"
search_list = ["Jaipur", "NEET", "IPL","UPSC_Exam"]
window_days = 240
overlap_days = 30

start = datetime.strptime("2021-01-01", "%Y-%m-%d").strftime("%Y-%m-%d")
end = datetime.today().strftime("%Y-%m-%d")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, "Data")

# Create the 'data' folder if it doesn't exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

for search in search_list:

    safe_keyword = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in search)

    csv_file = os.path.join(DATA_FOLDER, f"{safe_keyword}.csv")

    file_exists = os.path.isfile(csv_file)

    # get last date from csv
    last_date = syncer.get_last_date(csv_file, "date")
    if last_date:
        print(f"fetching from the date:{last_date}")

        start_dt = (last_date - timedelta(days=overlap_days)).strftime("%Y-%m-%d")
    else:
        start_dt = start

    # generate windows
    try:
        windows = syncer.generate_windows(start_dt, end, window_days, overlap_days)
        print(f"Windows generated of length:{len(windows)}")
    except Exception as e:
        print(f"error generating window: {e}")

    for window in windows:

        w_start, w_end = window
        print(f"fetching window :{w_start}->{w_end}")

        # fetch data
        try:
            chunk = fetcher.fetch_chunk(search, w_start, w_end, "youtube")
            print(f"chunk fetched sucessfully")
        except Exception as e:
            print(f"error fetching {e}")

        if not chunk:
            print(f"empty chunk, skipping")
            continue

        scale = None

        with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["date", search])
                file_exists = True

        # get last 30 days data from the csv file to find overlap
        if file_exists:
            print(f"‼️Safety check , file exists! {csv_file}")

            with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
                reader = list(csv.reader(f))

                last_30 = reader[1:]  # actual data rows

                past = last_30[-30:]
                print(f"‼️safety check, captured past of length {len(past)}")
                if not past:
                    past = chunk
                    print(f"no past scale = 1")
                    scale = 1

            print(
                f"‼️finding overlap:length of past: {len(past)}, chunk size: {len(chunk)}"
            )
            if scale is None:
                overlap = syncer.find_overlap(past, chunk)
                scale = syncer.compute_scaling_factor(overlap)
                print(f"‼️safety check, overlap of length {len(overlap)} formed")

        print(f"‼️safety check , normalising with scale = {scale}")
        try:
            normalised_chunk = syncer.normalize_new_data(chunk, scale)
            print(f"‼️safety check , chunk normalised length : {len(normalised_chunk)}")
        except Exception as e:
            print(f"Normalization error:{e}")
            continue

        filtered_normalised_chunk = normalised_chunk
        if file_exists and last_30:
            last_saved_date = last_30[-1][0]
            # Filter chunk to keep only dates after the last saved date
            filtered_normalised_chunk = [
                row for row in normalised_chunk if row["date"] > last_saved_date
            ]

        # append the data in file
        if filtered_normalised_chunk:  # Only write if there is data left
            print(f"‼️safety check , saving in csv file ")
            with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                #

                for row in filtered_normalised_chunk:
                    writer.writerow([row["date"], row["value"]])
            print(f"Data saved to {csv_file}")
        else:
            print("No new data to save for this window.")
