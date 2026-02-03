from datetime import datetime, timedelta
from statistics import mean
import csv

# from gtrends.db.keywords import GoogleTrendsRepository

class GoogleTrendSyncer:
    """
    Handles normalization and continuity of Google Trends time series.
    """
    DATE_FMT = "%Y-%m-%d"  # Adjust if SerpApi returns different format

    # def __init__(self):
    #     self.gtr = GoogleTrendsRepository()
    #     self.db = self.gtr.db  # Access the DB connector

    # def find_overlap(self, past, new):
    #     """Finds common dates between two datasets."""
    #     past_map = {p["date"]: p["value"] for p in past}
    #     overlap = []
    #     for row in new:
    #         if row["date"] in past_map:
    #             overlap.append(
    #                 (past_map[row["date"]], row["value"])
    #             )
    #     return overlap

    def find_overlap(self, past, new):
        """
        past: last rows read from CSV (DictReader)
        new: freshly fetched chunk
        """

        # Build map from past using timestamp
        past_map = {
            int(p["timestamp"]): float(p["value"])
            for p in past
            if "timestamp" in p
        }

        overlap = []

        for row in new:
            ts = int(row["timestamp"])

            if ts in past_map:
                overlap.append(
                    (past_map[ts], row["value"])
                )

        return overlap


    def compute_scaling_factor(self, overlap):
        """Calculates ratio: mean(past_values) / mean(new_values)."""
        if len(overlap) < 1: return 1.0
        
        past_vals = [p for p, _ in overlap]
        new_vals = [n for _, n in overlap]
        
        if mean(new_vals) == 0: return 1.0
        return mean(past_vals) / mean(new_vals)

    def normalize_new_data(self, new_data, scale):
        """Applies scaling factor to new data chunk."""
        normalized = []
        date_fmt = "%Y-%m-%d"

        for row in new_data:
            # --- normalize date---
            if"timestamp" in row:
                date_obj = datetime.fromtimestamp(int(row["timestamp"]))
            elif isinstance(row.get("date"), datetime):
                date_obj = row["date"]
            elif isinstance(row.get("date"), str):
                date_obj = datetime.strptime(row["date"], "%Y-%m-%d")
            else:
                raise ValueError(f"Unsupported date format: {row}")

            normalized.append({
                "date": date_obj.strftime(date_fmt),
                "value": round(row["value"] * scale, 2)
            })
                
        return normalized


    def normalise_past_data(self, past_rows):
        """matching the format of existing data with fetched data"""
        # id, timestamp, value
        normalized = []
        for row in past_rows:
                normalized.append({
                        "id":row["id"],
                        "date":datetime.fromtimestamp(row["timestamp"]).strftime("%Y-%m-%d"),
                        "value": row["value"]
                })
        return normalized

    def merge_timeseries(self, past, normalised_new):
        """Combines past data with normalized new data."""
        merged = {str(row["date"]): row["value"] for row in past}

        for row in normalised_new:
            merged[str(row["date"])] = row["value"]
        
        return [{"date":d, "value": v} for d,v in sorted(merged.items())]



    def generate_windows(self, start_date, end_date, window_days=240, overlap_days=30):
        """
        Accepts start_date and end_date as either:
        - datetime objects
        - strings in YYYY-MM-DD format
        """

        # 🔹 Convert strings → datetime
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, self.DATE_FMT)

        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, self.DATE_FMT)

        w_end = end_date
        windows = []

        while w_end > start_date:
            w_start = max(
                w_end - timedelta(days=window_days),
                start_date
            )

            windows.append((
                w_start.strftime(self.DATE_FMT),
                w_end.strftime(self.DATE_FMT)
            ))

            # move window backward with overlap
            w_end = w_end - timedelta(days=window_days - overlap_days)

        return windows[::-1]
    
    def get_last_date(self, csv_file, date_col="date", date_fmt="%Y-%m-%d",days = 30):

        try:
            with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                if not rows:
                    return None

                if date_col not in rows[0]:
                    raise KeyError(
                        f"Column '{date_col}' not found. "
                        f"Available columns: {list(rows[0].keys())}"
                    )

                last_date_str = rows[-1][date_col]
                return datetime.strptime(last_date_str, date_fmt) - timedelta(days=days)

        except FileNotFoundError:
            return None




    def upsert_timeseries(self,keyword:str, geo:str, data, search_id:int):
        """
        Upsert normalized Google Trends time-series into DB
        """
        query = """
        INSERT INTO google_trends_iot (search_id, keyword, geo, timestamp, value)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """
        params = []
        for row in data:
            date = row["date"]

            if isinstance(date, datetime):
                timestamp = int(date.timestamp())
            else:
                timestamp = int(
                    datetime.combine(date, datetime.min.time()).timestamp()
                )
            params.append((search_id,keyword,geo,timestamp,row["value"]))
            
        if params:
            try:
                self.db.execute_many(query,params)
            except Exception as e:
                print("ERROR SAVING THE DATA:",e)
            finally:
                self.db.close()

    
                
            