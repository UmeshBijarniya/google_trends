from datetime import datetime, timedelta
from statistics import mean
from typing import List, Dict, Callable

# Import from your existing modules
from gtrends.db.keywords import GoogleTrendsRepository
from gtrends.chunk.fetch import SerpApiTrendsFetcher

class GoogleTrendSyncer:
    """
    Handles normalization and continuity of Google Trends time series.
    """
    DATE_FMT = "%Y-%m-%d"  # Adjust if SerpApi returns different format

    def __init__(self):
        self.gtr = GoogleTrendsRepository()
        self.db = self.gtr.db  # Access the DB connector from the repository

    def _find_overlap(self, past, new):
        """Finds common dates between two datasets."""
        past_map = {p["date"]: p["value"] for p in past}
        overlap = []
        for row in new:
            if row["date"] in past_map:
                overlap.append((past_map[row["date"]], row["value"]))
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
        for row in new_data:
            normalized.append({
                "date": row["date"],
                "value": round(row["value"] * scale, 2)
            })
        return normalized

    def merge_timeseries(self, past, normalized_new):
        """Combines past data with normalized new data."""
        merged = {str(row["date"]): row["value"] for row in past}
        
        for row in normalized_new:
            merged[str(row["date"])] = row["value"]
        
        # Return sorted list
        return [{"date": d, "value": v} for d, v in sorted(merged.items())]

    def generate_windows(self, start_date, end_date, window_days=240, overlap_days=30):
        """Generates overlapping time windows moving backwards."""
        w_end = end_date
        windows = []
        while w_end > start_date:
            w_start = max(w_end - timedelta(days=window_days), start_date)
            windows.append((w_start.strftime(self.DATE_FMT), w_end.strftime(self.DATE_FMT)))
            w_end = w_end - timedelta(days=window_days - overlap_days)
        return windows[::-1]

    def upsert_timeseries(self, keyword, geo, data):
        """Inserts final stitched data into database."""
        # Ensure this table exists in your DB!
        query = """
        INSERT INTO google_trends_timeseries (keyword, geo, date, value)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """
        params = [
            (keyword, geo, row["date"], row["value"]) 
            for row in data
        ]
        self.db.execute_many(query, params)
        print(f"Upserted {len(data)} rows for {keyword}")