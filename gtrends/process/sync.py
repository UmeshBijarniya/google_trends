from datetime import datetime, timedelta, timezone
from statistics import mean
import csv


# from gtrends.db.keywords import GoogleTrendsRepository


class GoogleTrendSyncer:
    """
    Handles normalization and continuity of Google Trends time series.
    """
    DATE_FMT = "%Y-%m-%d"  # Adjust if SerpApi returns different format

    def find_overlap(self, past, new):
        """
        past: list[list[str]] -> e.g., [['date', 'value'], ['2023-01-01', '50'], ...]
        new: list[dict] -> e.g., [{'timestamp': '1672531200', 'value': '75'}]
        """

        # --- Build Map from Past (CSV List) ---
        past_map = {}

        for row in past:
            # Safety check: Ensure row has at least 2 columns
            if not row or len(row) < 2:
                continue

            # Skip header row (if it contains "date" or text)
            if "date" in row[0].lower():
                continue

            try:
                # Store: "2023-01-01" -> 50.0
                past_map[row[0]] = float(row[1])
            except ValueError:
                continue  # Skip header or malformed lines

        overlap = []
        for item in new:
            # API items are Dicts, so we use keys
            if "timestamp" not in item:
                continue

            try:
                # Convert API Timestamp -> Date String to match CSV
                ts = int(item["timestamp"])
                date_key = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

                # CHECK OVERLAP
                if date_key in past_map:
                    overlap.append((past_map[date_key], float(item["value"])))

            except (ValueError, TypeError):
                continue

        return overlap

    def compute_scaling_factor(self, overlap):
        """Calculates ratio: mean(past_values) / mean(new_values)."""
        if len(overlap) < 1:
            return 1.0

        past_vals = [p for p, _ in overlap]
        new_vals = [n for _, n in overlap]

        avg_past = mean(past_vals)
        avg_new = mean(new_vals)

        # --- SAFETY CHECK ---
        if avg_new == 0:
            return 1.0

        return avg_past / avg_new

    def normalize_new_data(self, new_data, scale):
        """Applies scaling factor to new data chunk."""
        normalized = []
        date_fmt = "%Y-%m-%d"

        for row in new_data:
            # --- normalize date---
            if "timestamp" in row:
                date_obj = datetime.fromtimestamp(int(row["timestamp"]))
            elif isinstance(row.get("date"), datetime):
                date_obj = row["date"]
            elif isinstance(row.get("date"), str):
                date_obj = datetime.strptime(row["date"], "%Y-%m-%d")
            else:
                raise ValueError(f"Unsupported date format: {row}")

            scaled_value = row["value"] * scale

            # 2. Apply your specific condition
            if scaled_value > 100:
                scaled_value = 100  # Cap it at 100

            # 3. Append to the list
            normalized.append(
                {"date": date_obj.strftime(date_fmt), "value": round(scaled_value, 2)}
            )

        return normalized

    def normalise_past_data(self, past_rows):
        """matching the format of existing data with fetched data"""
        # id, timestamp, value
        normalized = []
        for row in past_rows:
            normalized.append(
                {
                    "id": row["id"],
                    "date": datetime.fromtimestamp(row["timestamp"]).strftime("%Y-%m-%d"),
                    "value": row["value"],
                }
            )
        return normalized

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
            w_start = max(w_end - timedelta(days=window_days), start_date)

            windows.append(
                (w_start.strftime(self.DATE_FMT), w_end.strftime(self.DATE_FMT))
            )

            # move window backward with overlap
            w_end = w_end - timedelta(days=window_days - overlap_days)

        return windows[::-1]

    def get_last_date(self, csv_file, date_col="date", date_fmt="%Y-%m-%d", days=30):

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
