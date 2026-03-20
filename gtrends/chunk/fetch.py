import os
import random
import time
import serpapi

# from gtrends.apis.utils import SerpApiKeyManager

from datetime import datetime, timedelta
# from gtrends.apis.utils import MAX_RETRIES

MAX_BACKOFF = 20
BASE_DELAY = (1, 3)

MAX_RETRIES = 3

class SerpApiTrendsFetcher:
    """
    Handles Google Trends timeseries fetching via SerpApi
    with retry logic, key rotation, and rate-limit handling.
    """

    def __init__(self, api_key, geo="IN", tz="330"):
        self.geo = geo
        self.tz = tz
        self.api_key = api_key
        # self.key_manager = SerpApiKeyManager()


    # def initialize(self):
    #     self.key_manager.initialize()
    
    def _build_params(self, keyword, timeframe, gprop = 'youtube'):
        return {
            "engine": "google_trends",
            "q": keyword,
            "geo": self.geo,
            "date": timeframe,
            "data_type": "TIMESERIES",
            "tz": self.tz,
            "gprop": "" if gprop == "web" else gprop,
        }

    def _validate_response(self, results):
        if "error" in results:
            err = results["error"]
            if isinstance(err, dict):
                raise ValueError(err.get("message", str(err)))
            raise ValueError(err)

    def _extract_timeline(self, results):
        timeline = results.get("interest_over_time", {}).get("timeline_data", [])
        if not timeline:
            raise ValueError("No timeline data found in SerpApi response")
        return timeline

    def _process_timeline(self, timeline):
        rows = []

        for entry in timeline:
            dt = int(entry["timestamp"])
            value = entry["values"][0].get("extracted_value", 0)

            rows.append({
                "timestamp": dt,
                "value": value
            })

        return rows

    def _classify_error(self, exception):
        msg = str(exception).lower()

        if any(k in msg for k in [
            "plan limit", "unauthorized", "invalid api key", "401", "403"
        ]):
            return "DEAD_KEY"

        if "429" in msg or "too many requests" in msg:
            return "RATE_LIMIT"

        return "GENERIC"

    def _handle_dead_key(self, api_key, error):
        print(f"⚠️ Key Failure ({api_key[:8]}...): {error}")
        self.key_manager.rotate_api_key(failed_key=api_key)
        time.sleep(1)

    def _handle_rate_limit(self, api_key):
        print("⏳ Rate Limit (429) — Sleeping 1s...")
        self.key_manager.mark_key_exhausted(api_key)
        time.sleep(1)

    def _handle_generic_error(self, error, attempt, keyword):
        print(
            f"⚠️ Error fetching [{keyword}] | attempt {attempt + 1}/{MAX_RETRIES}\n"
            f"   ↳ {type(error).__name__}: {error}"
        )

        delay = min(
            random.uniform(*BASE_DELAY) * (2 ** attempt),
            MAX_BACKOFF
        )

        print(f"🔁 Backoff Sleeping {delay:.1f}s")
        time.sleep(delay)

    def _safe_name(self, name: str):
        return "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in name).strip("_")

    def fetch_chunk(self, keyword, start, end, gprop="youtube"):
        timeframe = f"{start} {end}"
        params = self._build_params(keyword, timeframe, gprop)

        retries = 0
        key_rotations = 0

        while retries < MAX_RETRIES:
            # api_key = self.key_manager.get_current_key()
            # params["api_key"] = api_key
            params["api_key"] = self.api_key

            try:
                results = serpapi.search(params)
                self._validate_response(results)

                timeline = self._extract_timeline(results)
                # self.key_manager.mark_key_used(api_key)
                return self._process_timeline(timeline)

            except Exception as e:
                # error_type = self._classify_error(e)

                # if error_type == "DEAD_KEY":
                #     self._handle_dead_key(api_key, e)
                #     key_rotations += 1
                #     if key_rotations > MAX_RETRIES:
                #         break
                #     continue

                # if error_type == "RATE_LIMIT":
                #     self._handle_rate_limit(api_key)
                #     continue

                # self._handle_generic_error(e, retries, keyword)
                # retries += 1

                print(f"⚠️ Error fetching [{keyword}] | attempt {retries + 1}/{MAX_RETRIES}")
                print(f"   ↳ {type(e).__name__}: {e}")

                delay = min(
                    random.uniform(*BASE_DELAY) * (2 ** retries),
                    MAX_BACKOFF
                )
                time.sleep(delay)
                retries += 1

        return None
