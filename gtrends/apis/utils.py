import threading
import random
import time
from datetime import datetime

import gtrends.db.utils as db_utils


SERP_API_KEYS = None

# 🔒 THREAD SAFETY GLOBALS
KEY_LOCK = threading.Lock()
CURRENT_KEY_INDEX = 0

START_DATE = "2021-01-01"
WINDOW_DAYS = 270

MAX_BACKOFF = 20
BASE_DELAY = (1, 3)

BATCH_SIZE = 5
BATCH_NUMBER = 1

MAX_THREADS = 10
MAX_RETRIES = 3

class SerpApiKeyManager:
    """
    Thread-safe SerpApi key manager with DB-backed key loading,
    safe rotation, and shared backoff utilities.
    """

    def __init__(self):
        self.max_retries = MAX_RETRIES
        self.base_delay = BASE_DELAY
        self.max_backoff = MAX_BACKOFF

        self._key_lock = threading.Lock()
        self._keys = []
        self._current_index = 0

    def initialize(self):
        self._current_index = 0
        self._load_keys()
    
    def _load_keys(self):
        select_query = """
            SELECT `id`, `token`
            FROM `external_tokens`
            WHERE `valid_from` < CURRENT_TIMESTAMP
            AND `platform` = 'serpapi'
            AND `token_type` = 'api_key'
        """

        try:
            active_keys = db_utils.execute_select_query(
                select_query,
                db='office_crm'
            )

            if not active_keys:
                raise RuntimeError("No active SerpApi keys found in database")

            self._keys = [row.get('token') for row in active_keys if row.get('token')]

            if not self._keys:
                raise RuntimeError("SerpApi keys fetched but all tokens are empty/null")

        except Exception as e:
            # 🔴 Critical system error — key loading failure should stop execution
            error_msg = (
                "Failed to load SerpApi API keys from database | "
                f"{type(e).__name__}: {e}"
            )

            # Optional: replace with your logger
            print(f"❌ {error_msg}")

            # Re-raise to prevent silent failures
            raise RuntimeError(error_msg) from e
    
    def get_current_key(self):
        """
        Thread-safe way to get the current key.
        """
        if not self._keys:
            raise RuntimeError("SerpApiKeyManager not initialized")
        
        with self._key_lock:
            return self._keys[self._current_index]
    
    def rotate_api_key(self, failed_key=None):
        """
        Thread-safe rotation.

        If another thread already rotated the key,
        this call becomes a NO-OP to avoid skipping keys.
        """
        with self._key_lock:
            current_active_key = self._keys[self._current_index]

            # Another thread already rotated
            if failed_key and current_active_key != failed_key:
                return True

            prev_index = self._current_index
            self._current_index = (self._current_index + 1) % len(self._keys)

            print(
                f"\n🔑 Rotating Key: {prev_index} -> {self._current_index} "
                f"(Total Keys: {len(self._keys)})"
            )

            return True
    
    def mark_key_exhausted(self, api_key):
        query = f"""
            UPDATE external_tokens
            SET valid_from = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 30 DAY)
            WHERE token = '{api_key}'
            """
        status = db_utils.execute_query(query, db = 'office_crm')
    
    def mark_key_used(self, api_key):
        query = f"""
            UPDATE external_tokens
            SET last_used_at = CURRENT_TIMESTAMP
            WHERE token = '{api_key}'
            """
        status = db_utils.execute_query(query, db = 'office_crm')
    
    def backoff_sleep(self, attempt, exam_name="Unknown"):
        delay = min(
            random.uniform(*self.base_delay) * (2 ** attempt),
            self.max_backoff
        )

        print(
            f"\n[Thread-{exam_name}] 🔁 Retry {attempt + 1}/{self.max_retries} "
            f"— Sleeping {delay:.1f}s"
        )

        time.sleep(delay)