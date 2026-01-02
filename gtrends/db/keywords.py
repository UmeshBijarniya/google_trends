import time
from typing import List, Dict
from gtrends.db.connector import DataBaseConnector

class GoogleTrendsRepository:
    def __init__(self):
        self.db = DataBaseConnector("office_crm")
    
    def searches_to_update(self) -> List[Dict]:
        """
        Fetch exams having usable Google Trends keywords
        """
        searches_to_update = []
        timestamp_24hrs_ago = int(time.time()) - 24 * 60 * 60 + 15 * 60 # an additional offset of 15 minutes to tackle the routine function time executions
        query = """
            SELECT `id`,`search_keyword`,`search_platform`,`search_geo` 
            FROM 
            `google_trends` 
            WHERE `id` not in 
            (
            SELECT `google_trends_iot`.`search_id` 
            FROM 
            `google_trends_iot` 
            WHERE `google_trends_iot`.`timestamp` > %s
            )
        """
        try:
            self.db.connect()
            searches_to_update = self.db.execute(query, (timestamp_24hrs_ago,), fetch_all=True)
        finally:
            self.db.close()

        return searches_to_update

    def get_exams(self) -> List[Dict]:
        """
        Fetch exams having usable Google Trends keywords
        """
        query = """
            SELECT 
                id,
                google_trends_keywords,
                geo
            FROM exams
            WHERE 
                google_trends_keywords IS NOT NULL
                AND geo IS NOT NULL
        """
        try:
            self.db.connect()
            trend_keywords = self.db.execute(query, fetch_all=True)
        finally:
            self.db.close()

        return trend_keywords

    def sync_search_ids(self) -> List[Dict]:
        """
        Ensure YOUTUBE entries exist for each exam
        """
        exams = self.get_exams()

        for exam in exams:
            keyword = exam.get("google_trends_keywords").split(",")[0]
            
            if not keyword:
                continue

            keyword = keyword.strip()
            geo = exam.get("geo") or "IN"

            for platform in ["youtube"]:
                self._sync_search_ids(
                    exam_id = exam["id"],
                    keyword = keyword,
                    geo = geo,
                    platform = platform
                )
    
    def _sync_search_ids(
        self,
        exam_id: int,
        keyword: str,
        geo: str,
        platform: str = "youtube",
        result_type: str = "iot"
    ) -> int:
        """
        Fetch existing google_trends.id or create if missing
        """
        sql = """
        SELECT 1
        FROM google_trends
        WHERE exam_id = %s
          AND search_platform = %s
          AND result_type = %s
        LIMIT 1
        """
        try:
            self.db.connect()
            row_exists = self.db.execute(
                sql,
                params=(exam_id, platform, result_type),
                fetch_one=True)
        finally:
            self.db.close()

        if row_exists:
            return True

        insert_sql = """
            INSERT INTO google_trends
            (exam_id, search_keyword, search_platform, search_geo, result_type)
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            self.db.connect()
            self.db.insert(
                insert_sql,
                (
                    exam_id,
                    keyword,
                    platform,
                    geo or "IN",
                    result_type
                ),
                return_id = False
            )
        finally:
            self.db.close()
        
        return True