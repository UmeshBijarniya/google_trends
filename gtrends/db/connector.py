import os
import mysql.connector
from mysql.connector import Error
from typing import Any, List, Tuple, Optional, Dict
from dotenv import load_dotenv

load_dotenv('gtrends/db/.env')

class DataBaseConnector:
    """
    Default database connector for ToppersNotes databases
    Supports: books, office_crm, apps
    """

    def __init__(self, db: str = "office_crm"):
        self.db = db
        self.connection = None
        self.cursor = None

    def _get_db_config(self) -> Dict[str, Any]:
        DB_PORT = None

        if self.db == 'books':
            DB_HOST = os.environ.get("DB_HOST")
            DB_USER = os.environ.get("DB_USER")
            DB_PASSWORD = os.environ.get("DB_PASSWORD")
            DB_NAME = os.environ.get("DB_NAME")

        elif self.db == 'office_crm':
            DB_HOST = os.environ.get("CRM_DB_HOST")
            DB_USER = os.environ.get("CRM_DB_USER")
            DB_PASSWORD = os.environ.get("CRM_DB_PASSWORD")
            DB_NAME = os.environ.get("CRM_DB_NAME")

        elif self.db == 'apps':
            DB_HOST = os.environ.get("APPDB_HOST")
            DB_USER = os.environ.get("APPDB_USERNAME")
            DB_PASSWORD = os.environ.get("APPDB_PASSWORD")
            DB_NAME = os.environ.get("APPDB_NAME")
            DB_PORT = os.environ.get("APPDB_PORT")

        else:
            raise ValueError(f"Unsupported database: {self.db}")

        config = {
            "host": DB_HOST,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "database": DB_NAME,
        }

        if DB_PORT:
            config["port"] = int(DB_PORT)

        return config

    def connect(self):
        if self.connection is None or not self.connection.is_connected():
            self.connection = mysql.connector.connect(**self._get_db_config())
            self.cursor = self.connection.cursor(dictionary=True)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()

    # -------------------------------------------------
    # CORE EXECUTION METHODS
    # -------------------------------------------------

    def execute(
        self,
        query: str,
        params: Optional[Tuple] = None,
        fetch_one: bool = False,
        fetch_all: bool = False,
        commit: bool = False
    ) -> Optional[Any]:
        """
        Execute a single query safely
        """
        try:
            self.connect()
            self.cursor.execute(query, params)

            if fetch_one:
                return self.cursor.fetchone()

            if fetch_all:
                return self.cursor.fetchall()

            if commit:
                self.connection.commit()

        except Error as e:
            if commit:
                self.connection.rollback()
            raise RuntimeError(f"Database error: {e}")

        return None

    # -------------------------------------------------
    # INSERT / UPDATE HELPERS
    # -------------------------------------------------

    def insert(self, query: str, params: Tuple, return_id: bool = False) -> int:
        """
        Execute INSERT and return last inserted ID
        """
        self.execute(query, params=params, commit=True)
        if return_id:
            return self.cursor.lastrowid
        return

    def update(self, query: str, params: Tuple, return_id: bool = False) -> int:
        """
        Execute UPDATE and return affected rows
        """
        self.execute(query, params=params, commit=True)
        if return_id:
            return self.cursor.rowcount
        return

    # -------------------------------------------------
    # BULK EXECUTION
    # -------------------------------------------------

    def execute_many(
        self,
        query: str,
        params_list: List[Tuple]
    ) -> int:
        """
        Execute multiple INSERT/UPDATE queries
        """
        try:
            self.connect()
            self.cursor.executemany(query, params_list)
            self.connection.commit()
            return self.cursor.rowcount

        except Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Bulk execution failed: {e}")
