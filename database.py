import sqlite3
from pathlib import Path


class CensusDatabase:

    def __init__(self, database_file="data/folketaelling.db"):

        self.database_file = Path(database_file)

        self.database_file.parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(self.database_file)

        self.connection.row_factory = sqlite3.Row

        self.create_database()

    # -----------------------------------------------------

    def create_database(self):

        cursor = self.connection.cursor()

        cursor.execute("""

        CREATE TABLE IF NOT EXISTS persons (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            filename TEXT,
            filepath TEXT,

            kipnr TEXT,

            navn TEXT,

            koen TEXT,

            alder INTEGER,

            foedeaar INTEGER,

            foedested TEXT,

            civilstand TEXT,

            erhverv TEXT,

            stilling TEXT,

            husstand TEXT,

            ftaar TEXT,

            kommune TEXT,

            amt TEXT,

            sogn TEXT,

            original_json TEXT

        )

        """)

        self.connection.commit()

        self.create_indexes()

    # -----------------------------------------------------

    def create_indexes(self):

        cursor = self.connection.cursor()

        indexes = [

            "CREATE INDEX IF NOT EXISTS idx_navn ON persons(navn)",

            "CREATE INDEX IF NOT EXISTS idx_foedeaar ON persons(foedeaar)",

            "CREATE INDEX IF NOT EXISTS idx_alder ON persons(alder)",

            "CREATE INDEX IF NOT EXISTS idx_foedested ON persons(foedested)",

            "CREATE INDEX IF NOT EXISTS idx_husstand ON persons(husstand)",

            "CREATE INDEX IF NOT EXISTS idx_kipnr ON persons(kipnr)",

            "CREATE INDEX IF NOT EXISTS idx_filename ON persons(filename)",

            "CREATE INDEX IF NOT EXISTS idx_ftaar ON persons(ftaar)"

        ]

        for sql in indexes:
            cursor.execute(sql)

        self.connection.commit()

    # -----------------------------------------------------

    def execute(self, sql, values=None):

        cursor = self.connection.cursor()

        if values is None:
            cursor.execute(sql)

        else:
            cursor.execute(sql, values)

        self.connection.commit()

        return cursor

    # -----------------------------------------------------

    def executemany(self, sql, rows):

        cursor = self.connection.cursor()

        cursor.executemany(sql, rows)

        self.connection.commit()

    # -----------------------------------------------------

    def query(self, sql, values=None):

        cursor = self.connection.cursor()

        if values is None:
            cursor.execute(sql)

        else:
            cursor.execute(sql, values)

        return cursor.fetchall()

    # -----------------------------------------------------

    def count(self):

        cursor = self.connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM persons")

        return cursor.fetchone()[0]

    # -----------------------------------------------------

    def clear(self):

        self.execute("DELETE FROM persons")

    # -----------------------------------------------------

    def optimize(self):

        cursor = self.connection.cursor()

        cursor.execute("VACUUM")

        cursor.execute("ANALYZE")

        self.connection.commit()

    # -----------------------------------------------------

    def close(self):

        self.connection.close()