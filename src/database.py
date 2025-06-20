from dataclasses import dataclass, field
from typing import Dict
import psycopg2
from psycopg2 import sql
from src.market import Market

@dataclass
class Database:
    connection_string: str
    connection: psycopg2.extensions.connection = field(init=False, repr=False)
    cursor: psycopg2.extensions.cursor = field(init=False, repr=False)

    def __post_init__(self):
        self.connection = psycopg2.connect(self.connection_string)
        self.cursor = self.connection.cursor()

    def reset_safe(self):
        ans = input("Are you sure you want to reset the database? This will delete all data. (yes/no): ")
        if ans.lower() != "yes":
            print("Database reset cancelled.")
            return
        print("Resetting database...")
        self.cursor.execute("DROP TABLE IF EXISTS markets")
        self.cursor.execute(
            sql.SQL("""
                CREATE TABLE markets (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    probability FLOAT NOT NULL
                )
            """)
        )
        self.connection.commit()

    def add_market(self, idd: str, market: Market):
        self.cursor.execute(
            sql.SQL("INSERT INTO markets (id, title, probability) VALUES (%s, %s, %s)"),
            (idd, market.title, market.probability)
        )
        self.connection.commit()

    def get_market(self, idd: str) -> Market:
        self.cursor.execute(
            sql.SQL("SELECT title, probability FROM markets WHERE id = %s"),
            (idd,)
        )
        result = self.cursor.fetchone()
        if result is None:
            raise KeyError(f"Market with id '{idd}' does not exist.")
        return Market(title=result[0], probability=result[1])
