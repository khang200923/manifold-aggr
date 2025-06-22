from dataclasses import dataclass, field
from typing import Dict
import logging
import numpy as np
import psycopg2
from src.market import Market

def db_error_handler(func):
    def wrapper(self: "Database", *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except psycopg2.Error as e:
            self.connection.rollback()
            logging.error("Database error: %s", e)
            raise
    return wrapper

@dataclass
class Database:
    connection_string: str
    connection: psycopg2.extensions.connection = field(init=False, repr=False)
    cursor: psycopg2.extensions.cursor = field(init=False, repr=False)

    def __post_init__(self):
        self.connection = psycopg2.connect(self.connection_string)
        self.cursor = self.connection.cursor()

    @db_error_handler
    def reset_safe(self, confirm: bool = False):
        if not confirm:
            ans = input("Are you sure you want to reset the database? This will delete all data. (yes/no): ")
            if ans.lower() != "yes":
                logging.info("Database reset cancelled.")
                return
        logging.info("Resetting database...")
        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        self.cursor.execute("DROP INDEX IF EXISTS idx_embedding")
        self.cursor.execute("DROP TABLE IF EXISTS markets")

        self.cursor.execute(
            """
                CREATE TABLE markets (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    probability FLOAT NOT NULL,
                    embedding VECTOR(1536) NULL
                )
            """
        )
        self.connection.commit()
        logging.info("Database reset complete.")

    @db_error_handler
    def add_market(self, idd: str, market: Market):
        self.cursor.execute(
            "INSERT INTO markets (id, title, probability, embedding) VALUES (%s, %s, %s, %s)",
            (idd, market.title, market.probability, market.embedding.tolist() if market.embedding is not None else None)
        )
        self.connection.commit()
        self.optimize_embeddings()

    @db_error_handler
    def get_market(self, idd: str) -> Market:
        self.cursor.execute(
            "SELECT title, probability, embedding FROM markets WHERE id = %s",
            (idd,)
        )
        result = self.cursor.fetchone()
        if result is None:
            raise KeyError(f"Market with id '{idd}' does not exist.")
        return Market(title=result[0], probability=result[1], embedding=np.array(result[2]) if result[2] is not None else None)

    @db_error_handler
    def get_num_markets(self) -> int:
        self.cursor.execute("SELECT COUNT(*) FROM markets")
        result = self.cursor.fetchone()
        return result[0] if result else 0

    @db_error_handler
    def optimize_embeddings(self):
        if self.get_num_markets() < 10000:
            return
        self.cursor.execute(
            """
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes WHERE tablename = 'markets' AND indexname = 'idx_embedding'
                )
            """
        )
        index_exists = self.cursor.fetchone()
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_embedding ON markets USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
        )
        self.connection.commit()
        assert index_exists is not None, "Failed to check if index exists."
        if not index_exists[0]:
            logging.info("Created index on embeddings for faster search.")

    @db_error_handler
    def update_market(self, idd: str, market: Market):
        self.cursor.execute(
            "UPDATE markets SET title = %s, probability = %s, embedding = %s WHERE id = %s",
            (market.title, market.probability, market.embedding.tolist() if market.embedding is not None else None, idd)
        )
        self.connection.commit()

    @db_error_handler
    def delete_market(self, idd: str):
        self.cursor.execute(
            "DELETE FROM markets WHERE id = %s",
            (idd,)
        )
        self.connection.commit()

    @db_error_handler
    def get_markets_by_embedding(self, embedding: np.ndarray, limit: int = 10) -> Dict[str, Market]:
        self.cursor.execute(
            "SELECT id, title, probability, embedding FROM markets ORDER BY embedding <#> %s LIMIT %s",
            (embedding.tolist(), limit)
        )
        results = self.cursor.fetchall()
        markets = {}
        for row in results:
            idd, title, probability, emb = row
            markets[idd] = Market(title=title, probability=probability, embedding=np.array(emb, dtype=np.float32) if emb is not None else None)
        return markets

    def close(self):
        self.cursor.close()
        self.connection.close()
        logging.info("Database connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        if exc_type is not None:
            logging.error("An error occurred: %s", exc_value)
        return False

    def __del__(self):
        self.close()
