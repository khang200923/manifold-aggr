from dataclasses import dataclass, field
from typing import List
import requests
import numpy as np
from src.ai import embeddings_create

@dataclass
class Market:
    title: str
    probability: float
    embedding: np.ndarray | None = field(default=None)

def get_markets_info():
    offset = 0
    while True:
        url = f"https://api.manifold.markets/v0/search-markets?term=&filter=open&contractType=BINARY&limit=1000&offset={offset}"
        print("invoke")
        response = requests.get(url, timeout=5)
        data = response.json()
        for market in data:
            yield Market(
                title=market["title"],
                probability=market["probability"]
            )
        if not data:
            break
        offset += len(data)

def inject_embeddings(markets: List[Market], model="text-embedding-3-small"):
    res = embeddings_create(
        model=model,
        input=[market.title for market in markets]
    )
    for market, embedding in zip(markets, res.data):
        market.embedding = np.array(embedding.embedding)
