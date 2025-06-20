from dataclasses import dataclass, field
from typing import List
import requests
import numpy as np
import tiktoken
from src.ai import embeddings_create

@dataclass
class Market:
    title: str
    probability: float
    embedding: np.ndarray | None = field(default=None)

def get_markets_info():
    offset = 0
    while True:
        url = f"https://api.manifold.markets/v0/search-markets?term=&filter=all&contractType=BINARY&limit=1000&offset={offset}"
        response = requests.get(url, timeout=5)
        data = response.json()
        for market in data:
            yield Market(
                title=market["question"],
                probability=market["probability"]
            )
        if not data:
            break
        offset += len(data)

def get_embeddings_single(markets: List[Market], model="text-embedding-3-small") -> List[np.ndarray]:
    res = embeddings_create(
        model=model,
        input=[market.title for market in markets]
    ).data
    res = [np.array(item.embedding) for item in res]
    return res

def get_embeddings(markets: List[Market], model="text-embedding-3-small"):
    culmulative_tokens = 0
    THRESHOLD = 30000
    shtuff = tiktoken.get_encoding("cl100k_base")
    ii = 0
    res = []
    for i, market in enumerate(markets):
        if i % 100 == 0:
            print(f"Processing market {i}/{len(markets)}: {market.title}")
        these_tokens = len(shtuff.encode(market.title))
        if culmulative_tokens + these_tokens > THRESHOLD:
            print(f"Reached token limit at market {i}, processing batch from {ii} to {i}")
            res.extend(get_embeddings_single(markets[ii:i], model=model))
            culmulative_tokens = 0
            ii = i
        culmulative_tokens += these_tokens
    if ii < len(markets):
        res.extend(get_embeddings_single(markets[ii:], model=model))
    assert len(res) == len(markets), "Number of embeddings does not match number of markets"
    return res
