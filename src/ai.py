from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()

client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
llm_parse = client.beta.chat.completions.parse
llm_create = client.chat.completions.create
def get(filename: str) -> str:
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read()

def systemp(value: str):
    return {"role": "system", "content": value}
def userp(value: str):
    return {"role": "user", "content": value}
def assistantp(value: str):
    return {"role": "assistant", "content": value}

embeddings_create = client.embeddings.create
