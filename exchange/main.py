from fastapi import FastAPI
import uvicorn
from requests import get
import os
import json
import asyncio
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
API_URL = os.getenv("API_URL", "https://api.exchangerate-api.com/v4/latest/")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

app = FastAPI()

redis_client: redis.Redis | None = None

@app.on_event("startup")
async def startup_event():
    global redis_client
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    print("Redis client initialized")

@app.on_event("shutdown")
async def shutdown_event():
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None
    print("Redis client closed")

async def get_cached_rate(base_currency: str):
    global redis_client
    if redis_client is None:
        print(f"Redis not available, fetching rates for {base_currency} from API")
        resp = get(f"{API_URL}{base_currency}")
        return resp.json()

    key = f"rates:{base_currency}"
    cached = await redis_client.get(key)
    if cached:
        print(f"Retrieved cached rates for {base_currency}")
        try:
            return json.loads(cached)
        except Exception:
            await redis_client.delete(key)

    print(f"Fetching rates for {base_currency} from API")
    resp = get(f"{API_URL}{base_currency}")
    data = resp.json()
    try:
        await redis_client.set(key, json.dumps(data), ex=CACHE_TTL)
        print(f"Cached rates for {base_currency}")
    except Exception as e:
        print(f"Failed to cache rates for {base_currency}: {e}")
    return data

@app.get("/cotacao/dolar")
async def get_cotacao_dolar():
    data = await get_cached_rate("USD")
    brl = data.get("rates", {}).get("BRL")
    if brl is None:
        return {"error": "Rate not available"}
    return {"cotacao": f"R${brl:.2f}"}

@app.get("/cotacao/convert/{amount}/brl/usd")
async def convert_brl_to_usd(amount: float):
    data = await get_cached_rate("BRL")
    usd_rate = data.get("rates", {}).get("USD")
    if usd_rate is None:
        return {"error": "Rate not available"}
    converted_amount = amount * usd_rate
    return {"converted_amount": f"${converted_amount:.2f}"}

@app.get("/cotacao/convert/{amount}/usd/brl")
async def convert_usd_to_brl(amount: float):
    data = await get_cached_rate("USD")
    brl_rate = data.get("rates", {}).get("BRL")
    if brl_rate is None:
        return {"error": "Rate not available"}
    converted_amount = amount * brl_rate
    return {"converted_amount": f"R${converted_amount:.2f}"}

@app.get("/cache")
async def get_cache():
    print(f"Redis client: {redis_client}")
    if redis_client is None:
        return {"error": "Redis not connected"}
    
    keys = await redis_client.keys("rates:*")
    print(f"Keys found: {keys}")
    if not keys:
        return {"cached_rates": {}}
    
    values = await asyncio.gather(*[redis_client.get(key) for key in keys])
    cached = {key: value for key, value in zip(keys, values)}
    return {"cached_rates": cached}

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)