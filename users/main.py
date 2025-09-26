from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import uvicorn
import os
import json
import asyncio
import redis.asyncio as redis
import sqlite3
from pathlib import Path
from typing import Optional, List

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
DB_PATH = Path(os.getenv("DB_PATH", "users.db"))
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

redis_client: redis.Redis | None = None


app = FastAPI()

@app.on_event("startup")
async def startup_event():
    global redis_client
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    await asyncio.to_thread(repo.ensure_table)
    print("Redis client initialized and DB table ensured")

@app.on_event("shutdown")
async def shutdown_event():
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None
    print("Redis client closed")


class User(BaseModel):
    id: str
    name: str


class UpdateUser(BaseModel):
    name: str


class UserRepository:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_table(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)")
            conn.commit()
        finally:
            conn.close()

    def get_user(self, user_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                return {"id": row["id"], "name": row["name"]}
            return None
        finally:
            conn.close()

    def insert_default_user(self, user_id: str, default_name: str = "John Doe") -> dict:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO users (id, name) VALUES (?, ?)", (user_id, default_name)
            )
            conn.commit()
            return {"id": user_id, "name": default_name}
        finally:
            conn.close()

    def create_user(self, user_id: str, name: str) -> dict:
        conn = self._connect()
        try:
            cur = conn.cursor()
            # Try a plain INSERT so IntegrityError is raised if exists
            cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (user_id, name))
            conn.commit()
            return {"id": user_id, "name": name}
        finally:
            conn.close()

    def update_user(self, user_id: str, name: str) -> Optional[dict]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE users SET name = ? WHERE id = ?", (name, user_id))
            conn.commit()
            if cur.rowcount == 0:
                return None
            return {"id": user_id, "name": name}
        finally:
            conn.close()

    def delete_user(self, user_id: str) -> bool:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def list_users(self) -> List[dict]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM users")
            rows = cur.fetchall()
            return [{"id": r["id"], "name": r["name"]} for r in rows]
        finally:
            conn.close()


repo = UserRepository(DB_PATH)


async def get_cached_user(user_id: str) -> Optional[User]:
    if not redis_client:
        print(f"Redis not available, querying user {user_id} from DB")
        return None
    
    key = f"user:{user_id}"
    cached = await redis_client.get(key)
    if cached:
        print(f"Retrieved cached user {user_id}")
        try:
            data = json.loads(cached)
            return User(**data)
        except Exception:
            await redis_client.delete(key)
    return None


async def cache_user(user: User) -> None:
    if not redis_client:
        return
    
    try:
        await redis_client.set(f"user:{user.id}", user.json(), ex=CACHE_TTL)
        print(f"Cached user {user.id}")
    except Exception as e:
        print(f"Failed to cache user {user.id}: {e}")


async def delete_cached_user(user_id: str) -> None:
    if not redis_client:
        return
    
    try:
        await redis_client.delete(f"user:{user_id}")
        print(f"Deleted cached user {user_id}")
    except Exception as e:
        print(f"Failed to delete cached user {user_id}: {e}")


@app.get("/user/{user_id}", response_model=User)
async def get_user(user_id: str):
    cached = await get_cached_user(user_id)
    if cached:
        return cached

    db_user = await asyncio.to_thread(repo.get_user, user_id)

    if not db_user:
        print(f"User {user_id} not found, creating default user")
        db_user = await asyncio.to_thread(repo.insert_default_user, user_id, "John Doe")

    user = User(**db_user)
    await cache_user(user)
    return user


@app.get("/users", response_model=List[User])
async def list_users():
    users = await asyncio.to_thread(repo.list_users)
    return [User(**u) for u in users]


@app.post("/user", response_model=User, status_code=status.HTTP_201_CREATED)
async def create_user(user: User):
    try:
        created = await asyncio.to_thread(repo.create_user, user.id, user.name)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")
    
    created_user = User(**created)
    await cache_user(created_user)
    return created_user


@app.put("/user/{user_id}", response_model=User)
async def update_user(user_id: str, update: UpdateUser):
    updated = await asyncio.to_thread(repo.update_user, user_id, update.name)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    user = User(**updated)
    await cache_user(user)
    return user


@app.delete("/user/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: str):
    deleted = await asyncio.to_thread(repo.delete_user, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    await delete_cached_user(user_id)
    return None

@app.get("/cache")
async def get_cache():
    print(f"Redis client: {redis_client}")
    if redis_client is None:
        return {"error": "Redis not connected"}
    
    keys = await redis_client.keys("user:*")
    print(f"Keys found: {keys}")
    if not keys:
        return {"cached_users": {}}
    
    values = await asyncio.gather(*[redis_client.get(key) for key in keys])
    cached = {key: value for key, value in zip(keys, values)}
    return {"cached_users": cached}


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)