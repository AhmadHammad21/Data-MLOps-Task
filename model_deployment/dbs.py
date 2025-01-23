import sqlite3
import redis
import os
import json
from logger import logger
from dotenv import load_dotenv
load_dotenv()

# Database initialization
DATABASE = "logging/logging.db"

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = os.environ['REDIS_PORT']
REDIS_USERNAME = os.environ['REDIS_USERNAME']
REDIS_PW = os.environ['REDIS_PW']

def init_db():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            recommended_products TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def insert_sqlite(product_id: str, recommended_product_ids: list) -> None:
    # Save log to database
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute("INSERT INTO logs (product_id, recommended_products) VALUES (?, ?)", 
              (product_id, str(recommended_product_ids)))
    conn.commit()
    # conn.close()

conn = init_db()

def init_redis_db():
    # Initialize Redis connection
    cache = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PW,
    )

    return cache

cache = init_redis_db()


def check_product_id_cache(product_id: str, recommended_product_ids: list) -> None:
     # Check cache
    cached_recommendations = cache.get(product_id)
    if cached_recommendations:
        recommendations = json.loads(cached_recommendations)
        logger.info(f"Product Id: {product_id} recommendation: {recommendations} already found in Redis")
    else:
        # Use static recommendations if not in cache
        recommendations = recommended_product_ids
        cache.set(product_id, json.dumps(recommendations), ex=3600)  # Cache for 1 hour
        logger.info(f"Set a Product Id: {product_id} with recommendation: {recommendations} in Redis")

    