import pandas as pd
import requests
import mysql.connector
from pymongo import MongoClient
import base64
import os 
from concurrent.futures import ThreadPoolExecutor

# Connect to MySQL 
mysql_conn = mysql.connector.connect(
    host="localhost",
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "password"),
    database=os.getenv("MYSQL_DB", "arena")
)
mysql_cursor = mysql_conn.cursor()

# REQUIRED CHANGE: Create users table schema with match records
db_name = os.getenv("MYSQL_DB", "arena")
mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
mysql_cursor.execute(f"USE {db_name}")
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        uid VARCHAR(20) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        elo_rating INT DEFAULT 1200,
        is_online BOOLEAN DEFAULT FALSE,
        wins INT DEFAULT 0,
        losses INT DEFAULT 0,
        draws INT DEFAULT 0
    )
""")
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS matches (
        id INT AUTO_INCREMENT PRIMARY KEY,
        player1_uid VARCHAR(20),
        player2_uid VARCHAR(20),
        winner_uid VARCHAR(20),
        draw BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
mysql_conn.commit()

# Connect to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["arena"]
images_collection = mongo_db["photos"]


# LOAD INPUT DATA
df = pd.read_csv("batch_data.csv")
df.columns = df.columns.str.strip()


def fetch_image(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status() 
        return response.content      
    except Exception as e:
        print(f"[ERROR] Fetch failed: {url}")
        return None

def insert_mysql(uid, name):
    try:
        mysql_cursor.execute(
            "INSERT INTO users (uid, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name)",
            (uid, name)
        )
        mysql_conn.commit()
    except Exception as e:
        print(f"[ERROR] MySQL failed for {uid}: {e}")

def insert_mongo(uid, image_data):
    try:
        encoded = base64.b64encode(image_data).decode("utf-8")
        images_collection.update_one({"uid": uid}, {"$set": {"image": encoded}}, upsert=True)
    except Exception as e:
        print(f"[ERROR] MongoDB failed for {uid}: {e}")


# MAIN PIPELINE
for _, row in df.iterrows():
    uid      = str(row["uid"]).strip()
    name     = str(row["name"]).strip()
    base_url = str(row["website_url"]).strip()

    print(f"[INFO] Processing {uid}")

    image_url = "http://" + base_url.rstrip("/") + "/images/pfp.jpg"
    image_data = fetch_image(image_url)

    # REQUIRED CHANGE: Simultaneously execute INSERT into MySQL and UPSERT into MongoDB
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(insert_mysql, uid, name)
        if image_data is not None:
            executor.submit(insert_mongo, uid, image_data)
        else:
            print(f"[WARNING] Image not found for {uid}, skipping MongoDB insert")

print("\n Pipeline completed.")
