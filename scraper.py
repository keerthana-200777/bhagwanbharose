import pandas as pd
import requests
import mysql.connector
from pymongo import MongoClient
import base64
import os 
from concurrent.futures import ThreadPoolExecutor

# setting up mysql connection
# using the env variables but kept the password as a fallback just in case
mysql_conn = mysql.connector.connect(
    host="localhost",
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "Isstasarecute"),
    database=os.getenv("MYSQL_DB", "arena")
)
mysql_cursor = mysql_conn.cursor()

# making sure the db and tables actually exist before we start [cite: 35, 40]
db_name = os.getenv("MYSQL_DB", "arena")
mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
mysql_cursor.execute(f"USE {db_name}")

# users table schema as per the rubric [cite: 41]
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

# match history table to satisfy the phase 4 requirements later [cite: 96]
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

# mongo connection for the images [cite: 42, 43]
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["arena"]
images_collection = mongo_db["photos"]


# loading the csv with all the student info [cite: 44]
df = pd.read_csv("batch_data.csv")
df.columns = df.columns.str.strip()


def fetch_image(url):
    # try to get the pfp, if it 404s or times out just skip it [cite: 51, 52]
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status() 
        return response.content      
    except Exception as e:
        print(f"[ERROR] failed to fetch: {url}")
        return None

def insert_mysql(uid, name):
    # just inserting the basic profile info [cite: 48]
    try:
        mysql_cursor.execute(
            "INSERT INTO users (uid, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name)",
            (uid, name)
        )
        mysql_conn.commit()
    except Exception as e:
        print(f"[ERROR] mysql failed for {uid}: {e}")

def insert_mongo(uid, image_data):
    # converting image to base64 and upserting into mongo [cite: 49, 50]
    try:
        encoded = base64.b64encode(image_data).decode("utf-8")
        images_collection.update_one({"uid": uid}, {"$set": {"image": encoded}}, upsert=True)
    except Exception as e:
        print(f"[ERROR] mongodb failed for {uid}: {e}")


# main loop to go through the batch [cite: 44]
for _, row in df.iterrows():
    uid      = str(row["uid"]).strip()
    name     = str(row["name"]).strip()
    base_url = str(row["website_url"]).strip()

    print(f"[INFO] doing {uid}")

    # images are always at this path [cite: 45]
    image_url = "http://" + base_url.rstrip("/") + "/images/pfp.jpg"
    image_data = fetch_image(image_url)

    # using threads so sql and mongo updates happen at the same time [cite: 46]
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(insert_mysql, uid, name)
        if image_data is not None:
            executor.submit(insert_mongo, uid, image_data)
        else:
            print(f"[WARNING] image missing for {uid}, skipping mongo")

print("\n done with the scraping.")