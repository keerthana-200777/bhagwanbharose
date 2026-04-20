"""
this is the main backend. handled everything here—login, websockets for the game, 
elo calculations, and the leaderboard. merged it all so we only run one uvicorn command.
"""

import sys
import os
import uuid
import base64
import json

# helping python find our face recognition helper in the utils folder
# if we dont do this, it wont find the facial_recognition_module file
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

import mysql.connector
from pymongo import MongoClient
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
from facial_recognition_module import find_closest_match, build_encodings_cache

# database setup - using root and the password from the lab instructions
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "Isstasarecute"), 
        database=os.environ.get("MYSQL_DB", "arena")
    )

# mongodb stuff for the profile pics
mongo_client = MongoClient("mongodb://localhost:27017/")
photos_collection = mongo_client["arena"]["photos"]

# build face encodings cache once at startup so we dont do it for every login
# basically loads all images from mongo into memory so compare is fast
print("[startup] loading faces from mongo...")
all_images = {doc["uid"]: doc["image"] for doc in photos_collection.find()}
encodings_cache = build_encodings_cache(all_images)
print(f"[startup] cache ready: {len(encodings_cache)} faces loaded")

app = FastAPI()

# needed this because the browser was blocking our local fetches
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# mounting the frontend folder so we can load css/js
app.mount("/static", StaticFiles(directory="frontend"), name="static")

# just a simple dict to keep track of logged in sessions
sessions = {}

# mysql helpers - updating the online status so the lobby knows who is there
def set_online(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_online = TRUE WHERE uid = %s", (uid,))
    conn.commit()
    cursor.close(); conn.close()

def set_offline(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_online = FALSE WHERE uid = %s", (uid,))
    conn.commit()
    cursor.close(); conn.close()

def get_rating(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT elo_rating FROM users WHERE uid = %s", (uid,))
    result = cursor.fetchone()
    cursor.close(); conn.close()
    return result[0] if result else 1200

def get_user_by_uid(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE uid = %s", (uid,))
    user = cursor.fetchone()
    cursor.close(); conn.close()
    return user

# standard elo formula logic
def expected_score(r_player, r_opponent):
    # this math calculates win probability. closer the ratings, closer it is to 0.5
    return 1 / (1 + 10 ** ((r_opponent - r_player) / 400))

def update_ratings(p1, p2, r1, r2, s1, s2, draw=False, winner_uid=None):
    # k factor is 32. calculating new ratings based on game result.
    K = 32
    e1 = expected_score(r1, r2)
    e2 = expected_score(r2, r1)

    r1_new = round(r1 + K * (s1 - e1))
    r2_new = round(r2 + K * (s2 - e2))

    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    # updating ratings and win/loss/draw stats in one go
    cursor.execute("UPDATE users SET elo_rating = %s WHERE uid = %s", (r1_new, p1))
    cursor.execute("UPDATE users SET elo_rating = %s WHERE uid = %s", (r2_new, p2))
    
    if draw:
        cursor.execute("UPDATE users SET draws = draws + 1 WHERE uid IN (%s, %s)", (p1, p2))
    else:
        loser_uid = p2 if winner_uid == p1 else p1
        cursor.execute("UPDATE users SET wins = wins + 1 WHERE uid = %s", (winner_uid,))
        cursor.execute("UPDATE users SET losses = losses + 1 WHERE uid = %s", (loser_uid,))
        
    # log match into the matches table for match history
    cursor.execute("""
        INSERT INTO matches (player1_uid, player2_uid, winner_uid, draw)
        VALUES (%s, %s, %s, %s)
    """, (p1, p2, winner_uid, draw))
    
    conn.commit()
    cursor.close(); conn.close()
    return r1_new, r2_new

# facial login route
class LoginRequest(BaseModel):
    image: str

@app.post("/login")
async def login(request: LoginRequest, response: Response):
    # decode image from webcam and find who it is using the blackbox module
    try:
        login_image_data = base64.b64decode(request.image)
    except:
        return {"status": "fail", "message": "broken image"}

    matched_uid = find_closest_match(login_image_data, encodings_cache)
    if matched_uid is None:
        return {"status": "fail", "message": "no face found"}

    user = get_user_by_uid(matched_uid)
    if not user:
        return {"status": "fail", "message": "not in database"}

    set_online(matched_uid)
    
    # simple cookie based session
    sid = str(uuid.uuid4())
    sessions[sid] = {"uid": matched_uid, "name": user["name"]}
    response.set_cookie(key="session_id", value=sid, httponly=True)
    
    return {"status": "success", "uid": user["uid"], "name": user["name"], "elo_rating": user["elo_rating"]}

@app.get("/leaderboard")
def leaderboard():
    # just sort everyone by rating and send it
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT uid, name, elo_rating, wins, losses, draws FROM users ORDER BY elo_rating DESC")
    res = cursor.fetchall()
    cursor.close(); conn.close()
    return res

# paths to our html pages
frontend_dir = os.path.join(os.path.dirname(__file__), '..', 'frontend')

@app.get("/")
async def root(): return RedirectResponse(url="/login")

@app.get("/login")
async def login_page(): return FileResponse(os.path.join(frontend_dir, "login.html"))

@app.get("/app")
async def app_page(): return FileResponse(os.path.join(frontend_dir, "app.html"))

@app.get("/leaderboard-page")
async def leaderboard_page(): return FileResponse(os.path.join(frontend_dir, "leaderboard.html"))

# lobby and game logic state trackers
active_users = {}  # maps uid to their websocket connection
user_names = {}
rooms = {}         # room_id maps to [uid1, uid2]
game_states = {}   # stores the tic tac toe board for each room
user_rooms = {}    # tracks which player is in which room

async def broadcast_lobby():
    # tells everyone in the lobby who else is online
    online = [{"uid": u, "name": user_names[u], "elo_rating": get_rating(u)} for u in active_users]
    msg = json.dumps({"type": "lobby_update", "users": online})
    for ws in active_users.values():
        try: await ws.send_text(msg)
        except: pass

def check_winner(b):
    # checking all 8 ways to win tic tac toe
    wins = [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]]
    for a, b_idx, c in wins:
        if b[a] and b[a] == b[b_idx] == b[c]: return b[a]
    return None

async def end_game(rid, win_uid=None, draw=False):
    # wraps up the game and cleans the room out of memory
    if rid not in rooms: return
    p1, p2 = rooms[rid]
    r1, r2 = get_rating(p1), get_rating(p2)

    # scores for elo update
    s1, s2 = (0.5, 0.5) if draw else (1.0, 0.0) if win_uid == p1 else (0.0, 1.0)
    update_ratings(p1, p2, r1, r2, s1, s2, draw=draw, winner_uid=win_uid)

    msg = {"type": "game_over", "winner": win_uid, "draw": draw}
    for p in [p1, p2]:
        if p in active_users: await active_users[p].send_json(msg)

    # clear the room trackers
    rooms.pop(rid, None); game_states.pop(rid, None)
    user_rooms.pop(p1, None); user_rooms.pop(p2, None)
    await broadcast_lobby()

@app.websocket("/ws/{uid}")
async def websocket_endpoint(ws: WebSocket, uid: str):
    await ws.accept()
    user = get_user_by_uid(uid)
    if not user:
        await ws.close(); return

    active_users[uid] = ws
    user_names[uid] = user["name"]
    set_online(uid)
    await broadcast_lobby()

    try:
        while True:
            data = await ws.receive_json()

            # someone clicked challenge on the lobby
            if data["type"] == "challenge":
                to = data["to"]
                # make sure they arent already in a game
                if to in active_users and to not in user_rooms and uid not in user_rooms:
                    await active_users[to].send_json({"type": "challenge_request", "from": uid, "from_name": user_names[uid]})
                else:
                    await ws.send_json({"type": "error", "message": "cant challenge them"})

            # accepting or declining a challenge
            elif data["type"] == "challenge_response":
                challenger = data["to"]
                if data["accepted"]:
                    rid = str(uuid.uuid4())
                    rooms[rid] = [challenger, uid]
                    user_rooms[challenger] = rid; user_rooms[uid] = rid
                    game_states[rid] = {"board": [""]*9, "turn": challenger, "symbols": {challenger: "X", uid: "O"}}
                    
                    # start the match for both sides
                    for p in [challenger, uid]:
                        await active_users[p].send_json({
                            "type": "start_game", "room_id": rid, "symbol": game_states[rid]["symbols"][p],
                            "turn": challenger, "opponent_name": user_names[uid if p == challenger else challenger]
                        })
                else:
                    if challenger in active_users: await active_users[challenger].send_json({"type": "challenge_declined"})

            # a player clicked a square on the grid
            elif data["type"] == "move":
                rid, idx = data.get("room_id"), data.get("index")
                state = game_states.get(rid)
                # server side check: is it actually their turn and is the spot empty?
                if state and uid == state["turn"] and state["board"][idx] == "":
                    state["board"][idx] = state["symbols"][uid]
                    win = check_winner(state["board"])
                    if win: await end_game(rid, win_uid=uid)
                    elif "" not in state["board"]: await end_game(rid, draw=True)
                    else:
                        # flip turns and send update
                        state["turn"] = rooms[rid][1] if uid == rooms[rid][0] else rooms[rid][0]
                        for p in rooms[rid]: await active_users[p].send_json({"type": "game_update", "board": state["board"], "turn": state["turn"]})

    except WebSocketDisconnect:
        # clean up if someone closes the tab
        active_users.pop(uid, None); set_offline(uid)
        if uid in user_rooms:
            rid = user_rooms[uid]
            # if they quit mid game, other person wins by forfeit
            other = rooms[rid][0] if rooms[rid][1] == uid else rooms[rid][1]
            await end_game(rid, win_uid=other)
        await broadcast_lobby()