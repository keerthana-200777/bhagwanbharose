"""
Unified backend server for the Arena project.
Handles: facial login, WebSocket lobby/game, Elo ratings, leaderboard.
Run with: uvicorn backend.main:app --reload
"""

import sys
import os
import uuid
import base64

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

import mysql.connector
from pymongo import MongoClient
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
from facial_recognition_module import find_closest_match, build_encodings_cache
import json

# ─────────────────────────────────────────────
# Database Connections
# ─────────────────────────────────────────────

def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "password"),
        database=os.environ.get("MYSQL_DB", "arena")
    )

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
photos_collection = mongo_client["arena"]["photos"]

# Build face encodings cache once at startup
print("[STARTUP] Building face encodings cache from MongoDB...")
_all_images = {doc["uid"]: doc["image"] for doc in photos_collection.find()}
encodings_cache = build_encodings_cache(_all_images)
print(f"[STARTUP] Cache ready with {len(encodings_cache)} face encodings.")

# ─────────────────────────────────────────────
# FastAPI App Setup
# ─────────────────────────────────────────────

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (like CSS)
app.mount("/static", StaticFiles(directory="frontend"), name="static")

# Server-side session store: {session_id: {"uid": str, "name": str}}
sessions = {}

# ─────────────────────────────────────────────
# MySQL Helper Functions
# ─────────────────────────────────────────────

def set_online(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_online = TRUE WHERE uid = %s", (uid,))
    conn.commit()
    cursor.close()
    conn.close()

def set_offline(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_online = FALSE WHERE uid = %s", (uid,))
    conn.commit()
    cursor.close()
    conn.close()

def get_rating(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT elo_rating FROM users WHERE uid = %s", (uid,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else 1200

def get_user_by_uid(uid):
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE uid = %s", (uid,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user

def get_online_users():
    """Get all currently online users with their details."""
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT uid, name, elo_rating FROM users WHERE is_online = TRUE")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users

# ─────────────────────────────────────────────
# Elo Rating System
# ─────────────────────────────────────────────

def expected_score(r_player, r_opponent):
    """Compute expected win probability using the Elo formula."""
    return 1 / (1 + 10 ** ((r_opponent - r_player) / 400))

def update_ratings(p1, p2, r1, r2, s1, s2, draw=False, winner_uid=None):
    """
    Update Elo ratings for both players after a match.
    K=32, uses pre-match ratings for both calculations (no sequential dependency).
    Also updates match records (wins, losses, draws).
    """
    K = 32
    e1 = expected_score(r1, r2)
    e2 = expected_score(r2, r1)

    r1_new = round(r1 + K * (s1 - e1))
    r2_new = round(r2 + K * (s2 - e2))

    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    # Update ratings
    cursor.execute("UPDATE users SET elo_rating = %s WHERE uid = %s", (r1_new, p1))
    cursor.execute("UPDATE users SET elo_rating = %s WHERE uid = %s", (r2_new, p2))
    
    # Update match record
    if draw:
        cursor.execute("UPDATE users SET draws = draws + 1 WHERE uid IN (%s, %s)", (p1, p2))
    else:
        loser_uid = p2 if winner_uid == p1 else p1
        cursor.execute("UPDATE users SET wins = wins + 1 WHERE uid = %s", (winner_uid,))
        cursor.execute("UPDATE users SET losses = losses + 1 WHERE uid = %s", (loser_uid,))
        
    # Record the match
    cursor.execute("""
        INSERT INTO matches (player1_uid, player2_uid, winner_uid, draw)
        VALUES (%s, %s, %s, %s)
    """, (p1, p2, winner_uid, draw))
    
    conn.commit()
    cursor.close()
    conn.close()
    return r1_new, r2_new

# ─────────────────────────────────────────────
# Leaderboard
# ─────────────────────────────────────────────

def get_leaderboard():
    """Get ALL players sorted by elo_rating descending."""
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT uid, name, elo_rating, wins, losses, draws FROM users ORDER BY elo_rating DESC")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

# ─────────────────────────────────────────────
# Session Helpers
# ─────────────────────────────────────────────

def create_session(uid, name):
    """Create a server-side session and return the session ID."""
    session_id = str(uuid.uuid4())
    sessions[session_id] = {"uid": uid, "name": name}
    return session_id

def get_session(session_id):
    """Retrieve session data by session ID."""
    return sessions.get(session_id)

def delete_session(session_id):
    """Delete a session."""
    sessions.pop(session_id, None)

# ─────────────────────────────────────────────
# REST API Routes
# ─────────────────────────────────────────────

class LoginRequest(BaseModel):
    image: str  # base64 string

@app.post("/login")
async def login(request: LoginRequest, response: Response):
    """Facial recognition login endpoint."""
    try:
        login_image_data = base64.b64decode(request.image)
    except Exception:
        return {"status": "fail", "message": "Invalid image data"}

    # Use the cached encodings for matching
    matched_uid = find_closest_match(login_image_data, encodings_cache)
    if matched_uid is None:
        return {"status": "fail", "message": "Face not recognized"}

    # Cross-reference with MySQL to confirm user exists
    user = get_user_by_uid(matched_uid)
    if not user:
        return {"status": "fail", "message": "User not found in database"}

    # Set is_online = TRUE in MySQL
    set_online(matched_uid)

    # Create a secure server-side session
    session_id = create_session(matched_uid, user["name"])

    # Set session cookie
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        samesite="lax",
        max_age=86400  # 24 hours
    )

    return {
        "status": "success",
        "uid": user["uid"],
        "name": user["name"],
        "elo_rating": user["elo_rating"],
        "session_id": session_id
    }

@app.post("/logout")
async def logout(request: Request, response: Response):
    """Logout endpoint — clears session and sets user offline."""
    session_id = request.cookies.get("session_id")
    if session_id:
        session = get_session(session_id)
        if session:
            set_offline(session["uid"])
        delete_session(session_id)
    response.delete_cookie("session_id")
    return {"status": "success"}

@app.get("/me")
async def get_current_user(request: Request):
    """Get current logged-in user from session."""
    session_id = request.cookies.get("session_id")
    if not session_id:
        return {"status": "fail", "message": "Not logged in"}
    session = get_session(session_id)
    if not session:
        return {"status": "fail", "message": "Session expired"}
    user = get_user_by_uid(session["uid"])
    if not user:
        return {"status": "fail", "message": "User not found"}
    return {"status": "success", "uid": user["uid"], "name": user["name"], "elo_rating": user["elo_rating"]}

@app.get("/leaderboard")
def leaderboard():
    """Global leaderboard — all players sorted by Elo rating descending."""
    return get_leaderboard()

@app.get("/users")
def get_users():
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT uid, name, elo_rating, is_online FROM users")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users

# ─────────────────────────────────────────────
# Serve Frontend Pages
# ─────────────────────────────────────────────

frontend_dir = os.path.join(os.path.dirname(__file__), '..', 'frontend')

@app.get("/")
async def root():
    return RedirectResponse(url="/login")

@app.get("/login")
async def login_page():
    return FileResponse(os.path.join(frontend_dir, "login.html"))

@app.get("/app")
async def app_page():
    return FileResponse(os.path.join(frontend_dir, "app.html"))

@app.get("/leaderboard-page")
async def leaderboard_page():
    return FileResponse(os.path.join(frontend_dir, "leaderboard.html"))

# ─────────────────────────────────────────────
# WebSocket: Lobby + Game
# ─────────────────────────────────────────────

# Global State
active_users = {}    # {uid: websocket}
user_names = {}      # {uid: name}  — for lobby display
rooms = {}           # {room_id: [uid1, uid2]}
game_states = {}     # {room_id: {"board": [], "turn": uid, "symbols": {uid: "X"/"O"}}}
user_rooms = {}      # {uid: room_id} — quick lookup for disconnect handling

async def broadcast_lobby():
    """Send updated online user list to all connected users."""
    online_users = []
    for user_id in active_users:
        name = user_names.get(user_id, user_id)
        rating = get_rating(user_id)
        online_users.append({"uid": user_id, "name": name, "elo_rating": rating})

    message = json.dumps({"type": "lobby_update", "users": online_users})
    disconnected = []
    for user_id, user_ws in active_users.items():
        try:
            await user_ws.send_text(message)
        except Exception:
            disconnected.append(user_id)
    # Clean up any broken connections
    for user_id in disconnected:
        active_users.pop(user_id, None)

def check_winner(board):
    """Check if there's a winner on the board. Returns 'X', 'O', or None."""
    wins = [
        [0, 1, 2], [3, 4, 5], [6, 7, 8],  # rows
        [0, 3, 6], [1, 4, 7], [2, 5, 8],  # cols
        [0, 4, 8], [2, 4, 6]               # diagonals
    ]
    for a, b, c in wins:
        if board[a] and board[a] == board[b] == board[c]:
            return board[a]
    return None

async def end_game(room_id, winner_uid=None, draw=False):
    """Resolve a completed game: update Elo, notify players, clean up."""
    if room_id not in rooms:
        return

    players = rooms[room_id]
    p1, p2 = players

    # Get pre-match ratings
    r1, r2 = get_rating(p1), get_rating(p2)

    # Determine scores
    if draw:
        s1, s2 = 0.5, 0.5
    elif winner_uid == p1:
        s1, s2 = 1.0, 0.0
    else:
        s1, s2 = 0.0, 1.0

    # Update ratings using pre-match values
    new_r1, new_r2 = update_ratings(p1, p2, r1, r2, s1, s2, draw=draw, winner_uid=winner_uid)

    # Notify both players
    result_message = {
        "type": "game_over",
        "winner": winner_uid,
        "draw": draw,
        "ratings": {p1: new_r1, p2: new_r2}
    }
    for player in players:
        if player in active_users:
            try:
                await active_users[player].send_json(result_message)
            except Exception:
                pass

    # Clean up room state
    rooms.pop(room_id, None)
    game_states.pop(room_id, None)
    for p in players:
        user_rooms.pop(p, None)

    # Broadcast updated lobby (ratings changed)
    await broadcast_lobby()


@app.websocket("/ws/{uid}")
async def websocket_endpoint(ws: WebSocket, uid: str):
    await ws.accept()

    # Look up user name from MySQL
    user = get_user_by_uid(uid)
    if not user:
        await ws.send_json({"type": "error", "message": "User not found"})
        await ws.close()
        return

    active_users[uid] = ws
    user_names[uid] = user["name"]
    set_online(uid)
    await broadcast_lobby()

    try:
        while True:
            data = await ws.receive_json()

            # ─── CHALLENGE ───
            if data["type"] == "challenge":
                target = data["to"]
                if target in active_users:
                    # Check neither player is already in a game
                    if uid in user_rooms:
                        await ws.send_json({"type": "error", "message": "You are already in a game"})
                        continue
                    if target in user_rooms:
                        await ws.send_json({"type": "error", "message": "That player is already in a game"})
                        continue
                    await active_users[target].send_json({
                        "type": "challenge_request",
                        "from": uid,
                        "from_name": user_names.get(uid, uid)
                    })
                else:
                    await ws.send_json({"type": "error", "message": "Player is offline"})

            # ─── CHALLENGE RESPONSE ───
            elif data["type"] == "challenge_response":
                challenger = data["to"]  # the person who sent the original challenge
                accepted = data["accepted"]

                if accepted:
                    # Create a dedicated room for the two players
                    room_id = str(uuid.uuid4())
                    rooms[room_id] = [challenger, uid]  # challenger is X (goes first)
                    game_states[room_id] = {
                        "board": [""] * 9,
                        "turn": challenger,
                        "symbols": {challenger: "X", uid: "O"}
                    }
                    user_rooms[challenger] = room_id
                    user_rooms[uid] = room_id

                    # Notify both players with their symbol assignments
                    for p in [challenger, uid]:
                        if p in active_users:
                            await active_users[p].send_json({
                                "type": "start_game",
                                "room_id": room_id,
                                "symbol": game_states[room_id]["symbols"][p],
                                "turn": challenger,
                                "opponent": uid if p == challenger else challenger,
                                "opponent_name": user_names.get(uid if p == challenger else challenger, ""),
                                "board": [""] * 9
                            })
                else:
                    if challenger in active_users:
                        await active_users[challenger].send_json({
                            "type": "challenge_declined",
                            "from": uid,
                            "from_name": user_names.get(uid, uid)
                        })

            # ─── MOVE ───
            elif data["type"] == "move":
                room_id = data.get("room_id")
                index = data.get("index")

                if room_id not in game_states:
                    await ws.send_json({"type": "error", "message": "Game not found"})
                    continue

                state = game_states[room_id]
                players = rooms[room_id]

                # Server-side validation: is it this player's turn?
                if uid != state["turn"]:
                    await ws.send_json({"type": "error", "message": "Not your turn"})
                    continue

                # Server-side validation: is the target cell empty?
                if not (0 <= index <= 8) or state["board"][index] != "":
                    await ws.send_json({"type": "error", "message": "Invalid move"})
                    continue

                # Apply the move
                symbol = state["symbols"][uid]
                state["board"][index] = symbol

                # Check for winner
                winner_symbol = check_winner(state["board"])
                if winner_symbol:
                    winner_uid = players[0] if winner_symbol == "X" else players[1]
                    await end_game(room_id, winner_uid=winner_uid)
                elif "" not in state["board"]:
                    # Board full — draw
                    await end_game(room_id, draw=True)
                else:
                    # Switch turns
                    state["turn"] = players[1] if uid == players[0] else players[0]

                    # Broadcast updated board to both players in the room
                    update_msg = {
                        "type": "game_update",
                        "board": state["board"],
                        "turn": state["turn"]
                    }
                    for p in players:
                        if p in active_users:
                            try:
                                await active_users[p].send_json(update_msg)
                            except Exception:
                                pass

    except WebSocketDisconnect:
        # Remove from active users
        active_users.pop(uid, None)
        user_names.pop(uid, None)
        set_offline(uid)

        # Handle disconnect during active game → forfeit
        if uid in user_rooms:
            room_id = user_rooms[uid]
            if room_id in rooms:
                players = rooms[room_id]
                other = players[0] if players[1] == uid else players[1]
                await end_game(room_id, winner_uid=other)

        await broadcast_lobby()

    except Exception as e:
        print(f"[WS ERROR] {uid}: {e}")
        active_users.pop(uid, None)
        user_names.pop(uid, None)
        set_offline(uid)

        if uid in user_rooms:
            room_id = user_rooms[uid]
            if room_id in rooms:
                players = rooms[room_id]
                other = players[0] if players[1] == uid else players[1]
                await end_game(room_id, winner_uid=other)

        await broadcast_lobby()
