"""
merged this file into main.py so we don't have to run two separate servers.
everything (login, websocket, leaderboard) is now in one place.

just run the unified server:
uvicorn backend.main:app --reload --port 8000
"""