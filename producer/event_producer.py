import json
import time
import random
import uuid
from datetime import datetime

import pandas as pd
import sqlite3
from kafka import KafkaProducer

KAFKA_TOPIC = "soccer_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

DB_PATH = "data/raw/database.sqlite"

print("Connecting to SQLite database...")
conn = sqlite3.connect(DB_PATH)

print("Loading match data...")

matches_df = pd.read_sql_query("""
SELECT
    id AS match_id,
    season,
    league_id
FROM Match
WHERE season IS NOT NULL
LIMIT 2000
""", conn)


print("Loading team data...")

teams_df = pd.read_sql_query("""
SELECT
    id AS team_id,
    team_long_name AS team_name
FROM Team
WHERE team_long_name IS NOT NULL
""", conn)


print("Loading player data...")

players_df = pd.read_sql_query("""
SELECT
    id AS player_id,
    player_name
FROM Player
WHERE player_name IS NOT NULL
LIMIT 1000
""", conn)


print("Loading league mapping...")

leagues_df = pd.read_sql_query("""
SELECT
    id,
    name
FROM League
""", conn)


league_map = dict(zip(leagues_df.id, leagues_df.name))


print("Data Loaded Successfully:")
print(f"Matches: {len(matches_df)}")
print(f"Teams: {len(teams_df)}")
print(f"Players: {len(players_df)}")
print(f"Leagues: {len(league_map)}")

EVENT_TYPES = [
    "GOAL",
    "SHOT",
    "FOUL",
    "YELLOW_CARD",
    "RED_CARD",
    "SUBSTITUTION"
]


def generate_event():
    """
    Generate a realistic soccer match event
    """

    match = matches_df.sample(1).iloc[0]

    league_name = league_map.get(match["league_id"], "Unknown League")

    event = {
        "event_id": str(uuid.uuid4()),
        "match_id": int(match["match_id"]),
        "league": league_name,
        "season": match["season"],
        "minute": random.randint(1, 90),
        "event_type": random.choice(EVENT_TYPES),
        "player": random.choice(players_df["player_name"].tolist()),
        "team": random.choice(teams_df["team_name"].tolist()),
        "xg": round(random.uniform(0.05, 0.85), 2),
        "is_home": random.choice([True, False]),
        "event_time": datetime.utcnow().isoformat(),
        "ingestion_time": datetime.utcnow().isoformat()
    }

    return event

if __name__ == "__main__":

    print("=====================================")
    print("Starting Soccer Real-Time Event Stream")
    print("Kafka Topic:", KAFKA_TOPIC)
    print("=====================================")

    try:
        while True:

            event = generate_event()

            producer.send(KAFKA_TOPIC, event)
            producer.flush()

            print("Event Sent:", event)

            # Control streaming speed (2 seconds per event)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.close()
        conn.close()
        print("Connections closed successfully.")