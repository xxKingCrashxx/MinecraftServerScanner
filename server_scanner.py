
import time
import os
import signal
import sys
import math
from dotenv import load_dotenv
from datetime import datetime, timezone
from pymongo import MongoClient
from zoneinfo import ZoneInfo
from mcstatus import JavaServer
from bson.codec_options import CodecOptions

class Player:
    def __init__(self, name, uuid, join_time=None, left_time=None):
        self.name = name
        self.id = uuid
        self.join_time = join_time
        self.last_seen = join_time
        self.confidence_score = 1.0
    
    def __str__(self):
        return f"{self.name}:{self.id}:join_time {self.join_time}:left_time {self.left_time}:"
    
    def __eq__(self, other):
        if not isinstance(other, Player):
            return False
        return self.id == other.id
    
    def __hash__(self):
        return hash(self.id)

EVENT_TYPE = {
    "PLAYER_JOIN": 0,
    "PLAYER_LEAVE": 1,
    "NEW_PLAYER": 2
}

EVENT_TYPE_REV_MAP = {
    EVENT_TYPE["PLAYER_JOIN"]: "PLAYER_JOIN",
    EVENT_TYPE["PLAYER_LEAVE"]: "PLAYER_LEAVE",
    EVENT_TYPE["NEW_PLAYER"]: "NEW_PLAYER"
}

load_dotenv()

MONGO_STRING = os.getenv("MONGO_STRING")
MC_SERVER_IP = os.getenv("MC_SERVER_IP")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")

BASE_SLEEP_TIME = 30
MIN_SLEEP_TIME = 5
MAX_SLEEP_TIME = 60
BASE_ABSENCE_THRESHOLD = 225
MIN_ABSENCE_THRESHOLD = 90
MAX_ABSENCE_THRESHOLD = 700

client = MongoClient(MONGO_STRING)
db = client[DB_NAME]


#global collections.
player_sessions = None
player_events = None
players = None
server_status = None

if "player_sessions" not in db.list_collection_names(): 
    player_sessions = db.create_collection(
        "player_sessions",
        codec_options=CodecOptions(
            tz_aware=True,
            tzinfo=timezone.utc,
        ),
        timeseries={
            "timeField": "join_timestamp",
            "metaField": "session_info",
            "granularity": "seconds"
        }
    )
else:
    player_sessions = db.get_collection("player_sessions")

if "player_events" not in db.list_collection_names():
    player_events = db.create_collection(
        "player_events",
        codec_options=CodecOptions(
            tz_aware=True,
            tzinfo=timezone.utc,
        ),
        timeseries={
            "timeField": "timestamp",
            "metaField": "event_info",
            "granularity": "seconds"
        },
    )
else:
    player_events = db.get_collection("player_events")


if "server_status" not in db.list_collection_names():
    server_status = db.create_collection(
        "server_status",
        codec_options=CodecOptions(
            tz_aware=True,
            tzinfo=timezone.utc,
        ),
        timeseries={
            "timeField": "timestamp",
            "granularity": "seconds"
        }
    )
else:
    server_status = db.get_collection("server_status")

if "Players" not in db.list_collection_names():
    players = db.create_collection(
        "Players",
        codec_options=CodecOptions(
            tz_aware=True,
            tzinfo=timezone.utc,
        ),
    )
else:
    players = db.get_collection("Players")

def create_session(player, join_timestamp, leave_timestamp):
    play_time_minutes = round(calculate_playtime(join_timestamp, leave_timestamp))

    if play_time_minutes == 0:
        return
    
    player_sessions.insert_one({
        "session_info": {
            "player_id": player.id,
            "player_name": player.name
        },
        "left_timestamp": leave_timestamp,
        "join_timestamp": join_timestamp,
        "play_time": play_time_minutes,
    })

def create_event(player, event_type, event_timestamp):
    player_events.insert_one({
        "timestamp": event_timestamp,
        "event_type": EVENT_TYPE_REV_MAP[event_type],
        "event_info": {
            "player_name": player.name,
            "player_id": str(player.id),
        }
    })
def create_server_status(player_count, player_list, timestamp):
    server_status.insert_one({
        "timestamp": timestamp,
        "player_list": player_list,
        "player_count": player_count
    })

def create_player(player, join_timestamp):
    players.insert_one({
        "_id": str(player.id),
        "player_name": player.name,
        "first_joined": join_timestamp,
        "last_seen": join_timestamp,
        "play_time": 0,
    })

def update_player(player, join_timestamp, leave_timestamp):
    play_time_minutes = round(calculate_playtime(join_timestamp, leave_timestamp))
    players.update_one(
        {"_id": str(player.id)},
        {
            "$inc": {"play_time": play_time_minutes},
            "$set": {"last_seen": leave_timestamp}
        }
    )

def calculate_playtime(isotime_start:datetime, isotime_end:datetime):
    duration = isotime_end - isotime_start
    total_minutes = duration.total_seconds() / 60
    return total_minutes

def player_exists(player_id):
    return players.find_one({"_id": str(player_id)}) is not None

def log_event(eventType, player_obj, timestamp):

    if eventType == EVENT_TYPE["PLAYER_JOIN"]:
        if not player_exists(player_obj.id):
            create_player(player_obj, timestamp)
            create_event(player_obj, EVENT_TYPE["NEW_PLAYER"], timestamp)

    elif eventType == EVENT_TYPE["PLAYER_LEAVE"]:
        update_player(player_obj, player_obj.join_time, timestamp)
        create_session(player_obj, player_obj.join_time, timestamp)

    create_event(player_obj, eventType, timestamp)

def handle_shutdown(player_map):
    for player in player_map.values():
        print(f"Logging leave event for {player.name} due to shutdown.")
        log_event(EVENT_TYPE["PLAYER_LEAVE"], player, player.last_seen )
    sys.exit(0)

def calculate_sampling_ratio(sampled_list_count: int, total_player_count: int):
    return (sampled_list_count / total_player_count if total_player_count > 0 else 0)


def calculate_absence_time_threshold(
        sample_size, 
        total_online,
        base_threshold=BASE_ABSENCE_THRESHOLD, 
        min_threshold=MIN_ABSENCE_THRESHOLD, 
        max_threshold=MAX_ABSENCE_THRESHOLD
):
    if total_online == 0 or sample_size == 0:
        return max_threshold
    
    sampling_ratio = calculate_sampling_ratio(sample_size, total_online)
    visibility_scale = 1 / sampling_ratio
    server_size_scale = math.log1p(total_online) / math.log1p(12)
    adjusted_threshold = base_threshold * (visibility_scale ** 0.425) * server_size_scale ** 0.100

    return max(min(adjusted_threshold, max_threshold), min_threshold)
    

def calculate_dynamic_sleep_time(
    sample_size: int,
    total_online: int,
    base_sleep: int = BASE_SLEEP_TIME,
    min_sleep: int = MIN_SLEEP_TIME,
    max_sleep: int = MAX_SLEEP_TIME,      
) :
    if total_online == 0 or sample_size == 0:
        return max_sleep
    
    sampling_ratio = calculate_sampling_ratio(sample_size, total_online)
    visibility_scale = 1 / sampling_ratio
    adjusted_sleep = math.ceil(base_sleep / (visibility_scale ** 0.8))

    return max(min(adjusted_sleep, max_sleep), min_sleep)
     

def calculate_confidence_score(absence_time, absence_time_threshold, sampling_ratio):
    confidence_time = max(1.0 - (absence_time / absence_time_threshold), 0.0)
    return confidence_time

def main():
    # set of Player instances
    last_players_online = set()
    player_map = {}

    def handler(signal, frame):
        handle_shutdown(player_map)
        

    signal.signal(signal.SIGTERM, handler)
    
    try:
        print(f"[{datetime.now(timezone.utc).astimezone().isoformat()}][Server Scanner] Scanning Minecraft Server on: {MC_SERVER_IP}")
        while True:
            try:
                #Get server object and status object to query the server.
                server = JavaServer.lookup(address=MC_SERVER_IP, timeout=10)
                status = server.status()
                current_time_utc = datetime.now(timezone.utc)
                current_time_local = current_time_utc.astimezone()

                # get sampled list of players currently online then map them to a player object inside a set.
                current_sample = status.players.sample or []
                online_players = status.players.online or 0
                current_players = {Player(p.name, p.id) for p in current_sample}

                # calculate sleep time based on the sampling ratio
                dynamic_sleep_time = calculate_dynamic_sleep_time(
                    len(current_players),
                    online_players,
                )

                # determine the recently joined players vs the players that left.
                joined_now = current_players - last_players_online
                left_now = last_players_online - current_players

                if joined_now or left_now:
                    print(f"[{current_time_local.isoformat()}][Server Scanner] Server IP: {server.address}\tPlayers Online: {status.players.online}")
        

                # create event for joined players
                # save them locally in memory
                for player in joined_now:

                    if player.name not in player_map:
                        player_map[player.name] = player
                        player.join_time = current_time_utc
                        player.last_seen = current_time_utc
                        print(f"[{current_time_local.isoformat()}][Server Scanner] {player.name} joined.")
                        log_event(EVENT_TYPE["PLAYER_JOIN"], player, current_time_utc)
                    

                #mark existing players for potential pruning & prune after
                #the absence time reaches a given threshhold.
                absence_time_threshold = calculate_absence_time_threshold(
                    len(current_players), 
                    online_players, 
                )

                for name, player in list(player_map.items()):

                    if player in current_players:
                        player.last_seen = current_time_utc
                        player.confidence_score = 1
                    else:
                        absence_duration = (current_time_utc - player.last_seen).total_seconds()
                        player.confidence_score = calculate_confidence_score(
                            absence_duration, 
                            absence_time_threshold, 
                            calculate_sampling_ratio(
                                len(current_players),
                                online_players
                            )
                        )
                        if absence_duration >= absence_time_threshold:

                            log_event(EVENT_TYPE["PLAYER_LEAVE"], player, player.last_seen)
                            print(f"[{current_time_local.isoformat()}][Server Scanner] {name} left the server.")
                            player_map.pop(name, None)

                if joined_now or left_now:
                     #log player list and count to server_session:
                    create_server_status(
                        online_players, 
                        [
                            {"player_name": p.name, "player_id": p.id, "confidence_score_online": p.confidence_score} 
                            for p in player_map.values()
                        ], 
                        current_time_utc
                    )
                
                last_players_online = current_players.copy()
            except Exception as e:
                print(f"[{datetime.now(ZoneInfo('America/New_York')).isoformat()}] Error: {e}")
            time.sleep(dynamic_sleep_time)

    except KeyboardInterrupt:
        handle_shutdown(player_map)

if __name__ == "__main__":
    main()
