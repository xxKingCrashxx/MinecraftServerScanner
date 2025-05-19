
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
MIN_SLEEP_TIME = 10
MAX_SLEEP_TIME = 90
BASE_ABSENCE_THRESHOLD = 600
MIN_ABSENCE_THRESHOLD = 300
MAX_ABSENCE_THRESHOLD = 900

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
        log_event(EVENT_TYPE["PLAYER_LEAVE"], player, datetime.now(tz=timezone.utc))
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
    
    ratio = calculate_sampling_ratio(sample_size, total_online)
    adjusted = base_threshold + math.ceil(math.log10(1 / ratio)) * 60
    return max(min(adjusted, max_threshold), min_threshold)

def calculate_dynamic_sleep_time(
    sample_size: int,
    total_online: int,
    base_sleep: int = BASE_SLEEP_TIME,
    min_sleep: int = MIN_SLEEP_TIME,
    max_sleep: int = MAX_SLEEP_TIME,      
) :
    if total_online == 0 or sample_size == 0:
        return max_sleep
    
    ratio = calculate_sampling_ratio(sample_size, total_online)

    if ratio >= 1.0:
        return base_sleep
    
    if ratio <= 0:
        return max_sleep
    
    adjusted_sleep_time = round(base_sleep * ratio)
    return max(min(adjusted_sleep_time, max_sleep), min_sleep)


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
                online_players = status.players.online
                current_players = {Player(p.name, p.id) for p in current_sample}

                # calculate sleep time based on the sampling ratio
                dynamic_sleep_time = calculate_dynamic_sleep_time(
                    len(current_players),
                    online_players,
                )

                # reset absence for all players in the current_players list
                for player in current_players:
                    if player.name in player_map:
                        player_map[player.name].last_seen = current_time_utc

                

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
                absence_time_threshold = calculate_absence_time_threshold(len(current_players), online_players, base_threshold=BASE_ABSENCE_THRESHOLD)
                for name, player_object in list(player_map.items()):
                    if player_object not in current_players:
                        absence_duration = (current_time_utc - player_object.last_seen).total_seconds()

                        if absence_duration >= absence_time_threshold:

                            log_event(EVENT_TYPE["PLAYER_LEAVE"], player_object, player_object.last_seen)
                            print(f"[{current_time_local.isoformat()}][Server Scanner] {name} left the server.")
                            player_map.pop(name, None)

                if joined_now or left_now:
                     #log player list and count to server_session:
                    create_server_status(
                        online_players, 
                        [
                            {"player_name": p.name, "player_id": p.id} 
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
