# Minecraft Server Scanner

## Overview

This application leverages publicly accessible metadata provided by vanilla Minecraft servers. By querying a server at regular intervals, it determines who is online, when players join or leave, and how long they stay. Unless server administrators actively obfuscate this metadata, it remains openly available for analysis.

## Libraries Used

- `mcstatus`
- `pymongo`
- `python-dotenv`
- `tzdata`

`mcstatus` is used for querying server data, while `pymongo` manages data persistence in MongoDB.

## How It Works

Vanilla Minecraft servers allow basic pinging to reveal the number of players online and a sample list (typically up to 15) of their usernames. This script pings the server every minute, compares the current player list with the previous one, and detects join or leave events accordingly. It logs these events and calculates session durations for each player.
```
┌────────────────┐                                             
│                │                                             
│   Mongodb      │                                             
│   Database     │                                             
│                │                                             
└──────────┬─────┘                                             
     ▲     │                                                   
     │     │                                                   
     │     │                                                   
     │     │                                                   
     │     │                                                   
     │     │                                                   
     │     │                                                   
     │     │                                                   
     │     │                                                   
┌────┼─────▼──────┐            ┌────────────────┐              
│                 │            │                │              
│ Minecraft Server│◄───────────┼    Targeted    │              
│ Scanner.        │            │    Server      │              
│                 │            │                │              
└─────────────────┘            └────────────────┘              
 1.)Scanner queries the target server for a sampled list       
    of players, total players online, etc.                     
                                                               
 2.)The Scanner transforms this information and keeps track    
    of timestamps and a map of unique players with state       
    information. Based on the current sample, previous sample, 
    and map of unique players seen, this information gets      
    seperated into player_events, player_sessions, players,    
    and server_status.                                         
                                                               
 3.)sorted information is then written to the specified mongodb
    database for later review and data aggregations.           
```
## Database Design

Four main MongoDB collections store the gathered data:

### 1. `Players`

Tracks unique players and their activity over time.

```javascript
{
    _id: String (mcuuid),
    player_name: String,
    total_playtime: Int,
    first_joined: Date,
    last_seen: Date
}
```

### 2. `Player_Events`

Captures events such as joining, leaving, and new player detection.

```javascript
{
    timestamp: Date,
    event_type: String ["PLAYER_JOIN" | "PLAYER_LEAVE" | "NEW_PLAYER"],
    event_info: {
        player_id: String,
        player_name: String
    }
}
```

### 3. `Player_Sessions`

Stores the start and end of player sessions, as well as their duration.

```javascript
{
    join_timestamp: Date,
    leave_timestamp: Date,
    play_time: Int,
    session_info: {
        player_id: String,
        player_name: String
    }
}
```

### 4. `Server_Status`

(Optional) Stores a snapshot of the server state at each query.

```javascript
{
    timestamp: Date,
    player_count: Int,
    player_list: [
        {
            player_id: String,
            player_name: String
        }
    ]
}
```
### Time Series
The following collections are set up as a timeseries collection:
- Player_Events
- Player_Sessions
- Server_Status

The Meta information is the event_info, session_info for Player_Events and Player_Sessions.  
The time property is timestamp and join_timestamp for Player_Events, Server_Status, and Player_Sessions.

## Data Collection Flow

1. Ping the server using `mcstatus` every minute.
2. Retrieve the current list of sampled online players.
3. Compare against the previous list to determine joins and leaves.
4. Log `PLAYER_JOIN` and `PLAYER_LEAVE` events.
5. Aggregate these into `Player_Sessions`.
6. Update cumulative playtime in the `Players` collection.

## Example Queries

**1. Total playtime for a specific player:**

```javascript
db.player_sessions.aggregate([
  { $match: { "session_info.player_name": "King_Crash" } },
  { $group: { _id: "$session_info.player_name", total_time: { $sum: "$play_time" } } }
]);
```

**2. Active players in the last 24 hours:**

```javascript
db.player_events.find({
  timestamp: { $gte: new Date(Date.now() - 1000 * 60 * 60 * 24) },
  event_type: "PLAYER_JOIN"
});
```

## Potential Metrics

- Total playtime per player
- Average session durations (daily, monthly, yearly)
- Peak server hours and concurrent user counts
- Player retention and return frequency
- Hourly and weekly activity heatmaps

These metrics can be visualized using tools like `matplotlib`, `seaborn`, `Plotly Dash`, or `Grafana`.

## Security Considerations

Unless actively restricted, Minecraft servers expose this metadata to anyone who queries them. Server administrators can reduce exposure by using plugins that:

- Disable or obfuscate the player sample list
- Randomize usernames returned in ping responses
- Restrict detail in ping queries

## Final Thoughts

While simple in design, this script provides powerful insights into server and player behavior. The more active the server and the longer the script runs, the more valuable and accurate the data becomes. Even without advanced analytics experience, this tool enables meaningful analysis of server population trends and player habits.

Server owners concerned about privacy should proactively configure protections to limit exposure to this form of passive data collection.

