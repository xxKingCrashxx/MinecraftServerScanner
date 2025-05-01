# Environment Setup
## Script Dependencies:
- `tzdata`
- `pymongo`
- `mcstatus`
- `python-dotenv`

## Script Environment:
**In project directory:** 
- `Start a python venv`  
- Run `pip install -r requirements.txt`  
- Create a .env file with the required keys filled out.
	- `NOTE` - `MC_SERVER_IP` is a string in the format: `[IP]:[PORT]` for example, "192.168.4.23:25565"
	- Look at `dotenv.txt` to see the required environment variables.

## Database Environment:
You can setup and configure the database in whatever way to fit your needs, but you must have these following things set up:

**Required Collections:**
- `Players`,
- `player_sessions`
- `player_events`
- `server_status`

**Required Collection Configurations:**
- All the collections except the `Players` collection should be setup as a timeseries collection.

**Collection Schemas:**
```javascript
//Players
{
    _id: String (mcuuid),
    player_name: String,
    total_playtime: Int,
    first_joined: Date,
    last_seen: Date
}

//events
{
    timestamp: Date,
    event_type: String ["PLAYER_JOIN" | "PLAYER_LEAVE" | "NEW_PLAYER"],
    event_info: {
        player_id: String,
        player_name: String
    }
}

//sessions
{
    join_timestamp: Date,
    leave_timestamp: Date,
    play_time: Int,
    session_info: {
        player_id: String,
        player_name: String
    }
}

//server_status
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

## Minecraft Server:

The script was tested on the most recent vanilla Minecraft server `1.21.5`.  
For the script to gather any meaningful information, make sure the Minecraft server does not have any plugins that scramble the player list.