import pandas as pd
import os
from collections import defaultdict

input_folder = "/Users/arunramji/Desktop/Waterloo2425"
master_list = []

# Goalies to ignore
goalies = ["Onuska Matt", "Murphy Daniel"]

for file in os.listdir(input_folder):
    if not file.endswith(".csv"):
        continue

    file_path = os.path.join(input_folder, file)
    data = pd.read_csv(file_path)

    # Only ice time matters
    data = data[data["action"] == "Even strength shifts"]

    # Get Waterloo players
    waterloodata = data[data["team"] == "Waterloo Warriors"]
    waterlooplayers = set(waterloodata["player"])

    # Initialize time against dict
    time_against = defaultdict(lambda: defaultdict(float))

    for _, row in data.iterrows():
        if row["team"] == "Waterloo Warriors" and row["player"] not in goalies:
            player = row["player"]
            start = row["start"]
            end = row["end"]
            key = frozenset([player])

            opposingplayers = data[
                (data["team"] != "Waterloo Warriors") &
                (data["start"] < end) &
                (data["end"] > start)
            ]

            for _, opprow in opposingplayers.iterrows():
                oppname = opprow["player"]
                oppteam = opprow["team"]
                overlapstart = max(start, opprow["start"])
                overlapend = min(end, opprow["end"])
                duration = max(0, overlapend - overlapstart)

                if duration > 0:
                    time_against[key][(oppname, oppteam)] += duration

    # Remove most common opponent (likely goalie)
    for key, opponent_times in time_against.items():
        if opponent_times:
            max_opp = max(opponent_times, key=opponent_times.get)
            del opponent_times[max_opp]

    # Flatten into rows for master dataframe
    for key, opponents in time_against.items():
        player = list(key)[0]
        for (oppname, oppteam), duration in opponents.items():
            master_list.append({
                "file": file,
                "player": player,
                "opponent": oppname,
                "opponent_team": oppteam,
                "duration": duration
            })

# Convert to DataFrame
master_df = pd.DataFrame(master_list)
# Group by player-opponent-team and sum duration
merged_df = master_df.groupby(["player", "opponent", "opponent_team"], as_index=False)["duration"].sum()
merged_df.to_csv("merged_time_against.csv", index=False)




    



