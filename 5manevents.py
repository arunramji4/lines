import pandas as pd
import os
import datetime
import re
from sqlalchemy import create_engine
import psycopg2


#  Connect to PostgreSQL
DB_USER = "postgres"  # Default PostgreSQL username
DB_PASSWORD = ""  # Leave blank if no password was set
DB_HOST = "localhost"  # Default for local setup
DB_PORT = "5432"  # Default PostgreSQL port
DB_NAME = "uwhockey"  # The database you created


engine = create_engine(f"postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

input_folder = "/Users/arunramji/Desktop/Waterloo2425"
output_folder = "/Users/arunramji/Desktop/oppocorsi"

def extract_date(filename):
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", filename)  # Match DD.MM.YYYY
    if match:
        day, month, year = match.groups()
        return datetime.datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
    return datetime.datetime.min  # Default to very old date if no match

csv_files = sorted(
    [f for f in os.listdir(input_folder) if f.endswith(".csv")], 
    key=extract_date
)

# Master file logic
all_games_shifts = []  # empty list to collect all Final_file
targetteam = "Waterloo Warriors"
# Goalie names for logic below 
goalies = ["Onuska Matt","Murphy Daniel"]
for i, file in enumerate(csv_files, start=1):
    data = pd.read_csv(os.path.join(input_folder, file))
    # === Fix missing time values ===
    if data["start"].isnull().any():
    
        data["start"].fillna(0, inplace=True)
    if data["end"].isnull().any():
        data["end"].fillna(0, inplace=True)

    
   # Filter logic
    data = data[(data["team"] == targetteam) | (data["action"] == "Shots") | (data["action"] == "Goals")]
    
    # Score effects 
    goals = data[data["action"] == "Goals"]
    score_effects = 0 
    
    shifts = []
    on_ice = []
    for index, row in data.iterrows():
        if row["action"] == "Even strength shifts":
            if len(on_ice) < 6:
                on_ice.append([row["player"], row["start"], row["end"]])
            if len(on_ice) == 6:
                players = [p[0] for p in on_ice]
                start_time = max([p[1] for p in on_ice])
                end_time = min([p[2] for p in on_ice])
                if end_time - start_time > 0 and  any(p[0] in goalies for p in on_ice):
                    shifts.append(players + [start_time, end_time, end_time - start_time])
    
                on_ice.sort(key=lambda x: x[2], reverse=True)
                on_ice.pop()
    
    # Convert to DataFrame
    shift_df = pd.DataFrame(shifts, columns=["player1", "player2", "player3", "player4", "player5", "player6","start", "end", "duration"])
    shift_df[["DZ Start", "OZ Start", "Neutral Start", "Shots For", "Shots Against", "Score","GF","GA"]] = 0 
    
    # Just faceoffs and shots data set 
    shots_and_faceoffs = data[data["action"] != "Even strength shifts"]
    
    # Ensure everything is in order
    shift_df.sort_values(by="start", inplace=True)
    shots_and_faceoffs.sort_values(by="start", inplace=True)
    
    for shot_index, shot_row in shots_and_faceoffs.iterrows():
        if shot_row["action"] == "Shots" and shot_row["team"] == targetteam:
            time = shot_row["start"]
            for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "Shots For"] += 1
                    break
        if shot_row["action"] == "Shots" and shot_row["team"] != targetteam:
            time = shot_row["start"]
            for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "Shots Against"] += 1
                    break
        if shot_row["action"] == "Faceoffs in DZ":
            time = shot_row["end"]
            for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "DZ Start"] += 1
                    break
        if shot_row["action"] == "Faceoffs in OZ":
            time = shot_row["end"]
            for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "OZ Start"] += 1
                    break
        if shot_row["action"] == "Goals" and shot_row["team"] == targetteam:
           for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "GF"] += 1
                    break 
        
        if shot_row["action"] == "Goals" and shot_row["team"] != targetteam:
           for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "GA"] += 1
                    break 

    
    shift_and_goals = pd.concat([shift_df, goals], ignore_index=True)
    shift_and_goals.to_csv("check.csv")
    shift_and_goals.drop(columns=["pos_x", "pos_y", "half", "player", "ID"], inplace=True)
    shift_and_goals.sort_values(by="start", inplace=True)
    shift_and_goals.fillna(0, inplace=True)
   
    
    for index, row in shift_and_goals.iterrows():
        if row["action"] == "Goals" and row["team"] == targetteam:
            score_effects += 1
        if row["action"] == "Goals" and row["team"] != targetteam:
            score_effects -= 1
        shift_and_goals.at[index, "Score"] = score_effects
    
    Final_file = shift_and_goals[shift_and_goals["action"] != "Goals"]
    Final_file["Game ID"] = i
    Final_file[["Tied", "Up1", "Up2", "Up3plus", "Down1", "Down2", "Down3plus"]] = 0

    for index, row in Final_file.iterrows():
        score = row["Score"]

        if score == 0:
            Final_file.at[index, "Tied"] = row["duration"]
        elif score == 1:
            Final_file.at[index, "Up1"] = row["duration"]
        elif score == 2:
            Final_file.at[index, "Up2"] = row["duration"]
        elif score >= 3:
            Final_file.at[index, "Up3plus"] = row["duration"]
        elif score == -1:
            Final_file.at[index, "Down1"] = row["duration"]
        elif score == -2:
            Final_file.at[index, "Down2"] = row["duration"]
        elif score <= -3:
            Final_file.at[index, "Down3plus"] = row["duration"]
    all_games_shifts.append(Final_file)


    


# Combine all games into one big master file
master_shift_df = pd.concat(all_games_shifts, ignore_index=True)
print("\n=== Master Shift DF ===")
print(f"Total shifts: {len(master_shift_df)}")
print(f"Columns: {master_shift_df.columns.tolist()}")



from collections import defaultdict

player_stats = defaultdict(lambda: {
    "Shots For": 0,
    "Shots Against": 0,
    "DZ Start": 0,
    "OZ Start": 0,
    "Neutral Start": 0,
    "Score": 0,
    "GF": 0,
    "GA": 0,
    "duration": 0,
    "Shifts": 0,
    "Tied": 0,
    "Up1": 0,
    "Up2": 0,
    "Up3plus": 0,
    "Down1": 0,
    "Down2": 0,
    "Down3plus": 0
})



for index, row in master_shift_df.iterrows():
    players = [row["player1"], row["player2"], row["player3"], row["player4"], row["player5"], row["player6"]]
    for player in players:
        player_stats[player]["Shots For"] += row["Shots For"]
        player_stats[player]["Shots Against"] += row["Shots Against"]
        player_stats[player]["DZ Start"] += row["DZ Start"]
        player_stats[player]["OZ Start"] += row["OZ Start"]
        player_stats[player]["duration"] += row["duration"]
        player_stats[player]["Tied"] += row["Tied"]
        player_stats[player]["Up1"] += row["Up1"]
        player_stats[player]["Up2"] += row["Up2"]
        player_stats[player]["Up3plus"] += row["Up3plus"]
        player_stats[player]["Down1"] += row["Down1"]
        player_stats[player]["Down2"] += row["Down2"]
        player_stats[player]["Down3plus"] += row["Down3plus"]

# Convert to DataFrame
player_stats_df = pd.DataFrame.from_dict(player_stats, orient="index").reset_index()
player_stats_df.rename(columns={"index": "Player"}, inplace=True)
# Make sure no division by zero


player_stats_df["Zone Start Total"] = player_stats_df["DZ Start"] + player_stats_df["OZ Start"]
#
# Only compute Zone Starts where total > 0
player_stats_df["O Zone Start pct"] = player_stats_df["OZ Start"] / player_stats_df["Zone Start Total"]
player_stats_df["O Zone Start pct"] = player_stats_df["O Zone Start pct"].fillna(0)  # replace NaN from 0/0 with 0

player_stats_df["Shotsper60"] = (player_stats_df["Shots For"] / player_stats_df["duration"]) * 3600
player_stats_df["Shotsagainst60"] = (player_stats_df["Shots Against"] / player_stats_df["duration"]) * 3600

player_stats_df = player_stats_df[[
    "Player",
    "Tied",
    "Up1",
    "Up2",
    "Up3plus",
    "Down1",
    "Down2",
    "Down3plus",
    "Shotsper60",
    "Shotsagainst60"

]]
player_stats_df["team"] = targetteam
# Save final player totals
player_stats_df.to_csv("/Users/arunramji/Desktop/oppocorsi/UW.csv", index=False)




   




   







