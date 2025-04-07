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

input_folder = "/Users/arunramji/Desktop/WaterlooGames"
output_folder = "/Users/arunramji/Desktop/WaterlooGamesProcessed"

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

# Goalie names for logic below 
goalies = ["Onuska Matt","Murphy Daniel"]
for i, file in enumerate(csv_files, start=1):
    data = pd.read_csv(os.path.join(input_folder, file))
    
   # Filter logic
    data = data[(data["team"] == "Waterloo Warriors") | (data["action"] == "Shots") | (data["action"] == "Goals")]
    
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
        if shot_row["action"] == "Shots" and shot_row["team"] == "Waterloo Warriors":
            time = shot_row["start"]
            for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "Shots For"] += 1
                    break
        if shot_row["action"] == "Shots" and shot_row["team"] != "Waterloo Warriors":
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
        if shot_row["action"] == "Goals" and shot_row["team"] == "Waterloo Warriors":
           for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "GF"] += 1
                    break 
        
        if shot_row["action"] == "Goals" and shot_row["team"] != "Waterloo Warriors":
           for shift_index, shift_row in shift_df.iterrows():
                if time >= shift_row["start"] and time <= shift_row["end"]:
                    shift_df.at[shift_index, "GA"] += 1
                    break 

    
    shift_and_goals = pd.concat([shift_df, goals], ignore_index=True)
    shift_and_goals.drop(columns=["pos_x", "pos_y", "half", "player", "ID"], inplace=True)
    shift_and_goals.sort_values(by="start", inplace=True)
    shift_and_goals.fillna(0, inplace=True)
    
    for index, row in shift_and_goals.iterrows():
        if row["action"] == "Goals" and row["team"] == "Waterloo Warriors":
            score_effects += 1
        if row["action"] == "Goals" and row["team"] != "Waterloo Warriors":
            score_effects -= 1
        shift_and_goals.at[index, "Score"] = score_effects
    
    Final_file = shift_and_goals[shift_and_goals["action"] != "Goals"]
    Final_file.drop(columns=["team", "action"], inplace=True)
    Final_file["Game ID"] = i

    output_filename = os.path.join(output_folder, f"game_{i}.csv")
    Final_file.to_csv(output_filename, index=False)
   







