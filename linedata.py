import pandas as pd
import os
import datetime
import re
from sqlalchemy import create_engine
import psycopg2

forward_headers = ["F1", "F2", "F3"] + [
    "start", "end", "duration",
    "DZ Start", "OZ Start", "Neutral Start",
    "Shots For", "Shots Against",
    "Score", "GF", "GA", "Game ID"
]  # 15 total


defence_headers = ["D1", "D2"] + [
    "start", "end", "duration",
    "DZ Start", "OZ Start", "Neutral Start",
    "Shots For", "Shots Against",
    "Score", "GF", "GA", "Game ID"
]  # 14 total


# to help split our 5v5 data into line summaries
defence = ["McKinney Sam","Robinson Payton","Ruscheinski Kieran","Hendry Jordan","Wilson Owen","Benson Matthew","Rose Simon","Bays Brendan"]
forwards = [
    "Pierce Emmett",
    "Grein Adam",
    "Monette Max",
    "Bierd Nate",
    "Fedak Liam",
    "Murray Jaxson",
    "Goldie Jaden",
    "Fishman Jesse",
    "Bowie Brendan",
    "Fraser Cole",
    "Lopez Marco",
    "Popple Tate",
    "Phibbs Jack",
    "Santia Adamo"
]
  
wildcards = ["Davidson Aaron","Wood Gavin"] 

# Add sort through files logic 
input_folder = "/Users/arunramji/Desktop/WaterlooGamesProcessed"
output_folder= "/Users/arunramji/desktop/WaterlooLineData"

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

for i, file in enumerate(csv_files, start=1):
    df = pd.read_csv(os.path.join(input_folder, file))
    df.columns = df.columns.str.strip()


    # Initilaizations
    F = []
    D = []
    F_lines = []
    D_pairs = []
    wanted_cols = [
        "start",
        "end",
        "duration",
        "DZ Start",
        "OZ Start",
        "Neutral Start",
        "Shots For",
        "Shots Against",
        "Score",
        "GF",
        "GA",
        "Game ID"
    ]


    for index, row in df.iterrows():
        for j in range(1, 7):
            player = row[f"player{j}"]
            if player in forwards:
                F.append(player)
            if player in defence:
                D.append(player)
        # Special Logic for Davo and Woody
        if len(F) < 3:
            for j in range(1,6):
                player = row[f"player{j}"]
                if player == "Davidson Aaron":
                    F.append(player)
        if len(F) < 3:
            for j in range(1,6):
                player = row[f"player{j}"]
                if player == "Wood Gavin":
                    F.append(player)
        if len(F) < 3:
            for j in range(1,6):
                player = row[f"player{j}"]
                if player == "Davidson Aaron": 
                    F.append(player)
        if len(D) < 2:
            for j in range(1,6):
                player = row[f"player{j}"]
                if player == "Wood Gavin":
                    D.append(player)
        # Only capture if there are 3F and 2D on the ice 
        if len(F) == 3 and len(D) == 2:
            # Helps later with merging as each trio will appear the same 
            F.sort()
            D.sort()
            combinedF = F + row[wanted_cols].tolist()
            F_lines.append(combinedF)
            combinedD = D + row[wanted_cols].tolist()
            D_pairs.append(combinedD)
        D = []
        F = []


        F_file = pd.DataFrame(F_lines, columns=forward_headers)
        D_file = pd.DataFrame(D_pairs, columns=defence_headers)


    output_filename_F = os.path.join(output_folder, f"game_{i}_forwards.csv")
    output_filename_D = os.path.join(output_folder, f"game_{i}_defence.csv")

    F_file.to_csv(output_filename_F, index=False)
    D_file.to_csv(output_filename_D, index=False)

    F_lines = []
    D_pairs = []

    
    
    
    


    


    

