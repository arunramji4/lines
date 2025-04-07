import os
import pandas as pd
from sqlalchemy import create_engine

# CONFIGURE
csv_folder = '/Users/arunramji/desktop/WaterlooLineData'  # ⬅️ Change this to your actual path
db_user = 'arunramji'
db_name = 'uwhockey'
engine = create_engine(f'postgresql://{db_user}@localhost:5432/{db_name}')

# Prepare two lists
forward_dfs = []
defence_dfs = []

# Loop through files
for filename in os.listdir(csv_folder):
    if filename.endswith('.csv'):
        path = os.path.join(csv_folder, filename)
        df = pd.read_csv(path)

        if 'forwards' in filename.lower():
            forward_dfs.append(df)
        elif 'defence' in filename.lower():
            defence_dfs.append(df)

# Combine and push
if forward_dfs:
    combined_forwards = pd.concat(forward_dfs, ignore_index=True)
    combined_forwards.to_sql('uwlines2425F', con=engine, index=False, if_exists='replace')
    print(f"✅ Loaded {len(combined_forwards)} rows into 'uwlines2425F'")

if defence_dfs:
    combined_defence = pd.concat(defence_dfs, ignore_index=True)
    combined_defence.to_sql('uwlines2425D', con=engine, index=False, if_exists='replace')
    print(f"✅ Loaded {len(combined_defence)} rows into 'uwlines2425D'")


