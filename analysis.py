import pandas as pd

df = pd.read_csv("/Users/arunramji/Desktop/WaterlooGamesProcessed/game_1.csv")

sum = 0 
players = []
analyze = []

for index, row in df.iterrows():
    players.append(row["player1"])
    players.append(row["player2"])
    players.append(row["player3"])
    players.append(row["player4"])
    players.append(row["player5"])
    players.append(row["player6"])
    if "Rose Simon" in players and "Robinson Payton" in players:
        sum += row["duration"]
        
    players = []
print(sum)
  
analyze = pd.DataFrame(analyze)
analyze.to_csv("Bruh.csv", index=False)




guys = []
amount = 0
df1 = pd.read_csv("/Users/arunramji/Desktop/WaterlooLineData/game_1_defence.csv")
for index,row in df1.iterrows():
    guys.append(row["D1"])
    guys.append(row["D2"])


    if "Rose Simon" in guys and "Robinson Payton" in guys:
        amount += row["duration"]
    guys = []

print(amount)


    

