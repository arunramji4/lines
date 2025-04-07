import pandas as pd


dfF = pd.read_csv("/Users/arunramji/desktop/WaterlooLineData/uwlines2425F_filtered.csv")
dfD = pd.read_csv("/Users/arunramji/desktop/WaterlooLineData/uwlines2425D_filtered.csv")

columns = ["total_shots_for","total_shots_against","total_goals_for","total_goals_against"]
time_columns = [
    "time_up_3",
    "time_up_2", 
    "time_up_1",
    "time_tied",
    "time_down_1",
    "time_down_2",
    "time_down_3",
    "total_duration"
]




for column in columns:
    dfF[f"{column}_per60"] = dfF[column] / dfF['total_duration'] * 3600

for column in columns:
    dfD[f"{column}_per60"] = dfD[column] / dfD['total_duration'] * 3600
   
for timecol in time_columns:
    dfF[timecol] = dfF[timecol] /60

for timecol in time_columns:
    dfD[timecol] = dfD[timecol] /60

dfF["ShotAtt%"] = dfF["total_shots_for"] / (dfF["total_shots_against"] + dfF["total_shots_for"]) * 100
dfD["ShotAtt%"] = dfD["total_shots_for"] / (dfD["total_shots_against"] + dfD["total_shots_for"]) * 100

dfF["GF%"] = dfF["total_goals_for"] / (dfF["total_goals_against"] + dfF["total_goals_for"]) * 100
dfD["GF%"] = dfD["total_goals_for"] / (dfD["total_goals_against"] + dfD["total_goals_for"]) * 100



# Adding scoring chance data that was hand tracked
SCF = pd.read_csv("/Users/arunramji/desktop/WaterlooLineData/ScoringChancesF.csv")
SCD = pd.read_csv("/Users/arunramji/desktop/WaterlooLineData/ScoringChanceD.csv")

SCD[["D1","D2"]] = SCD[["D1","D2"]].apply(lambda x: sorted(x),axis=1,result_type="expand")
SCF[["F1","F2","F3"]] = SCF[["F1","F2","F3"]].apply(lambda x: sorted(x),axis=1,result_type="expand")

dfF = pd.merge(dfF,SCF,on =["F1","F2","F3"],how="left")
dfF.fillna(0,inplace=True)
dfD = pd.merge(dfD,SCD,on=["D1","D2"],how="left")
dfD.fillna(0,inplace=True)

dfF["SC%"] = dfF["SCF"] / (dfF["SCF"]+ dfF["SCA"]) * 100
dfD["SC%"] = dfD["SCF"] / (dfD["SCF"]+ dfD["SCA"]) * 100



dfF.drop(columns=[
    "Unnamed: 5",
    "Unnamed: 6",
    "Unnamed: 7",
    "Unnamed: 8",
    "Unnamed: 9",
    "Unnamed: 10",
    "Unnamed: 11",
    "num_shifts",
    "total_neutral_start",
    "total_start",
    "total_end"
]
,inplace=True)

dfD.drop(columns=[
    "Unnamed: 5",
    "Unnamed: 6",
    "Unnamed: 7",
    "Unnamed: 8",
    "Unnamed: 9",
     "num_shifts",
    "total_neutral_start",
    "total_start",
    "total_end"
]
,inplace=True)

# Need to Round 

needround = [
    "time_up_3",
    "time_up_2",
    "time_up_1",
    "time_tied",
    "time_down_1",
    "time_down_2",
    "time_down_3",
    "total_shots_for_per60",
    "total_shots_against_per60",
    "total_goals_for_per60",
    "total_goals_against_per60",
    "ShotAtt%",
    "GF%",
    "SCA",
    "SC%",
    "total_duration"
]

for col in needround:
    dfF[col] = dfF[col].round(1)

for col in needround:
    dfD[col] = dfD[col].round(1)




dfF.to_csv("ForwardLinesClean.csv",index=False)
dfD.to_csv("DpairsClean.csv",index=False)