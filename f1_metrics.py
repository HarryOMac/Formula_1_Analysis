import pandas as pd
import os
import glob
from timeit import default_timer as timer

def getDeltaNorm(results,races):

    resultsDfList = []

    for eachRace in races.index:

        #print("\n\n\n***   eachRace iter", str(eachRace),"ELAPSED:   ***\n")

        #eachRaceStart = timer()

        raceResults = results[
            results['raceId'].map(lambda x: x == eachRace)
            ][['raceId',
            'driverId',
            'constructorId',
            'milliseconds',
            'fastestLapTime']]
        
        bestRaceTime = raceResults['milliseconds'].min(numeric_only = True)
        #raceResults['fastestLapTime'] = raceResults['fastestLapTime'].apply(getMilli)
        #bestRaceLap = raceResults['fastestLapTime'].min(numeric_only = True)

        #input(raceResults)
        
        raceDfList = []
        constructorIdList = []

        for eachDriver in raceResults.index:

            #print(eachDriver)

            #eachDriverStart = timer()

            if raceResults['constructorId'].loc[eachDriver] in constructorIdList:
                #print("continue")
                continue
            
            #print(raceResults['constructorId'].loc[eachDriver])

            constructorIdList.append(raceResults['constructorId'].loc[eachDriver])

            teamRaceResults = raceResults[
                raceResults['constructorId'].map(lambda x: x == raceResults['constructorId'].loc[eachDriver])
                ][['raceId',
                'driverId',
                'constructorId',
                'milliseconds']]

            bestTeamTime = teamRaceResults['milliseconds'].min(numeric_only = True)
            teamRaceResults['teamDelta'] = teamRaceResults['milliseconds'] - bestTeamTime
            teamRaceResults['teamDeltaNorm'] = ((teamRaceResults['teamDelta'] / bestRaceTime)) * 10000000

            #print(teamRaceResults)

            raceDfList.append(teamRaceResults)

            #eachDriverEnd = timer()
            #print(eachDriverEnd - eachDriverStart)

        try:
            raceDf = pd.concat(raceDfList)
            resultsDfList.append(raceDf)
        except:
            print(eachRace)

        #eachRaceEnd = timer()
        #print("\n\n***   eachRace ELAPSED:   ***")
        #print(eachRaceEnd - eachRaceStart)

    resultsNew = pd.concat(resultsDfList)
    #print(resultsNew.index)
    #print(results.index)
    resultsNew = results[[
            'raceId',
            'driverId',
            'constructorId',
            'milliseconds'
        ]].join(
            resultsNew[[
                'teamDelta',
                'teamDeltaNorm'
            ]], 
            how="outer", 
            sort=True, 
            validate="1:1"
        ).set_index(
            [
                'raceId',
                'driverId'
            ]
        )

    return resultsNew

def getMilli(time):
    try:
        time = sum(x * float(t) for x, t in zip([1, 60, 3600], reversed(time.split(":")))) * 1000
    except:
        time = None
    return time

path = "\\".join(__file__.split("\\")[:len(__file__.split("\\"))-1])
csvFolder = path + "\\f1_data"
outputFolder = path + "\\f1_data_output"
csvList = glob.glob(os.path.join(csvFolder, "*.csv"))

dataDict = {}

for eachCSV in csvList:
    df = pd.read_csv(eachCSV)
    newData = {os.path.basename(eachCSV)[::-1].split('.',1)[1][::-1] : df}
    dataDict.update(newData)

for tables in ["circuits",
               "constructors",
               "constructor_results",
               "constructor_standings",
               "drivers",
               "driver_standings",
               "qualifying",
               "races",
               "results",
               "seasons",
               "sprint_results",
               "status"]:
    dataDict[tables] = dataDict[tables].set_index(dataDict[tables].columns[0])



#print(dataDict)

print("\n\n\n\n\n***   FUNCTION STARTED   ***")

start = timer()
resultsNew = getDeltaNorm(dataDict["results"],dataDict["races"])
end = timer()

print("\n\n\n\n***  ELAPSED:   ***")
print(end - start)

dataDict['results'] = dataDict['results'].set_index(['raceId','driverId'])

standingsNew = dataDict['driver_standings'].join(
        dataDict['results'][dataDict['results']['statusId'] == 1][['statusId','positionText']],
        rsuffix = '_race',
        on=['raceId','driverId'],
        how="left",
        sort=False
    ).join(
        dataDict['races'][['year']],
        on='raceId',
        how="left",
        sort=False,
        validate="m:1"
    )

points = {
    '1' : 25,
    '2' : 18,
    '3' : 15,
    '4' : 12,
    '5' : 10,
    '6' : 8,
    '7' : 6,
    '8' : 4,
    '9' : 2,
    '10' : 1
}

standingsNew['positionText_race'] = standingsNew['positionText_race'].astype(str)
standingsNew['pointsNew'] = standingsNew.apply(lambda row: points.setdefault(row['positionText_race'], 0), axis=1)

seasonalStandings = standingsNew.groupby(
        ['year','driverId']
    ).agg(
        seasonWins = pd.NamedAgg('wins','max'),
        seasonPoints = pd.NamedAgg('pointsNew','sum'),
        seasonRacesFinished = pd.NamedAgg('statusId','count')
    )

seasonalStandings = seasonalStandings[seasonalStandings['seasonRacesFinished'] > 0]
seasonalStandings['winsPerRace'] = seasonalStandings['seasonWins'] / seasonalStandings['seasonRacesFinished']
seasonalStandings['pointsPerRace'] = seasonalStandings['seasonPoints'] / seasonalStandings['seasonRacesFinished']

overallStandings = seasonalStandings.groupby(
    ['driverId']
    ).agg(
        avgWins = pd.NamedAgg('seasonWins','mean'),
        avgPoints = pd.NamedAgg('seasonPoints','mean'),
        totalWins = pd.NamedAgg('seasonWins','sum'),
        totalPoints = pd.NamedAgg('seasonPoints','sum'),
        avgWinsNorm = pd.NamedAgg('winsPerRace','mean'),
        avgPointsNorm = pd.NamedAgg('pointsPerRace','mean'),
    )

resultsAgg = resultsNew.groupby(
        ['driverId']
    ).agg(
        teamDeltaAvg = pd.NamedAgg('teamDelta','mean'),
        teamDeltaNormAvg = pd.NamedAgg('teamDeltaNorm','mean'),
        teamDeltaSum = pd.NamedAgg('teamDelta','sum'),
        teamDeltaNormSum = pd.NamedAgg('teamDeltaNorm','sum'),
    ).join(
        dataDict['drivers'][['driverRef']], 
        how="left", 
        sort=False, 
        validate="1:1"
    ).join(
        overallStandings,
        how="left",
        sort=False,
        validate="1:1"
    ).sort_values(
        by=[
            'teamDeltaNormAvg',
            'teamDeltaAvg',
            'teamDeltaNormSum',
            'teamDeltaSum'
        ]
    ).sort_values(
        by=[
            'avgWinsNorm',
            'avgPointsNorm',
            'avgWins',
            'avgPoints',
            'totalWins',
            'totalPoints'
        ],
        ascending=False
    )

resultsAggWinners = resultsAgg[(resultsAgg['totalWins'] > 0) & (resultsAgg['teamDeltaAvg'] != 0)]

print("\n\n***   OUTPUT:   ***\n")
print(seasonalStandings)
print("\n\n")
print(overallStandings)
print("\n\n")
print(resultsNew)
print("\n\n")
print(resultsAgg)
print("\n\n")
print(resultsAggWinners)

resultsAgg.to_csv(outputFolder + "\\driver_rankings.csv", index=True)