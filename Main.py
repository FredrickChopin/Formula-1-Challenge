import pandas as pd
from fastapi import FastAPI, HTTPException, Path
from json import loads

# DF is short for DataFrame
databasesPath = "Databases\\"

def convertMillis(millis):
    #This function gets time in milliseconds and returns the time in minutes:seconds:tenths format
    temp = (millis / 1000) % 60
    seconds = int(temp)
    minutes = int((millis / (1000 * 60)) % 60)
    tenths = int((temp - seconds) * 10)
    return str.format("{0}:{1}:{2}", str(minutes).zfill(2), str(seconds).zfill(2), tenths)

def GetBestDriversOfSeason(year):
    #Main code for question 2.a
    mergedDF = MergeDriversRacesStandings(year)
    groupedByDrivers = mergedDF.groupby(by = "driverId")
    winsSeries = groupedByDrivers.agg({"wins":"sum"})
    result = groupedByDrivers.first()
    result["wins"] = winsSeries
    result = result.sort_values(by = "wins", ascending = False)
    return result

def MergeDriversRacesStandings(desiredYear = None):
    #Merges and returns the drivers, races, driver_standings files for further processing
    print(databasesPath + "!!!!!!!!!!!!!")
    driversDF = pd.read_csv(databasesPath + "drivers.csv")
    racesDF = pd.read_csv(databasesPath + "races.csv")
    driver_standingsDF = pd.read_csv(databasesPath + "driver_standings.csv")
    if desiredYear is not None:
        racesDF = racesDF.loc[racesDF["year"] == desiredYear] #By filtering before merging, drivers who
        #did not participate in the season do not show up at all
    mergedDF = pd.merge(racesDF[["raceId", "year"]], driver_standingsDF[["raceId", "driverId", "wins"]], on="raceId")
    mergedDF = pd.merge(driversDF, mergedDF, on="driverId")
    return mergedDF

def GetAllTimeRanking(topN : int = 3):
    #Main code for question 2.b
    mergedDF = MergeDriverRacesResults()
    groupedByYears = mergedDF.groupby(by = "year", sort = True)
    result = groupedByYears.apply(lambda currYearGroup: GetTopDriversByPointsInSeason(currYearGroup, topN))
    return result

def MergeDriverRacesResults():
    # Merges and returns the drivers, races, driver_standings files for further processing
    driversDF = pd.read_csv(databasesPath + "drivers.csv")
    racesDF = pd.read_csv(databasesPath + "races.csv")
    qualifyingDF = pd.read_csv(databasesPath + "results.csv")
    mergedDF = pd.merge(racesDF[["raceId", "year"]], qualifyingDF[["raceId", "driverId", "points"]], on="raceId")
    mergedDF = pd.merge(driversDF, mergedDF, on = "driverId")
    return mergedDF

def GetTopDriversByPointsInSeason(currYearGroup,topN : int):
    #Within a year group, which is a dataframe containing information about the drivers and their results
    #at all races which took place that, returns the top N drivers in that year
    # (with respect to total points gained)
    groupedByDrivers = currYearGroup.groupby(by = "driverId")
    totalPointsSeries = groupedByDrivers["points"].sum()
    result = groupedByDrivers.first()
    result["points"] = totalPointsSeries
    result = result.sort_values(by = "points", ascending=False)
    result = result.head(topN)
    return result

def IDExistsInDrivers(ID):
    #Chekcs if there exists a driver with the given ID in the drivers.csv file
    driversDF = pd.read_csv(databasesPath + "drivers.csv")
    driversDF = driversDF.loc[driversDF["driverId"] == ID]
    return len(driversDF) != 0

def FindDriverId(forename, surname):
    #Finds the ID of a driver with a certain forename and surname.
    #Adequete failure strings will be returned if does not exist such a unique drive
    driversDF = pd.read_csv(databasesPath + "drivers.csv")
    driversDF = driversDF.loc[(driversDF["forename"] == forename) & (driversDF["surname"] == surname)]
    resultLength = len(driversDF)
    fullname = forename + " " + surname
    if resultLength == 0:
        return fullname + " was not found in the drivers database"
    if resultLength > 1:
        return "There are multiple drivers named " + fullname
    return driversDF["driverId"].iloc[0]

def GetAllRacesOfDriver(ID):
    #Main code for 2.c
    resultsDF = pd.read_csv(databasesPath + "results.csv")
    resultsDF = ChooseSpecificDriver(resultsDF, ID)
    mergedDF = AddLapInformation(resultsDF, ID)
    mergedDF = AddPitStopsInformation(mergedDF, ID)
    racesDF = pd.read_csv(databasesPath + "races.csv")
    mergedDF = pd.merge(mergedDF, racesDF[["raceId", "circuitId"]], on = "raceId")
    circuitsDF = pd.read_csv(databasesPath + "circuits.csv")
    mergedDF = pd.merge(mergedDF, circuitsDF[["circuitId", "name"]], on = "circuitId")
    del mergedDF["circuitId"]
    mergedDF.rename(columns = {"name" : "circuit_name"}, inplace=True)
    return mergedDF

def ChooseSpecificDriver(DF, ID):
    #Gets a dataframe which has "driverID" column. Selects only the rows with the given ID
    return DF.loc[DF["driverId"] == ID]

def AddLapInformation(resultsDF, ID, lapStatisticals = ("min", "max", "mean")):
    #Adds the desired statistical information (given in lapStatisticals)
    # about the laps of a driver with a given ID
    #to resultsDF, which is an intermediate dataframe in our selection
    lap_timesDF = pd.read_csv(databasesPath + "lap_times.csv")
    lap_timesDF = ChooseSpecificDriver(lap_timesDF, ID)
    resultsDF = resultsDF[["raceId", "points", "position"]]
    mergedDF = pd.merge(resultsDF, lap_timesDF[["milliseconds", "raceId"]], on="raceId")
    mergedDF = mergedDF.groupby(by="raceId", sort=True, as_index=False).agg({"milliseconds": lapStatisticals})
    for statistical in lapStatisticals:
        mergedDF["milliseconds", statistical] = mergedDF["milliseconds", statistical].transform(convertMillis)
    mergedDF.columns = mergedDF.columns.droplevel(1)
    mergedDF.columns = ["raceId"] + [statistical + "_lap_time" for statistical in lapStatisticals]
    return pd.merge(resultsDF, mergedDF, on = "raceId")

def AddPitStopsInformation(mergedDF, ID, pitStopsStatisticals = ("min", "max")):
    #Adds the desired statistical information (given in pitStopsStatisticals)
    #about the pit stops of a driver with a given ID
    #to mergedDF, which is an intermediate dataframe in our selection
    pit_stopsDF = pd.read_csv(databasesPath + "pit_stops.csv")
    pit_stopsDF = ChooseSpecificDriver(pit_stopsDF, ID)
    mergedWithPitStops = pd.merge(mergedDF, pit_stopsDF[["raceId", "duration","stop"]], on = "raceId")
    groupedByRaces = mergedWithPitStops.groupby(by = "raceId", as_index=True)
    pit_stops_statistics = groupedByRaces.agg({"stop" : "max", "duration": pitStopsStatisticals})
    pit_stops_statistics.columns = pit_stops_statistics.columns.droplevel(1)
    pit_stops_statistics.columns = ["number_of_stops"] + [statistical + "_pit_stop" for statistical in pitStopsStatisticals] #This allows for scalability
    mergedDF = pd.merge(mergedDF, pit_stops_statistics, on = "raceId")
    return mergedDF

def SeasonInDatabase(seasonYear):
    #Checks if a certain season exists in the seasons.csv file
    seasonDF = pd.read_csv(databasesPath + "seasons.csv")
    if seasonYear in seasonDF["year"].values:
        return True
    else:
        return False

def ConvertDFToJSON(DF, orient = "records"):
    return json.loads(DF.to_json(orient = orient))

app = FastAPI()

@app.get("/2.a/{year}")
def DriversBySeason(year : int = Path(None, description="The year of the desired season")):
    if not SeasonInDatabase(year):
        raise HTTPException(status_code=404, detail="The year does not exist in the seasons database")
    return ConvertDFToJSON(GetBestDriversOfSeason(year).reset_index())

@app.get("/2.b")
def SeasonsAllTimeRanking():
    return ConvertDFToJSON(GetAllTimeRanking().reset_index(1))

@app.get("/2.c/by-id/{ID}")
def DriverProfileByID(ID : int = Path(None, description="The ID of the desired driver")):
    if not IDExistsInDrivers(ID):
        raise HTTPException(status_code=404, detail="The ID does not exist in the drivers database")
    return ConvertDFToJSON(GetAllRacesOfDriver(ID))

@app.get("/2.c/by-name/{forename}/{surname}")
def DriverProfileByFullname(forename: str = Path(None, description="The forename of the driver"),
                            surname: str = Path(None, description="The surname of the driver")):
    ID = FindDriverId(forename, surname)
    if isinstance(ID, str):
        raise HTTPException(status_code=400, detail = ID)
    return ConvertDFToJSON(GetAllRacesOfDriver(ID))
