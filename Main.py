import pandas as pd

# DF is short for DataFrame
databasesPath = "Databases\\"

def convertMillis(millis):
    #This function gets time in milliseconds and returns the time in minutes:seconds:tenths format
    temp = (millis / 1000) % 60
    seconds = int(temp)
    minutes = int((millis / (1000 * 60)) % 60)
    tenths = int((temp - seconds) * 10)
    return str.format("{0}:{1}:{2}", str(minutes).zfill(2), str(seconds).zfill(2), tenths)

def LetUserChoosePrintingOptions():
    #This function is called before a dataframe is being printed out to the user
    #If the user chooses full print the dataframe will be fully printed
    #Otherwise, default printing options will be chosen
    print("Would you like to print the table fully? (y / n)")
    choice = input()
    if choice == "y":
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", None)
    elif choice == "n":
        pd.reset_option("display.max_rows")
        pd.reset_option("display.max_columns")

def QueryNumberOne():
    #Question 2.a
    print("Input the season (a year):")
    year = input()
    inputIsValid = True
    try:
        year = int(year)
    except:
        inputIsValid = False
    if inputIsValid:
        inputIsValid = SeasonIsValid(year)
    if inputIsValid:
        LetUserChoosePrintingOptions()
        print("The best drivers of the series:\n")
        print(GetBestDriversOfSeason(year))
    else:
        print("Invalid season. The season does not exist in the seasons database")

def SeasonIsValid(seasonYear):
    #Checks if a certain season exists in the seasons.csv file
    seasonDF = pd.read_csv(databasesPath + "seasons.csv")
    if seasonYear in seasonDF["year"].values:
        return True
    else:
        return False

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
    driversDF = pd.read_csv(databasesPath + "drivers.csv")
    racesDF = pd.read_csv(databasesPath + "races.csv")
    driver_standingsDF = pd.read_csv(databasesPath + "driver_standings.csv")
    if desiredYear is not None:
        racesDF = racesDF.loc[racesDF["year"] == desiredYear] #By filtering before merging, drivers who
        #did not participate in the season do not show up at all
    mergedDF = pd.merge(racesDF[["raceId", "year"]], driver_standingsDF[["raceId", "driverId", "wins"]], on="raceId")
    mergedDF = pd.merge(driversDF, mergedDF, on="driverId")
    return mergedDF

def QueryNumberTwo():
    #Code for question 2.b
    LetUserChoosePrintingOptions()
    print(GetAllTimeRanking())

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

def QueryNumberThree():
    #Code for question 2.c
    print("Would you like to enter fullname or id? (full / id)")
    choice = input()
    ID = None
    if choice == "full":
        print("Enter forename")
        forename = input()
        print("Enter surname")
        surname = input()
        ID = FindDriverId(forename, surname)
        if isinstance(ID, str):
            print(ID)
            ID = None
    elif choice == "id":
        print("enter id:")
        ID = input()
        try:
            ID = int(ID)
            if not IDExistsInDrivers(ID):
                ID = None
                print("There does not exist a driver with id " + str(ID) + " in the drivers database")
        except:
            print("id must be an integer")
            ID = None
    if ID is not None:
        LetUserChoosePrintingOptions()
        print(GetAllRacesOfDriver(ID))

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

def AddLapInformation(resultsDF, ID, lapStatisticals = ["min", "max", "mean"]):
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

def AddPitStopsInformation(mergedDF, ID, pitStopsStatisticals = ["min", "max"]):
    #Adds the desired statistical information (given in pitStopsStatisticals)
    # about the pit stops of a driver with a given ID
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

def Main():
    #Our program's entry point
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    queries = [QueryNumberOne, QueryNumberTwo, QueryNumberThree] #This allows for future scalability
    #If there is a new query, just add the query function to the queries list
    queryRange = range(1, len(queries) + 1)
    choiceString = "Choose the number of query: (" + str.join(" /", (str(i) for i in queryRange)) + ")"
    while True:
        print("Choose the number of query: (1 / 2 / 3)")
        choice = input()
        try:
            choice = int(choice)
            if choice not in queryRange:
                raise Exception()
        except:
            print("Invalid choice\n")
            continue
        queries[choice - 1]()

Main()