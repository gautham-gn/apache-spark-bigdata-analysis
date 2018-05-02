from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
from datetime import datetime as date

if __name__ == "__main__":

    sc = SparkContext(appName="SparkDatasetAnalysis")
    sqlContext = SQLContext(sc)
    years = list(range(2000, 2019))
	
    #Yearly Analysis
    for year in years:
        lines = sc.textFile("dataset/%s.csv" % year)
        parts = lines.map(lambda l: l.split(","))
        obs = parts.map(lambda p: Row(station=p[0], date=p[1], measurement=p[2], value=float(p[3]) * 0.1, m=p[4], q=p[5], s=p[6], time=p[7]))

        df = sqlContext.createDataFrame(obs)
        df = df.filter(df.q == '')

        print("\n")
        print("Stats for year %s" % year)
        print("###################")

        #Average Min Temperature
        print("##########Average Min Temperature##########")
        tmin = df.filter(df.measurement=="TMIN").groupBy().avg("value").first()

        print("Average TMIN = %0.2f C" % (tmin["avg(value)"]))
        #Average Max Temperature
        print("##########Average Max Temperature##########")
        tmax = df.filter(df.measurement=="TMAX").groupBy().avg("value").first()

        print("Average TMAX = %0.2f C" % (tmax["avg(value)"]))

        #Min Temperature
        print("##########Minimum Temperature##########")
        min_tmax = df.filter(df.measurement=="TMIN").sort(func.asc("value")).first()

        print("Minimum TMIN = %0.2f C" % (min_tmax.value))

        #Maximum Temperature
        print("##########Maximum Temperature##########")
        max_tmax = df.filter(df.measurement=="TMAX").sort(func.desc("value")).first()

        print("Maximum TMAX = %0.2f C" % (max_tmax.value))

        # Average Hottest Stations
        print("##########Hottest Stations Based on Average##########")
        hotStations = df.filter(df.measurement == "TMAX").groupBy(df.station).agg(func.avg("value")).sort(func.desc("avg(value)")).limit(5).collect()

        sta = 1
        for stations in hotStations:
            print("Hottest Station %s: %s - Average Max Temperature = %0.2f C" % (sta, stations.station, float(stations["avg(value)"])))
            sta = sta + 1

        # Average Coldest Stations
        print("##########Coldest Stations Based on Average##########")
        coldStations = df.filter(df.measurement == "TMIN").groupBy(df.station).agg(func.avg("value")).sort(func.asc("avg(value)")).limit(5).collect()

        sta = 1
        for stations in coldStations:
            print("Coldest Station %s: %s - Average Min Temperature = %0.2f C" % (sta, stations.station, float(stations["avg(value)"])))
            sta = sta + 1
            
        # Maximum Hottest Stations
        print("##########Hottest Stations Based on Temperature##########")
        hotStations = df.filter(df.measurement == "TMAX").groupBy(df.station).agg(func.max("value")).sort(func.desc("max(value)")).limit(5).collect()

        sta = 1
        for stations in hotStations:
            print("Hottest Station %s: %s - Highest Max Temperature = %0.2f C" % (sta, stations.station, float(stations["max(value)"])))
            sta = sta + 1
        
        #Coldest Stations
        print("##########Coldest Stations Based on Temperature##########")
        coldStations = df.filter(df.measurement == "TMIN").groupBy(df.station).agg(func.min("value")).sort(func.asc("min(value)")).limit(5).collect()

        sta = 1
        for stations in coldStations:
            print("Coldest Station %s: %s - Lowest Min Temperature = %0.2f C" % (sta, stations.station, float(stations["min(value)"])))
            sta = sta + 1
    
    #Entire Dataset Analysis
    print("\n")
    print("###########################################")
    print("##########Entire Dataset Analysis##########")
    print("###########################################")
    lines = sc.textFile("dataset/20*.csv")
    parts = lines.map(lambda l: l.split(","))
    obs = parts.map(lambda p: Row(station=p[0], date=p[1], measurement=p[2], value=float(p[3]) * 0.1, m=p[4], q=p[5], s=p[6], time=p[7]))

    df = sqlContext.createDataFrame(obs)
    df = df.filter(df.q == '')
    print("\n")
    print("---------------------------------------------")
    print("Coldest Day and Corresponding Weather Station")
    print("---------------------------------------------")
    
    coldStation = df.filter(df.measurement == "TMIN").groupBy(df.station, df.date).min("value").sort(func.asc("min(value)")).first()
    
    print("Coldest Temperature has been observed on %s in City %s with a temperature of %0.2f C\n" % (coldStation.date, coldStation.station, float(coldStation["min(value)"])))
    
    print("\n")
    print("---------------------------------------------")
    print("Hottest Day and Corresponding Weather Station")
    print("---------------------------------------------")
    
    hotStation = df.filter(df.measurement == "TMAX").groupBy(df.station, df.date).max("value").sort(func.desc("max(value)")).first()
    
    print("Hottest Temperature has been observed on %s in City %s with a temperature of %0.2f C\n" % (hotStation.date, hotStation.station, float(hotStation["max(value)"])))