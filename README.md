**BigData Analysis Project - Weather** 

This project will have you perform Data Analysis and processing using Apache Spark. 
The Project will use the weather dataset from https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/ .
This project will use only 16 years of data ( 2000 - 2018) for all the stations starting with US and elements TMAX, TMIN. 

The following shell script is used to generate the dataset:  
```
for i in `seq 2000 2018`
do  
wget https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/${i}.csv.gz  
gzip -cd ${i}.csv.gz  | grep -e TMIN -e TMAX | grep ^US > ${i}.csv  
done 
```

The following information serves as a definition of each field in one line of data covering one station-day.  
Each field   described below is separated by a comma ( , ) and follows the order presented in this document.  
ID = 11 character station identification code  
YEAR/MONTH/DAY = 8 character date in YYYYMMDD format (e.g. 19860529 = May 29, 1986) ELEMENT = 4 character indicator of element type  
DATA VALUE = 5 character data value for ELEMENT  
M-FLAG = 1 character Measurement Flag  
Q-FLAG = 1 character Quality Flag  
S-FLAG = 1 character Source Flag  
OBS-TIME = 4-character time of observation in hour-minute format (i.e. 0700 =7:00 am)  
  
See section III of the GHCN-Daily ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt file for an explanation of ELEMENT   codes and their units as well as the M-FLAG, Q-FLAGS and S-FLAGS.  
The OBS-TIME field is populated with the observation times contained in NOAA/NCDC’s Multinetwork Metadata System (MMS).  
  
**In particular, it will have you build Apache Spark that yields the following analysis:**  
1. Average TMIN, TMAX for each year excluding abnormalities or missing data  
2. Maximum TMAX, Minimum TMIN for each year excluding abnormalities or missing data  
3. 5 hottest , 5 coldest weather stations for each year excluding abnormalities or missing data  
4. Hottest and coldest day and corresponding weather stations in the entire dataset  
