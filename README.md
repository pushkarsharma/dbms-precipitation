# dbms-precipitation

<h2>Execution</h2>

Execute the JAR with suffix '-with-dependencies'. The JAR files have been compiled with Maven support in Eclipse IDE. Execute the Spark job using the run.sh BASH script in the project folder. Following arguments are required for the execution:

Arg 1:  Folder path to locations file<br>
Arg 2:  Folder path to recordings file<br>
Arg 3:  Folder path to output<br>

Example: `sh run.sh /FakePath1/Locations/ /FakePath2/Recordings/ /Fakepath3/Output/`


<h2>Description</h2>

SparkSQL is used to compute the results. The following steps were performed to compute the results:

1. `STATION_RECORDS`: The Locations dataset (`WEATHER_STATIONS`) is joined with the Recordings (`RECORDS`) on `USAF` and `STN`. Then the results is grouped by `STATE, MNTH` while taking the average of the Precipitation.
2. `MIN_RECORDS`: From table in `STATION_RECORDS`, minimum average precipitation value is extracted for each state.
3. `MAX_RECORDS`: From table in `STATION_RECORDS`, maximum average precipitation value is extracted for each state.
4. `MIN_STATE_RECORDS`: Table `STATION_RECORDS` and `MIN_RECORDS` are joined on `STATE, AVG_PRCP` to extract the minimum average precipitation month.
5. `MAX_STATE_RECORDS`: Table `STATION_RECORDS` and `MAX_RECORDS` are joined on `STATE, AVG_PRCP` to extract the maximum average precipitation month.
6. `MIN_MAX_RECORDS`: Table `MIN_STATE_RECORDS` and `MAX_STATE_RECORDS` are joined on `STATE` to keep the minimum and maximum month and average precipitation values in the same table.
7. `AVG_DIFFERENCE`: Difference is calculated between the minimum and maximum average values.

Runtime: `15 mins`
