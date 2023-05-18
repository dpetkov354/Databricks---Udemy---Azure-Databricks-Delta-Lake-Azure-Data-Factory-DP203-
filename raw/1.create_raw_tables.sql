-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
                                           circuitRef STRING,
                                           name STRING,
                                           location STRING, 
                                           country STRING,
                                           lat DOUBLE,
                                           lng DOUBLE,
                                           alt INT,
                                           url STRING)
USING CSV
OPTIONS (path "/mnt/formula1dpdl1/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
                                        year INT,
                                        round INT,
                                        circuitId INT, 
                                        name STRING,
                                        date DATE,
                                        time TIMESTAMP,
                                        url STRING
                                        )
USING CSV
OPTIONS (path "/mnt/formula1dpdl1/raw/races.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT,
                                        constructorRef STRING,
                                        name STRING,
                                        nationality STRING,
                                        url STRING
                                       )
USING JSON
OPTIONS (path "/mnt/formula1dpdl1/raw/constructors.json");

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(driverId INT,
                                              driverRef STRING,
                                              number INT,
                                              code STRING,
                                              name STRUCT<forname:STRING, surname:STRING>,
                                              dob DATE,
                                              nationality STRING,
                                              url STRING
                                              )
USING JSON
OPTIONS (path "/mnt/formula1dpdl1/raw/drivers.json");

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(resultId INT,
                                          raceId INT,
                                          constructorId INT,
                                          driverId INT,
                                          number INT,
                                          grid INT,
                                          position INT,
                                          positionText STRING,
                                          positionOrder INT,
                                          point FLOAT,
                                          laps INT,
                                          time STRING,
                                          miliseconds INT,
                                          fastestlap INT,
                                          rank INT,
                                          fastestLapTime STRING,
                                          fastestLapSpeed STRING,
                                          statusId INT
                                          )
USING JSON
OPTIONS (path "/mnt/formula1dpdl1/raw/results.json");

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(raceId INT,
                                          driverId INT,
                                          stop INT,
                                          lap INT,
                                          time STRING,
                                          duration STRING,
                                          millisecond STRING
                                          )
USING JSON
OPTIONS (path "/mnt/formula1dpdl1/raw/pit_stops.json", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(raceId INT,
                                            driverId INT,
                                            lap INT,
                                            position INT,
                                            time STRING,
                                            milliseconds INT
                                            )
USING CSV
OPTIONS (path "/mnt/formula1dpdl1/raw/lap_times");

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(qualifyId INT,
                                             raceId INT,
                                             driverId INT,
                                             constructorId INT,
                                             number INT,
                                             position INT,
                                             q1 STRING,
                                             q2 STRING,
                                             q3 STRING
                                             )
USING json
OPTIONS (path "/mnt/formula1dpdl1/raw/qualifying", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

