# Two Phase Heuristic Method to Solve CVRP in Spark

## Requirements
MapReduce programming model is used to design the algorithm, which allows
us to perform parallel processing across Big Data using a large number of
nodes (multiple computers).
Apache spark is used in Python3 to implement the MapReduce framework of
the algorithm. The following are required to run the algorithm:
* Apache Spark
* Java Development Kit(JDK)
* SBT
* Scala
* Python3

## Benchmark
To measure the performance of solving CVRP using the proposed heuristic
method (Route-first-cluster-second method) an example of CVRP with in-
stances has been solved by the proposed method and with VRP [spreadsheet
solver](https://github.com/taylankabbani/Two-phase-heuristic-method-to-solve-CVRP-in-Spark/blob/master/VRP_Spreadsheet_Solver_v3.02_HW.xlsm)

## Problem example
A humanitarian relief agency has to distribute boxes that contain emergency
supplies to serve the displaced people that are in need of food. The sup-
plies arrive at the port, where there is a depot, and then distributed to the
locations in need. The demand of each location (in terms of boxes of food
needed) is provided. A 
eet of 10 capacitated trucks can be used to deliver
the food and the limit on the total distance for each truck is 300 km.
Data instances are provided as [CSV file](https://github.com/taylankabbani/Two-phase-heuristic-method-to-solve-CVRP-in-Spark/blob/master/CVRP_instances)
