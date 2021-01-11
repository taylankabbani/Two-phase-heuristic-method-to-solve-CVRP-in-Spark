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
