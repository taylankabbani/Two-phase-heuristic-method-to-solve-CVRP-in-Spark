import findspark
findspark.init()
findspark.find()
import math
import pyspark
findspark.find()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("PySpark_Testing").getOrCreate()
sc = spark.sparkContext

class CVRP_SOLVER():
    def __init__(self, File_name, Number_of_vehicles, capacity_max, dist_max):
        self.data_RDD = sc.textFile(File_name).filter(lambda line: "Node_id" not in line)\
                .map(lambda x: (x.split('\t')[0],[float(item) for item in x.split('\t')[1:]])).persist()
        
        self.distance_rdd = self.get_distance_rdd()
        
        self.giant_route = self.NNM()
        
        self.Number_of_vehicles = Number_of_vehicles
        
        self.capacity_max = capacity_max
        
        self.dist_max = dist_max
        
        
        
    def get_distance_rdd(self):
        xy_list = self.data_RDD.map(lambda x: (x[1][0:3], x[0])).collect()
        distance_rdd = self.data_RDD.mapValues(lambda source: [(node[1],\
                                    math.sqrt((source[0]-node[0][0])**2 + (source[1]-node[0][1])**2),node[0][2])\
                                         for node in xy_list]).persist()
        return distance_rdd
        
    def NNM(self):
        '''
        Find the Giant Route for given the an RDD of the distance matrix.

        Starts from depot and find the nearest node from the current node based on their eculidean distances.

        Returns a RDD from the (key, value1,value2) shape, where the key is the node_id and the value is the 
        distance traveled to the node and the demand of boxes in that node

        The returned Rdd is the Giant Route
                                                    '''
        route_nodes = ['Depot']
        source_node = "Depot"
        demand = [float(0)]
        route_distance = [float(0)]
        number_of_nodes = self.distance_rdd.count()
        nodes_visited = 1

        while nodes_visited <= number_of_nodes:
            current_node = route_nodes[-1]
            current_distances =  self.distance_rdd.filter(lambda x: x[0] == current_node)\
                                    .flatMap(lambda x: x[1]).sortBy(lambda x: x[1]).collect()
            for node in current_distances:
                if node[0] not in route_nodes:
                    route_nodes.append(node[0])
                    route_distance.append(node[1])
                    demand.append(node[2])
                    break
                else:
                    continue
            nodes_visited += 1
#             print("{}\n".format(route_nodes))
        last_node_rdd = self.distance_rdd.filter(lambda x: x[0] == route_nodes[-1]).flatMap(lambda x: x[1])\
                        .filter(lambda x: x[0]=='Depot').collect()
        route_nodes.insert(len(route_nodes), last_node_rdd[0][0])
        route_distance.insert(len(route_distance), last_node_rdd[0][1])
        demand.insert(len(demand), float(0))

        output = sc.parallelize(zip(route_nodes, route_distance, demand))

        return output
    
    def solve(self):
        '''
        Takes the giant route Rdd and the distance matrix as input.
        '''
        total_net_profits = []
        distance_rdd = self.distance_rdd
        giant_route = self.giant_route.collect()
        num_vehicle = self.Number_of_vehicles
        max_capacity = self.capacity_max
        max_dist = self.dist_max
        clusters = []

        cluster = []
        capacity = 0
        dis = 0

        for node in giant_route:
            # Send a new vehicle if the capacity or dis is max
            if capacity+node[2] >= max_capacity or dis+node[1] >= max_dist:
                # add the route of the vehicle to the clusters
                clusters.append(cluster)
                # add net profit of the tour
                total_net_profits.append(-dis)
                # start a new route
                cluster = [node[0]]
                capacity = node[2] 
                dis = node[1]
            else:
                cluster.append(node[0])
                dis += node[1]
                capacity += node[2]

        # sending vehicle to the last node if not included:
        clusters.insert(len(clusters),[giant_route[-2][0]])

         # If number of vehicles usedexceed 10
        if len(clusters) > num_vehicle:
            print("More vehicles needed to solve the problem")
            return 

        #Adding distances from and to depot to the cost
        costs_from_depot = []
        for cluster in clusters:
            if cluster[0] != 'Depot':
                from_depot_cost = distance_rdd.filter(lambda x: x[0] == 'Depot').flatMap(lambda x: x[1])\
                        .filter(lambda x: x[0]==cluster[0]).collect()[0][1]
                costs_from_depot.append(-from_depot_cost)
            else:
                costs_from_depot.append(float(0))

        costs_to_depot = []
        for cluster in clusters:
            to_depot_cost = distance_rdd.filter(lambda x: x[0] == cluster[-1]).flatMap(lambda x: x[1])\
                        .filter(lambda x: x[0]=='Depot').collect()[0][1]
            costs_to_depot.append(-to_depot_cost)
            # Updating total net profit:
        total_net_profits.append(0)
        total_net_profits = [a + b + c for a, b, c in zip(costs_from_depot,costs_to_depot,total_net_profits)]

        # Adding depot to the begin and end of each vehicle's tour
        for tour in clusters:
            if tour[0] != "Depot":
                tour.insert(0,"Depot")
            tour.insert(len(tour), 'Depot')

        # printing the solutions:
        print("Number of Vehicle used: {}\n".format(len(clusters)))
        for i in range(len(clusters)):
            print("Net_profit: {} \ncustomers visited by vehicle {}: {}\n\n".format(total_net_profits[i],\
                                                                                    str(i+1),clusters[i]))
        print("\n\nTotal Net profits: {}".format(sum(total_net_profits)))
