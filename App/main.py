import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sin, cos, radians, min, max, mean, stddev, avg, count, lit, acos
  
def find_vehicles(spark, latitude_point, longitude_point, proximity_size, time_start, time_end):
    
    start_time = time.time()
    data = spark.read.option("header", "true").option("delimiter", ",").csv('hdfs://namenode:9000/data/fcd.csv', inferSchema=True)

    data_filter = data.filter((col("timestep_time") >= lit(time_start)) & (col("timestep_time") <= lit(time_end)))
    
    df_with_distance = data_filter.withColumn(
        "distance",
        6371 * acos(
            sin(radians(lit(latitude_point))) * sin(radians(col("vehicle_x"))) + 
            cos(radians(lit(latitude_point))) * cos(radians(col("vehicle_x"))) * cos(radians(col("vehicle_y")) - radians(lit(longitude_point)))
        )
    )

    df_filtered = df_with_distance.filter(col("distance") < proximity_size).dropDuplicates(["vehicle_id"])
    
    print(f"Broj jedinstvenih vozila u okoloni: {df_filtered.count()}")  
    df_filtered.show()

    return time.time() - start_time
    
def air_fuel_task(spark, task, time_start, time_end):
    
    start_time = time.time()

    data = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://namenode:9000/data/emissions.csv", inferSchema=True)

    data_filter = data.filter((col("timestep_time") >= time_start) & (col("timestep_time") <= time_end))
    
    columns = []

    if task == 'z':
    # zagadjenje
        columns = ["vehicle_CO", "vehicle_CO2", "vehicle_HC", "vehicle_NOx", "vehicle_PMx", "vehicle_noise"]
    elif task == 'g':
    # gorivo
        columns = ["vehicle_electricity", "vehicle_fuel"]
    
    for col_name in columns:
        data_filter.groupBy("vehicle_lane").agg(
                count(col(col_name)).alias(f"count_{col_name}"),
                min(col(col_name)).alias(f"min_{col_name}"),
                max(col(col_name)).alias(f"max_{col_name}"),
                mean(col(col_name)).alias(f"mean_{col_name}"),
                avg(col(col_name)).alias(f"avg_{col_name}"),
                stddev(col(col_name)).alias(f"sddev_{col_name}")
            ).show()

    return time.time() - start_time
         
 
if __name__ == "__main__":

    ts = time.time()
    args = sys.argv
    print(args)

    spark = SparkSession.builder.appName(args[1]).master(args[2]).getOrCreate()

    t=0
    if args[3] == '1':
        print("task 1")
        t = find_vehicles(spark,float(args[5]),float(args[4]),float(args[6]), float(args[7]), float(args[8]))

    elif args[3] == '2':
        print("task 2")
        t = air_fuel_task(spark, args[4],float(args[5]),float(args[6]))

    spark.stop()
    
    print(f"Vreme izvrsenja task{args[3]}: {t}")
    print(f"Vreme izvrsenja aplikacije: {time.time()-ts}")

# root
#  |-- timestep_time: double (nullable = true)
#  |-- vehicle_CO: double (nullable = true)
#  |-- vehicle_CO2: double (nullable = true)
#  |-- vehicle_HC: double (nullable = true)
#  |-- vehicle_NOx: double (nullable = true)
#  |-- vehicle_PMx: double (nullable = true)
#  |-- vehicle_angle: double (nullable = true)
#  |-- vehicle_eclass: string (nullable = true)
#  |-- vehicle_electricity: double (nullable = true)
#  |-- vehicle_fuel: double (nullable = true)
#  |-- vehicle_id: string (nullable = true)
#  |-- vehicle_lane: string (nullable = true)
#  |-- vehicle_noise: double (nullable = true)
#  |-- vehicle_pos: double (nullable = true)
#  |-- vehicle_route: string (nullable = true)
#  |-- vehicle_speed: double (nullable = true)
#  |-- vehicle_type: string (nullable = true)
#  |-- vehicle_waiting: double (nullable = true)
#  |-- vehicle_x: double (nullable = true)
#  |-- vehicle_y: double (nullable = true)