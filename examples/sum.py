from sparkjq import SLURMCluster
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SLURM cluster
    cluster = SLURMCluster()

    # From here onwards you can use normal pyspark code
    # Only the master node will proceed here, workers will not

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Sum Example") \
        .getOrCreate()

    # Create a DataFrame with some data
    data = [(1,), (2,), (3,), (4,), (5,)]
    df = spark.createDataFrame(data, ["number"])

    # Calculate the sum of the numbers
    total_sum = df.agg({"number": "sum"}).collect()[0][0]
    print(f"The sum is: {total_sum}")

    # Stop the Spark session
    spark.stop()

    # Now you can shutdown the cluster

    # Shutdown the SLURM cluster
    cluster.shutdown()
