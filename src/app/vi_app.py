from pyspark.sql import SparkSession, DataFrame


class App:
    def __init__(self):
        print("Building spark session...")
        self.spark = SparkSession.builder.appName("ViApp").getOrCreate()
        print("Spark session built!")

    def load_data(self, input_path: str) -> DataFrame:
        print("Loading data...")
        return self.spark.read.csv(input_path, header=True, inferSchema=True)

    def stop(self):
        self.spark.stop()
