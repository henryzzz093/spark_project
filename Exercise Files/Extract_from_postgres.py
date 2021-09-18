##import required libraries
import pyspark

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "/Users/henryzou1/Downloads/postgresql-42.2.23.jar") \
   .getOrCreate()


##read table from db using spark jdbc
trans_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/test") \
   .option("dbtable", "test.transactions") \
   .option("user", "postgres") \
   .option("password", "admin") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the transaction_df
print(trans_df.show())




