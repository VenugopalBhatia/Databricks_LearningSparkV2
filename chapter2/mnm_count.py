from pyspark.sql import SparkSession,functions
import sys
if __name__ == '__main__':
    if len(sys.argv)!=2:
        raise(f"Error using file %s" %(sys.stderr))
        sys.exit(-1)

spark = SparkSession \
        .builder\
        .appName("MnMCount")\
        .getOrCreate()
    

mnm_df = spark\
        .read\
        .format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(sys.argv[-1])

count_mnm_df = (mnm_df
                .select("State","Color","Count")
                .groupBy("State","Color")
                .agg(functions.sum("Count").alias("Total"))
                .orderBy("Total",ascending = False))

count_mnm_df.show(n=60,truncate=False)
print("Total Rows %d"%(count_mnm_df.count()))

ca_count_mnm_df = count_mnm_df.where(count_mnm_df["State"] == "CA")

ca_count_mnm_df.show()

print("Total Rows %d"%(ca_count_mnm_df.count()))

spark.stop()