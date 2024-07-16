import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
S3_BUCKET = "s3://aws-glue-eran-bucket"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_data_to_s3(df, key, file_name):
    try:
        result_path = f"{S3_BUCKET}/output_folder/{key}/{file_name}"
        logger.info(f"Loading df for {key} to destination -> {result_path}")
        df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv(result_path)
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    print("Building spark session...")
    sc = SparkContext()
    sc.setSystemProperty("spark.sql.legacy.timeParserPolicy", "LEGACY")
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    print("Spark session built!")
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    try:
        print("Spark session start Processing...")
        df = spark.read.csv(f"{S3_BUCKET}/input_data/stock_prices.csv", header=True, inferSchema=True)
        df.createOrReplaceTempView("stocks")
        #####
        result_df_1 = spark.sql("""WITH daily_returns AS (
                                        SELECT date, ticker, 
                                               (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)) / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS daily_return
                                        FROM stocks
                                    )
                                    SELECT date, AVG(daily_return) AS avg_daily_return
                                    FROM daily_returns
                                    GROUP BY date
                                    ORDER BY date""")
        print("Q1 results :")
        result_df_1.show()
        write_data_to_s3(result_df_1, "q_1_tbl", "q_1_results")
        ####
        result_df_2 = spark.sql("""SELECT ticker, AVG(close * volume) AS avg_traded_value
                                    FROM stocks
                                    GROUP BY ticker
                                    ORDER BY avg_traded_value DESC
                                    LIMIT 1""")
        print("Q2 results :")
        result_df_2.show()
        write_data_to_s3(result_df_2, "q_2_tbl", "q_2_results")
        ####
        result_df_3 = spark.sql("""WITH daily_returns AS (
                                    SELECT ticker, 
                                           (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)) / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS daily_return
                                    FROM stocks
                                )
                                SELECT ticker, SQRT(252) * STDDEV(daily_return) AS annualized_volatility
                                FROM daily_returns
                                GROUP BY ticker
                                ORDER BY annualized_volatility DESC
                                LIMIT 1""")
        print("Q3 results :")
        result_df_3.show()
        write_data_to_s3(result_df_3, "q_3_tbl", "q_3_results")
        ####
        result_df_4 = spark.sql("""WITH lagged_data AS (
                                        SELECT date, ticker, close, 
                                               LAG(close, 30) OVER (PARTITION BY ticker ORDER BY date) AS close_30_days_prior
                                        FROM stocks
                                    )
                                    SELECT date, ticker, 
                                           ((close - close_30_days_prior) / close_30_days_prior) * 100 AS pct_increase
                                    FROM lagged_data
                                    WHERE close_30_days_prior IS NOT NULL
                                    ORDER BY pct_increase DESC
                                    LIMIT 3""")
        print("Q4 results :")
        result_df_4.show()
        write_data_to_s3(result_df_4, "q_4_tbl", "q_4_results")
        ####
        print("Spark session successfully finished")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        print("Terminating Spark session....")
        spark.stop()
        job.commit()
