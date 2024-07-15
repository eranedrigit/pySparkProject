from src.app.vi_app import App, DataFrame
import pandas as pd


def q_1(app: App, df: DataFrame, output_path: str):
    df.createOrReplaceTempView("stocks")
    result_df = app.spark.sql("""WITH daily_returns AS (
                                    SELECT date, ticker, 
                                           (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)) / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS daily_return
                                    FROM stocks
                                )
                                SELECT date, AVG(daily_return) AS avg_daily_return
                                FROM daily_returns
                                GROUP BY date
                                ORDER BY date""")
    result_df.toPandas().to_csv(f"{output_path}/q_1_res.csv", index=False)
    result_df.show()


def q_2(app: App, df: DataFrame, output_path: str):
    df.createOrReplaceTempView("stocks")
    result_df = app.spark.sql("""SELECT ticker, AVG(close * volume) AS avg_traded_value
                                FROM stocks
                                GROUP BY ticker
                                ORDER BY avg_traded_value DESC
                                LIMIT 1""")
    result_df.toPandas().to_csv(f"{output_path}/q_2_res.csv", index=False)
    result_df.show()


def q_3(app: App, df: DataFrame, output_path: str):
    df.createOrReplaceTempView("stocks")
    result_df = app.spark.sql("""WITH daily_returns AS (
                                SELECT ticker, 
                                       (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)) / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS daily_return
                                FROM stocks
                            )
                            SELECT ticker, SQRT(252) * STDDEV(daily_return) AS annualized_volatility
                            FROM daily_returns
                            GROUP BY ticker
                            ORDER BY annualized_volatility DESC
                            LIMIT 1""")
    result_df.toPandas().to_csv(f"{output_path}/q_3_res.csv", index=False)
    result_df.show()


def q_4(app: App, df: DataFrame, output_path: str):
    df.createOrReplaceTempView("stocks")
    result_df = app.spark.sql("""WITH lagged_data AS (
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
    result_df.toPandas().to_csv(f"{output_path}/q_4_res.csv", index=False)
    result_df.show()


if __name__ == "__main__":
    app = App()
    try:
        input_path = "data/input/stock_prices.csv"
        output_path = "data/output"
        df = app.load_data(input_path)
        q_1(app, df, output_path)
        q_2(app, df, output_path)
        q_3(app, df, output_path)
        q_4(app, df, output_path)
        print("App jobs successfully finished")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        app.stop()
