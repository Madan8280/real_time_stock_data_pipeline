import snowflake.connector.connect


conn = snowflake.connector.connect(
    user="Slowflake_password",
    password="snowflake_password",
    account="account_id",
    warehouse="computer_wh",
    database="stock_db",
    schema="public"
)


cs = conn.cursor()
cs.execute(""" CREATE TABLE IF NOT EXISTS stock_prices (
symbol STRING, timestamp String, open Flaot, high Float,
low Float, close FLOAT, volume INT ) """
           )
cs.close()
conn.close()
