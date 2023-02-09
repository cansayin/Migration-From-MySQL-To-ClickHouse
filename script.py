import mysql.connector
import clickhouse_driver
from concurrent.futures import ThreadPoolExecutor

def migrate_data(offset, limit):
    # Connect to MySQL database
    mysql_conn = mysql.connector.connect(
        host="mysql_host",
        user="mysql_user",
        password="mysql_password",
        database="mysql_database"
    )
    
    mysql_cursor = mysql_conn.cursor()
    
    # Fetch data from MySQL
    mysql_query = "SELECT * FROM my_table LIMIT %s OFFSET %s"
    mysql_cursor.execute(mysql_query, (limit, offset))
    mysql_data = mysql_cursor.fetchall()
    
    # Connect to ClickHouse database
    clickhouse_conn = clickhouse_driver.Client(
        host="clickhouse_host",
        user="clickhouse_user",
        password="clickhouse_password",
        database="clickhouse_database"
    )
    
    # Insert data into ClickHouse
    clickhouse_query = "INSERT INTO my_table (column1, column2, column3) VALUES"
    for row in mysql_data:
        clickhouse_query += f"({row[0]}, '{row[1]}', {row[2]}),"
        
    clickhouse_query = clickhouse_query[:-1]
    clickhouse_conn.execute(clickhouse_query)
    
    # Close connections
    mysql_cursor.close()
    mysql_conn.close()
    clickhouse_conn.disconnect()
    
# Run migration with multiple threads
with ThreadPoolExecutor(max_workers=4) as executor:
    offset = 0
    limit = 10000
    for i in range(0, 100):
        executor.submit(migrate_data, offset, limit)
        offset += limit
