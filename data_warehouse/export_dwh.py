import pandas as pd
import mysql.connector

# Function to export a table to a Parquet file
def export_table_to_parquet(table_name, output_file, connection):
    """
    Export a database table to a Parquet file.
    :param table_name: Name of the table to export.
    :param output_file: Path to the output Parquet file.
    :param connection: Database connection object.
    """
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    df.to_parquet(output_file, engine='pyarrow', index=False)
    print(f"Table '{table_name}' exported to {output_file}")

# Main script
try:
    # Establish database connection
    connection = mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin",
        database="salesdb",
    )

    # List of tables to export
    tables_to_export = ["Date_Dimension", "Customer_Dimension", "Product_Dimension",
                        "Location_Dimension", "Ship_Mode_Dimension", "Sales_Fact"]

    # Directory to save Parquet files
    output_dir = "result/datawarehouse_exports/"

    # Export each table to a Parquet file
    for table in tables_to_export:
        output_path = f"{output_dir}{table}.parquet"
        export_table_to_parquet(table, output_path, connection)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if 'connection' in locals() and connection:
        connection.close()
