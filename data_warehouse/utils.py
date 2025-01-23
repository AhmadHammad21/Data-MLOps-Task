
import logging


def fetch_dimension_key(cursor, table, key_column, lookup_column, value):
    """
    Fetch the primary key of a dimension record. If it doesn't exist, insert it and fetch the new key.
    """
    cursor.execute(f"SELECT {key_column} FROM {table} WHERE {lookup_column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # Insert new record and fetch the new key
        cursor.execute(f"INSERT INTO {table} ({lookup_column}) VALUES (%s) RETURNING {key_column}", (value,))
        return cursor.fetchone()[0]


def transform_sales_data(cursor, sales_data):
    """
    Transform raw sales data (2D list) by replacing dimension values with foreign keys.
    """
    transformed_data = []
    failed = 0
    for record in sales_data:
        try:
            order_id = record[0]
            order_date = fetch_dimension_key(cursor, "Date_Dimension", "Date_Key", "Full_Date", record[1])
            ship_date = fetch_dimension_key(cursor, "Date_Dimension", "Date_Key", "Full_Date", record[2])
            customer_key = fetch_dimension_key(cursor, "Customer_Dimension", "Customer_Key", "Customer_ID", record[3])
            product_key = fetch_dimension_key(cursor, "Product_Dimension", "Product_Key", "Product_ID", record[4])
            location_key = fetch_dimension_key(cursor, "Location_Dimension", "Location_Key", "Postal_Code", record[5])
            ship_mode_key = fetch_dimension_key(cursor, "Ship_Mode_Dimension", "Ship_Mode_Key", "Ship_Mode", record[6])
            sales = record[7]

            # Append the transformed record
            transformed_data.append(
                (order_id, order_date, ship_date, customer_key, product_key, location_key, ship_mode_key, sales)
            )
        except:
            failed += 1
    logging.info(f"Number of failed records: {failed}")
    return transformed_data


def insert_fact_table(connection, cursor, fact_table, transformed_data):
    """
    Insert transformed data into the fact table.
    """
    cursor.executemany(f"""
        INSERT INTO {fact_table} 
        (Order_ID, Date_Key, Ship_Date_Key, Customer_Key, Product_Key, Location_Key, Ship_Mode_Key, Sales)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, transformed_data)

    connection.commit()