import mysql.connector

# Database connection
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin",
        database="salesdb",
    )

connection = get_connection()
cursor = connection.cursor()

def insert_dimensions():
    # Insert data into Date_Dimension
    date_dimension_data = [
        ('2025-01-01', 2025, 1, 1, 1, 'Monday'),
        ('2025-01-02', 2025, 1, 1, 2, 'Tuesday'),
        ('2025-01-03', 2025, 1, 1, 3, 'Wednesday')
    ]
    cursor.executemany("""
        INSERT INTO Date_Dimension (Full_Date, Year, Quarter, Month, Week, Day_Of_Week)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, date_dimension_data)

    # Insert data into Customer_Dimension
    customer_dimension_data = [
        ('C001', 'Alice Smith', 'Consumer'),
        ('C002', 'Bob Johnson', 'Corporate'),
        ('C003', 'Charlie Brown', 'Small Business')
    ]
    cursor.executemany("""
        INSERT INTO Customer_Dimension (Customer_ID, Customer_Name, Segment)
        VALUES (%s, %s, %s)
    """, customer_dimension_data)

    # Insert data into Product_Dimension
    product_dimension_data = [
        ('P001', 'Laptop', 'Electronics', 'Computers'),
        ('P002', 'Smartphone', 'Electronics', 'Phones'),
        ('P003', 'Desk', 'Furniture', 'Office Furniture')
    ]
    cursor.executemany("""
        INSERT INTO Product_Dimension (Product_ID, Product_Name, Category, Sub_Category)
        VALUES (%s, %s, %s, %s)
    """, product_dimension_data)

    # Insert data into Location_Dimension
    location_dimension_data = [
        ('USA', 'West', 'California', 'Los Angeles', '90001'),
        ('USA', 'East', 'New York', 'New York City', '10001'),
        ('Canada', 'Ontario', 'Ontario', 'Toronto', 'M5H 2N2')
    ]
    cursor.executemany("""
        INSERT INTO Location_Dimension (Country, Region, State, City, Postal_Code)
        VALUES (%s, %s, %s, %s, %s)
    """, location_dimension_data)

    # Insert data into Ship_Mode_Dimension
    ship_mode_dimension_data = [
        ('Air',),
        ('Sea',),
        ('Ground',)
    ]
    cursor.executemany("""
        INSERT INTO Ship_Mode_Dimension (Ship_Mode)
        VALUES (%s)
    """, ship_mode_dimension_data)

    connection.commit()
    print("Dimension data inserted successfully.")


# def insert_fact_table():

#     # Insert data into Sales_Fact
#     sales_fact_data = [
#         ('ORD001', 10, 10, 10, 10, 10, 4, 100.50),  # Use valid foreign key IDs
#         ('ORD002', 11, 11, 11, 11, 11, 5, 250.75),
#         ('ORD003', 12, 12, 12, 12, 12, 6, 180.00)
#     ]
#     cursor.executemany("""
#         INSERT INTO Sales_Fact (Order_ID, Date_Key, Ship_Date_Key, Customer_Key, Product_Key, Location_Key, Ship_Mode_Key, Sales)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#     """, sales_fact_data)

#     connection.commit()
#     cursor.close()
#     connection.close()
#     print("Fact table data inserted successfully.")

def fetch_dimension_keys(cursor, table_name, key_column, id_column):
    """
    Fetch all keys from a dimension table and return a lookup dictionary.
    :param cursor: Database cursor.
    :param table_name: Name of the dimension table.
    :param key_column: Primary key column in the table.
    :param id_column: ID column to map raw data.
    :return: Dictionary mapping ID to the primary key.
    """
    cursor.execute(f"SELECT {id_column}, {key_column} FROM {table_name}")
    return {row[0]: row[1] for row in cursor.fetchall()}


def insert_sales_fact(cursor, sales_fact_data, dimension_lookups):
    """
    Insert data into the Sales_Fact table using valid foreign keys from dimension lookups.
    :param cursor: Database cursor.
    :param sales_fact_data: List of raw sales fact data.
    :param dimension_lookups: Dictionary of dimension lookups.
    """
    transformed_data = []
    for record in sales_fact_data:
        transformed_record = (
            record[0],  # Order_ID
            dimension_lookups['Date_Dimension'].get(record[1], None),  # Date_Key
            dimension_lookups['Date_Dimension'].get(record[2], None),  # Ship_Date_Key
            dimension_lookups['Customer_Dimension'].get(record[3], None),  # Customer_Key
            dimension_lookups['Product_Dimension'].get(record[4], None),  # Product_Key
            dimension_lookups['Location_Dimension'].get(record[5], None),  # Location_Key
            dimension_lookups['Ship_Mode_Dimension'].get(record[6], None),  # Ship_Mode_Key
            record[7]  # Sales
        )
        if None not in transformed_record[1:7]:  # Ensure all foreign keys are valid
            transformed_data.append(transformed_record)
    
    # Insert transformed data
    cursor.executemany("""
        INSERT INTO Sales_Fact (Order_ID, Date_Key, Ship_Date_Key, Customer_Key, Product_Key, Location_Key, Ship_Mode_Key, Sales)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, transformed_data)

# Main ETL Pipeline
def etl_pipeline():

    # Step 1: Fetch dimension keys
    dimension_lookups = {
        'Date_Dimension': fetch_dimension_keys(cursor, 'Date_Dimension', 'Date_Key', 'Full_Date'),
        'Customer_Dimension': fetch_dimension_keys(cursor, 'Customer_Dimension', 'Customer_Key', 'Customer_ID'),
        'Product_Dimension': fetch_dimension_keys(cursor, 'Product_Dimension', 'Product_Key', 'Product_ID'),
        'Location_Dimension': fetch_dimension_keys(cursor, 'Location_Dimension', 'Location_Key', 'Postal_Code'),
        'Ship_Mode_Dimension': fetch_dimension_keys(cursor, 'Ship_Mode_Dimension', 'Ship_Mode_Key', 'Ship_Mode')
    }

    # Step 2: Prepare raw sales fact data
    raw_sales_fact_data = [
        ('ORD001', '2025-01-01', '2025-01-02', 'CUST001', 'PROD001', 'LOC001', 'Air', 100.50),
        ('ORD002', '2025-01-03', '2025-01-04', 'CUST002', 'PROD002', 'LOC002', 'Express', 250.75),
        ('ORD003', '2025-01-05', '2025-01-06', 'CUST003', 'PROD003', 'LOC003', 'Standard', 180.00)
    ]

    # Step 3: Insert data into fact table
    insert_sales_fact(cursor, raw_sales_fact_data, dimension_lookups)

    connection.commit()
    cursor.close()
    connection.close()
    print("ETL pipeline executed successfully.")

if __name__ == "__main__":
    insert_dimensions()  # Insert data into dimension tables
    etl_pipeline()  # Insert data into the fact table
