import pandas as pd
import logging
import mysql.connector
from utils import transform_sales_data, insert_fact_table


class ETL_Pipeline():
    def __init__(self, file_path: str):
        self.file_path = file_path
        # Database connection
        self.connection = mysql.connector.connect(
                host="localhost",
                user="admin",
                password="admin",
                database="salesdb",
            )
        self.cursor = self.connection.cursor(buffered=True)

    def process(self):
        
        df = self.extract()

        dfs_list, sales_facts = self.transform(df)

        self.load(dfs_list, sales_facts)

    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path)

        return df

    def transform(self, df: pd.DataFrame):

        df = df.dropna().reset_index(drop=True)
        temp_df = pd.DataFrame(columns=['date_column'])

        one_dates_list = list(df['Order Date'].unique()) + list(df['Ship Date'].unique())
        temp_df['date_column'] = one_dates_list
        
        temp_df['date_column'] = pd.to_datetime(temp_df["date_column"], format="mixed")
        df['Order Date'] = pd.to_datetime(df["Order Date"], format="mixed")

        date_dimension = pd.DataFrame({
            "Full_Date": temp_df["date_column"],
            "Year": temp_df["date_column"].dt.year,
            "Quarter": temp_df["date_column"].dt.quarter,
            "Month": temp_df["date_column"].dt.month,
            "Week": temp_df["date_column"].dt.isocalendar().week,
            "Day_Of_Week": temp_df["date_column"].dt.day_name(),
        }).drop_duplicates().reset_index(drop=True)

        # Customer Dimension Table
        customer_dimension = df[["Customer ID", "Customer Name", "Segment"]].drop_duplicates().reset_index(drop=True)
        customer_dimension.rename(columns={
            "Customer ID": "Customer_ID",
            "Customer Name": "Customer_Name"
        }, inplace=True)

        # Product Dimension Table
        product_dimension = df[["Product ID", "Product Name", "Category", "Sub-Category"]].drop_duplicates().reset_index(drop=True)
        product_dimension.rename(columns={
            "Product ID": "Product_ID",
            "Product Name": "Product_Name",
            "Sub-Category": "Sub_Category"
        }, inplace=True)

        # Location Dimension Table
        location_dimension = df[["Country", "Region", "State", "City", "Postal Code"]].drop_duplicates().reset_index(drop=True)
        location_dimension.rename(columns={"Postal Code": "Postal_Code"}, inplace=True)

        # Ship Mode Dimension Table
        ship_mode_dimension = df[["Ship Mode"]].drop_duplicates().reset_index(drop=True)
        ship_mode_dimension.rename(columns={"Ship Mode": "Ship_Mode"}, inplace=True)

        df["Order Date"] = pd.to_datetime(df["Order Date"]).dt.strftime('%Y-%m-%d')
        df["Ship Date"] = pd.to_datetime(df["Ship Date"], format="mixed").dt.strftime('%Y-%m-%d')

        sales_facts = df[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID',
            'Postal Code', 'Ship Mode', 'Sales']].drop_duplicates()

        dfs_list = [
            date_dimension, customer_dimension, product_dimension, 
            location_dimension, ship_mode_dimension
        ]

        return dfs_list, sales_facts

    def load(self, dfs_list: list, sales_facts: pd.DataFrame):
        
        self._load_dimension_tables(dfs_list)

        self._load_sales_fact_table(sales_facts)

    def _load_df_to_table(self, query: str, df: pd.DataFrame):
        # Prepare data for insertion
        data_to_insert = df.values.tolist()

        # Execute the query using executemany
        self.cursor.executemany(query, data_to_insert)
        self.connection.commit()
        logging.info("Done!")

    def _load_dimension_tables(self, dfs_list: list):

        insert_queries_list = [
            """
                INSERT INTO Date_Dimension (Full_Date, Year, Quarter, Month, Week, Day_Of_Week)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
            """
                INSERT INTO Customer_Dimension (Customer_ID, Customer_Name, Segment)
                VALUES (%s, %s, %s)
            """,
            """
                INSERT INTO Product_Dimension (Product_ID, Product_Name, Category, Sub_Category)
                VALUES (%s, %s, %s, %s)
            """,
            """
                INSERT INTO Location_Dimension (Country, Region, State, City, Postal_Code)
                VALUES (%s, %s, %s, %s, %s)
            """,
            """
                INSERT INTO Ship_Mode_Dimension (Ship_Mode)
                VALUES (%s)
            """
        ]

        # dimenions
        for query, df_ in zip(insert_queries_list, dfs_list):
            self._load_df_to_table(query, df_)


    def _load_sales_fact_table(self, sales_facts: pd.DataFrame) -> None:

        raw_sales_fact_data = sales_facts.values.tolist()
        transformed_sales_data = transform_sales_data(self.cursor, raw_sales_fact_data)
        insert_fact_table(self.connection, self.cursor, "Sales_Fact", transformed_sales_data)



if __name__ == "__main__":
    # Task 1: ETL Ingestion and preprocessing Pipeline and Loading it a Data Warehouse
    data_file_file = "data/Sales.csv"
    pipline = ETL_Pipeline(data_file_file).process()