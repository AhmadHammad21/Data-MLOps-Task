import pandas as pd


class ETL_Pipeline():
    def __init__(self):
        pass

    def process(self):
        
        df = self.extract()

        self.transform(df)

        self.load()

    def extract(self) -> pd.DataFrame:
        df = pd.read_csv("data/Sales.csv")

        return df

    def transform(self, df: pd.DataFrame):
        # Date Dimension Table
        date_dimension = pd.DataFrame({
            "Full_Date": pd.to_datetime(df["Order Date"]),
            "Year": pd.to_datetime(df["Order Date"]).dt.year,
            "Quarter": pd.to_datetime(df["Order Date"]).dt.quarter,
            "Month": pd.to_datetime(df["Order Date"]).dt.month,
            "Week": pd.to_datetime(df["Order Date"]).dt.isocalendar().week,
            "Day_Of_Week": pd.to_datetime(df["Order Date"]).dt.day_name(),
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

        # Sales Fact Table
        sales_fact = pd.DataFrame({
            "Order_ID": df["Order ID"],
            "Date_Key": pd.factorize(df["Order Date"])[0] + 1,
            "Ship_Date_Key": pd.factorize(df["Ship Date"])[0] + 1,
            "Customer_Key": pd.factorize(df["Customer ID"])[0] + 1,
            "Product_Key": pd.factorize(df["Product ID"])[0] + 1,
            "Location_Key": pd.factorize(df["City"] + df["State"] + df["Postal Code"].astype(str))[0] + 1,
            "Ship_Mode_Key": pd.factorize(df["Ship Mode"])[0] + 1,
            "Sales": df["Sales"],
        })

        return 

    def load(self):
        pass