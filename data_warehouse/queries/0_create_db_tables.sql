-- Create the database if it doesn't already exist
CREATE DATABASE IF NOT EXISTS SalesDB;

-- Use the database
USE SalesDB;

-- Create the Date Dimension Table
CREATE TABLE IF NOT EXISTS Date_Dimension (
    Date_Key INT PRIMARY KEY AUTO_INCREMENT,
    Full_Date DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    Week INT NOT NULL,
    Day_Of_Week VARCHAR(15) NOT NULL
);

-- Create the Customer Dimension Table
CREATE TABLE IF NOT EXISTS Customer_Dimension (
    Customer_Key INT PRIMARY KEY AUTO_INCREMENT,
    Customer_ID VARCHAR(50) NOT NULL,
    Customer_Name VARCHAR(100) NOT NULL,
    Segment VARCHAR(50) NOT NULL
);

-- Create the Product Dimension Table
CREATE TABLE IF NOT EXISTS Product_Dimension (
    Product_Key INT PRIMARY KEY AUTO_INCREMENT,
    Product_ID VARCHAR(50) NOT NULL,
    Product_Name VARCHAR(200) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    Sub_Category VARCHAR(50) NOT NULL
);

-- Create the Location Dimension Table
CREATE TABLE IF NOT EXISTS Location_Dimension (
    Location_Key INT PRIMARY KEY AUTO_INCREMENT,
    Country VARCHAR(100) NOT NULL,
    Region VARCHAR(50) NOT NULL,
    State VARCHAR(50) NOT NULL,
    City VARCHAR(100) NOT NULL,
    Postal_Code VARCHAR(20) NOT NULL
);

-- Create the Ship Mode Dimension Table
CREATE TABLE IF NOT EXISTS Ship_Mode_Dimension (
    Ship_Mode_Key INT PRIMARY KEY AUTO_INCREMENT,
    Ship_Mode VARCHAR(50) NOT NULL
);

-- Create the Sales Fact Table
CREATE TABLE IF NOT EXISTS Sales_Fact (
    Fact_Key INT PRIMARY KEY AUTO_INCREMENT,
    Order_ID VARCHAR(50) NOT NULL,
    Date_Key INT,
    Ship_Date_Key INT,
    Customer_Key INT,
    Product_Key INT,
    Location_Key INT,
    Ship_Mode_Key INT,
    Sales DECIMAL(10, 10) NOT NULL,
    FOREIGN KEY (Date_Key) REFERENCES Date_Dimension(Date_Key),
    FOREIGN KEY (Ship_Date_Key) REFERENCES Date_Dimension(Date_Key),
    FOREIGN KEY (Customer_Key) REFERENCES Customer_Dimension(Customer_Key),
    FOREIGN KEY (Product_Key) REFERENCES Product_Dimension(Product_Key),
    FOREIGN KEY (Location_Key) REFERENCES Location_Dimension(Location_Key),
    FOREIGN KEY (Ship_Mode_Key) REFERENCES Ship_Mode_Dimension(Ship_Mode_Key)
);
