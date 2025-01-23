-- Disable foreign key checks to avoid constraint violations
SET FOREIGN_KEY_CHECKS = 0;

-- Truncate each table
TRUNCATE TABLE Sales_Fact;
TRUNCATE TABLE Date_Dimension;
TRUNCATE TABLE Customer_Dimension;
TRUNCATE TABLE Product_Dimension;
TRUNCATE TABLE Location_Dimension;
TRUNCATE TABLE Ship_Mode_Dimension;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;
