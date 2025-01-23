import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt


# Main script
try:
    # Establish database connection
    connection = mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin",
        database="salesdb",
    )
    cursor = connection.cursor(buffered=True)

    cursor.execute(f"""
        WITH ORDERS_SUM AS (
            SELECT Order_ID, SUM(Sales) as total_sales
            FROM sales_fact
            GROUP BY Order_ID
        )

        SELECT AVG(total_sales)
        FROM ORDERS_SUM 
        """)
    result = cursor.fetchone()
    cursor = connection.cursor(buffered=True)
    print(f"The Average Total Value of an Order is: {result[0]:.2f}")

    # Task 2
    cursor.execute(f"""
        SELECT 
        DATE_FORMAT(Full_Date, '%Y-%m') AS Month,
        AVG(Sales) AS Average_Revenue
        FROM 
            Sales_Fact sf
        JOIN 
            Date_Dimension dd
        ON 
            sf.Date_Key = dd.Date_Key
        GROUP BY 
            DATE_FORMAT(Full_Date, '%Y-%m')
        ORDER BY 
            Month;
        """)
    result = cursor.fetchall()

    result_df = pd.DataFrame(result, columns=['Month', 'Average_Revenue'])

    # Convert 'Month' to datetime for better handling
    result_df["Month"] = pd.to_datetime(result_df["Month"])

    # Plot the data
    plt.figure(figsize=(10, 6))
    plt.plot(result_df["Month"], result_df["Average_Revenue"], marker='o', linestyle='-', color='b')
    plt.title("Average Monthly Revenue", fontsize=16)
    plt.xlabel("Month", fontsize=12)
    plt.ylabel("Average Revenue", fontsize=12)
    plt.grid(True)

    # Save the plot as a PNG file
    plt.savefig("result/visualization/average_monthly_revenue.png", dpi=300, bbox_inches="tight")

    # Show the plot (optional)
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if 'connection' in locals() and connection:
        connection.close()
