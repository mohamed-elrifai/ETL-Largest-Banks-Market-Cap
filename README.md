# ETL Pipeline for the World's Largest Banks Data
-------------------------------------------

### Project Description:

This repository provides an ETL (Extract, Transform, Load) pipeline for the data of the world's largest banks by market capitalization. It scrapes the data from Wikipedia, transforms it with currency conversions, and stores it in both a CSV file and a SQL database table.

### Project Scenario:

A multi-national firm has hired me as a data engineer. My job was to access and process data as per requirements.
My boss asked me to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, I needed to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate information made available to me as a CSV file. I should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

### Features:

-   Scrapes data from the "List of largest banks" on Wikipedia (URL provided in the code).
-   Extracts bank names and market capitalization in USD.
-   Converts market cap to GBP, EUR, and INR based on exchange rates provided in a separate CSV file.
-   Saves processed data as a CSV file for easy access and analysis.
-   Loads the data into a SQLite3 database table for structured querying.
-   Includes predefined queries to retrieve specific data subsets (e.g., top 5 banks, average market cap).

### Dependencies:

-   Python 3.x
-   Pandas
-   BeautifulSoup4
-   requests
-   sqlite3


### Usage:

The script automatically performs the ETL process and runs a few predefined queries on the database table. The results are printed to the console. You can modify the script and query statements to explore the data further.

### Benefits:

-   Access and analyze market cap data of the top 10 banks in multiple currencies.
-   Streamline currency adjustments and gain deeper financial insights.
-   Easy to use and customize for your specific needs.


### Additional Notes:

-   The code logs the progress of the process to a file named `code_log.txt`.
-   You can modify the queries in the script to retrieve different data subsets.

