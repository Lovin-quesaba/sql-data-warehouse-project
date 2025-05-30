/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- 1. Drop the database if it exists
--    'IF EXISTS' prevents an error if the database doesn't exist
DROP DATABASE IF EXISTS DataWarehouse;

-- 2. Create the new database
CREATE DATABASE DataWarehouse;


CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;


