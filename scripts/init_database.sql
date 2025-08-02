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



use master;
go
-- drop and recreate the datawarehouse database
if exists (select 1 from sys.databases where name = 'dwhdb')
begin 
    alter database dwhdb set single_user with rollback immediate;
    drop database dwhdb;
end;
go
-- create database dwhdb

create database dwhdb;
use dwhdb;

-- create schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO