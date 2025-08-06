============================================
  --Creating DATABASE and SCHEMA
============================================

USE master;
GO

--DROP and Recreate the Database "DataWarehouse"
  IF EXISTS(SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END

--Create the Databse named "DataWarehouse"
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO


--Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
