/*
=========================================================================================================
Loading data from CRM & ERP systems
=========================================================================================================
Execution : 
Execting the following queries will load data from CRM and ERP CSV file into the tables in bronze database
=========================================================================================================
Warning:
Executing the below codes will lead to loss of data in the table with the renewed data. Kindly ensure the 
Backup Data before executing query to avoid loss of any data.
=========================================================================================================

*/

Truncate cust_info.csv ;
-- Truncating the table to avoid repeat data


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'                                 
IGNORE 1 LINES ;
-- Loading data from the CSV into crm_cust_info table 

-- ----------------------------------------------------------------------------------------------

/* Loading crm_prod_info table in the Bronze layer */

Truncate crm_prd_info;
-- Truncating the table to avoid repeat data

 

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
INTO TABLE crm_prod_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'                                 
IGNORE 1 LINES ;
-- Loading data from the CSV into crm_prod_info table



-- ---------------------------------------------------------------------------------------------------
/* Loading crm_sales_details table in the Bronze layer */

Truncate crm_sales_details ;
-- Truncating the table to avoid repeat data

 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'                                 
IGNORE 1 LINES ;
-- -----------------------------------------------------------------------------------------------------
/* Loading erp_cust_az12 table in the Bronze layer */

Truncate erp_CUST_AZ12 ;
-- Truncating the table to avoid repeat data

 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CUST_AZ12.csv'
INTO TABLE erp_CUST_AZ12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'                                 
IGNORE 1 LINES ;

-- ------------------------------------------------------------------------------------------------------

/* Loading LOC_A101 table in the Bronze layer */

Truncate erp_CUST_AZ12 ;
-- Truncating the table to avoid repeat data

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/LOC_A101.csv'
INTO TABLE erp_LOC_A101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'                                 
IGNORE 1 LINES ;

-- -----------------------------------------------------------------------------------------------------

/* Loading PX_CAT_G1V2 table in the Bronze layer */

Truncate erp_CUST_AZ12 ;
-- Truncating the table to avoid repeat data

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PX_CAT_G1V2.csv'
INTO TABLE erp_PX_CAT_G1V2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'                                 
IGNORE 1 LINES ;


