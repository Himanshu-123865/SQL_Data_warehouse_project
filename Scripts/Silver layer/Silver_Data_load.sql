/* 
===================================================================================================================
Silver Schema - Data Load
====================================================================================================================
Execution : 
Creating Procedure LOAD_SILVER_SCHEMA() for extracting data from Brone Layer to perform data transformations  
for Null, incorrect values & improper formats and loading the cleansed data in Silver Schema. 
====================================================================================================================
Data cleaning steps performed on bronze layer data: 
1)  Data profiling: Analyzing data & tables to understand structure & relationships to identify anamolies in data
2)  Data Validation: Checking all field values fall within the data specific constraints Like Data values.
3)  Data Normalization: Standard represenation of all data values Like Date formats in 'YYY-MM-DD' & replacing
    abbrevations with full names for country. 
4)  Data Enrichment : Enhancing missing or invalid data by calculated fields or referencing from associated tables.
=====================================================================================================================
Warning:
- Running the LOAD_SILVER_SCHEMA() procedure will delete the exisiting data in the table * load new data. Ensure 
  there is data back up before execution to avoid loss of any critical data.
=====================================================================================================================
*/



DELIMITER $$

CREATE PROCEDURE LOAD_SILVER_SCHEMA()
BEGIN

DECLARE  Start_time DATETIME ;
DECLARE   End_time DATETIME ; 

-- ------------------------------------------------------------------------------------------------------
TRUNCATE silver.crm_cust_info;
SELECT 'Silver.crm_cust_info truncated succesfully' ;

SET start_time = current_timestamp() ; 
INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,   --  Remove unwanted spaces 
TRIM(cst_lastname) AS cst_lastname,     --  Remove unwanted spaces 
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
ELSE 'n/a' END AS  cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
ELSE 'n/a' END AS  cst_gndr,        -- Data standardization to ensure Fields are either 'Male'|'Female'|'NA'
 CASE
        WHEN cleaned_date IS NULL THEN NULL
        WHEN cleaned_date = '' THEN NULL
        ELSE cleaned_date END AS  cst_create_date  -- DATA Normalization Handling Empty columns. 
FROM 
(
SELECT *,
REPLACE(REPLACE(REPLACE(cst_create_date, CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS Cleaned_date, -- Ha
DENSE_RANK() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) AS R1
FROM Bronze.crm_cust_info ) A
WHERE R1 = 1  ;

SET End_time = current_timestamp() ;

SELECT ' New Data Loaded in silver.crm_cust_info Sucessfully',
CONCAT( 'Load Time:' , CAST( TIMESTAMPDIFF(MICROSECOND,start_time,End_time) AS CHAR) )  AS Load_time ;

-- ------------------------------------------------------------------------------------------------------

TRUNCATE silver.crm_prod_info ;
SELECT 'Silver.crm_prod_info truncated succesfully' ;

SET start_time = current_timestamp() ;

INSERT INTO silver.crm_prod_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_date)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Extract category_id
Substring(prd_key,7,length(prd_key)) AS prd_key,   -- Extract prd_key 
prd_nm,
IFNULL(prd_cost,0.00) AS prd_cost, -- Replacing negative and null values in product cost
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountains'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roads'
WHEN UPPER(TRIM(prd_line)) = 's' THEN 'Other sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'n/a' END AS prd_line,       -- Data standardization of prod_line with full names
prd_start_dt, 
CAST(DATE_SUB(LAG(prd_start_dt) OVER(PARTITION BY prd_key ) ,INTERVAL 1 DAY) AS DATE) 
AS prd_end_date -- creating the new end_date basis the next startdate 
FROM (
SELECT 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
DENSE_RANK() OVER( PARTITION BY prd_key ORDER BY prd_start_dt DESC) as r1
FROM Bronze.crm_prod_info ) A 
ORDER BY prd_key, prd_start_dt ;

SET End_time = current_timestamp() ;
SELECT ' New Data Loaded in Silver.crm_prod_info Sucessfully',
CONCAT( 'Load Time:' , CAST( TIMESTAMPDIFF(MICROSECOND,start_time,End_time) AS CHAR) ) AS Load_time ;

-- -------------------------------------------------------------------------------------------------------

TRUNCATE silver.crm_sales_details ;

SELECT 'silver.crm_sales_details truncated succesfully' ;
SET start_time = current_timestamp() ;

INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prod_key,sls_cust_id,sls_order_dt,
sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price) 
SELECT 
sls_ord_num,
sls_prod_key,
sls_cust_id,
CASE WHEN length(sls_order_dt) != 8 OR  sls_order_dt < 1 THEN NULL
ELSE
CAST( Concat( substring(sls_order_dt,1,4),'-',substring(sls_order_dt,5,2),'-',substring(sls_order_dt,7,2))
 AS DATE) END AS sls_order_dt,
 CASE WHEN length(sls_ship_dt) != 8 OR  sls_ship_dt < 1 THEN null
ELSE
CAST( Concat( substring(sls_ship_dt,1,4),'-',substring(sls_ship_dt,5,2),'-',substring(sls_ship_dt,7,2))
 AS DATE)  END AS sls_ship_dt,
 CASE WHEN length(sls_due_dt) != 8 OR sls_due_dt < 1 THEN null 
ELSE
CAST( Concat( substring(sls_due_dt,1,4),'-',substring(sls_due_dt,5,2),'-',substring(sls_due_dt,7,2))
 AS DATE) END AS sls_due_dt,-- Data_formating by correcting the field value & converting from string to date type.
 
CASE WHEN sls_sales < 1 
THEN ROUND((sls_price * sls_quantity),2) 
WHEN sls_price > 0 AND  sls_sales != sls_price * sls_quantity 
THEN ROUND((sls_price * sls_quantity),2) 
ELSE sls_sales END AS sls_sales,  -- Recalculating the data for incorrect & missing values(Data Normaliztion)
sls_quantity,
ROUND(CASE WHEN sls_sales < 1 
THEN ROUND((sls_price * sls_quantity),2) 
WHEN sls_price > 0 AND  sls_sales != sls_price * sls_quantity 
THEN ROUND((sls_price * sls_quantity),2) 
ELSE sls_sales END / sls_quantity,2) AS sls_price -- Normalizing Price for missing & incorrect values
FROM 
Bronze.crm_sales_details ;

SET End_time = current_timestamp() ;
SELECT ' New Data Loaded in Silver.crm_sales_details Sucessfully',
CONCAT( 'Load Time:' , CAST( TIMESTAMPDIFF(MICROSECOND,start_time,End_time) AS CHAR) ) AS Load_time ;

-- -------------------------------------------------------------------------------------------------------

TRUNCATE  Silver.erp_CUST_AZ12 ;

SELECT 'Silver.erp_CUST_AZ12 truncated succesfully' ;
SET start_time = current_timestamp() ;

INSERT INTO Silver.erp_CUST_AZ12(CID,BDATE,GEN)
SELECT 
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID)) END AS CID, -- 'NAS' prefix removed for the column 
DATE_FORMAT( STR_TO_DATE(BDATE,'%m-%d-%Y'),'%Y-%m-%d') AS BDATE, -- Converting date from string to date type & formatting
CASE WHEN GEN IS NULL THEN NULL
     WHEN UPPER(REPLACE(REPLACE(TRIM(GEN), CHAR(13), ''), CHAR(10), '')) IN('M','MALE') THEN 'Male'
     WHEN UPPER(REPLACE(REPLACE(TRIM(GEN), CHAR(13), ''), CHAR(10), '')) IN('F','FEMALE') THEN 'Female'
     ELSE 'n/a' END AS GEN -- Standardized the gender values and handle unknown cases 
     FROM 
     (
SELECT 
CID,
REPLACE(BDATE,'/','-' ) AS BDATE, -- Changing the date in valid format 
GEN
FROM 
BRONZE.erp_CUST_AZ12 ) A ;

SET End_time = current_timestamp() ;
SELECT ' New Data Loaded in erp_CUST_AZ12 Sucessfully',
CONCAT( 'Load Time:' , CAST( TIMESTAMPDIFF(MICROSECOND,start_time,End_time) AS CHAR) ) AS Load_time ;

-- ------------------------------------------------------------------------------------------------------

TRUNCATE Silver.ERP_LOC_A101 ;

SELECT 'Silver.ERP_LOC_A101 truncated succesfully' ;
SET start_time = current_timestamp() ;
 
INSERT INTO SILVER.ERP_LOC_A101(CID,CNTRY)
SELECT 
REPLACE(CID,'-','') AS CID, -- Data Normalization to have correcct values for the columns
CASE WHEN CNTRY IN ('US','USA') THEN 'United States'
WHEN CNTRY = 'DE' THEN 'Germany'
WHEN CNTRY = 'DE' THEN 'Germany'
WHEN CNTRY = ''  OR  CNTRY IS NULL THEN NULL
WHEN REPLACE(REPLACE(TRIM(CNTRY),CHAR(13),""),CHAR(10),"") IN ('') THEN 'n/a'
ELSE TRIM(CNTRY)   
END AS CNTRY   -- Removing incorrect values & Data normalization 
FROM  BRONZE.ERP_LOC_A101 ;

SET End_time = current_timestamp() ;
SELECT ' New Data Loaded in ERP_LOC_A101 Sucessfully',
CONCAT( 'Load Time:' , CAST( TIMESTAMPDIFF(MICROSECOND,start_time,End_time) AS CHAR) ) AS Load_time ;

-- ------------------------------------------------------------------------------------------------------
TRUNCATE   Silver.erp_PX_CAT_G1V2 ;

SELECT 'Silver.erp_PX_CAT_G1V2 truncated succesfully' ;

 SET start_time = current_timestamp() ;

INSERT INTO SILVER.erp_PX_CAT_G1V2(ID,CAT,SUB_CAT,MAINTENANCE) 
SELECT * FROM BRONZE.erp_PX_CAT_G1V2  ;
 
 SET End_time = current_timestamp() ;

SELECT ' New Data Loaded in erp_PX_CAT_G1V2 Sucessfully',
CONCAT( 'Load Time:' , CAST( TIMESTAMPDIFF(MICROSECOND,start_time,End_time) AS CHAR) )  AS Load_time ;

	
END $$ 

DELIMITER ;

CALL Load_Silver_Schema() ;











 

