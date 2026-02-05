/*

=======================================================
Creating Tables for loading data in broze database.
=======================================================
Execution:
initiate_bronze_architechture() procedure is created to create tables in the bronze database to load
data from the ERP & CRM source systems
========================================================================================================
WARNING :
 Running the script will delete all the existing tables in the Bronze database & create new tables. 
 Proceed with caution as excuting the query will delete all the existing tables in the database before 
 creating new tables. Backup Data before executing query to avoid loss of any data.
========================================================================================================
*/

DELIMITER $$ 

CREATE PROCEDURE initiate_bronze_architechture()



DROP TABLE IF exists  crm_cust_info ;
-- Deleting the table if exists 

CREATE TABLE crm_cust_info 
( cst_id INTEGER NULL,
cst_key VARCHAR(100),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr  VARCHAR(50),
cst_create_date VARCHAR(50) )  ;
-- Creating new crm_cust_info table 

 SELECT ' crm_cust_info table created in bronze database' as Message ;

 DROP  TABLE IF EXISTS crm_prod_info;
 -- Deleting the table if exists 
 
CREATE TABLE crm_prod_info 
( prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost decimal(10,2),
prd_line VARCHAR(50),
prd_start_dt VARCHAR(50) ,
prd_end_date VARCHAR(50)  );
-- Creating new crm_prod_info table 

 SELECT 'crm_prod_info table created in bronze database' as Message ;

 DROP  TABLE IF EXISTS crm_sales_details;
  -- Deleting the table if exists 

CREATE TABLE crm_sales_details
(
sls_ord_num VARCHAR(50),
sls_prod_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt VARCHAR(50) ,
sls_ship_dt VARCHAR(50) ,
sls_due_dt VARCHAR(50) ,
sls_sales DECIMAL(10,2),
sls_quantity INT,
sls_price DECIMAL(10,2) ) ;
-- Creating new crm_sales_details table 


DROP  TABLE IF EXISTS erp_CUST_AZ12 ;
 -- Deleting the table if exists 

CREATE TABLE erp_CUST_AZ12
(CID VARCHAR(50),
BDATE VARCHAR(50) ,
GEN VARCHAR(50) ) ;
-- Creating new erp_CUST_AZ12 table

 SELECT 'erp_CUST_AZ12 table created in bronze database' as Message ;


DROP  TABLE IF EXISTS erp_LOC_A101 ;
 -- Deleting the table if exists

CREATE TABLE erp_LOC_A101
( CID VARCHAR(50),
CNTRY VARCHAR(50) ) ;
-- Creating new erp_LOC_A101 table

 SELECT 'erp_LOC_A101 table created in bronze database' as Message ;

DROP  TABLE IF EXISTS erp_PX_CAT_G1V2 ;
 -- Deleting the table if exists
 
CREATE TABLE erp_PX_CAT_G1V2
( ID VARCHAR(50),
CAT VARCHAR(50),
SUB_CAT VARCHAR(50),
MAINTENANCE VARCHAR(50)) ;
-- Creating new erp_PX_CAT_G1V2 table

 SELECT 'erp_PX_CAT_G1V2 table created in bronze database' as Message ;


END $$

DELIMITER ;


CALL initiate_bronze_architechture() ;


                           