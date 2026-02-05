/*
==========================================================================================================
Execution:
    DDL scripts to create final views in the Gold layer. Following queries extract data from
    Silver layer to transform and load it to Gold layer for final consumption of the stakeholders

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

USE GOLD ;

SELECT * FROM Silver.CRM_Cust_info ;

-- ------------------------------------------------------------------------------------------------------
DROP VIEW IF EXISTS gold.dim_customers ;
 
CREATE VIEW  gold.dim_customers AS

SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- creating a surrogate key for the table 
cst_id AS customer_id,
cst_key AS  customer_number,
cst_firstname AS first_name, 
cst_lastname AS last_name,
cst_marital_status AS marital_status,
CASE  WHEN cst_gndr = 'n/a' THEN gen  
WHEN cst_gndr != gen THEN cst_gndr -- CRM is the primary source for customer details 
ELSE cst_gndr END AS gender, -- Data enrichment for gen column to update missing & incorrect values 
CNTRY AS country,
BDATE AS birth_date,
cst_create_date AS create_date
FROM 
silver.crm_cust_info C
LEFT JOIN silver.erp_cust_az12 CI
ON  C.cst_key = CI.CID 
LEFT JOIN silver.erp_loc_a101 CL
ON C.cst_key = CL.CID  --  Joining tables from CRM & ERP soruces with customer details in single table
ORDER BY create_date DESC ;


-- ------------------------------------------------------------------------------------------------------
DROP VIEW IF EXISTS gold.dim_products ;

CREATE VIEW gold.dim_products AS

SELECT 
Row_number() over(order by prd_start_dt,prd_key) AS product_key,
prd_id AS product_id,
cat_id AS category_id,
prd_key AS Product_number,
prd_nm AS product_name,
prd_cost AS product_cost,
prd_line AS product_line,
CAT AS product_category,
SUB_cAT AS product_subcategory,
maintenance,
prd_start_dt AS start_date
FROM Silver.CRM_prod_info P
JOIN Silver.erp_px_cat_g1v2 PI
ON  P.cat_id = PI.ID 
WHERE  prd_end_date IS NULL ;

-- -------------------------------------------------------------------------------------------------------
DROP VIEW IF EXISTS gold.fact_sales ;

CREATE VIEW  gold.fact_sales AS
SELECT 
sls_ord_num AS order_number,
customer_key,
product_key,
sls_cust_id AS customer_id,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price AS price
FROM silver.crm_sales_details S
JOIN gold.dim_customers C
ON s.sls_cust_id = C.customer_id
JOIN gold.dim_products P
ON p.product_number = s.sls_prod_key ;
