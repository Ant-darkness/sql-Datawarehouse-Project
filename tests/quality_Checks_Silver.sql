/*
========================================================================================
QUALITY CHECKS
========================================================================================
SCRIPT PURPOSE:
    This Script performs various quality checks for data consistency, across and standardization across 'silver' schema.
    It includes Check for;
    - Null or Duplicates of Primary keys.
    - Unwanted Spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency beween related Fields
USAGE
    - Run these checks after data loading silver layer.
    - Investigate and resolve any discrepancies found during data checks.
============================================================================================
*/
-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO

--CHECK FOR UNWANTED SPACE
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
GO

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
GO

--CHECK DATA STANDARDIZATION & CONSISTENCY
SELECT  
cst_gndr,
cst_material_status
FROM silver.crm_cust_info;
GO

-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO
--CHECK FOR UNWANTED SPACE
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
GO

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
GO
--CHECK DATA STANDARDIZATION & CONSISTENCY
SELECT  
cst_gndr,
cst_material_status
FROM silver.crm_cust_info;
GO

------------------PRD_INFO TABLE-------------------

---CHECK FOR UNAWANTED SPACES
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
GO
--CHECK FOR NULLS AND NEGATIVE NUMBERS
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
GO

---DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;
GO

------------------SALES_DETAILS-----------------

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > '20500101'
OR sls_order_dt < '19000101';
GO


SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > '20500101'
OR sls_due_dt < '19000101';
GO


SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > '2050-01-01'
OR sls_ship_dt < '1900-01-01';
GO



----Check Data Consistency: Between Sales, Quantity and Price
-->> Sales = Quantity * price
--->> Values must not be NULL , Zero or Negative
SELECT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;
GO

SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt;
GO



-----------------CUST_AZ12 TABLE----------------------
SELECT * FROM silver.erp_cust_az12;
GO
---CHECK cid ( For ralationship with other tabel(cust_inftable))
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);
GO

--Identify Out-range Dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate > '1924-01-01' OR bdate > GETDATE();
GO
---Data Standardization & Consistency
SELECT DISTINCT 
gen
FROM silver.erp_cust_az12




----------------------LOC_A101 TABLE--------------------
SELECT
cid
FROM silver.erp_loc_q101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);
GO

---Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_q101;
GO


--------------------------PX_CAT_G1V2 TABLE-----------------------

SELECT
id,
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2;
GO

--CHECK FOR UNAWNTED SPACES
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
OR subcat != TRIM(subcat) 
OR maintenance != TRIM(maintenance);
GO

---Data Standardization & Consistency

SELECT DISTINCT
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2;
GO

