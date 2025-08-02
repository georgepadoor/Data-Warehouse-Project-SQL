SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[crm_cust_info]
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[crm_prd_info]
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[crm_sales_details]
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[erp_cust_az12]
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[erp_loc_a101]
SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]

SELECT TOP (1000) * FROM [DataWarehouse].[silver].[crm_cust_info]

-- checks for null or duplicates in primary key

SELECT
cst_id,
count(*) 
FROM bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null


SELECT
* ,
row_number () over (partition by cst_id order by cst_create_date desc) as rank_latest
FROM bronze.crm_cust_info
where cst_id = 29466



select
*
from

(SELECT
* ,
row_number () over (partition by cst_id order by cst_create_date desc) as rank_latest
FROM bronze.crm_cust_info) t
where rank_latest = 1 and cst_id = 29466

select
*
from

(SELECT
* ,
row_number () over (partition by cst_id order by cst_create_date desc) as rank_latest
FROM bronze.crm_cust_info
where cst_id is not null
) t
where rank_latest = 1




-- check for unwanted spaces

SELECT cst_firstname
FROM bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

SELECT cst_gndr
FROM bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)


-- data standardization and consistency
select distinct cst_gndr
FROM bronze.crm_cust_info

-- after loading silver doing quality checks

SELECT TOP (1000) * FROM [DataWarehouse].[silver].[crm_cust_info]

-- checks for null or duplicates in primary key

SELECT
cst_id,
count(*) 
FROM silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- check for unwanted spaces

SELECT cst_firstname
FROM silver.crm_cust_info
where cst_firstname != trim(cst_firstname)


SELECT cst_lastname
FROM silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

SELECT cst_gndr
FROM silver.crm_cust_info
where cst_gndr != trim(cst_gndr)

-- data standardization and consistency
select distinct cst_gndr
FROM silver.crm_cust_info

--clean and load crm_prd_info

SELECT TOP (1000) * FROM [DataWarehouse].[bronze].[crm_prd_info]

SELECT
prd_id,
count(*) 
FROM bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null


SELECT
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
FROM bronze.crm_prd_info
where REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') not in 
(select distinct id from bronze.erp_px_cat_g1v2)

SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
LEN(prd_key) AS len_prd_key   
FROM bronze.crm_prd_info


SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
FROM bronze.crm_prd_info

where REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') not in 
(select distinct id from bronze.erp_px_cat_g1v2)
