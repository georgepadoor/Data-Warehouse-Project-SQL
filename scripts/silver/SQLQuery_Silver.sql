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
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
FROM bronze.crm_prd_info
where REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') not in 
(select distinct id from bronze.erp_px_cat_g1v2)

SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
LEN(prd_key) AS len_prd_key   
FROM bronze.crm_prd_info


SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info

where REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') not in 
(select distinct id from bronze.erp_px_cat_g1v2)

select sls_prd_key from bronze.crm_sales_details

SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info
where SUBSTRING(prd_key, 7, LEN(prd_key)) not in (
select sls_prd_key from bronze.crm_sales_details)


select sls_prd_key 
from bronze.crm_sales_details 
where sls_prd_key like 'FK-1639'


select sls_prd_key 
from bronze.crm_sales_details 
where sls_prd_key like 'FK-1%'


SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info
where SUBSTRING(prd_key, 7, LEN(prd_key)) in (
select sls_prd_key from bronze.crm_sales_details)

-- check for unwanted spaces

SELECT prd_nm
FROM bronze.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for nulls or negative numbers

SELECT prd_cost
FROM bronze.crm_prd_info
where prd_cost  < 0 or prd_cost is null

-- data standardization and consistency

SELECT distinct prd_line
FROM bronze.crm_prd_info



-- check for invalid date orders

SELECT *
FROM bronze.crm_prd_info
where prd_end_dt < prd_start_dt

-- creating new end date
SELECT *
FROM bronze.crm_prd_info
where prd_key in('AC-HE-HL-U509-R','AC-HE-HL-U509')


SELECT *,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info

where prd_key in('AC-HE-HL-U509-R','AC-HE-HL-U509')


SELECT *,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt
FROM bronze.crm_prd_info

-- after loading silver layer quality checks
SELECT
prd_id,
count(*) 
FROM silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

-- check for unwanted spaces

SELECT prd_nm
FROM silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for nulls or negative numbers

SELECT prd_cost
FROM silver.crm_prd_info
where prd_cost  < 0 or prd_cost is null

-- data standardization and consistency

SELECT distinct prd_line
FROM silver.crm_prd_info


-- check for invalid date orders

SELECT *
FROM silver.crm_prd_info
where prd_end_dt < prd_start_dt


SELECT *
FROM silver.crm_prd_info

-- clean and load crm_sales_details
SELECT *
FROM bronze.crm_sales_details
where sls_ord_num !=trim(sls_ord_num)

SELECT *
FROM bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

SELECT *
FROM bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

SELECT *
FROM bronze.crm_sales_details
where sls_order_dt <= 0

SELECT 
nullif(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
where sls_order_dt <= 0  or len(sls_order_dt)!=8 or sls_order_dt > 20300101

SELECT 
nullif(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
where sls_order_dt > 20300101 or sls_order_dt < 20000101

SELECT 
nullif(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
where sls_ship_dt <= 0  or len(sls_ship_dt)!=8 or sls_ship_dt > 20300101


SELECT 
nullif(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
where sls_due_dt <= 0  or len(sls_due_dt)!=8 or sls_due_dt > 20300101

select *
FROM bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

select *
FROM bronze.crm_sales_details
where sls_ship_dt > sls_due_dt

select *
FROM bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price

select *
FROM bronze.crm_sales_details
where sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by  sls_sales, sls_quantity, sls_price 


select *
FROM bronze.crm_sales_details
where sls_sales is null or sls_quantity is null or sls_price is null
order by  sls_sales, sls_quantity, sls_price 



select distinct
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
where sls_sales is null or sls_quantity is null or sls_price is null
order by  sls_sales, sls_quantity, sls_price 


select
sls_sales as sls_sales_old,
sls_quantity,
sls_price as sls_price_old
FROM bronze.crm_sales_details
where sls_sales is null or sls_quantity is null or sls_price is null
order by  sls_sales, sls_quantity, sls_price 

-- Recalculate sales if original value is missing or incorrect
select
sls_quantity,
sls_price,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
sls_sales as sls_sales_old
FROM bronze.crm_sales_details
where sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)

 -- Derive price if original value is invalid
  select
sls_quantity,
sls_price as sls_price_old ,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 
	END AS sls_price,
sls_sales
FROM bronze.crm_sales_details
where  sls_price IS NULL OR sls_price <= 0

--loaded to silver layer do quality checks

select distinct
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
where sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
or sls_sales != sls_quantity * sls_price
order by  sls_sales, sls_quantity, sls_price 

select *
FROM silver.crm_sales_details

-- clean and load data erp_cust_az12

select
    cid,
    bdate,
    gen
from bronze.erp_cust_az12

select * from [silver].[erp_cust_az12]

select
    cid,
    bdate,
    gen
from bronze.erp_cust_az12
where cid like '%AW00011004'

select
    cid,
	case when cid like 'nas%' then substring (cid,4,len(cid))
	else cid
end cid_new,
    bdate,
    gen
select

from bronze.erp_cust_az12 


-- to count duplicates
select
count(*)
from bronze.erp_cust_az12 
group by cid
HAVING COUNT(*) > 1;


select
    cid,count(*) as cid_duplicate
from bronze.erp_cust_az12 
group by cid
HAVING COUNT(*) > 1;


select
    cid,count(*) as cid_duplicate
from bronze.erp_cust_az12 
group by cid
HAVING COUNT(*) > 1;

select
    cid,
	case when cid like 'nas%' then substring (cid,4,len(cid))
	else cid
end as cid_new,
    bdate,
	count(*)as count_column,
    gen
from bronze.erp_cust_az12 
group by cid,bdate,gen,
case when cid like 'nas%' then substring (cid,4,len(cid))
	else cid
end 
HAVING COUNT(*) = 1;


select
    cid,
	case when cid like 'nas%' then substring (cid,4,len(cid))
	else cid
end as cid,
    bdate,
    gen
from bronze.erp_cust_az12 
where case when cid like 'nas%' then substring (cid, 4, len (cid))
else cid
end not in (select distinct cst_key from silver.crm_cust_info)

-- dentify out of range dates

select distinct bdate 
from bronze.erp_cust_az12
where bdate <'1925-01-01' or  bdate > getdate()

--data standardization cinsistency
select distinct 
gen
from bronze.erp_cust_az12

select distinct 
gen,
case 
	when trim(gen) in ('M', 'MALE') then 'Male'
	when trim(gen) in ('F', 'FEMALE') then 'Female'
	ELSE 'n/a'
END AS gen
from bronze.erp_cust_az12



--testing the table

SELECT
		CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12

--quality check by silver table

--data standardization cinsistency
select distinct 
gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12


--load bronze.erp_loc_a101
select *
from bronze.erp_loc_a101

select 
cid,
cntry
from bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info

select 
replace (cid, '-','') cid
from bronze.erp_loc_a101

select cst_key from silver.crm_cust_info


select distinct cntry
from bronze.erp_loc_a101
order by cntry
-- data standardization n consistency
select distinct
cntry as cntry_old,
case
when trim(cntry) in ('DE', 'Germany') then 'Germany'
when trim(cntry) in ('United States','US','USA') then 'United States'
when trim(cntry) in ('UK', 'United Kingdom') then 'United Kingdom'
when trim(cntry) ='' or cntry is null then 'n/a'
else cntry
end as cntry
from bronze.erp_loc_a101
order by cntry

select distinct cntry
from silver.erp_loc_a101
order by cntry

select *
from silver.erp_loc_a101


--load erp_px_cat_g1v2 to silver layer
select
id,
cat,
subcat,
maintenance

from bronze.erp_px_cat_g1v2

-- check for unwanted spaces

select * from bronze.erp_px_cat_g1v2
where cat !=(cat)


select * from bronze.erp_px_cat_g1v2
where subcat !=(subcat)

select * from bronze.erp_px_cat_g1v2
where maintenance !=(maintenance)

select * from bronze.erp_px_cat_g1v2
where cat !=(cat) or subcat !=(subcat) or maintenance !=(maintenance)

-- data  standarization n consistency

select distinct cat from bronze.erp_px_cat_g1v2

select distinct subcat from bronze.erp_px_cat_g1v2

select distinct maintenance from bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2





