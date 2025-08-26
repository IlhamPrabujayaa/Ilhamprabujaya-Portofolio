------- Data Cleaning ------

CREATE SCHEMA data_cleaning_penjualan_toko_retail;

CREATE TABLE data_cleaning_penjualan_toko_retail.retail_sales_dirty_final (
    Order_ID TEXT,
    Order_Date TEXT,
    Region TEXT,
    Product_Category TEXT,
    Product_Name TEXT,
    Quantity TEXT,
    Unit_Price TEXT,
    Total_Sales TEXT,
    Customer_Segment TEXT
);

SELECT * FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_final;

--Upload file
COPY data_cleaning_penjualan_toko_retail.retail_sales_dirty_final
FROM 'C:\Program Files\PostgreSQL\17\data\retail_sales_dirty_final.csv'
DELIMITER ';' 
CSV HEADER 
ENCODING 'UTF8';

CREATE TABLE data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
(LIKE data_cleaning_penjualan_toko_retail.retail_sales_dirty_final INCLUDING ALL);

SELECT * FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging;

INSERT INTO data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
SELECT * 
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_final;

--menandai setiap baris duplikat dengan nomor urut
SELECT order_id,order_date,region,product_category,quantity,unit_price,total_sales,customer_segment,
ROW_NUMBER() OVER (PARTITION BY order_id, order_date,region,product_category,quantity,unit_price,total_sales,customer_segment) AS row_num
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging;

--menampilkan hanya data duplikat
WITH cte AS (
SELECT order_id,order_date,region,product_category,quantity,unit_price,total_sales,customer_segment,
ROW_NUMBER() OVER (PARTITION BY order_id, order_date, region, product_category, quantity, unit_price, total_sales, customer_segment
ORDER BY order_id) AS row_num
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
)
SELECT *
FROM cte
WHERE row_num > 1;

-- Deleted Duplicate
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY order_id, order_date, region, product_category, quantity, unit_price, total_sales, customer_segment
ORDER BY order_id) AS row_num
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
)
DELETE FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
WHERE (order_id, order_date, region, product_category, quantity, unit_price, total_sales, customer_segment) IN (
    SELECT order_id, order_date, region, product_category, quantity, unit_price, total_sales, customer_segment
    FROM duplicate_cte
    WHERE row_num > 1
);

SELECT COUNT(*) AS total_baris
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging;



----- Standarisasi Data

SELECT *
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging;


-- Mendeteksi nilai kosong/NULL

SELECT *
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
WHERE Order_ID = ''
   OR Order_Date = ''
   OR Region = ''
   OR Product_Category = ''
   OR Product_Name = ''
   OR Quantity = ''
   OR Unit_Price = ''
   OR Total_Sales = ''
   OR Customer_Segment = '';

-- Mengubah data kosong & spasi jadi NULL
UPDATE data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
SET 
    Order_ID = NULLIF(TRIM(Order_ID), ''),
    Order_Date = NULLIF(TRIM(Order_Date), ''),
    Region = NULLIF(TRIM(Region), ''),
    Product_Category = NULLIF(TRIM(Product_Category), ''),
    Product_Name = NULLIF(TRIM(Product_Name), ''),
    Quantity = NULLIF(TRIM(Quantity), ''),
    Unit_Price = NULLIF(TRIM(Unit_Price), ''),
    Total_Sales = NULLIF(TRIM(Total_Sales), ''),
    Customer_Segment = NULLIF(TRIM(Customer_Segment), '');
	

-- Mengubah NULL jadi unknown/0

UPDATE data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
SET 
    Order_ID = COALESCE(Order_ID, 'Unknown'),
    Order_Date = COALESCE(Order_Date, 'Unknown'),
    Region = COALESCE(Region, 'Unknown'),
    Product_Category = COALESCE(Product_Category, 'Unknown'),
    Product_Name = COALESCE(Product_Name, 'Unknown'),
    Customer_Segment = COALESCE(Customer_Segment, 'Unknown'),
    Quantity = COALESCE(Quantity, '0'),
    Unit_Price = COALESCE(Unit_Price, '0'),
    Total_Sales = COALESCE(Total_Sales, '0');


--Mendeteksi Spasi berlebihan

SELECT *
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
WHERE 
    Order_ID LIKE ' %' OR Order_ID LIKE '% ' OR Order_ID LIKE '%  %'
 OR Order_Date LIKE ' %' OR Order_Date LIKE '% ' OR Order_Date LIKE '%  %'
 OR Region LIKE ' %' OR Region LIKE '% ' OR Region LIKE '%  %'
 OR Product_Category LIKE ' %' OR Product_Category LIKE '% ' OR Product_Category LIKE '%  %'
 OR Product_Name LIKE ' %' OR Product_Name LIKE '% ' OR Product_Name LIKE '%  %'
 OR Customer_Segment LIKE ' %' OR Customer_Segment LIKE '% ' OR Customer_Segment LIKE '%  %'
 OR Quantity LIKE ' %' OR Quantity LIKE '% ' OR Quantity LIKE '%  %'
 OR Unit_Price LIKE ' %' OR Unit_Price LIKE '% ' OR Unit_Price LIKE '%  %'
 OR Total_Sales LIKE ' %' OR Total_Sales LIKE '% ' OR Total_Sales LIKE '%  %';


-- Untuk mendeteksi simbol lain selain angka
SELECT 
    Order_ID,
    CASE 
        WHEN Order_ID ~ '^[0-9]+$' OR Order_ID IS NULL THEN 'VALID'
        ELSE 'INVALID'
    END AS order_id_status,

    Quantity,
    CASE 
        WHEN Quantity ~ '^[0-9]+$' OR Quantity IS NULL THEN 'VALID'
        ELSE 'INVALID'
    END AS quantity_status,

    Unit_Price,
    CASE 
        WHEN Unit_Price ~ '^[0-9]+(\.[0-9]+)?$' OR Unit_Price IS NULL THEN 'VALID'
        ELSE 'INVALID'
    END AS unit_price_status,

    Total_Sales,
    CASE 
        WHEN Total_Sales ~ '^[0-9]+(\.[0-9]+)?$' OR Total_Sales IS NULL THEN 'VALID'
        ELSE 'INVALID'
    END AS total_sales_status

FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
WHERE 
    NOT (Order_ID ~ '^[0-9]+$' OR Order_ID IS NULL)
    OR NOT (Quantity ~ '^[0-9]+$' OR Quantity IS NULL)
    OR NOT (Unit_Price ~ '^[0-9]+(\.[0-9]+)?$' OR Unit_Price IS NULL)
    OR NOT (Total_Sales ~ '^[0-9]+(\.[0-9]+)?$' OR Total_Sales IS NULL);

--bersihkan simbol lain selain angka
UPDATE data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
SET 
    Order_ID = REGEXP_REPLACE(Order_ID, '[^0-9]', '', 'g'),
    Quantity = REGEXP_REPLACE(Quantity, '[^0-9]', '', 'g'),
    Unit_Price = REGEXP_REPLACE(Unit_Price, '[^0-9]', '', 'g'),
    Total_Sales = REGEXP_REPLACE(Total_Sales, '[^0-9]', '', 'g');

--Ubah tipe data
ALTER TABLE data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
ALTER COLUMN Order_ID TYPE INT USING Order_ID::INT,
ALTER COLUMN Order_Date TYPE DATE USING TO_DATE(Order_Date, 'YYYY-MM-DD'),
ALTER COLUMN Region TYPE VARCHAR(100),
ALTER COLUMN Product_Category TYPE VARCHAR(100),
ALTER COLUMN Product_Name TYPE VARCHAR(200),
ALTER COLUMN Quantity TYPE INT USING Quantity::INT,
ALTER COLUMN Unit_Price TYPE DECIMAL(10,2) USING Unit_Price::DECIMAL,
ALTER COLUMN Total_Sales TYPE DECIMAL(15,2) USING Total_Sales::DECIMAL,
ALTER COLUMN Customer_Segment TYPE VARCHAR(100);

--Tambahkan primary key
ALTER TABLE data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
ADD PRIMARY KEY (Order_ID);

--Menampilkan jumlah duplikat
SELECT Order_ID, COUNT(*)
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Order_ID
HAVING COUNT(*) > 1;

--Menampilkan jumlah seluruh baris duplikat
SELECT *
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
WHERE Order_ID IN (
    SELECT Order_ID
    FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
    GROUP BY Order_ID
    HAVING COUNT(*) > 1
);

--menghapus baris sesuai keinginan
DELETE FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
WHERE Order_ID = 1041
  AND Product_Name = 'Unknown';



---- Exploratory Data Analysis (EDA)

-- Total Penjualan per Kategori Produk
SELECT Product_Category, 
       SUM(Total_Sales) AS Total_Penjualan
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Product_Category
ORDER BY Total_Penjualan DESC;

-- Total Penjualan per Wilayah
SELECT Region, 
       SUM(Total_Sales) AS Total_Penjualan
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Region
ORDER BY Total_Penjualan DESC;

-- Tren Penjualan Bulanan
SELECT TO_CHAR(Order_Date, 'YYYY-MM-DD') AS Bulan,
       SUM(Total_Sales) AS Total_Penjualan
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Bulan
ORDER BY Bulan;

-- Rata-rata Pembelian per Segmen Pelanggan
SELECT Customer_Segment,
       ROUND(AVG(Total_Sales), 2) AS Rata_Rata_Pembelian
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Customer_Segment
ORDER BY Rata_Rata_Pembelian DESC;

-- Jumlah transaksi per segmen
SELECT Customer_Segment, COUNT(*) AS Jumlah_Transaksi
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Customer_Segment
ORDER BY Jumlah_Transaksi DESC;

--Rata-rata, total, min, max per kategori/wilayah
SELECT Region,
       COUNT(*) AS Jumlah_Transaksi,
       SUM(Total_Sales) AS Total_Penjualan,
      ROUND(AVG(Total_Sales), 2) AS Rata_Rata_Pembelian,
       MIN(Total_Sales) AS Min_Pembelian,
       MAX(Total_Sales) AS Max_Pembelian
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Region
ORDER BY Total_Penjualan DESC;


-- Tren bulanan per kategori produk
SELECT TO_CHAR(Order_Date, 'YYYY-MM') AS Bulan,
       Product_Category,
       SUM(Total_Sales) AS Total_Penjualan
FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
GROUP BY Bulan, Product_Category
ORDER BY Bulan, Total_Penjualan DESC;


SELECT * FROM data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging;


COPY data_cleaning_penjualan_toko_retail.retail_sales_dirty_staging
TO 'D:/PROJECT Data Analis/retail_sales_staging.csv'
DELIMITER ';'
CSV HEADER
ENCODING 'UTF8';

