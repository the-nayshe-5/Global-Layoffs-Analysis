-- Data Cleaning
SELECT * FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardise the data
-- 3. Deal with Null Values or Blanks
-- 4. Remove any unnecessary column (last resort)

-- Create a staging table separate from raw dataset to work on

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;

-- 1. Remove Duplicates

SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised) AS row_num
FROM layoffs_staging;

-- Using a CTE to check for rows with row_num > 1 (duplicate values)
WITH duplicate_cte AS
(
	SELECT *, ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
    stage, funds_raised) AS row_num
	FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- Checking we got duplicates for sure

SELECT * FROM layoffs_staging 
WHERE company = 'Beyond Meat';

-- Create another table with row_num column also 
-- as CTE dows not allow Updation and Deletion

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, funds_raised) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2;

DELETE FROM layoffs_staging2 
WHERE row_num > 1;

SELECT * FROM layoffs_staging2 
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;

-- Duplicates removed.

-- 2. Standardise Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2 
ORDER BY 1; 

-- If there are industries that mean the same but just have been written differently, 
-- then we need standardise them to the same name so that they don't create an issue 
-- while visualising the data. e.g., Crypto, Cryptocurrency, Crypto Currency all mean the same.

SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check for issues for each of the columns, identify them and rectify them.

SELECT DISTINCT(location), country
FROM layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE location = 'Brisbane';

SELECT * FROM layoffs_staging2
WHERE company = 'Arch Oncology';

UPDATE layoffs_staging2
SET country = 'United Arab Emirates'
WHERE location = 'Dubai';

UPDATE layoffs_staging2
SET country = 'India'
WHERE location = 'Chennai';

UPDATE layoffs_staging2
SET location = 'Dusseldorf'
WHERE location = 'DÃ¼sseldorf';

SELECT * FROM layoffs_staging2;

CREATE TEMPORARY TABLE location_details
SELECT DISTINCT(location) as location, country, COUNT(*) OVER(PARTITION BY location, country) AS count
FROM layoffs_staging2;

CREATE TEMPORARY TABLE location_details2
SELECT DISTINCT(location) AS location, country, COUNT(*) OVER(PARTITION BY location) AS count
FROM layoffs_staging2;

SELECT DISTINCT(l1.location) AS location, l1.country
FROM location_details l1
JOIN location_details2 l2
ON l1.location = l2.location
AND l1.location = l2.location
WHERE l1.count = l2.count-1;

UPDATE layoffs_staging2 l3
JOIN (
SELECT DISTINCT(l1.location) AS location, l1.country
FROM location_details l1
JOIN location_details2 l2
ON l1.location = l2.location
AND l1.location = l2.location
WHERE l1.count = l2.count-1
) t
ON t.location = l3.location
SET l3.country = t.country
WHERE l3.location != 'Non-U.S.';

-- Change Date Format to Standard Format

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

-- Change `date` column type to date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2;

-- 3. Deal with Nulls or Blank Spaces

SELECT * FROM layoffs_staging2
WHERE industry = '';

-- Try to populate these blanks or nulls by looking at existing data

SELECT * FROM layoffs_staging2
WHERE company = 'Appsmith';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND (t2.industry IS NOT NULL);

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = '';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = '';

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL;

ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT DEFAULT NULL;

ALTER TABLE layoffs_staging2
MODIFY COLUMN percentage_laid_off DOUBLE DEFAULT NULL;

-- For places where both total and percentage laid off values have NULLs
-- We cannot always be a 100% sure if the data would be useful 
-- But if we feel that yes these values are unnecessary then we can DELETE the data
-- But you have to be sure that these are unnecessary before deleting the rows

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

-- 4. Remove unnecessary columns

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;