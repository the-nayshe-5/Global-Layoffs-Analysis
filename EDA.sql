SELECT * FROM layoffs_staging2;

-- Checking for highest total_laid_off and percentage_laid_off

SELECT MAX(total_laid_off), MAX(percentage_laid_off) 
FROM layoffs_staging2;

-- 1 = 100% laid off - means the company went under.

SELECT * FROM layoffs_staging2
WHERE percentage_laid_off IN (
	SELECT MAX(percentage_laid_off) FROM layoffs_staging2
);

-- Funds raised
SELECT * FROM layoffs_staging2
ORDER BY funds_raised DESC;

SELECT *, SUM(funds_raised) OVER(PARTITION BY company) AS total_funds_raised
FROM layoffs_staging2
ORDER BY total_funds_raised DESC;

-- People laid off

SELECT DISTINCT(company), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT DISTINCT(industry), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT DISTINCT(country), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Time range

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;
-- Starting date: 2020-03-11
-- Start of pandemic

SELECT * FROM layoffs_staging2
WHERE industry = 'Consumer';




