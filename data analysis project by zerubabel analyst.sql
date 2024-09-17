CREATE TABLE `layoff_staging2` (
  `id` int(11) NOT NULL,
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- import raw data
-- crete staging database for storing row data set
-- remove duplicate
-- standrdize the data
-- blank and null values
-- remove unnecessary rows and column
-- import raw data
select * from layoffs;
-- crete staging database for storing row data set
create table layoff_staging like layoffs;
insert into layoff_staging
select * from layoffs;
select * from layoff_staging;
alter table layoff_staging drop column id;
select *, row_number() over(partition by company,location,industry,total_laid_off,'date',stage,country) as row_num from layoff_staging; 
select * from layoff_staging2;
-- remove duplicate
insert into layoff_staging2
select *, row_number() over(partition by company,location,industry,total_laid_off,'date',stage,country) as row_num from layoff_staging;
select * from layoff_staging2;
SET SQL_SAFE_UPDATES = 0;
delete from layoff_staging2 where row_num >1;
select * from layoff_staging2;
-- standrdize the data
select company ,trim(company) from layoff_staging2;
update layoff_staging2
set company = trim(company);
select industry from layoff_staging2 where industry='crypto';
select distinct industry from layoff_staging2;
update layoff_staging2 
set industry ='crypto' where industry like 'crypto%';
select distinct country  from layoff_staging2 order by 1;
select distinct country from layoff_staging2 where country like 'united states%';
select distinct country, trim(trailing '.' from country) as st_country from layoff_staging2 where country like 'united states%';
update layoff_staging2 
set country =trim(trailing '.' from country) where country like 'united states%';
-- standardize date column
select `date`,str_to_date(`date`,'%m/%d/%y') from layoff_staging2;
update layoff_staging2 
set `date` =str_to_date(`date`,'%m/%d/%y');
-- identify null 
SELECT * FROM layoff_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off is null;
-- remove null value
delete FROM layoff_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off is null;
-- identify where industry is null
SELECT * FROM layoff_staging2 WHERE industry IS NULL or industry ='';
-- self join to replace null value
SELECT t1.industry,t2.industry  FROM layoff_staging2 t1 join layoff_staging2 t2
on t1.company=t2.company and t1.location=t2.location where t1.industry is null or t1.industry=''and t2.industry is not null;
update layoff_staging2 set industry = null where industry='';
update layoff_staging2 t1 join layoff_staging2 t2
on t1.company=t2.company
set t1.industry= t2.industry where t1.industry is null and t2.industry is not null;
select * from layoff_staging2 where company ='airbnb';
select * from layoff_staging2;
alter table layoff_staging2 drop column row_num;
alter table layoff_staging2 drop column id;
-- Explatory data analysis full project
select * from layoff_staging2;
select max(total_laid_off),max(percentage_laid_off) from layoff_staging2;
select company ,sum(total_laid_off) from layoff_staging2 group by company order by 2 desc;
select industry ,sum(total_laid_off) from layoff_staging2 group by industry order by 2 desc;
select country ,sum(total_laid_off) from layoff_staging2 group by country order by 2 desc;
select year(`date`) ,sum(total_laid_off) from layoff_staging2 group by year(`date`) order by 2 desc;
select min(`date`),max(`date`) from layoff_staging2;
SELECT SUBSTRING('Hello World', 7, 5) as string_function;
alter table layoff_staging2 modify column `date` date ;
select substring(`date`,1,7) as `Month` , sum(total_laid_off) from layoff_staging2 where substring(`date`,1,7) is not null group by `Month` order by 1 asc;
with rolling_total as
(
select substring(`date`,1,7) as `Month` , sum(total_laid_off) as total_off from layoff_staging2 where substring(`date`,1,7) is not null group by `Month` order by 1 asc
)
select `Month` ,total_off,sum(total_off) over(order by `Month` asc) as total_rolling from rolling_total;
select * from layoff_staging2;
select count(distinct industry) as total_industry from layoff_staging2;