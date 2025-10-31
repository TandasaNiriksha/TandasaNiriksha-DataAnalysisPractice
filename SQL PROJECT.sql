use layoff_world;
Select * 
From layoffs;

Create table layoffs_staging
like layoffs;

insert layoffs_staging
select *
from layoffs;

Select * 
From layoffs_staging;

select *,
	row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging;

With duplic_cte as
	(
	select *,
	row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
	from layoffs_staging
	) 
  select *
  from duplic_cte
  where row_num>1 ;
  
   select * 
  from layoffs_staging
  where company = '100 Thieves' ;
  
  
  With duplicate_cte as
	(
	select *,
	row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
	from layoff_staging
	) 
  select *
  from duplicate_cte
  where row_num > 1 ;
  
  CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

	select *
	from layoffs_staging2;

    insert into layoffs_staging2
    select *,
	row_number() 
    over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date',
    stage,country,funds_raised_millions) as row_num
	from layoffs_staging;
    

	select *
	from layoffs_staging2
    where row_num > 1;
    
    
    -------- standardizing data ---
    
    
    select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2;

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

Select Distinct country, trim(trailing '.' From country)
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry =  trim(trailing '.' From country)
where industry like 'United States%';

select distinct country
from layoffs_staging2;

Update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
;

alter table layoffs_staging2
modify column `date` date;


---- Handling Null ---

select * 
from layoffs_staging2
where industry is null
or industry = ' ';


update layoffs_staging2
set industry = null
where industry = ' '
;

select * 
from layoffs_staging2
where company = 'Airbnb';

select * 
from layoffs_staging2 t1
join layoffs_staging2  t2
     on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
set t1.industry = t2.industry     
where t1.industry is null 
and t2.industry is not null;

select * 
from layoffs_staging2
where company like 'Bally%';

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * 
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select country , sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select company , sum(total_laid_off)
from layoffs_staging2;


select stage , sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;


select company , sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select substring(`date` , 1 , 7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date` , 1 , 7) is not null
group by `month`
order by 1 asc
;

with rolling_table as
(
select substring(`date` , 1 , 7) as `month`, sum(total_laid_off) as total_off 
from layoffs_staging2
where substring(`date` , 1 , 7) is not null
group by `month`
order by 1 asc
)
select `month` , total_off,
 sum(total_off) over(order by `month`) as rolling_total
from rolling_table
;

select company , sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc
;

select company , year(`date`) , sum(total_laid_off)
from layoffs_staging2
group by company, year(`date` )
order by 3 desc
;


with company_year(company,years,total_laid_off) as 
( select company , year(`date`) , sum(total_laid_off)
from layoffs_staging2
group by company, year(`date` )
) , company_year_rank as
(
select *,
 dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <=5 
;


