-- Query Analysis 1 (Countries with income level 'UPPER MIDDLE INCOME')
SELECT DISTINCT country_name FROM moduna.country_detail 
WHERE UPPER(income_group)='UPPER MIDDLE INCOME'
ORDER BY country_name ASC;

-- Query Analysis 2 (Countries with income level 'LOW INCOME' per region)
SELECT DISTINCT region, country_name
FROM moduna.country_detail WHERE UPPER(income_group)='LOW INCOME' AND region != '' 
ORDER BY region, country_name ASC;

-- Query Analysis 3 (Region with highest proportion of 'HIGH INCOME' countries)
SELECT region,count(*) AS proportion 
FROM moduna.country_detail 
WHERE UPPER(income_group)='HIGH INCOME' 
GROUP BY region
ORDER BY proportion DESC LIMIT 1;

-- Query Analysis 4 (Cummlative and Running GDP per region, ordered by income_group and country_name from lowest to highest)
-- Assumption: GDP has been summed over region to gve the 'per region' view. Also individual income group, country_name and gdp
-- has been displayed. As mentioned in the mail, 2017 has been considered as the base year

SELECT t1.region, t2.income_group, t2.country_name, t2.gdp_country, t1.cummulative_gdp_region
FROM
(SELECT b.region,SUM(a.gdp) AS cummulative_gdp_region
FROM 
moduna.country_gdp a
LEFT JOIN
moduna.country_detail b
ON a.country_code=b.country_code
WHERE a.year='2017' AND b.region != '' 
GROUP BY b.region) t1
LEFT JOIN
(
SELECT b.region,b.income_group,b.country_name,a.gdp as gdp_country
FROM moduna.country_gdp a
INNER JOIN
moduna.country_detail b
ON a.country_code = b.country_code
WHERE b.region != '' AND b.income_group != '' AND a.year='2017'
ORDER BY 
b.region,
CASE WHEN UPPER(b.income_group)='LOW INCOME' THEN '1'
	WHEN UPPER(b.income_group)='LOWER MIDDLE INCOME' THEN '2' 
	WHEN UPPER(b.income_group)='UPPER MIDDLE INCOME' THEN '3' 
	WHEN UPPER(b.income_group)='HIGH INCOME' THEN '4' 
END,b.country_name)t2
ON TRIM(UPPER(t1.region)) = TRIM(UPPER(t2.region));


-- Query Analysis 5 (Percentage value of difference year-on-year per country)
SELECT  
b.country_name,
a.year,
a.gdp,
ROUND(((a.gdp - LAG(a.gdp,1) OVER (PARTITION BY a.country_code ORDER BY a.year asc)) *100 )
/LAG(a.gdp, 1) OVER (PARTITION BY a.country_code ORDER BY a.year asc),2) AS pct_change
FROM moduna.country_gdp a
INNER JOIN 
moduna.country_detail b
ON a.country_code = b.country_code
ORDER BY b.country_name, a.year;


-- Query Analysis 6 (3 countries per region with lowest GDP)
-- Assumption - 2017 has been considered as the base year as mentioned in the mail.
SELECT t1.region, t1.country_name, t1.gdp 
FROM
(
SELECT b.region, b.country_name, a.gdp,RANK() OVER (PARTITION BY b.region ORDER BY a.gdp ASC) as gdp_rank
FROM 
moduna.country_gdp a
LEFT JOIN
moduna.country_detail b
ON a.country_code = b.country_code
WHERE year='2017' AND b.region != ''
) t1
WHERE t1.gdp_rank IN (1,2,3)
ORDER BY t1.region, t1.gdp_rank;
	