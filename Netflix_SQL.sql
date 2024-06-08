-- created table as per the maximum size, using jupyter notebook
-- to check max size of the variable
CREATE TABLE netflix_raw (
   show_id varchar(10) primary key,
   type varchar(10),
   title varchar(200),
   director varchar(250),
   cast varchar(800),
   country varchar(150),
   date_added varchar(20),
   release_year int DEFAULT NULL,
   rating varchar(10),
   duration varchar(10),
   listed_in varchar(100),
   description varchar(500)
 );

-- handling foreignkeys

-- checking duplicates if there is any in show_id
select show_id,count(*) from netflix_raw
group by show_id having count(*)>1;
-- There are no duplicate values in show_id

-- checking duplicates if there is any in title
select * from netflix_raw where title in (
select title from netflix_raw
group by title having count(*)>1) order by title;
-- there are some duplicate values in title but type is different
-- so i want to check duplicates for the title and type together
select * from netflix_raw
where concat(title,type) in
 (select concat(title,type) from netflix_raw
group by title,type having count(*)>1) order by title;
-- so we know that there are some duplicates based on title and type
-- we need to handle the duplicates

-- updated date_added column datatype as date
update netflix_raw set date_added = str_to_date(date_added, '%M %e, %Y');

-- Based on title and type, removed duplicate values by using row_number()
with cte as(
select *,row_number() over(partition by title,type order by show_id) as rn
from netflix_raw)
select * from cte where rn=1;

-- created new table for listed_in(genre)  
create table netflix_listed_in
SELECT show_id, trim(SUBSTRING_INDEX(SUBSTRING_INDEX(listed_in, ',', n.digit+1), ',', -1)) AS listed_in
FROM netflix_raw
JOIN (
    SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    -- Add more UNION ALL SELECT statements as needed to cover maximum possible elements in your column
) AS n
ON LENGTH(REPLACE(listed_in, ',' , '')) <= LENGTH(listed_in)-n.digit
ORDER BY show_id;

-- created new table for director 
create table netflix_director
SELECT show_id, trim(SUBSTRING_INDEX(SUBSTRING_INDEX(director, ',', n.digit+1), 
',', -1)) AS director
FROM netflix_raw
JOIN (
    SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    -- Add more UNION ALL SELECT statements as needed to cover maximum possible elements in your column
) AS n
ON LENGTH(REPLACE(director, ',' , '')) <= LENGTH(director)-n.digit
ORDER BY show_id;

-- created new table for country 
create table netflix_country
SELECT show_id, trim(SUBSTRING_INDEX(SUBSTRING_INDEX(country, ',', n.digit+1), 
',', -1)) AS country
FROM netflix_raw
JOIN (
    SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    -- Add more UNION ALL SELECT statements as needed to cover maximum possible elements in your column
) AS n
ON LENGTH(REPLACE(ountry, ',' , '')) <= LENGTH(country)-n.digit
ORDER BY show_id;

-- created new table for cast 
create table netflix_cast
SELECT show_id, trim(SUBSTRING_INDEX(SUBSTRING_INDEX(cast, ',', n.digit+1), 
',', -1)) AS cast
FROM netflix_raw
JOIN (
    SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    -- Add more UNION ALL SELECT statements as needed to cover maximum possible elements in your column
) AS n
ON LENGTH(REPLACE(cast, ',' , '')) <= LENGTH(cast)-n.digit
ORDER BY show_id;

-- Fetching data from new tables
select * from netflix_country;
select * from netflix_cast;
select * from netflix_listed_in;
select * from netflix_director;

 -- In the duration column some data is missing , 
 -- but that data  was by mistakenly added in the rating column so transfered that data into
  -- duration column by using case statemnet 
 -- Created new table as netflix after cleaning the data
create table netflix
with cte as(
select *,row_number() over(partition by title,type order by show_id) as rn
from netflix_raw
)
select show_id,type,title, date_added,
release_year,
rating , case when duration is null then rating else duration end as duration,
description from cte ;
select * from netflix;

-- populate missing values in country column
insert into netflix_country
select show_id,m.country from netflix_raw nr 
inner join (
select director,country
from netflix_country nc inner join netflix_directors nd 
on nc.show_id=nd.show_id 
group by director,country )m on nr.director=m.director
where nr.country is null;

-- netflix data analysis

/* for each director count the num of movies and tv shows 
created by them in separated columns for directors who have created
tv shows and movies both*/
select nd.director,
count(distinct case 
when n.type = 'movie' then n.show_id end) as no_of_movies,
count(distinct case 
when n.type= 'Tv Show' then n.show_id end) as no_of_tvshow
from netflix n join netflix_directors nd 
on nd.show_id = n.show_id
group by nd.director
having count(distinct n.type)>1 ;


-- which country has highest number of comedy movies
select nc.country,count(distinct ng.show_id) as num_of_comedy
 from netflix_listed_in ng
join netflix_country nc on ng.show_id=nc.show_id
join netflix n on n.show_id = nc.show_id
where ng.listed_in='comedies' and n.type = 'movie'
group by nc.country
order by count(ng.show_id) desc limit 1
;

-- for each year (as per data added to netflix ), 
-- which director has maximum number of movies released
with cte as (
select nd.director,year(n.date_added) as year_added,
count(*) as no_of_movies from netflix_directors nd 
join netflix n on n.show_id=nd.show_id
where n.type = 'movie'
group by nd.director,year(n.date_added)),
cte2 as(
select *, 
row_number()over(partition by year_added order by no_of_movies desc,
director )as rn
from cte)
select * from cte2 where rn=1    
;

-- what is the  average duration of movie in each genre
select ng.listed_in,round(avg(n.duration),2) as avg_duration
from netflix_listed_in ng join netflix n on
n.show_id=ng.show_id where n.type='movie'
group by ng.listed_in;

-- find the list of directors who have created horror and comedy movies both
-- display director names along with number of comedy and
-- horror movies directed by them.
select nd.director ,
count(distinct case when ng.listed_in = 'comedies' then n.show_id end )
as no_of_comedy,
count(distinct case when ng.listed_in = 'Horror movies' then n.show_id end)
as no_of_horror
 from netflix_listed_in ng 
join netflix_directors nd on ng.show_id=nd.show_id 
join netflix n on n.show_id=ng.show_id
 where n.type='movie' and ng.listed_in in('comedies',
'Horror Movies')
group by nd.director having count(distinct ng.listed_in)=2;
select * from netflix_listed_in where show_id in(
select show_id  from netflix_directors where director = 'steve brill') 
order by listed_in ;