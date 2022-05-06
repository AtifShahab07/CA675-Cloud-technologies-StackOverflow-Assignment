— Data Extraction queries 

select top 50000 * from posts where posts.ViewCount > 120000 ORDER BY posts.ViewCount desc;

select count(*) from posts where posts.ViewCount > 74410 and posts.ViewCount < 127079; 
select top 50000 * from posts where posts.ViewCount >74410 and posts.ViewCount < 127079 order by posts.ViewCount desc;

select count(*) from posts where posts.ViewCount>52900 and posts.ViewCount < 74410; 
select top 50000 * from posts where posts.ViewCount > 52900 and posts.ViewCount < 74410 order by posts.ViewCount desc;

select count(*) from posts where posts.ViewCount > 41121 and posts.ViewCount < 52900;
select top 50000 * from posts where posts.ViewCount > 41121 and posts.ViewCount < 52900 order by posts.ViewCount desc;
