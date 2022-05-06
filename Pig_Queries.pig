-- read all four CV files

data1 = LOAD '/data_storage/QueryResults1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, contentlicense:chararray);

data2 = LOAD '/data_storage/QueryResults2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, contentlicense:chararray);

data3 = LOAD '/data_storage/QueryResults3.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, contentlicense:chararray);

data4 = LOAD '/data_storage/QueryResults4.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, contentlicense:chararray);


-- concat all the four CSV files

stackex_data = UNION data1,data2,data3,data4;


-- View the number of rows in the loaded data

Stackex_data_all = GROUP Stackex_data ALL;
Stackex_count = FOREACH Stackex_data_all GENERATE COUNT(Stackex_data.id);
DUMP Stackex_count;


-- Transform the Extracted data
-- Select only required columns (i.e Id, Score, ViewCount, Body, OwnerUserId, Title, Tags)

Stackex_data = FOREACH Stackex_data GENERATE id, score, viewcount, body, owneruserid, title, tags;
Stackex_data = FOREACH Stackex_data GENERATE id, score, viewcount, owneruserid, title, tags, (REPLACE(body,'[\r\n]+','')) AS body;

-- Remove Duplicate Tuples (if any)

Stackex_data = DISTINCT Stackex_data;

-- Load cleaned data to hive

STORE Stackex_data INTO 'stackex_data.stackex_Transformed' USING org.apache.hive.hcatalog.pig.HCatStorer();

--TASK 4 Cleanup for TF-IDF

-- Load data to pig

stack_data = UNION data1,data2,data3,data4;

-- Get top 10 users by post

stack_data_distinct_users_post = GROUP stack_data BY owneruserid;
stack_data_users_by_max_score = FOREACH stack_data_distinct_users_post GENERATE group AS userid, SUM(stack_data.score) AS maxscore;
stack_data_users_by_max_score_desc_order = ORDER stack_data_users_by_max_score BY maxscore DESC;
stack_data_limit_10 = LIMIT stack_data_users_by_max_score_desc_order 10;
stack_data_top_10_user_id = FOREACH stack_data_limit_10 GENERATE userid;
stack_data_posts_by_10_users = JOIN stack_data BY owneruserid, stack_data_top_10_user_id BY userid;
stack_data_posts_by_10_users = FOREACH stack_data_posts_by_10_users GENERATE stack_data::owneruserid, LOWER(TRIM(REPLACE(stack_data::body,'[ ]{2,}',' '))) AS stack_data::body;

--store the result in hdfs

STORE post_data_posts_by_10_users INTO 'hdfs:///PostDataOutput' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');
