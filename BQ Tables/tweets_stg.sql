--DDL for tweets staging table

CREATE TABLE `image-interpreter-384310.img_twt_dataset.tweets_stg`
(
  unique_id STRING,
  image_name STRING,
  tags JSON,
  raw_tweets JSON,
  processing_datetime DATETIME,
  created_date DATE
)
PARTITION BY (created_date);