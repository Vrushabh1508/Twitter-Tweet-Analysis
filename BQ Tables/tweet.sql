--DDL for tweet table

CREATE TABLE `image-interpreter-384310.img_twt_dataset.tweet`
(
  batch_id STRING,
  image_name STRING,
  processing_datetime DATETIME,
  created_date DATE,
  tags STRING,
  tweet_id INT64,
  tweet_text STRING,
  sentimental_score FLOAT64,
  sentiment STRING
)
PARTITION BY (created_date)
OPTIONS (
  partition_expiration_days = 120
);
