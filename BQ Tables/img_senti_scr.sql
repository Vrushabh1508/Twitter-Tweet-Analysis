--DDL for img_senti_scr table

CREATE TABLE `image-interpreter-384310.img_twt_dataset.img_senti_scr`
(
  unique_id STRING,
  keyword STRING,
  image_name STRING,
  thumbnail_URL STRING,
  text STRING,
  tags STRING,
  batch_id INT64,
  positive_sentiment INT64,
  negative_sentiment INT64,
  neutral_sentiment INT64,
  processing_datetime DATETIME,
  created_date DATE
)
PARTITION BY (created_date)
OPTIONS (
  partition_expiration_days = 120
);