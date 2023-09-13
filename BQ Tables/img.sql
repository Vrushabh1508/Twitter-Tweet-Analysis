-- DDL for image table

CREATE TABLE img_twt_dataset.img(
	unique_id STRING,
	keyword STRING,
	image_id STRING,
	image_name STRING,
	thumbnail_url STRING,
	text STRING,
	text_flag BOOL,
	tags JSON,
	processing_datetime DATETIME,
	created_date DATE,
)
PARTITION BY (created_date)
OPTIONS (
  partition_expiration_days = 120
)