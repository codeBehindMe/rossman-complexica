/* Set's up a train table for AutoML Forecast 
 * 
 * Does things like setting up a Store-Day ID for the train set 
 */
CREATE TABLE
  rss.train_aml AS SELECT
  CAST(CONCAT(Store, UNIX_DATE(ObsDate)) AS INT64) AS Id,
  *
FROM
  `rss.train_raw`;
