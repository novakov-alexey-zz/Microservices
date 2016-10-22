CREATE SCHEMA IF NOT EXISTS dbo;
CREATE TABLE IF NOT EXISTS dbo.Event (
  EVENT_ID      INT AUTO_INCREMENT NOT NULL,
  NAME          VARCHAR(30),
  TIME_OF_START TIMESTAMP,
  FILE_NAME VARCHAR(30)
);
