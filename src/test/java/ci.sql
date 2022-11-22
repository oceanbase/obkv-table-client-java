CREATE TABLE IF NOT EXISTS `test_varchar_table` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_blob_table` (
    `c1` varchar(20) NOT NULL,
    `c2` blob DEFAULT NULL,
    PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_longblob_table` (
    `c1` varchar(20) NOT NULL,
    `c2` longblob DEFAULT NULL,
    PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_varchar_table_for_exception` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) NOT NULL,
    PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_tinyint_table` (
    `c1` varchar(20) NOT NULL,
    `c2` tinyint(4) DEFAULT NULL,
    PRIMARY KEY (`c1`)
  );

CREATE TABLE IF NOT EXISTS `test_smallint_table` (
    `c1` varchar(20) NOT NULL,
    `c2` smallint(8) DEFAULT NULL,
    PRIMARY KEY (`c1`)
  );

CREATE TABLE IF NOT EXISTS `test_int_table` (
      `c1` int(12) NOT NULL,
      `c2` int(12) DEFAULT NULL,
      PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_bigint_table` (
      `c1` bigint(20) NOT NULL,
      `c2` bigint(20) DEFAULT NULL,
      PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_double_table` (
       `c1` varchar(20) NOT NULL,
       `c2` double DEFAULT NULL,
       PRIMARY KEY (`c1`)
     );

CREATE TABLE IF NOT EXISTS `test_float_table` (
       `c1` varchar(20) NOT NULL,
       `c2` float DEFAULT NULL,
       PRIMARY KEY (`c1`)
     );

CREATE TABLE IF NOT EXISTS `test_varbinary_table` (
    `c1` varchar(20) NOT NULL,
    `c2` varbinary(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_timestamp_table` (
     `c1` varchar(20) NOT NULL,
     `c2` timestamp DEFAULT NULL,
     PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_datetime_table` (
     `c1` varchar(20) NOT NULL,
     `c2` datetime DEFAULT NULL,
     PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `testHash`(
    `K` bigint,
    `Q` varbinary(256),
    `T` bigint,
    `V` varbinary(1024),
    PRIMARY KEY(`K`, `Q`, `T`)
) partition by hash(`K`) partitions 16;

CREATE TABLE IF NOT EXISTS `testPartition` (
    `K` varbinary(1024),
    `Q` varbinary(256),
    `T` bigint,
    `V` varbinary(1024),
    K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4)),
    PRIMARY KEY(`K`, `Q`, `T`)
) partition by key(K_PREFIX) partitions 15;

CREATE TABLE IF NOT EXISTS `test_increment` (
    `c1` varchar(255),
    `c2` int,
    `c3` int,
    PRIMARY KEY(`c1`)
);

CREATE TABLE IF NOT EXISTS `test_append`(
    `c1` varchar(255),
    `c2` varbinary(1024),
    `c3` varchar(255),
    PRIMARY KEY(`c1`)
);

CREATE TABLE IF NOT EXISTS `testRange` (
    `K` varbinary(1024),
    `Q` varbinary(256),
    `T` bigint,
    `V` varbinary(102400),
    PRIMARY KEY(`K`, `Q`, `T`)
) partition by range columns (`K`) (
    PARTITION p0 VALUES LESS THAN ('a'),
    PARTITION p1 VALUES LESS THAN ('w'),
    PARTITION p2 VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS `test_hbase$fn` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `test_batch_query` (
 `c1` bigint NOT NULL,
 `c2` varchar(20) DEFAULT NULL,
PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));

CREATE TABLE `test_query_filter_mutate` (
 `c1` bigint NOT NULL,
 `c2` varbinary(1024) DEFAULT NULL,
 `c3` varchar(20) DEFAULT NULL,
 `c4` bigint DEFAULT NULL,
  PRIMARY KEY(`c1`)) partition by range columns (`c1`) (
      PARTITION p0 VALUES LESS THAN (300),
      PARTITION p1 VALUES LESS THAN (1000),
      PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `test_mutation` (
    `c1` bigint NOT NULL,
    `c2` varchar(20) NOT NULL,
    `c3` varbinary(1024) DEFAULT NULL,
    `c4` bigint DEFAULT NULL,
    PRIMARY KEY(`c1`, `c2`)) partition by range columns (`c1`) (
          PARTITION p0 VALUES LESS THAN (300),
          PARTITION p1 VALUES LESS THAN (1000),
          PARTITION p2 VALUES LESS THAN MAXVALUE);
