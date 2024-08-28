USE TEST;

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
    `c2` timestamp(6) DEFAULT NULL,
    `c3` timestamp(3) DEFAULT NULL,
    `c4` timestamp DEFAULT NULL,
     PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `test_datetime_table` (
    `c1` varchar(20) NOT NULL,
    `c2` datetime(6) DEFAULT NULL,
    `c3` datetime(3) DEFAULT NULL,
    `c4` datetime DEFAULT NULL,
     PRIMARY KEY (`c1`)
    );

CREATE TABLE IF NOT EXISTS `testHash`(
    `K` bigint,
    `Q` varbinary(256),
    `T` bigint,
    `V` varbinary(1024),
    INDEX i1(`K`, `V`) local,
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

CREATE TABLE IF NOT EXISTS `testPartitionHashComplex` (
    `c1` int NOT NULL,
    `c2` bigint NOT NULL,
    `c3` varchar(20) default NULL,
PRIMARY KEY (`c1`, `c2`)
) DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_bin ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(`c1`) subpartition by hash(`c2`) subpartitions 4 partitions 16;

CREATE TABLE IF NOT EXISTS `testPartitionKeyComplex` (
    `c0` tinyint NOT NULL,
    `c1` int NOT NULL,
    `c2` bigint NOT NULL,
    `c3` varbinary(1024) NOT NULL,
    `c4` varchar(1024) NOT NULL,
    `c5` varchar(1024) NOT NULL,
    `c6` varchar(20) default NULL,
PRIMARY KEY (`c0`, `c1`, `c2`, `c3`, `c4`, `c5`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(`c0`, `c1`, `c2`, `c3`, `c4`) subpartition by key(`c5`) subpartitions 4 partitions 16;

CREATE TABLE IF NOT EXISTS `testDateTime` (
    `c0` DateTime NOT NULL,
    `c1` Datetime(6) NOT NULL,
    `c2` varchar(20) default NULL,
PRIMARY KEY (`c0`, `c1`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
PARTITION BY RANGE COLUMNS(`c0`) SUBPARTITION BY KEY(`c1`) (
    PARTITION `p0` VALUES LESS THAN ('2022-04-20 00:00:00') (
        SUBPARTITION `sp0`, SUBPARTITION `sp1`, SUBPARTITION `sp2`, SUBPARTITION `sp3`
    ),
    PARTITION `p1` VALUES LESS THAN ('2022-04-25 00:00:00') (
        SUBPARTITION `sp4`, SUBPARTITION `sp5`, SUBPARTITION `sp6`, SUBPARTITION `sp7`
    ),
    PARTITION `p2` VALUES LESS THAN ('2022-04-30 00:00:00') (
        SUBPARTITION `sp8`, SUBPARTITION `sp9`, SUBPARTITION `sp10`, SUBPARTITION `sp11`
    )
);

CREATE TABLE IF NOT EXISTS `testPartitionRangeComplex` (
    `c1` int NOT NULL,
    `c2` bigint NOT NULL,
    `c3` varbinary(1024) NOT NULL,
    `c4` varchar(1024) NOT NULL,
    `c5` varchar(20) default NULL,
PRIMARY KEY (`c1`, `c2`, `c3`, `c4`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range(`c1`) subpartition by range columns (`c2`, `c3`, `c4`) (
PARTITION p0 VALUES LESS THAN (500)
(
    SUBPARTITION p0sp0 VALUES LESS THAN (500, 't', 't'),
    SUBPARTITION p0sp1 VALUES LESS THAN (1000, 'T', 'T'),
    SUBPARTITION p0sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)
),
PARTITION p1 VALUES LESS THAN (1000)
(
    SUBPARTITION p1sp0 VALUES LESS THAN (500, 't', 't'),
    SUBPARTITION p1sp1 VALUES LESS THAN (1000, 'T', 'T'),
    SUBPARTITION p1sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)
),
PARTITION p2 VALUES LESS THAN MAXVALUE
(
    SUBPARTITION p2sp0 VALUES LESS THAN (500, 't', 't'),
    SUBPARTITION p2sp1 VALUES LESS THAN (1000, 'T', 'T'),
    SUBPARTITION p2sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)
));

CREATE TABLE IF NOT EXISTS `testKey` (
    `K` varbinary(1024),
    `Q` varbinary(256),
    `T` bigint,
    `V` varbinary(1024),
    INDEX i1(`K`, `V`) local,
    PRIMARY KEY(`K`, `Q`, `T`)
) partition by key(K) partitions 15;

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
    `V` varbinary(10240),
    INDEX i1(`K`, `V`) local,
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
    KEY `idx_k_v` (`K`, `V`) LOCAL,
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

CREATE TABLE `test_mutation_with_range` (
    `c1` bigint NOT NULL,
    `c1sk` varchar(20) NOT NULL,
    `c2` varbinary(1024) DEFAULT NULL,
    `c3` varchar(20) DEFAULT NULL,
    `c4` bigint DEFAULT NULL,
    PRIMARY KEY(`c1`, `c1sk`)) partition by range columns (`c1`) (
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

CREATE TABLE `test_mutation_column_reverse` (
     `c2` bigint NOT NULL,
     `c1` varchar(20) NOT NULL,
     `c3` varbinary(1024) DEFAULT NULL,
     `c4` bigint DEFAULT NULL,
     PRIMARY KEY(`c2`, `c1`)) partition by range columns (`c2`) (
PARTITION p0 VALUES LESS THAN (300),
PARTITION p1 VALUES LESS THAN (1000),
PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `test_throttle` (
    `c1` bigint NOT NULL,
    `c2` varchar(20) NOT NULL,
    `c3` varbinary(1024) DEFAULT NULL,
    `c4` bigint DEFAULT NULL,
    PRIMARY KEY(`c1`, `c2`)) partition by range columns (`c1`) (
        PARTITION p0 VALUES LESS THAN (500000),
        PARTITION p1 VALUES LESS THAN (1000000),
        PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE test_aggregation (
    `c1` varchar(255),
    `c2` int NOT NULL,
    `c3` bigint NOT NULL,
    `c4` float NOT NULL,
    `c5` double NOT NULL,
    `c6` tinyint NULL,
    `c7` datetime,
    PRIMARY KEY(`c1`)
);
        
CREATE TABLE `test_partition_aggregation` (
    `c1` bigint NOT NULL,
    `c2` bigint DEFAULT NULL,
    PRIMARY KEY (`c1`))partition by range(`c1`) (
        PARTITION p0 VALUES LESS THAN (200),
        PARTITION p1 VALUES LESS THAN (500),
        PARTITION p2 VALUES LESS THAN (900));

CREATE TABLE `test_ttl_timestamp` (
 `c1` bigint NOT NULL,
 `c2` varchar(20) DEFAULT NULL,
 `c3` bigint DEFAULT NULL,
 `expired_ts` timestamp(6),
PRIMARY KEY (`c1`)) TTL(expired_ts + INTERVAL 0 SECOND);

CREATE TABLE `test_ttl_timestamp_5s` (
    `c1` bigint NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    `c3` bigint DEFAULT NULL,
    `expired_ts` timestamp(6),
PRIMARY KEY (`c1`)) TTL(expired_ts + INTERVAL 5 SECOND);

CREATE TABLE IF NOT EXISTS `test_auto_increment_rowkey` (
    `c1` int auto_increment,
    `c2` int NOT NULL,
    `c3` int DEFAULT NULL,
    `c4` varchar(255) DEFAULT NULL,
    PRIMARY KEY(`c1`, `c2`)) partition by range columns(`c2`) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000));

CREATE TABLE IF NOT EXISTS `test_auto_increment_not_rowkey` (
    `c1` int NOT NULL,
    `c2` int DEFAULT NULL,
    `c3` tinyint auto_increment,
    `c4` varchar(255) DEFAULT NULL,
    PRIMARY KEY(`c1`)) partition by range columns(`c1`) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (1000));

CREATE TABLE IF NOT EXISTS `test_global_hash_range` (
    `C1` int(11) NOT NULL,
    `C2` int(11) DEFAULT NULL,
    `C3` int(11) DEFAULT NULL,
    PRIMARY KEY (`C1`),
    KEY `idx` (`C2`)  GLOBAL partition by range(C2) (
        partition p0 values less than (100),
        partition p1 values less than (200),
        partition p2 values less than (300)),
    KEY `idx2` (`C3`)  LOCAL) partition by hash(c1) partitions 5;

CREATE TABLE IF NOT EXISTS `test_global_hash_hash` (
  `c1` int(11) NOT NULL,
  `c2` int(11) DEFAULT NULL,
  `c3` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`c1`),
  KEY `idx` (`c2`) GLOBAL partition by hash(`c2`) partitions 5) partition by hash(`c1`) partitions 7;

CREATE TABLE IF NOT EXISTS `test_global_key_key` (
  `c1` int(11) NOT NULL,
  `c2` int(11) DEFAULT NULL,
  `c3` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`c1`),
  KEY `idx` (`c2`) GLOBAL partition by key(`c2`) partitions 5) partition by key(`c1`) partitions 7;

CREATE TABLE IF NOT EXISTS `test_global_range_range` (
    `c1` int(11) NOT NULL,
    `c2` int(11) DEFAULT NULL,
    `c3` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`c1`),
    KEY `idx` (`c2`)  GLOBAL partition by range(c2) (
        partition p0 values less than (100),
        partition p1 values less than (200),
        partition p2 values less than (1000))) partition by range(c1) (
    partition p0 values less than (100),
    partition p1 values less than (200),
    partition p2 values less than (1000));

CREATE TABLE IF NOT EXISTS `test_global_index_no_part` (
  `C1` int(11) NOT NULL,
  `C2` int(11) DEFAULT NULL,
  `C3` int(11) DEFAULT NULL,
  PRIMARY KEY (`c1`),
  KEY `idx` (`c2`) GLOBAL,
  KEY `idx2` (c3) LOCAL) partition by hash(`c1`) partitions 7;

CREATE TABLE IF NOT EXISTS `test_global_all_no_part` (
  `C1` int(11) NOT NULL,
  `C2` int(11) DEFAULT NULL,
  `C3` int(11) DEFAULT NULL,
  PRIMARY KEY (`C1`),
  KEY `idx` (`C2`) GLOBAL,
  KEY `idx2` (C3) LOCAL);

CREATE TABLE IF NOT EXISTS `test_global_primary_no_part` (
  `C1` int(11) NOT NULL,
  `C2` int(11) DEFAULT NULL,
  `C3` int(11) DEFAULT NULL,
  PRIMARY KEY (`C1`),
  KEY `idx` (`C2`) GLOBAL partition by hash(`C2`) partitions 5,
  KEY `idx2` (C3) LOCAL);

CREATE TABLE IF NOT EXISTS `test_ttl_timestamp_with_index` (
`c1` varchar(20) NOT NULL,
`c2` bigint NOT NULL,
`c3` bigint DEFAULT NULL,
`c4` bigint DEFAULT NULL,
`expired_ts` timestamp(6),
PRIMARY KEY (`c1`, `c2`),
KEY `idx`(`c1`, `c4`) local,
KEY `idx2`(`c3`) global partition by hash(`c3`) partitions 4)
TTL(expired_ts + INTERVAL 0 SECOND) partition by key(`c1`) partitions 4;

CREATE TABLE IF NOT EXISTS  `error_message_table` (
    `c1` bigint(20) not null,
    `c2` varchar(5) not null,
    `c3` datetime default current_timestamp,
    `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
    `c5` double default 0,
    PRIMARY KEY (`c1`));

CREATE TABLE IF NOT EXISTS  `cse_index_1` (
     `measurement` VARBINARY(1024) NOT NULL,
     `tag_key` VARBINARY(1024) NOT NULL,
     `tag_value` VARBINARY(1024) NOT NULL,
     `series_ids` MEDIUMBLOB NOT NULL,
     PRIMARY KEY(`measurement`, `tag_key`, `tag_value`))
    partition by key(`measurement`) partitions 13;

CREATE TABLE IF NOT EXISTS `test_auto_increment_one_rowkey` (
    `c1` int auto_increment,
    `c2` int NOT NULL, PRIMARY KEY(`c1`));

CREATE TABLE IF NOT EXISTS `sync_item` (
    `uid` varchar(20) NOT NULL,
    `object_id` varchar(32) NOT NULL,
    `type` int(11) NULL,
    `ver_oid` varchar(32) NULL,
    `ver_ts` bigint(20) NULL,
    `data_id` varchar(32) NULL,
    CONSTRAINT `uid_object_id_unique` PRIMARY KEY (`uid`, `object_id`),
    index idx1(`uid`, `type`) local)
    DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci
    PARTITION BY KEY(`uid`) PARTITIONS 32;

CREATE TABLE `hash_key_sub_part` (
    `id` bigint(11) NOT NULL,
    `uid` varchar(20) NOT NULL,
    `object_id` varchar(32) NOT NULL,
    `type` int(11) DEFAULT NULL,
    `ver_oid` varchar(32) DEFAULT NULL,
    `ver_ts` bigint(20) DEFAULT NULL,
    `data_id` varchar(32) DEFAULT NULL,
    PRIMARY KEY (`id`, `uid`, `object_id`)
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci PARTITION BY HASH(`id`) SUBPARTITION BY KEY(`uid`) subpartitions 4 PARTITIONS 8;

CREATE TABLE IF NOT EXISTS `test_table_object` (
    `c1` tinyint primary key,
    `c2` smallint not null,
    `c3` int not null,
    `c4` bigint not null,
    `c5` varchar(128) not null,
    `c6` varbinary(128) not null,
    `c7` float not null,
    `c8` double not null,
    `c9` timestamp(6) not null,
    `c10` datetime(6) not null,
    `c11` int default null,
    `c12` tinytext DEFAULT NULL,
    `c13` text DEFAULT NULL,
    `c14` mediumtext DEFAULT NULL,
    `c15` longtext DEFAULT NULL,
    `c16` tinyblob DEFAULT NULL,
    `c17` blob DEFAULT NULL,
    `c18` mediumblob DEFAULT NULL,
    `c19` longblob DEFAULT NULL
);

CREATE TABLE `test_query_scan_order` (
  `c1` int(12) NOT NULL,
  `c2` int(11) NOT NULL,
  `c3` int(11) NOT NULL,
  PRIMARY KEY (`c1`, `c2`),
  KEY `idx` (`c1`, `c3`) BLOCK_SIZE 16384 LOCAL
) partition by hash(c1)
(partition `p0`,
partition `p1`,
partition `p2`);

CREATE TABLE IF NOT EXISTS `test_put` (
    `id` varchar(20) NOT NULL,
    `c1` bigint DEFAULT NULL,
    `c2` bigint DEFAULT NULL,
    `c3` varchar(32) DEFAULT NULL,
    `c4` bigint DEFAULT NULL,
    `expired_ts` timestamp(6) NOT NULL,
    PRIMARY KEY(`id`)) TTL(expired_ts + INTERVAL 1 SECOND) PARTITION BY KEY(`id`) PARTITIONS 32;

CREATE TABLE IF NOT EXISTS `test_put_with_local_index` (
    `id` varchar(20) NOT NULL,
    `c1` bigint DEFAULT NULL,
    index idx1(`c1`) local,
    `expired_ts` timestamp(6) NOT NULL,
    PRIMARY KEY(`id`)) TTL(expired_ts + INTERVAL 1 SECOND) PARTITION BY KEY(`id`) PARTITIONS 32;

CREATE TABLE IF NOT EXISTS `test_put_with_global_index` (
    `id` varchar(20) NOT NULL,
    `c1` bigint DEFAULT NULL,
    index idx1(`c1`) global,
    `expired_ts` timestamp(6) NOT NULL,
    PRIMARY KEY(`id`)) TTL(expired_ts + INTERVAL 1 SECOND) PARTITION BY KEY(`id`) PARTITIONS 32;

CREATE TABLEGROUP test SHARDING = 'ADAPTIVE';
CREATE TABLE `test$family_group` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test;

CREATE TABLEGROUP test2 SHARDING = 'ADAPTIVE';
CREATE TABLE `test2$family1` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test2;

CREATE TABLEGROUP test_start_end_keys_key SHARDING = 'ADAPTIVE';
CREATE TABLE `test_start_end_keys$family_key` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
) TABLEGROUP = test_start_end_keys_key
partition by key(K) partitions 17;

CREATE TABLEGROUP test_start_end_keys_range SHARDING = 'ADAPTIVE';
CREATE TABLE `test_start_end_keys_range$family_range` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (`K`, `Q`, `T`)
)  TABLEGROUP = test_start_end_keys_range
partition by range columns (`K`) (
    PARTITION p0 VALUES LESS THAN ('a'),
    PARTITION p1 VALUES LESS THAN ('w'),
    PARTITION p2 VALUES LESS THAN MAXVALUE
);

CREATE TABLE `test_batch_get` (
  `c1` int(11) NOT NULL,
  `c2` int(11) NOT NULL,
  `c3` int(11) DEFAULT NULL,
  `c4` int(11) DEFAULT NULL,
  PRIMARY KEY (`c1`, `c2`)
) partition by key(`c2`) partitions 3;

CREATE TABLE `test_local_index_with_vgen_col` (
  `name` varchar(512) NOT NULL DEFAULT '',
  `pk` varchar(512) NOT NULL,
  `adiu` varchar(512) NOT NULL DEFAULT '',
  `id` bigint(20) NOT NULL DEFAULT 0,
  `name_v` varchar(20) GENERATED ALWAYS AS (substr(`name`,1,5)) VIRTUAL,
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`adiu`, `pk`, `gmt_create`),
  KEY `idx_adiu_v_name` (`adiu`, `name_v`) BLOCK_SIZE 16384 LOCAL
) TTL (gmt_create + INTERVAL 300 SECOND) partition by key(adiu) partitions 8;

CREATE TABLE `test_global_index_with_vgen_col` (
  `name` varchar(512) NOT NULL DEFAULT '',
  `pk` varchar(512) NOT NULL,
  `adiu` varchar(512) NOT NULL DEFAULT '',
  `id` bigint(20) NOT NULL DEFAULT 0,
  `name_v` varchar(20) GENERATED ALWAYS AS (substr(`name`,1,5)) VIRTUAL,
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`adiu`, `pk`, `gmt_create`),
  KEY `idx_adiu_v_name` (`adiu`) global
) TTL (gmt_create + INTERVAL 300 SECOND) partition by key(adiu) partitions 8;

CREATE TABLE `test_current_timestamp` (
  `c1` int not null,
  `c2` varchar(255),
  `c3` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (`c1`),
   KEY `idx_adiu_v_name` (`c2`, `c3`) global partition by key(`c2`) partitions 5
) partition by key(`c1`) partitions 8;

CREATE TABLE `table_ttl_00` (
  `name` varchar(512) NOT NULL,
  `pk` varchar(512) NOT NULL,
  `adiu` varchar(512) NOT NULL DEFAULT '',
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name_v` varchar(20) GENERATED ALWAYS AS (substr(`name`,1,5)) VIRTUAL,
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`adiu`, `pk`),
  KEY `idx_adiu` (`adiu`, `pk`, `name`) LOCAL,
  KEY `idx_pk` (`pk`) GLOBAL
) TTL (gmt_create + INTERVAL 300 SECOND) partition by key(adiu) partitions 8;

CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));

 CREATE TABLE IF NOT EXISTS `test_p99` (
     `c1` bigint(20) NOT NULL,
     `c2` bigint(20) DEFAULT NULL,
     `c3` varchar(20) DEFAULT "hello",
     PRIMARY KEY (`c1`)
     );

 CREATE TABLE IF NOT EXISTS `test_auto_increment_rk_batch` (
     `c1` bigint(20) NOT NULL AUTO_INCREMENT,
     `c2` bigint(20) NOT NULL,
     `c3` varchar(20) DEFAULT "hello",
     PRIMARY KEY (`c1`)
     );

 CREATE TABLE IF NOT EXISTS `test_auto_increment_rk_batch_ttl` (
     `c1` bigint(20) NOT NULL AUTO_INCREMENT,
     `c2` bigint(20) NOT NULL,
     `c3` varchar(20) DEFAULT "hello",
     `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (`c1`)
     ) TTL(update_time + INTERVAL 300 SECOND);

 CREATE TABLE IF NOT EXISTS `test_auto_increment_batch` (
     `c1` bigint(20) NOT NULL,
     `c2` bigint(20) NOT NULL AUTO_INCREMENT,
     `c3` varchar(20) DEFAULT "hello",
     PRIMARY KEY (`c1`)
     );

 CREATE TABLE IF NOT EXISTS `test_auto_increment_batch_ttl` (
     `c1` bigint(20) NOT NULL,
     `c2` bigint(20) NOT NULL AUTO_INCREMENT,
     `c3` varchar(20) DEFAULT "hello",
     `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (`c1`)
     ) TTL(update_time + INTERVAL 300 SECOND);
alter system set kv_hotkey_throttle_threshold = 50;
