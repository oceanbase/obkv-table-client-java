/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc.protocol.payload;

import java.util.HashMap;
import java.util.Map;

/**
 * ob_errno.h
 *
 */
public enum ResultCodes {

    OB_MAX_ERROR_CODE(10000), //
    OB_LAST_ERROR_CODE(-9018), //
    OB_ERR_SQL_START(-5000), //
    OB_ERR_SQL_END(-5999), //
    OB_SUCCESS(0), //
    OB_ERROR(-4000), //
    OB_OBJ_TYPE_ERROR(-4001), //
    OB_INVALID_ARGUMENT(-4002), //
    OB_ARRAY_OUT_OF_RANGE(-4003), //
    OB_SERVER_LISTEN_ERROR(-4004), //
    OB_INIT_TWICE(-4005), //
    OB_NOT_INIT(-4006), //
    OB_NOT_SUPPORTED(-4007), //
    OB_ITER_END(-4008), //
    OB_IO_ERROR(-4009), //
    OB_ERROR_FUNC_VERSION(-4010), //
    OB_PACKET_NOT_SENT(-4011), //
    OB_TIMEOUT(-4012), //
    OB_ALLOCATE_MEMORY_FAILED(-4013), //
    OB_INNER_STAT_ERROR(-4014), //
    OB_ERR_SYS(-4015), //
    OB_ERR_UNEXPECTED(-4016), //
    OB_ENTRY_EXIST(-4017), //
    OB_ENTRY_NOT_EXIST(-4018), //
    OB_SIZE_OVERFLOW(-4019), //
    OB_REF_NUM_NOT_ZERO(-4020), //
    OB_CONFLICT_VALUE(-4021), //
    OB_ITEM_NOT_SETTED(-4022), //
    OB_EAGAIN(-4023), //
    OB_BUF_NOT_ENOUGH(-4024), //
    OB_PARTIAL_FAILED(-4025), //
    OB_READ_NOTHING(-4026), //
    OB_FILE_NOT_EXIST(-4027), //
    OB_DISCONTINUOUS_LOG(-4028), //
    OB_SCHEMA_ERROR(-4029), //
    OB_TENANT_OUT_OF_MEM(-4030), //
    OB_UNKNOWN_OBJ(-4031), //
    OB_NO_MONITOR_DATA(-4032), //
    OB_SERIALIZE_ERROR(-4033), //
    OB_DESERIALIZE_ERROR(-4034), //
    OB_AIO_TIMEOUT(-4035), //
    OB_NEED_RETRY(-4036), //
    OB_TOO_MANY_SSTABLE(-4037), //
    OB_NOT_MASTER(-4038), //
    OB_KILLED_BY_THROTTLING(-4039), //
    OB_DECRYPT_FAILED(-4041), //
    OB_USER_NOT_EXIST(-4042), //
    OB_PASSWORD_WRONG(-4043), //
    OB_SKEY_VERSION_WRONG(-4044), //
    OB_NOT_REGISTERED(-4048), //
    OB_WAITQUEUE_TIMEOUT(-4049), //
    OB_NOT_THE_OBJECT(-4050), //
    OB_ALREADY_REGISTERED(-4051), //
    OB_LAST_LOG_RUINNED(-4052), //
    OB_NO_CS_SELECTED(-4053), //
    OB_NO_TABLETS_CREATED(-4054), //
    OB_INVALID_ERROR(-4055), //
    OB_DECIMAL_OVERFLOW_WARN(-4057), //
    OB_DECIMAL_UNLEGAL_ERROR(-4058), //
    OB_OBJ_DIVIDE_ERROR(-4060), //
    OB_NOT_A_DECIMAL(-4061), //
    OB_DECIMAL_PRECISION_NOT_EQUAL(-4062), //
    OB_EMPTY_RANGE(-4063), //
    OB_SESSION_KILLED(-4064), //
    OB_LOG_NOT_SYNC(-4065), //
    OB_DIR_NOT_EXIST(-4066), //
    OB_SESSION_NOT_FOUND(-4067), //
    OB_INVALID_LOG(-4068), //
    OB_INVALID_DATA(-4070), //
    OB_ALREADY_DONE(-4071), //
    OB_CANCELED(-4072), //
    OB_LOG_SRC_CHANGED(-4073), //
    OB_LOG_NOT_ALIGN(-4074), //
    OB_LOG_MISSING(-4075), //
    OB_NEED_WAIT(-4076), //
    OB_NOT_IMPLEMENT(-4077), //
    OB_DIVISION_BY_ZERO(-4078), //
    OB_EXCEED_MEM_LIMIT(-4080), //
    OB_RESULT_UNKNOWN(-4081), //
    OB_NO_RESULT(-4084), //
    OB_QUEUE_OVERFLOW(-4085), //
    OB_TERM_LAGGED(-4097), //
    OB_TERM_NOT_MATCH(-4098), //
    OB_START_LOG_CURSOR_INVALID(-4099), //
    OB_LOCK_NOT_MATCH(-4100), //
    OB_DEAD_LOCK(-4101), //
    OB_PARTIAL_LOG(-4102), //
    OB_CHECKSUM_ERROR(-4103), //
    OB_INIT_FAIL(-4104), //
    OB_NOT_ENOUGH_STORE(-4106), //
    OB_BLOCK_SWITCHED(-4107), //
    OB_STATE_NOT_MATCH(-4109), //
    OB_READ_ZERO_LOG(-4110), //
    OB_BLOCK_NEED_FREEZE(-4111), //
    OB_BLOCK_FROZEN(-4112), //
    OB_IN_FATAL_STATE(-4113), //
    OB_IN_STOP_STATE(-4114), //
    OB_UPS_MASTER_EXISTS(-4115), //
    OB_LOG_NOT_CLEAR(-4116), //
    OB_FILE_ALREADY_EXIST(-4117), //
    OB_UNKNOWN_PACKET(-4118), //
    OB_RPC_PACKET_TOO_LONG(-4119), //
    OB_LOG_TOO_LARGE(-4120), //
    OB_RPC_SEND_ERROR(-4121), //
    OB_RPC_POST_ERROR(-4122), //
    OB_LIBEASY_ERROR(-4123), //
    OB_CONNECT_ERROR(-4124), //
    OB_NOT_FREE(-4125), //
    OB_INIT_SQL_CONTEXT_ERROR(-4126), //
    OB_SKIP_INVALID_ROW(-4127), //
    OB_RPC_PACKET_INVALID(-4128), //
    OB_NO_TABLET(-4133), //
    OB_SNAPSHOT_DISCARDED(-4138), //
    OB_DATA_NOT_UPTODATE(-4139), //
    OB_ROW_MODIFIED(-4142), //
    OB_VERSION_NOT_MATCH(-4143), //
    OB_BAD_ADDRESS(-4144), //
    OB_ENQUEUE_FAILED(-4146), //
    OB_INVALID_CONFIG(-4147), //
    OB_STMT_EXPIRED(-4149), //
    OB_ERR_MIN_VALUE(-4150), //
    OB_ERR_MAX_VALUE(-4151), //
    OB_ERR_NULL_VALUE(-4152), //
    OB_RESOURCE_OUT(-4153), //
    OB_ERR_SQL_CLIENT(-4154), //
    OB_META_TABLE_WITHOUT_USE_TABLE(-4155), //
    OB_DISCARD_PACKET(-4156), //
    OB_OPERATE_OVERFLOW(-4157), //
    OB_INVALID_DATE_FORMAT(-4158), //
    OB_POOL_REGISTERED_FAILED(-4159), //
    OB_POOL_UNREGISTERED_FAILED(-4160), //
    OB_INVALID_ARGUMENT_NUM(-4161), //
    OB_LEASE_NOT_ENOUGH(-4162), //
    OB_LEASE_NOT_MATCH(-4163), //
    OB_UPS_SWITCH_NOT_HAPPEN(-4164), //
    OB_EMPTY_RESULT(-4165), //
    OB_CACHE_NOT_HIT(-4166), //
    OB_NESTED_LOOP_NOT_SUPPORT(-4167), //
    OB_LOG_INVALID_MOD_ID(-4168), //
    OB_LOG_MODULE_UNKNOWN(-4169), //
    OB_LOG_LEVEL_INVALID(-4170), //
    OB_LOG_PARSER_SYNTAX_ERR(-4171), //
    OB_INDEX_OUT_OF_RANGE(-4172), //
    OB__UNDERFLOW(-4173), //
    OB_UNKNOWN_CONNECTION(-4174), //
    OB_ERROR_OUT_OF_RANGE(-4175), //
    OB_CACHE_SHRINK_FAILED(-4176), //
    OB_OLD_SCHEMA_VERSION(-4177), //
    OB_RELEASE_SCHEMA_ERROR(-4178), //
    OB_OP_NOT_ALLOW(-4179), //
    OB_NO_EMPTY_ENTRY(-4180), //
    OB_ERR_ALREADY_EXISTS(-4181), //
    OB_SEARCH_NOT_FOUND(-4182), //
    OB_BEYOND_THE_RANGE(-4183), //
    OB_CS_OUTOF_DISK_SPACE(-4184), //
    OB_COLUMN_GROUP_NOT_FOUND(-4185), //
    OB_CS_COMPRESS_LIB_ERROR(-4186), //
    OB_ITEM_NOT_MATCH(-4187), //
    OB_SCHEDULER_TASK_CNT_MISMATCH(-4188), //
    OB_HASH_EXIST(-4200), //
    OB_HASH_NOT_EXIST(-4201), //
    OB_HASH_GET_TIMEOUT(-4204), //
    OB_HASH_PLACEMENT_RETRY(-4205), //
    OB_HASH_FULL(-4206), //
    OB_PACKET_PROCESSED(-4207), //
    OB_WAIT_NEXT_TIMEOUT(-4208), //
    OB_LEADER_NOT_EXIST(-4209), //
    OB_PREPARE_MAJOR_FREEZE_FAILED(-4210), //
    OB_COMMIT_MAJOR_FREEZE_FAILED(-4211), //
    OB_ABORT_MAJOR_FREEZE_FAILED(-4212), //
    OB_MAJOR_FREEZE_NOT_FINISHED(-4213), //
    OB_PARTITION_NOT_LEADER(-4214), //
    OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT(-4215), //
    OB_CURL_ERROR(-4216), //
    OB_MAJOR_FREEZE_NOT_ALLOW(-4217), //
    OB_PREPARE_FREEZE_FAILED(-4218), //
    OB_INVALID_DATE_VALUE(-4219), //
    OB_INACTIVE_SQL_CLIENT(-4220), //
    OB_INACTIVE_RPC_PROXY(-4221), //
    OB_ERVAL_WITH_MONTH(-4222), //
    OB_TOO_MANY_DATETIME_PARTS(-4223), //
    OB_DATA_OUT_OF_RANGE(-4224), //
    OB_PARTITION_NOT_EXIST(-4225), //
    OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD(-4226), //
    OB_ERR_NO_DEFAULT_FOR_FIELD(-4227), //
    OB_ERR_FIELD_SPECIFIED_TWICE(-4228), //
    OB_ERR_TOO_LONG_TABLE_COMMENT(-4229), //
    OB_ERR_TOO_LONG_FIELD_COMMENT(-4230), //
    OB_ERR_TOO_LONG_INDEX_COMMENT(-4231), //
    OB_NOT_FOLLOWER(-4232), //
    OB_ERR_OUT_OF_LOWER_BOUND(-4233), //
    OB_ERR_OUT_OF_UPPER_BOUND(-4234), //
    OB_BAD_NULL_ERROR(-4235), //
    OB_OBCONFIG_RETURN_ERROR(-4236), //
    OB_OBCONFIG_APPNAME_MISMATCH(-4237), //
    OB_ERR_VIEW_SELECT_DERIVED(-4238), //
    OB_CANT_MJ_PATH(-4239), //
    OB_ERR_NO_JOIN_ORDER_GENERATED(-4240), //
    OB_ERR_NO_PATH_GENERATED(-4241), //
    OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH(-4242), //
    OB_FILE_NOT_OPENED(-4243), //
    OB_TIMER_TASK_HAS_SCHEDULED(-4244), //
    OB_TIMER_TASK_HAS_NOT_SCHEDULED(-4245), //
    OB_PARSE_DEBUG_SYNC_ERROR(-4246), //
    OB_UNKNOWN_DEBUG_SYNC_PO(-4247), //
    OB_ERR_ERRUPTED(-4248), //
    OB_ERR_DATA_TRUNCATED(-4249), //
    OB_NOT_RUNNING(-4250), //
    OB_INVALID_PARTITION(-4251), //
    OB_ERR_TIMEOUT_TRUNCATED(-4252), //
    OB_ERR_TOO_LONG_TENANT_COMMENT(-4253), //
    OB_ERR_NET_PACKET_TOO_LARGE(-4254), //
    OB_TRACE_DESC_NOT_EXIST(-4255), //
    OB_ERR_NO_DEFAULT(-4256), //
    OB_ERR_COMPRESS_DECOMPRESS_DATA(-4257), //
    OB_ERR_INCORRECT_STRING_VALUE(-4258), //
    OB_ERR_DISTRIBUTED_NOT_SUPPORTED(-4259), //
    OB_IS_CHANGING_LEADER(-4260), //
    OB_DATETIME_FUNCTION_OVERFLOW(-4261), //
    OB_ERR_DOUBLE_TRUNCATED(-4262), //
    OB_MINOR_FREEZE_NOT_ALLOW(-4263), //
    OB_LOG_OUTOF_DISK_SPACE(-4264), //
    OB_RPC_CONNECT_ERROR(-4265), //
    OB_MINOR_MERGE_NOT_ALLOW(-4266), //
    OB_CACHE_INVALID(-4267), //
    OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT(-4268), //
    OB_WORKING_PARTITION_EXIST(-4269), //
    OB_WORKING_PARTITION_NOT_EXIST(-4270), //
    OB_LIBEASY_REACH_MEM_LIMIT(-4271), //
    OB_MISS_ARGUMENT(-4272), //
    OB_CACHE_FREE_BLOCK_NOT_ENOUGH(-4273), //
    OB_SYNC_WASH_MB_TIMEOUT(-4274), //
    OB_NOT_ALLOW_MIGRATE_IN(-4275), //
    OB_GTS_NOT_READY(-4283), //
    OB_GTI_NOT_READY(-4383), //
    OB_IMPORT_NOT_IN_SERVER(-4505), //
    OB_CONVERT_ERROR(-4507), //
    OB_BYPASS_TIMEOUT(-4510), //
    OB_RS_STATE_NOT_ALLOW(-4512), //
    OB_NO_REPLICA_VALID(-4515), //
    OB_NO_NEED_UPDATE(-4517), //
    OB_CACHE_TIMEOUT(-4518), //
    OB_ITER_STOP(-4519), //
    OB_ZONE_ALREADY_MASTER(-4523), //
    OB_IP_PORT_IS_NOT_SLAVE_ZONE(-4524), //
    OB_ZONE_IS_NOT_SLAVE(-4525), //
    OB_ZONE_IS_NOT_MASTER(-4526), //
    OB_CONFIG_NOT_SYNC(-4527), //
    OB_IP_PORT_IS_NOT_ZONE(-4528), //
    OB_MASTER_ZONE_NOT_EXIST(-4529), //
    OB_ZONE_INFO_NOT_EXIST(-4530), //
    OB_GET_ZONE_MASTER_UPS_FAILED(-4531), //
    OB_MULTIPLE_MASTER_ZONES_EXIST(-4532), //
    OB_INDEXING_ZONE_INVALID(-4533), //
    OB_ROOT_TABLE_RANGE_NOT_EXIST(-4537), //
    OB_ROOT_MIGRATE_CONCURRENCY_FULL(-4538), //
    OB_ROOT_MIGRATE_INFO_NOT_FOUND(-4539), //
    OB_NOT_DATA_LOAD_TABLE(-4540), //
    OB_DATA_LOAD_TABLE_DUPLICATED(-4541), //
    OB_ROOT_TABLE_ID_EXIST(-4542), //
    OB_INDEX_TIMEOUT(-4543), //
    OB_ROOT_NOT_EGRATED(-4544), //
    OB_INDEX_INELIGIBLE(-4545), //
    OB_REBALANCE_EXEC_TIMEOUT(-4546), //
    OB_MERGE_NOT_STARTED(-4547), //
    OB_MERGE_ALREADY_STARTED(-4548), //
    OB_ROOTSERVICE_EXIST(-4549), //
    OB_RS_SHUTDOWN(-4550), //
    OB_SERVER_MIGRATE_IN_DENIED(-4551), //
    OB_REBALANCE_TASK_CANT_EXEC(-4552), //
    OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT(-4553), //
    OB_REBALANCE_TASK_NOT_IN_PROGRESS(-4554), //
    OB_DATA_SOURCE_NOT_EXIST(-4600), //
    OB_DATA_SOURCE_TABLE_NOT_EXIST(-4601), //
    OB_DATA_SOURCE_RANGE_NOT_EXIST(-4602), //
    OB_DATA_SOURCE_DATA_NOT_EXIST(-4603), //
    OB_DATA_SOURCE_SYS_ERROR(-4604), //
    OB_DATA_SOURCE_TIMEOUT(-4605), //
    OB_DATA_SOURCE_CONCURRENCY_FULL(-4606), //
    OB_DATA_SOURCE_WRONG_URI_FORMAT(-4607), //
    OB_SSTABLE_VERSION_UNEQUAL(-4608), //
    OB_UPS_RENEW_LEASE_NOT_ALLOWED(-4609), //
    OB_UPS_COUNT_OVER_LIMIT(-4610), //
    OB_NO_UPS_MAJORITY(-4611), //
    OB_INDEX_COUNT_REACH_THE_LIMIT(-4613), //
    OB_TASK_EXPIRED(-4614), //
    OB_TABLEGROUP_NOT_EMPTY(-4615), //
    OB_INVALID_SERVER_STATUS(-4620), //
    OB_WAIT_ELEC_LEADER_TIMEOUT(-4621), //
    OB_WAIT_ALL_RS_ONLINE_TIMEOUT(-4622), //
    OB_ALL_REPLICAS_ON_MERGE_ZONE(-4623), //
    OB_MACHINE_RESOURCE_NOT_ENOUGH(-4624), //
    OB_NOT_SERVER_CAN_HOLD_SOFTLY(-4625), //
    OB_RESOURCE_POOL_ALREADY_GRANTED(-4626), //
    OB_SERVER_ALREADY_DELETED(-4628), //
    OB_SERVER_NOT_DELETING(-4629), //
    OB_SERVER_NOT_IN_WHITE_LIST(-4630), //
    OB_SERVER_ZONE_NOT_MATCH(-4631), //
    OB_OVER_ZONE_NUM_LIMIT(-4632), //
    OB_ZONE_STATUS_NOT_MATCH(-4633), //
    OB_RESOURCE_UNIT_IS_REFERENCED(-4634), //
    OB_DIFFERENT_PRIMARY_ZONE(-4636), //
    OB_SERVER_NOT_ACTIVE(-4637), //
    OB_RS_NOT_MASTER(-4638), //
    OB_CANDIDATE_LIST_ERROR(-4639), //
    OB_PARTITION_ZONE_DUPLICATED(-4640), //
    OB_ZONE_DUPLICATED(-4641), //
    OB_NOT_ALL_ZONE_ACTIVE(-4642), //
    OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST(-4643), //
    OB_REPLICA_NUM_NOT_MATCH(-4644), //
    OB_ZONE_LIST_POOL_LIST_NOT_MATCH(-4645), //
    OB_INVALID_TENANT_NAME(-4646), //
    OB_EMPTY_RESOURCE_POOL_LIST(-4647), //
    OB_RESOURCE_UNIT_NOT_EXIST(-4648), //
    OB_RESOURCE_UNIT_EXIST(-4649), //
    OB_RESOURCE_POOL_NOT_EXIST(-4650), //
    OB_RESOURCE_POOL_EXIST(-4651), //
    OB_WAIT_LEADER_SWITCH_TIMEOUT(-4652), //
    OB_LOCATION_NOT_EXIST(-4653), //
    OB_LOCATION_LEADER_NOT_EXIST(-4654), //
    OB_ZONE_NOT_ACTIVE(-4655), //
    OB_UNIT_NUM_OVER_SERVER_COUNT(-4656), //
    OB_POOL_SERVER_ERSECT(-4657), //
    OB_NOT_SINGLE_RESOURCE_POOL(-4658), //
    OB_INVALID_RESOURCE_UNIT(-4659), //
    OB_STOP_SERVER_IN_MULTIPLE_ZONES(-4660), //
    OB_SESSION_ENTRY_EXIST(-4661), //
    OB_GOT_SIGNAL_ABORTING(-4662), //
    OB_SERVER_NOT_ALIVE(-4663), //
    OB_GET_LOCATION_TIME_OUT(-4664), //
    OB_UNIT_IS_MIGRATING(-4665), //
    OB_CLUSTER_NO_MATCH(-4666), //
    OB_CHECK_ZONE_MERGE_ORDER(-4667), //
    OB_ERR_ZONE_NOT_EMPTY(-4668), //
    OB_USE_DUP_FOLLOW_AFTER_DML(-4686), //
    OB_LS_NOT_EXIST(-4719), //
    OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST(-4723), //
    OB_TABLET_NOT_EXIST(-4725), //
    OB_ERR_PARSER_INIT(-5000), //
    OB_ERR_PARSE_SQL(-5001), //
    OB_ERR_RESOLVE_SQL(-5002), //
    OB_ERR_GEN_PLAN(-5003), //
    OB_ERR_PARSER_SYNTAX(-5006), //
    OB_ERR_COLUMN_SIZE(-5007), //
    OB_ERR_COLUMN_DUPLICATE(-5008), //
    OB_ERR_OPERATOR_UNKNOWN(-5010), //
    OB_ERR_STAR_DUPLICATE(-5011), //
    OB_ERR_ILLEGAL_ID(-5012), //
    OB_ERR_ILLEGAL_VALUE(-5014), //
    OB_ERR_COLUMN_AMBIGUOUS(-5015), //
    OB_ERR_LOGICAL_PLAN_FAILD(-5016), //
    OB_ERR_SCHEMA_UNSET(-5017), //
    OB_ERR_ILLEGAL_NAME(-5018), //
    OB_TABLE_NOT_EXIST(-5019), //
    OB_ERR_TABLE_EXIST(-5020), //
    OB_ERR_EXPR_UNKNOWN(-5022), //
    OB_ERR_ILLEGAL_TYPE(-5023), //
    OB_ERR_PRIMARY_KEY_DUPLICATE(-5024), //
    OB_ERR_KEY_NAME_DUPLICATE(-5025), //
    OB_ERR_CREATETIME_DUPLICATE(-5026), //
    OB_ERR_MODIFYTIME_DUPLICATE(-5027), //
    OB_ERR_ILLEGAL_INDEX(-5028), //
    OB_ERR_INVALID_SCHEMA(-5029), //
    OB_ERR_INSERT_NULL_ROWKEY(-5030), //
    OB_ERR_COLUMN_NOT_FOUND(-5031), //
    OB_ERR_DELETE_NULL_ROWKEY(-5032), //
    OB_ERR_USER_EMPTY(-5034), //
    OB_ERR_USER_NOT_EXIST(-5035), //
    OB_ERR_NO_PRIVILEGE(-5036), //
    OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY(-5037), //
    OB_ERR_WRONG_PASSWORD(-5038), //
    OB_ERR_USER_IS_LOCKED(-5039), //
    OB_ERR_UPDATE_ROWKEY_COLUMN(-5040), //
    OB_ERR_UPDATE_JOIN_COLUMN(-5041), //
    OB_ERR_INVALID_COLUMN_NUM(-5042), //
    OB_ERR_PREPARE_STMT_NOT_FOUND(-5043), //
    OB_ERR_SYS_VARIABLE_UNKNOWN(-5044), //
    OB_ERR_OLDER_PRIVILEGE_VERSION(-5046), //
    OB_ERR_LACK_OF_ROWKEY_COL(-5047), //
    OB_ERR_USER_EXIST(-5050), //
    OB_ERR_PASSWORD_EMPTY(-5051), //
    OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE(-5052), //
    OB_ERR_WRONG_DYNAMIC_PARAM(-5053), //
    OB_ERR_PARAM_SIZE(-5054), //
    OB_ERR_FUNCTION_UNKNOWN(-5055), //
    OB_ERR_CREAT_MODIFY_TIME_COLUMN(-5056), //
    OB_ERR_MODIFY_PRIMARY_KEY(-5057), //
    OB_ERR_PARAM_DUPLICATE(-5058), //
    OB_ERR_TOO_MANY_SESSIONS(-5059), //
    OB_ERR_TOO_MANY_PS(-5061), //
    OB_ERR_H_UNKNOWN(-5063), //
    OB_ERR_WHEN_UNSATISFIED(-5064), //
    OB_ERR_QUERY_ERRUPTED(-5065), //
    OB_ERR_SESSION_ERRUPTED(-5066), //
    OB_ERR_UNKNOWN_SESSION_ID(-5067), //
    OB_ERR_PROTOCOL_NOT_RECOGNIZE(-5068), //
    OB_ERR_WRITE_AUTH_ERROR(-5069), //
    OB_ERR_PARSE_JOIN_INFO(-5070), //
    OB_ERR_ALTER_INDEX_COLUMN(-5071), //
    OB_ERR_MODIFY_INDEX_TABLE(-5072), //
    OB_ERR_INDEX_UNAVAILABLE(-5073), //
    OB_ERR_NOP_VALUE(-5074), //
    OB_ERR_PS_TOO_MANY_PARAM(-5080), //
    OB_ERR_READ_ONLY(-5081), //
    OB_ERR_INVALID_TYPE_FOR_OP(-5083), //
    OB_ERR_CAST_VARCHAR_TO_BOOL(-5084), //
    OB_ERR_CAST_VARCHAR_TO_NUMBER(-5085), //
    OB_ERR_CAST_VARCHAR_TO_TIME(-5086), //
    OB_ERR_CAST_NUMBER_OVERFLOW(-5087), //
    OB_EGER_PRECISION_OVERFLOW(-5088), //
    OB_DECIMAL_PRECISION_OVERFLOW(-5089), //
    OB_SCHEMA_NUMBER_PRECISION_OVERFLOW(-5090), //
    OB_SCHEMA_NUMBER_SCALE_OVERFLOW(-5091), //
    OB_ERR_INDEX_UNKNOWN(-5092), //
    OB_NUMERIC_OVERFLOW(-5093), //
    OB_ERR_TOO_MANY_JOIN_TABLES(-5094), //
    OB_ERR_VARCHAR_TOO_LONG(-5098), //
    OB_ERR_SYS_CONFIG_UNKNOWN(-5099), //
    OB_ERR_LOCAL_VARIABLE(-5100), //
    OB_ERR_GLOBAL_VARIABLE(-5101), //
    OB_ERR_VARIABLE_IS_READONLY(-5102), //
    OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR(-5103), //
    OB_ERR_EXPIRE_INFO_TOO_LONG(-5104), //
    OB_ERR_EXPIRE_COND_TOO_LONG(-5105), //
    OB_INVALID_ARGUMENT_FOR_EXTRACT(-5106), //
    OB_INVALID_ARGUMENT_FOR_IS(-5107), //
    OB_INVALID_ARGUMENT_FOR_LENGTH(-5108), //
    OB_INVALID_ARGUMENT_FOR_SUBSTR(-5109), //
    OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC(-5110), //
    OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME(-5111), //
    OB_ERR_USER_VARIABLE_UNKNOWN(-5112), //
    OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME(-5113), //
    OB_INVALID_NUMERIC(-5114), //
    OB_ERR_REGEXP_ERROR(-5115), //
    OB_SQL_LOG_OP_SETCHILD_OVERFLOW(-5116), //
    OB_SQL_EXPLAIN_FAILED(-5117), //
    OB_SQL_OPT_COPY_OP_FAILED(-5118), //
    OB_SQL_OPT_GEN_PLAN_FALIED(-5119), //
    OB_SQL_OPT_CREATE_RAWEXPR_FAILED(-5120), //
    OB_SQL_OPT_JOIN_ORDER_FAILED(-5121), //
    OB_SQL_OPT_ERROR(-5122), //
    OB_SQL_RESOLVER_NO_MEMORY(-5130), //
    OB_SQL_DML_ONLY(-5131), //
    OB_ERR_NO_GRANT(-5133), //
    OB_ERR_NO_DB_SELECTED(-5134), //
    OB_SQL_PC_OVERFLOW(-5135), //
    OB_SQL_PC_PLAN_DUPLICATE(-5136), //
    OB_SQL_PC_PLAN_EXPIRE(-5137), //
    OB_SQL_PC_NOT_EXIST(-5138), //
    OB_SQL_PARAMS_LIMIT(-5139), //
    OB_SQL_PC_PLAN_SIZE_LIMIT(-5140), //
    OB_ERR_UNKNOWN_CHARSET(-5142), //
    OB_ERR_UNKNOWN_COLLATION(-5143), //
    OB_ERR_COLLATION_MISMATCH(-5144), //
    OB_ERR_WRONG_VALUE_FOR_VAR(-5145), //
    OB_UNKNOWN_PARTITION(-5146), //
    OB_PARTITION_NOT_MATCH(-5147), //
    OB_ER_PASSWD_LENGTH(-5148), //
    OB_ERR_INSERT_INNER_JOIN_COLUMN(-5149), //
    OB_TENANT_NOT_IN_SERVER(-5150), //
    OB_TABLEGROUP_NOT_EXIST(-5151), //
    OB_SUBQUERY_TOO_MANY_ROW(-5153), //
    OB_ERR_BAD_DATABASE(-5154), //
    OB_CANNOT_USER(-5155), //
    OB_TENANT_EXIST(-5156), //
    OB_TENANT_NOT_EXIST(-5157), //
    OB_DATABASE_EXIST(-5158), //
    OB_TABLEGROUP_EXIST(-5159), //
    OB_ERR_INVALID_TENANT_NAME(-5160), //
    OB_EMPTY_TENANT(-5161), //
    OB_WRONG_DB_NAME(-5162), //
    OB_WRONG_TABLE_NAME(-5163), //
    OB_WRONG_COLUMN_NAME(-5164), //
    OB_ERR_COLUMN_SPEC(-5165), //
    OB_ERR_DB_DROP_EXISTS(-5166), //
    OB_ERR_DATA_TOO_LONG(-5167), //
    OB_ERR_WRONG_VALUE_COUNT_ON_ROW(-5168), //
    OB_ERR_CREATE_USER_WITH_GRANT(-5169), //
    OB_ERR_NO_DB_PRIVILEGE(-5170), //
    OB_ERR_NO_TABLE_PRIVILEGE(-5171), //
    OB_INVALID_ON_UPDATE(-5172), //
    OB_INVALID_DEFAULT(-5173), //
    OB_ERR_UPDATE_TABLE_USED(-5174), //
    OB_ERR_COULUMN_VALUE_NOT_MATCH(-5175), //
    OB_ERR_INVALID_GROUP_FUNC_USE(-5176), //
    OB_CANT_AGGREGATE_2COLLATIONS(-5177), //
    OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD(-5178), //
    OB_ERR_TOO_LONG_IDENT(-5179), //
    OB_ERR_WRONG_TYPE_FOR_VAR(-5180), //
    OB_WRONG_USER_NAME_LENGTH(-5181), //
    OB_ERR_PRIV_USAGE(-5182), //
    OB_ILLEGAL_GRANT_FOR_TABLE(-5183), //
    OB_ERR_REACH_AUTOINC_MAX(-5184), //
    OB_ERR_NO_TABLES_USED(-5185), //
    OB_CANT_REMOVE_ALL_FIELDS(-5187), //
    OB_TOO_MANY_PARTITIONS_ERROR(-5188), //
    OB_NO_PARTS_ERROR(-5189), //
    OB_WRONG_SUB_KEY(-5190), //
    OB_KEY_PART_0(-5191), //
    OB_ERR_UNKNOWN_TIME_ZONE(-5192), //
    OB_ERR_WRONG_AUTO_KEY(-5193), //
    OB_ERR_TOO_MANY_KEYS(-5194), //
    OB_ERR_TOO_MANY_ROWKEY_COLUMNS(-5195), //
    OB_ERR_TOO_LONG_KEY_LENGTH(-5196), //
    OB_ERR_TOO_MANY_COLUMNS(-5197), //
    OB_ERR_TOO_LONG_COLUMN_LENGTH(-5198), //
    OB_ERR_TOO_BIG_ROWSIZE(-5199), //
    OB_ERR_UNKNOWN_TABLE(-5200), //
    OB_ERR_BAD_TABLE(-5201), //
    OB_ERR_TOO_BIG_SCALE(-5202), //
    OB_ERR_TOO_BIG_PRECISION(-5203), //
    OB_ERR_M_BIGGER_THAN_D(-5204), //
    OB_ERR_TOO_BIG_DISPLAYWIDTH(-5205), //
    OB_WRONG_GROUP_FIELD(-5206), //
    OB_NON_UNIQ_ERROR(-5207), //
    OB_ERR_NONUNIQ_TABLE(-5208), //
    OB_ERR_CANT_DROP_FIELD_OR_KEY(-5209), //
    OB_ERR_MULTIPLE_PRI_KEY(-5210), //
    OB_ERR_KEY_COLUMN_DOES_NOT_EXITS(-5211), //
    OB_ERR_AUTO_PARTITION_KEY(-5212), //
    OB_ERR_CANT_USE_OPTION_HERE(-5213), //
    OB_ERR_WRONG_OBJECT(-5214), //
    OB_ERR_ON_RENAME(-5215), //
    OB_ERR_WRONG_KEY_COLUMN(-5216), //
    OB_ERR_BAD_FIELD_ERROR(-5217), //
    OB_ERR_WRONG_FIELD_WITH_GROUP(-5218), //
    OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS(-5219), //
    OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION(-5220), //
    OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS(-5221), //
    OB_ERR_TRUNCATED_WRONG_VALUE(-5222), //
    OB_ERR_WRONG_IDENT_NAME(-5223), //
    OB_WRONG_NAME_FOR_INDEX(-5224), //
    OB_ILLEGAL_REFERENCE(-5225), //
    OB_REACH_MEMORY_LIMIT(-5226), //
    OB_ERR_PASSWORD_FORMAT(-5227), //
    OB_ERR_NON_UPDATABLE_TABLE(-5228), //
    OB_ERR_WARN_DATA_OUT_OF_RANGE(-5229), //
    OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR(-5230), //
    OB_ERR_VIEW_INVALID(-5231), //
    OB_ERR_OPTION_PREVENTS_STATEMENT(-5233), //
    OB_ERR_DB_READ_ONLY(-5234), //
    OB_ERR_TABLE_READ_ONLY(-5235), //
    OB_ERR_LOCK_OR_ACTIVE_TRANSACTION(-5236), //
    OB_ERR_SAME_NAME_PARTITION_FIELD(-5237), //
    OB_ERR_TABLENAME_NOT_ALLOWED_HERE(-5238), //
    OB_ERR_VIEW_RECURSIVE(-5239), //
    OB_ERR_QUALIFIER(-5240), //
    OB_ERR_WRONG_VALUE(-5241), //
    OB_ERR_VIEW_WRONG_LIST(-5242), //
    OB_SYS_VARS_MAYBE_DIFF_VERSION(-5243), //
    OB_ERR_AUTO_INCREMENT_CONFLICT(-5244), //
    OB_ERR_TASK_SKIPPED(-5245), //
    OB_ERR_NAME_BECOMES_EMPTY(-5246), //
    OB_ERR_REMOVED_SPACES(-5247), //
    OB_WARN_ADD_AUTOINCREMENT_COLUMN(-5248), //
    OB_WARN_CHAMGE_NULL_ATTRIBUTE(-5249), //
    OB_ERR_INVALID_CHARACTER_STRING(-5250), //
    OB_ERR_KILL_DENIED(-5251), //
    OB_ERR_COLUMN_DEFINITION_AMBIGUOUS(-5252), //
    OB_ERR_EMPTY_QUERY(-5253), //
    OB_ERR_CUT_VALUE_GROUP_CONCAT(-5254), //
    OB_ERR_FILED_NOT_FOUND_PART(-5255), //
    OB_ERR_PRIMARY_CANT_HAVE_NULL(-5256), //
    OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR(-5257), //
    OB_ERR_INVALID_BLOCK_SIZE(-5258), //
    OB_ERR_UNKNOWN_STORAGE_ENGINE(-5259), //
    OB_ERR_TENANT_IS_LOCKED(-5260), //
    OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF(-5261), //
    OB_ERR_AGGREGATE_ORDER_FOR_UNION(-5263), //
    OB_ERR_OUTLINE_EXIST(-5264), //
    OB_OUTLINE_NOT_EXIST(-5265), //
    OB_WARN_OPTION_BELOW_LIMIT(-5266), //
    OB_INVALID_OUTLINE(-5267), //
    OB_REACH_MAX_CONCURRENT_NUM(-5268), //
    OB_ERR_OPERATION_ON_RECYCLE_OBJECT(-5269), //
    OB_ERR_OBJECT_NOT_IN_RECYCLEBIN(-5270), //
    OB_ERR_CON_COUNT_ERROR(-5271), //
    OB_ERR_OUTLINE_CONTENT_EXIST(-5272), //
    OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST(-5273), //
    OB_ERR_VALUES_IS_NOT__TYPE_ERROR(-5274), //
    OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR(-5275), //
    OB_ERR_PARTITION_COLUMN_LIST_ERROR(-5276), //
    OB_ERR_TOO_MANY_VALUES_ERROR(-5277), //
    OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED(-5278), //
    OB_ERR_PARTITION_ERVAL_ERROR(-5279), //
    OB_ERR_SAME_NAME_PARTITION(-5280), //
    OB_ERR_RANGE_NOT_INCREASING_ERROR(-5281), //
    OB_ERR_PARSE_PARTITION_RANGE(-5282), //
    OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF(-5283), //
    OB_NO_PARTITION_FOR_GIVEN_VALUE(-5284), //
    OB_EER_NULL_IN_VALUES_LESS_THAN(-5285), //
    OB_ERR_PARTITION_CONST_DOMAIN_ERROR(-5286), //
    OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS(-5287), //
    OB_ERR_BAD_FT_COLUMN(-5288), //
    OB_ERR_KEY_DOES_NOT_EXISTS(-5289), //
    OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN(-5290), //
    OB_ERR_BAD_CTXCAT_COLUMN(-5291), //
    OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN(-5292), //
    OB_ERR_DEPENDENT_BY_GENERATED_COLUMN(-5293), //
    OB_ERR_TOO_MANY_ROWS(-5294), //
    OB_WRONG_FIELD_TERMINATORS(-5295), //
    OB_NO_READABLE_REPLICA(-5296), //
    OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED(-5302), //
    OB_ERR_DROP_PARTITION_NON_EXISTENT(-5303), //
    OB_ERR_PARTITION_MGMT_ON_TWOPART_TABLE(-5304), //
    OB_ERR_ONLY_ON_RANGE_LIST_PARTITION(-5305), //
    OB_ERR_DROP_LAST_PARTITION(-5306), //
    OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH(-5307), //
    OB_ERR_IGNORE_USER_HOST_NAME(-5308), //
    OB_IGNORE_SQL_IN_RESTORE(-5309), //
    OB_ERR_UNEXPECTED_TZ_TRANSITION(-5310), //
    OB_ERR_INVALID_COLUMN_ID(-5311), //
    OB_ERR_INVALID_JSON_TEXT(-5411), //
    OB_ERR_INVALID_JSON_TEXT_IN_PARAM(-5412), //
    OB_SCHEMA_EAGAIN(-5627), //
    OB_ERR_PARALLEL_DDL_CONFLICT(-5827), //
    OB_TRANSACTION_SET_VIOLATION(-6001), //
    OB_TRANS_ROLLBACKED(-6002), //
    OB_ERR_EXCLUSIVE_LOCK_CONFLICT(-6003), //
    OB_ERR_SHARED_LOCK_CONFLICT(-6004), //
    OB_TRY_LOCK_ROW_CONFLICT(-6005), //
    OB_CLOCK_OUT_OF_ORDER(-6201), //
    OB_MASK_SET_NO_NODE(-6203), //
    OB_TRANS_HAS_DECIDED(-6204), //
    OB_TRANS_INVALID_STATE(-6205), //
    OB_TRANS_STATE_NOT_CHANGE(-6206), //
    OB_TRANS_PROTOCOL_ERROR(-6207), //
    OB_TRANS_INVALID_MESSAGE(-6208), //
    OB_TRANS_INVALID_MESSAGE_TYPE(-6209), //
    OB_TRANS_TIMEOUT(-6210), //
    OB_TRANS_KILLED(-6211), //
    OB_TRANS_STMT_TIMEOUT(-6212), //
    OB_TRANS_CTX_NOT_EXIST(-6213), //
    OB_PARTITION_IS_FROZEN(-6214), //
    OB_PARTITION_IS_NOT_FROZEN(-6215), //
    OB_TRANS_INVALID_LOG_TYPE(-6219), //
    OB_TRANS_SQL_SEQUENCE_ILLEGAL(-6220), //
    OB_TRANS_CANNOT_BE_KILLED(-6221), //
    OB_TRANS_STATE_UNKNOWN(-6222), //
    OB_TRANS_IS_EXITING(-6223), //
    OB_TRANS_NEED_ROLLBACK(-6224), //
    OB_TRANS_UNKNOWN(-6225), //
    OB_ERR_READ_ONLY_TRANSACTION(-6226), //
    OB_PARTITION_IS_NOT_STOPPED(-6227), //
    OB_PARTITION_IS_STOPPED(-6228), //
    OB_PARTITION_IS_BLOCKED(-6229), //
    OB_TRANS_RPC_TIMEOUT(-6230), //
    OB_REPLICA_NOT_READABLE(-6231), //
    OB_TRANS_STMT_NEED_RETRY(-6241), //
    OB_LOG_ID_NOT_FOUND(-6301), //
    OB_LSR_THREAD_STOPPED(-6302), //
    OB_NO_LOG(-6303), //
    OB_LOG_ID_RANGE_ERROR(-6304), //
    OB_LOG_ITER_ENOUGH(-6305), //
    OB_CLOG_INVALID_ACK(-6306), //
    OB_CLOG_CACHE_INVALID(-6307), //
    OB_EXT_HANDLE_UNFINISH(-6308), //
    OB_CURSOR_NOT_EXIST(-6309), //
    OB_STREAM_NOT_EXIST(-6310), //
    OB_STREAM_BUSY(-6311), //
    OB_FILE_RECYCLED(-6312), //
    OB_REPLAY_EAGAIN_TOO_MUCH_TIME(-6313), //
    OB_MEMBER_CHANGE_FAILED(-6314), //
    OB_ELECTION_WARN_LOGBUF_FULL(-7000), //
    OB_ELECTION_WARN_LOGBUF_EMPTY(-7001), //
    OB_ELECTION_WARN_NOT_RUNNING(-7002), //
    OB_ELECTION_WARN_IS_RUNNING(-7003), //
    OB_ELECTION_WARN_NOT_REACH_MAJORITY(-7004), //
    OB_ELECTION_WARN_INVALID_SERVER(-7005), //
    OB_ELECTION_WARN_INVALID_LEADER(-7006), //
    OB_ELECTION_WARN_LEADER_LEASE_EXPIRED(-7007), //
    OB_ELECTION_WARN_INVALID_MESSAGE(-7010), //
    OB_ELECTION_WARN_MESSAGE_NOT_IME(-7011), //
    OB_ELECTION_WARN_NOT_CANDIDATE(-7012), //
    OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER(-7013), //
    OB_ELECTION_WARN_PROTOCOL_ERROR(-7014), //
    OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE(-7015), //
    OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE(-7021), //
    OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER(-7022), //
    OB_ELECTION_WARN_NO_PREPARE_MESSAGE(-7024), //
    OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE(-7025), //
    OB_ELECTION_NOT_EXIST(-7026), //
    OB_ELECTION_MGR_IS_RUNNING(-7027), //
    OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE(-7029), //
    OB_ELECTION_ASYNC_LOG_WARN_INIT(-7030), //
    OB_ELECTION_WAIT_LEADER_MESSAGE(-7031), //
    OB_SERVER_IS_INIT(-8001), //
    OB_SERVER_IS_STOPPING(-8002), //
    OB_PACKET_CHECKSUM_ERROR(-8003), //
    OB_PACKET_CLUSTER_ID_NOT_MATCH(-8004), //
    OB_URI_ERROR(-9001), //
    OB_FINAL_MD5_ERROR(-9002), //
    OB_OSS_ERROR(-9003), //
    OB_INIT_MD5_ERROR(-9004), //
    OB_OUT_OF_ELEMENT(-9005), //
    OB_UPDATE_MD5_ERROR(-9006), //
    OB_FILE_LENGTH_INVALID(-9007), //
    OB_NOT_READ_ALL_DATA(-9008), //
    OB_BUILD_MD5_ERROR(-9009), //
    OB_MD5_NOT_MATCH(-9010), //
    OB_OSS_FILE_NOT_EXIST(-9011), //
    OB_OSS_DATA_VERSION_NOT_MATCHED(-9012), //
    OB_OSS_WRITE_ERROR(-9013), //
    OB_RESTORE_IN_PROGRESS(-9014), //
    OB_AGENT_INITING_BACKUP_COUNT_ERROR(-9015), //
    OB_CLUSTER_NAME_NOT_EQUAL(-9016), //
    OB_RS_LIST_INVAILD(-9017), //
    OB_AGENT_HAS_FAILED_TASK(-9018), //
    OB_ERR_KV_GLOBAL_INDEX_ROUTE(-10500), //
    OB_KV_CREDENTIAL_NOT_MATCH(-10509), //
    OB_KV_ROWKEY_COUNT_NOT_MATCH(-10510), //
    OB_KV_COLUMN_TYPE_NOT_MATCH(-10511), //
    OB_KV_COLLATION_MISMATCH(-10512), //
    OB_KV_SCAN_RANGE_MISSING(-10513), //
    OB_KV_FILTER_PARSE_ERROR(-10514), //
    OB_KV_REDIS_PARSE_ERROR(-10515), //
    OB_KV_HBASE_INCR_FIELD_IS_NOT_LONG(-10516), //
    OB_KV_CHECK_FAILED(-10518), //
    OB_KV_TABLE_NOT_DISABLED(-10519), //
    OB_KV_TABLE_NOT_ENABLED(-10520), //
    OB_KV_HBASE_NAMESPACE_NOT_FOUND(-10521), //
    OB_KV_HBASE_TABLE_EXISTS(-10522), //
    OB_KV_HBASE_TABLE_NOT_EXISTS(-10523), //
    OB_KV_HBASE_TABLE_NOT_DISABLED(-10524), //
    OB_KV_ODP_TIMEOUT(-10650), //
    OB_ERR_KV_ROUTE_ENTRY_EXPIRE(-10653);

    public final int errorCode;

    ResultCodes(int errorCode) {
        this.errorCode = errorCode;
    }

    private static final Map<Integer, ResultCodes> errorCodeToName = new HashMap<Integer, ResultCodes>();

    static {
        for (ResultCodes resultCodes : ResultCodes.values()) {
            errorCodeToName.put(resultCodes.errorCode, resultCodes);
        }
    }

    /*
     * Value of.
     */
    public static ResultCodes valueOf(int errorCode) {
        return errorCodeToName.get(errorCode);
    }
}
