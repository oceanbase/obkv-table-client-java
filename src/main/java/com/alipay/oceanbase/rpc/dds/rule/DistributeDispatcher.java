/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.dds.rule;

import com.alipay.oceanbase.rpc.Lifecycle;
import com.alipay.oceanbase.rpc.exception.DistributeDispatchException;
import com.alipay.oceanbase.rpc.dds.config.DistributeConfigHandler;
import com.alipay.oceanbase.rpc.dds.parser.DistributeRuleSimpleFunc;
import com.alipay.oceanbase.rpc.util.StringUtil;
import com.alipay.sofa.dds.config.rule.AppRule;
import com.alipay.sofa.dds.config.rule.ShardRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

/**
 * @author zhiqi.zzq
 * @since 2021/7/9 下午4:18
 */
public class DistributeDispatcher implements Lifecycle {

    private static final Logger           logger = LoggerFactory
                                                     .getLogger(DistributeDispatcher.class);

    private final DistributeConfigHandler distributeConfigHandler;

    private Map<String, LogicalTable>     logicalTables;

    public DistributeDispatcher(DistributeConfigHandler distributeConfigHandler) {
        this.distributeConfigHandler = distributeConfigHandler;
    }

    @Override
     public void init() throws Exception {
         logicalTables = new HashMap<String, LogicalTable>();
         buildLogicalTables(logicalTables, distributeConfigHandler.getAppRule());
         ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
         scheduledExecutorService.scheduleAtFixedRate(new Thread(() -> {
             try {
                 buildLogicalTables(logicalTables, distributeConfigHandler.getAppRule());
             } catch (Exception e) {
                 logger.error("failed to update dds logical table info.", e);
             }
         }), 300, 300, TimeUnit.SECONDS);
     }

    private void buildLogicalTables(Map<String, LogicalTable> logicalTables, AppRule appRule)
                                                                                             throws Exception {
        if (appRule != null && appRule.getShardRules() != null) {
            for (Map.Entry<String, ShardRule> ruleEntry : appRule.getShardRules().entrySet()) {
                // 1、Trim the table name to avoid the unexpected char
                // 2. Upper the table name to unify the usage
                String tableName = ruleEntry.getKey().trim().toUpperCase();
                if (logicalTables.containsKey(tableName)) {
                    continue;
                }
                ShardRule rule = ruleEntry.getValue();
                LogicalTable table = new LogicalTable(tableName, rule.getTbRules(),
                    rule.getDbRules(), rule.getElasticRules(), rule.getTbNamePattern(),
                    rule.getRowKeyColumn());
                table.init();

                logicalTables.putIfAbsent(tableName, table);
            }
        }
    }

    public DatabaseAndTable resolveDatabaseAndTable(String tableName, Object[] columnValues) {

        // 1、Trim the table name to avoid the unexpected char
        // 2. Upper the table name to unify the usage
        tableName = tableName.trim().toUpperCase();

        LogicalTable logicalTable = logicalTables.get(tableName);

        if (logicalTable != null) {
            return resolveDatabaseAndTable(tableName, logicalTable.getRowKeyColumns(), columnValues);
        } else {
            return resolveDatabaseAndTable(tableName, null, columnValues);
        }
    }

    public DatabaseAndTable resolveDatabaseAndTable(String tableName, String[] columnNames,
                                                    Object[] columnValues) {

        // if (tableName == null || columnNames == null || columnValues == null) {
        //     logger.error("tableName, columnNames, columnValues is null");
        //     throw new DistributeDispatchException("invalid parameters for resolveDatabaseAndTable");
        // }
        // if (columnNames.length != columnValues.length) {
        //     logger.error(LCD.convert("02-00001"), tableName, columnNames, columnValues);
        //     throw new DistributeDispatchException("OBKV-ROUTER Logical table [" + tableName
        //                                           + "] 02-00001: the length of giving columns ["
        //                                           + Arrays.toString(columnNames)
        //                                           + "] is not match with giving column value ["
        //                                           + Arrays.toString(columnValues) + "]");
        // }
        // 1、Trim the table name to avoid the unexpected char
        // 2. Upper the table name to unify the usage
        tableName = tableName.trim().toUpperCase();

        LogicalTable logicalTable = logicalTables.get(tableName);

        if (logicalTable == null) {
            // when the logical table is not exist we will return default route result
            DatabaseAndTable dt = new DatabaseAndTable();
            dt.setDatabaseShardValue(0);
            dt.setTableShardValue(0);
            dt.setTableName(tableName);
            dt.setElasticIndexValue(-1);
            return dt;
        }

        Map<String, Object> giveColumns = new HashMap<String, Object>();
        for (int i = 0; i < columnNames.length; i++) {
            giveColumns.put(columnNames[i], columnValues[i]);
        }

        DistributeRuleSimpleFunc tableRuleFunc = determineTableRuleFunc(logicalTable,
            giveColumns.keySet());

        DistributeRuleSimpleFunc dbRuleFunc = determineDbRuleFunc(logicalTable,
            giveColumns.keySet());

        DistributeRuleSimpleFunc elasticRuleFunc = determineElasticRuleFunc(logicalTable,
            giveColumns.keySet());

        DatabaseAndTable databaseAndTable = new DatabaseAndTable();

        calculateTableRule(databaseAndTable, logicalTable, giveColumns, tableRuleFunc);

        calculateDbRule(databaseAndTable, logicalTable, giveColumns, dbRuleFunc);

        calculateElasticRule(databaseAndTable, logicalTable, giveColumns, elasticRuleFunc);

        return databaseAndTable;
    }

    public List<DatabaseAndTable> resolveDatabaseAndTable(String tableName, String[] columnNames,
                                                          Object[] startColumnValues,
                                                          boolean includeStart,
                                                          Object[] endColumnValues,
                                                          boolean includeEnd) {

        // 1、Trim the table name to avoid the unexpected char
        // 2. Upper the table name to unify the usage
        tableName = tableName.trim().toUpperCase();

        LogicalTable logicalTable = logicalTables.get(tableName);

        List<DatabaseAndTable> databaseAndTables = new ArrayList<DatabaseAndTable>();

        if (logicalTable == null) {
            // when the logical table is not exist we will return default route result
            DatabaseAndTable dt = new DatabaseAndTable();
            dt.setDatabaseShardValue(0);
            dt.setTableShardValue(0);
            dt.setTableName(tableName);
            dt.setElasticIndexValue(-1);
            databaseAndTables.add(dt);
            return databaseAndTables;
        }

        if (columnNames.length != startColumnValues.length) {
            logger.error(LCD.convert("02-00010"), tableName, columnNames, startColumnValues);
            throw new DistributeDispatchException("OBKV-ROUTER Logical table [" + tableName
                                                  + "] 02-00010: the length of giving columns ["
                                                  + Arrays.toString(columnNames)
                                                  + "] is not match with start column value ["
                                                  + Arrays.toString(startColumnValues) + "]");
        }

        if (columnNames.length != endColumnValues.length) {
            logger.error(LCD.convert("02-00011"), columnNames, startColumnValues);
            throw new DistributeDispatchException("OBKV-ROUTER Logical table [" + tableName
                                                  + "] 02-00011: the length of giving columns ["
                                                  + Arrays.toString(columnNames)
                                                  + "] is not match with end column value ["
                                                  + Arrays.toString(startColumnValues) + "]");
        }

        Map<String, Object> giveStartColumns = new HashMap<String, Object>();
        for (int i = 0; i < columnNames.length; i++) {
            giveStartColumns.put(columnNames[i], startColumnValues[i]);
        }

        Map<String, Object> giveEndColumns = new HashMap<String, Object>();
        for (int i = 0; i < columnNames.length; i++) {
            giveEndColumns.put(columnNames[i], endColumnValues[i]);
        }

        DistributeRuleSimpleFunc tableRuleFunc = determineTableRuleFunc(logicalTable,
            giveStartColumns.keySet());

        DistributeRuleSimpleFunc dbRuleFunc = determineDbRuleFunc(logicalTable,
            giveStartColumns.keySet());

        DistributeRuleSimpleFunc elasticRuleFunc = determineElasticRuleFunc(logicalTable,
            giveStartColumns.keySet());

        //TODO Distribute Query is not support yet so that we restrict queries to one shard.

        DatabaseAndTable databaseAndTable = new DatabaseAndTable();

        Integer tableShardValue = calculateTableRule(logicalTable, giveStartColumns,
            giveEndColumns, tableRuleFunc);

        Integer databaseShardValue = calculateDbRule(logicalTable, giveStartColumns,
            giveEndColumns, dbRuleFunc);

        Integer elasticIndex = calculateElasticRule(logicalTable, giveStartColumns, giveEndColumns,
            elasticRuleFunc);

        databaseAndTable.setTableShardValue(tableShardValue);

        databaseAndTable.setTableName(logicalTable.getTableNamePattern().wrapValue(
            String.valueOf(tableShardValue)));

        databaseAndTable.setDatabaseShardValue(databaseShardValue);

        databaseAndTable.setElasticIndexValue(elasticIndex);

        databaseAndTables.add(databaseAndTable);

        return databaseAndTables;
    }

    private Integer calculateElasticRule(LogicalTable logicalTable,
                                         Map<String, Object> giveStartColumns,
                                         Map<String, Object> giveEndColumns,
                                         DistributeRuleSimpleFunc elasticRuleFunc) {
        {

            if (elasticRuleFunc != null) {
                List<String> elasticReferColumns = elasticRuleFunc.getRefColumnNames();
                List<Object> toEvalElasticIndexStartObject = new ArrayList<Object>(
                    elasticReferColumns.size());
                for (String referColumns : elasticReferColumns) {
                    toEvalElasticIndexStartObject.add(giveStartColumns.get(referColumns));
                }

                List<Object> toEvalElasticIndexEndObject = new ArrayList<Object>(
                    elasticReferColumns.size());
                for (String referColumns : elasticReferColumns) {
                    toEvalElasticIndexEndObject.add(giveEndColumns.get(referColumns));
                }

                //TODO Distribute Query is not support yet so that we restrict queries to one shard.
                //FIXME Actually ,it is difficult to decided whether or not the query is limited in one shard.
                /** If we compare the start params with end param before evaluation ,it may be too strict.
                 * e.g.
                 * <p>function   :substr(K, 1, 2)</p>
                 * <p>column name:(K, Q , T) </p>
                 * <p>start param:(0000, 'test', 1626158170240) </p>
                 * <p>end param  :(0001, 'test', 1626158170240) </p>
                 * We found the start param is not same with the end param but they are all in the same shard.
                 *
                 * If we compare the start params with end param before evaluation ,it may be not strict enough。
                 * e.g.
                 * <p>function    :mod(K, 10) = K % 10 </p>
                 * <p>column name:(K, Q , T) </p>
                 * <p>start param:(0000, 'test', 1626158170240) </p>
                 * <p>end param  :(2000, 'test', 1626158170240) </p>
                 * We found the same result after evaluation but the range is in the same shard.
                 */
                //FIXME Generally ,the substr function is adapt to the situation so that we compare the start params with end param after evaluation.

                Object startElasticEvalValue;
                try {
                    startElasticEvalValue = elasticRuleFunc
                        .evalValue(toEvalElasticIndexStartObject);
                } catch (Exception e) {
                    logger.error(LCD.convert("02-00008"), logicalTable.getLogicalTableName(),
                        elasticReferColumns, toEvalElasticIndexStartObject, e);
                    throw new DistributeDispatchException(
                        "OBKV-ROUTER Logical table ["
                                + logicalTable.getLogicalTableName()
                                + "] 02-00008: elastic rule eval come across some error: refer columns ["
                                + elasticReferColumns + "] refer value ["
                                + toEvalElasticIndexStartObject + "]" + e);
                }
                String startElasticIndexEvalValueStr = startElasticEvalValue.toString();

                Object endElasticEvalValue;
                try {
                    endElasticEvalValue = elasticRuleFunc.evalValue(toEvalElasticIndexEndObject);
                } catch (Exception e) {
                    logger.error(LCD.convert("02-00008"), logicalTable.getLogicalTableName(),
                        elasticReferColumns, toEvalElasticIndexStartObject);
                    throw new DistributeDispatchException(
                        "OBKV-ROUTER Logical table ["
                                + logicalTable.getLogicalTableName()
                                + "] 02-00008: elastic rule eval come across some error: refer columns ["
                                + elasticReferColumns + "] refer value ["
                                + toEvalElasticIndexStartObject + "]");
                }
                String endElasticIndexEvalValueStr = endElasticEvalValue.toString();

                if (!StringUtil.equals(startElasticIndexEvalValueStr, endElasticIndexEvalValueStr)) {
                    logger.error(LCD.convert("02-00014"), logicalTable.getLogicalTableName(),
                        elasticReferColumns, toEvalElasticIndexStartObject,
                        startElasticIndexEvalValueStr, toEvalElasticIndexEndObject,
                        endElasticIndexEvalValueStr);
                    throw new DistributeDispatchException(
                        "OBKV-ROUTER Logical table ["
                                + logicalTable.getLogicalTableName()
                                + "] 02-00014: Distribute Query is not support yet and different elastic index is found from the refer columns ["
                                + elasticReferColumns + "]. start param is ["
                                + toEvalElasticIndexStartObject + "] ,start eval value is ["
                                + startElasticIndexEvalValueStr + "] end param is ["
                                + toEvalElasticIndexEndObject + "] end eval value is ["
                                + endElasticIndexEvalValueStr + "]");
                }

                Integer elasticIndex;

                try {
                    elasticIndex = Integer.parseInt(startElasticIndexEvalValueStr);
                } catch (NumberFormatException e) {
                    logger.error(LCD.convert("02-00009"), logicalTable.getLogicalTableName(),
                        elasticReferColumns, toEvalElasticIndexStartObject,
                        startElasticIndexEvalValueStr);
                    throw new DistributeDispatchException(
                        "OBKV-ROUTER Logical table ["
                                + logicalTable.getLogicalTableName()
                                + "] 02-00009: elastic rule eval value can not convert to INTEGER  refer columns ["
                                + elasticReferColumns + "] refer value ["
                                + toEvalElasticIndexStartObject + "] eval value str ["
                                + startElasticIndexEvalValueStr + "]");
                }

                return elasticIndex;
            }

            return null;
        }
    }

    private void calculateElasticRule(DatabaseAndTable databaseAndTable, LogicalTable logicalTable,
                                      Map<String, Object> giveColumns,
                                      DistributeRuleSimpleFunc elasticRuleFunc) {
        {

            if (elasticRuleFunc != null) {
                List<String> elasticReferColumns = elasticRuleFunc.getRefColumnNames();
                List<Object> toEvalElasticIndexObject = new ArrayList<Object>(
                    elasticReferColumns.size());
                for (String referColumns : elasticReferColumns) {
                    toEvalElasticIndexObject.add(giveColumns.get(referColumns));
                }
                Object elasticEvalValue;

                try {
                    elasticEvalValue = elasticRuleFunc.evalValue(toEvalElasticIndexObject);
                } catch (Exception e) {
                    logger.error(LCD.convert("02-00008"), logicalTable.getLogicalTableName(),
                        elasticReferColumns, toEvalElasticIndexObject);
                    throw new DistributeDispatchException(
                        "OBKV-ROUTER Logical table ["
                                + logicalTable.getLogicalTableName()
                                + "] 02-00008: elastic rule eval come across some error: refer columns ["
                                + elasticReferColumns + "] refer value ["
                                + toEvalElasticIndexObject + "]");
                }
                String elasticIndexEvalValueStr = elasticEvalValue.toString();

                Integer elasticIndex;

                try {
                    elasticIndex = Integer.parseInt(elasticIndexEvalValueStr);
                } catch (NumberFormatException e) {
                    logger.error(LCD.convert("02-00009"), logicalTable.getLogicalTableName(),
                        elasticReferColumns, toEvalElasticIndexObject, elasticIndexEvalValueStr);
                    throw new DistributeDispatchException(
                        "OBKV-ROUTER Logical table ["
                                + logicalTable.getLogicalTableName()
                                + "] 02-00009: elastic rule eval value can not convert to INTEGER  refer columns ["
                                + elasticReferColumns + "] refer value ["
                                + toEvalElasticIndexObject + "] eval value str ["
                                + elasticIndexEvalValueStr + "]");
                }

                databaseAndTable.setElasticIndexValue(elasticIndex);
            }

        }
    }

    private Integer calculateDbRule(LogicalTable logicalTable,
                                    Map<String, Object> giveStartColumns,
                                    Map<String, Object> giveEndColumns,
                                    DistributeRuleSimpleFunc dbRuleFunc) {
        {
            List<String> dbRuleReferColumns = dbRuleFunc.getRefColumnNames();
            List<Object> toEvalDatabaseShardValueStartObject = new ArrayList<Object>(
                dbRuleReferColumns.size());
            for (String referColumns : dbRuleReferColumns) {
                toEvalDatabaseShardValueStartObject.add(giveStartColumns.get(referColumns));
            }

            List<Object> toEvalDatabaseShardValueEndObject = new ArrayList<Object>(
                dbRuleReferColumns.size());
            for (String referColumns : dbRuleReferColumns) {
                toEvalDatabaseShardValueEndObject.add(giveEndColumns.get(referColumns));
            }

            //TODO Distribute Query is not support yet so that we restrict queries to one shard.
            //FIXME Actually ,it is difficult to decided whether or not the query is limited in one shard.
            /** If we compare the start params with end param before evaluation ,it may be too strict.
             * e.g.
             * <p>function   :substr(K, 1, 2)</p>
             * <p>column name:(K, Q , T) </p>
             * <p>start param:(0000, 'test', 1626158170240) </p>
             * <p>end param  :(0001, 'test', 1626158170240) </p>
             * We found the start param is not same with the end param but they are all in the same shard.
             *
             * If we compare the start params with end param before evaluation ,it may be not strict enough。
             * e.g.
             * <p>function    :mod(K, 10) = K % 10 </p>
             * <p>column name:(K, Q , T) </p>
             * <p>start param:(0000, 'test', 1626158170240) </p>
             * <p>end param  :(2000, 'test', 1626158170240) </p>
             * We found the same result after evaluation but the range is in the same shard.
             */
            //FIXME Generally ,the substr function is adapt to the situation so that we compare the start params with end param after evaluation.

            Object startDatabaseShardEvalValue;
            try {
                startDatabaseShardEvalValue = dbRuleFunc
                    .evalValue(toEvalDatabaseShardValueStartObject);
            } catch (Exception e) {
                logger.error(LCD.convert("02-00006"), logicalTable.getLogicalTableName(),
                    dbRuleReferColumns, toEvalDatabaseShardValueStartObject, e);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER 02-00008: db rule eval come across some error: refer columns ["
                            + dbRuleReferColumns + "] refer value ["
                            + toEvalDatabaseShardValueStartObject + "]" + e);
            }
            String startDatabaseShardEvalValueStr = startDatabaseShardEvalValue.toString();

            Object endDatabaseShardEvalValue;
            try {
                endDatabaseShardEvalValue = dbRuleFunc.evalValue(toEvalDatabaseShardValueEndObject);
            } catch (Exception e) {
                logger.error(LCD.convert("02-00006"), logicalTable.getLogicalTableName(),
                    dbRuleReferColumns, toEvalDatabaseShardValueStartObject, e);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER 02-00008: db rule eval come across some error: refer columns ["
                            + dbRuleReferColumns + "] refer value ["
                            + toEvalDatabaseShardValueStartObject + "]" + e);
            }
            String endDatabaseShardEvalValueStr = endDatabaseShardEvalValue.toString();

            if (!StringUtil.equals(startDatabaseShardEvalValueStr, endDatabaseShardEvalValueStr)) {
                logger.error(LCD.convert("02-00013"), logicalTable.getLogicalTableName(),
                    dbRuleReferColumns, toEvalDatabaseShardValueStartObject,
                    startDatabaseShardEvalValueStr, toEvalDatabaseShardValueEndObject,
                    endDatabaseShardEvalValueStr);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + logicalTable.getLogicalTableName()
                            + "] Distribute Query is not support yet and different database shard value is found from the refer columns ["
                            + dbRuleReferColumns + "]. start param is ["
                            + toEvalDatabaseShardValueStartObject + "] ,start eval value is ["
                            + startDatabaseShardEvalValueStr + "] end param is ["
                            + toEvalDatabaseShardValueEndObject + "] end eval value is ["
                            + endDatabaseShardEvalValueStr + "]");
            }

            Integer databaseShardValue;

            try {
                databaseShardValue = Integer.parseInt(startDatabaseShardEvalValueStr);
            } catch (NumberFormatException e) {
                logger.error(LCD.convert("02-00007"), logicalTable.getLogicalTableName(),
                    dbRuleReferColumns, toEvalDatabaseShardValueStartObject,
                    startDatabaseShardEvalValueStr);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + logicalTable.getLogicalTableName()
                            + "] 02-00007: DB rule eval eval value can not convert to INTEGER  refer columns ["
                            + dbRuleReferColumns + "] refer value ["
                            + toEvalDatabaseShardValueStartObject + "] eval value str ["
                            + startDatabaseShardEvalValueStr + "]");
            }

            return databaseShardValue;

        }
    }

    private void calculateDbRule(DatabaseAndTable databaseAndTable, LogicalTable logicalTable,
                                 Map<String, Object> giveColumns,
                                 DistributeRuleSimpleFunc dbRuleFunc) {
        {
            List<String> dbRuleReferColumns = dbRuleFunc.getRefColumnNames();
            List<Object> toEvalDatabaseShardValueObject = new ArrayList<Object>(
                dbRuleReferColumns.size());
            for (String referColumns : dbRuleReferColumns) {
                toEvalDatabaseShardValueObject.add(giveColumns.get(referColumns));
            }
            Object dbRuleEvalValue;

            try {
                dbRuleEvalValue = dbRuleFunc.evalValue(toEvalDatabaseShardValueObject);
            } catch (Exception e) {
                logger.error(LCD.convert("02-00006"), logicalTable.getLogicalTableName(),
                    dbRuleReferColumns, toEvalDatabaseShardValueObject, e);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table [" + logicalTable.getLogicalTableName()
                            + "] 02-00006: db rule eval come across some error: refer columns ["
                            + dbRuleReferColumns + "] refer value ["
                            + toEvalDatabaseShardValueObject + "]" + e);
            }
            String dbRuleEvalValueStr = dbRuleEvalValue.toString();

            Integer databaseShardValue;

            try {
                databaseShardValue = Integer.parseInt(dbRuleEvalValueStr);
            } catch (NumberFormatException e) {
                logger.error(LCD.convert("02-00007"), logicalTable.getLogicalTableName(),
                    dbRuleReferColumns, toEvalDatabaseShardValueObject, dbRuleEvalValueStr);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + logicalTable.getLogicalTableName()
                            + "] 02-00007: db rule eval value can not convert to INTEGER  refer columns ["
                            + dbRuleReferColumns + "] refer value ["
                            + toEvalDatabaseShardValueObject + "] eval value str ["
                            + dbRuleEvalValueStr + "]");
            }

            databaseAndTable.setDatabaseShardValue(databaseShardValue);

        }
    }

    private Integer calculateTableRule(LogicalTable logicalTable,
                                       Map<String, Object> giveStartColumns,
                                       Map<String, Object> giveEndColumns,
                                       DistributeRuleSimpleFunc tableRuleFunc) {
        {
            List<String> tableRuleReferColumns = tableRuleFunc.getRefColumnNames();
            List<Object> toEvalTableShardValueStartObject = new ArrayList<Object>(
                tableRuleReferColumns.size());
            for (String referColumns : tableRuleReferColumns) {
                toEvalTableShardValueStartObject.add(giveStartColumns.get(referColumns));
            }

            List<Object> toEvalTableShardValueEndObject = new ArrayList<Object>(
                tableRuleReferColumns.size());
            for (String referColumns : tableRuleReferColumns) {
                toEvalTableShardValueEndObject.add(giveEndColumns.get(referColumns));
            }

            //TODO Distribute Query is not support yet so that we restrict queries to one shard.
            //FIXME Actually ,it is difficult to decided whether or not the query is limited in one shard.
            /** If we compare the start params with end param before evaluation ,it may be too strict.
             * e.g.
             * <p>function   :substr(K, 1, 2)</p>
             * <p>column name:(K, Q , T) </p>
             * <p>start param:(0000, 'test', 1626158170240) </p>
             * <p>end param  :(0001, 'test', 1626158170240) </p>
             * We found the start param is not same with the end param but they are all in the same shard.
             *
             * If we compare the start params with end param before evaluation ,it may be not strict enough。
             * e.g.
             * <p>function    :mod(K, 10) = K % 10 </p>
             * <p>column name:(K, Q , T) </p>
             * <p>start param:(0000, 'test', 1626158170240) </p>
             * <p>end param  :(2000, 'test', 1626158170240) </p>
             * We found the same result after evaluation but the range is in the same shard.
             */
            //FIXME Generally ,the substr function is adapt to the situation so that we compare the start params with end param after evaluation.

            Object startTableShardEvalValue;
            try {
                startTableShardEvalValue = tableRuleFunc
                    .evalValue(toEvalTableShardValueStartObject);
            } catch (Exception e) {
                logger.error(LCD.convert("02-00004"), logicalTable.getLogicalTableName(),
                    tableRuleReferColumns, toEvalTableShardValueStartObject, e);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table [" + logicalTable.getLogicalTableName()
                            + "] 02-00004: table rule eval come across some error: refer columns ["
                            + tableRuleReferColumns + "] refer value ["
                            + toEvalTableShardValueStartObject + "]" + e);
            }
            String startTableShardEvalValueStr = startTableShardEvalValue.toString();

            Object endTableShardEvalValue;
            try {
                endTableShardEvalValue = tableRuleFunc.evalValue(toEvalTableShardValueEndObject);
            } catch (Exception e) {
                logger.error(LCD.convert("02-00004"), logicalTable.getLogicalTableName(),
                    tableRuleReferColumns, toEvalTableShardValueStartObject, e);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table [" + logicalTable.getLogicalTableName()
                            + "] 02-00004: table rule eval come across some error: refer columns ["
                            + tableRuleReferColumns + "] refer value ["
                            + toEvalTableShardValueStartObject + "]" + e);
            }
            String endTableShardEvalValueStr = endTableShardEvalValue.toString();

            if (!StringUtil.equals(startTableShardEvalValueStr, endTableShardEvalValueStr)) {
                logger.error(LCD.convert("02-00012"), logicalTable.getLogicalTableName(),
                    tableRuleReferColumns, toEvalTableShardValueStartObject,
                    startTableShardEvalValueStr, toEvalTableShardValueEndObject,
                    endTableShardEvalValueStr);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + logicalTable.getLogicalTableName()
                            + "] 02-00012: Distribute Query is not support yet and different table shard value is found from the refer columns ["
                            + tableRuleReferColumns + "]. start param is ["
                            + toEvalTableShardValueStartObject + "] ,start eval value is ["
                            + startTableShardEvalValueStr + "] end param is ["
                            + toEvalTableShardValueEndObject + "] end eval value is ["
                            + endTableShardEvalValueStr + "]");
            }

            Integer tableShardValue;

            try {
                tableShardValue = Integer.parseInt(startTableShardEvalValueStr);
            } catch (NumberFormatException e) {
                logger.error(LCD.convert("02-00005"), logicalTable.getLogicalTableName(),
                    tableRuleReferColumns, toEvalTableShardValueStartObject,
                    startTableShardEvalValueStr);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + logicalTable.getLogicalTableName()
                            + "] 02-00005: DB rule eval eval value can not convert to INTEGER  refer columns ["
                            + tableRuleReferColumns + "] refer value ["
                            + toEvalTableShardValueStartObject + "] eval value str ["
                            + startTableShardEvalValueStr + "]");
            }

            return tableShardValue;

        }
    }

    private void calculateTableRule(DatabaseAndTable databaseAndTable, LogicalTable logicalTable,
                                    Map<String, Object> giveColumns,
                                    DistributeRuleSimpleFunc tableRuleFunc) {

        {
            List<String> tableRuleReferColumns = tableRuleFunc.getRefColumnNames();
            List<Object> toEvalTableShardValueObject = new ArrayList<Object>(
                tableRuleReferColumns.size());
            for (String referColumns : tableRuleReferColumns) {
                toEvalTableShardValueObject.add(giveColumns.get(referColumns));
            }
            Object tableRuleEvalValue;

            try {
                tableRuleEvalValue = tableRuleFunc.evalValue(toEvalTableShardValueObject);
            } catch (Exception e) {
                logger.error(LCD.convert("02-00004"), tableRuleReferColumns,
                    toEvalTableShardValueObject, e);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table [" + logicalTable.getLogicalTableName()
                            + "] 02-00004: table rule eval come across some error: refer columns ["
                            + tableRuleReferColumns + "] refer value ["
                            + toEvalTableShardValueObject + "]" + e);
            }
            String tableRuleEvalValueStr = tableRuleEvalValue.toString();

            Integer tableShardValue;

            try {
                tableShardValue = Integer.parseInt(tableRuleEvalValueStr);
            } catch (NumberFormatException e) {
                logger.error(LCD.convert("02-00005"), tableRuleReferColumns,
                    toEvalTableShardValueObject, tableRuleEvalValueStr);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + logicalTable.getLogicalTableName()
                            + "] 02-00005: table rule eval value can not convert to INTEGER  refer columns ["
                            + tableRuleReferColumns + "] refer value ["
                            + toEvalTableShardValueObject + "] eval value str ["
                            + tableRuleEvalValueStr + "]");
            }

            databaseAndTable.setTableShardValue(tableShardValue);

            String targetTableName = logicalTable.getTableNamePattern().wrapValue(
                tableRuleEvalValueStr);

            databaseAndTable.setTableName(targetTableName);
        }
    }

    private DistributeRuleSimpleFunc determineElasticRuleFunc(LogicalTable logicalTable,
                                                              Set<String> givingColumnNames) {
        DistributeRuleSimpleFunc elasticRuleFunc = null;

        if (logicalTable.getElasticRuleFunctions() != null) {
            for (Map.Entry<Set<String>, DistributeRuleSimpleFunc> ruleSimpleFuncEntry : logicalTable
                .getElasticRuleFunctions().entrySet()) {
                Set<String> referColumnNameSet = ruleSimpleFuncEntry.getKey();
                DistributeRuleSimpleFunc referFunc = ruleSimpleFuncEntry.getValue();
                if (givingColumnNames.containsAll(referColumnNameSet)) {
                    // when the rule contains multi columns , it is hard to decide which is proper.
                    // We suppose that it will return the same result when the rule knock against other , so that we just use the first matched rule.
                    elasticRuleFunc = referFunc;
                    break;
                }
            }
        }
        return elasticRuleFunc;
    }

    private DistributeRuleSimpleFunc determineDbRuleFunc(LogicalTable logicalTable,
                                                         Set<String> givingColumnNames) {
        DistributeRuleSimpleFunc dbRuleFunc = null;
        for (Map.Entry<Set<String>, DistributeRuleSimpleFunc> ruleSimpleFuncEntry : logicalTable
            .getDbRuleFunctions().entrySet()) {
            Set<String> referColumnNameSet = ruleSimpleFuncEntry.getKey();
            DistributeRuleSimpleFunc referFunc = ruleSimpleFuncEntry.getValue();
            if (givingColumnNames.containsAll(referColumnNameSet)) {
                // when the rule contains multi columns , it is hard to decide which is proper.
                // We suppose that it will return the same result when the rule knock against other , so that we just use the first matched rule.
                dbRuleFunc = referFunc;
                break;
            }
        }

        if (dbRuleFunc == null) {
            logger.error(LCD.convert("02-00003"), logicalTable.getLogicalTableName(),
                givingColumnNames);
            throw new DistributeDispatchException(
                "OBKV-ROUTER Logical table [" + logicalTable.getLogicalTableName()
                        + "] 02-00003: unable to match db rule from giving columns ["
                        + givingColumnNames + "]");
        }

        return dbRuleFunc;
    }

    private DistributeRuleSimpleFunc determineTableRuleFunc(LogicalTable logicalTable,
                                                            Set<String> givingColumnNames) {
        DistributeRuleSimpleFunc tableRuleFunc = null;
        for (Map.Entry<Set<String>, DistributeRuleSimpleFunc> ruleSimpleFuncEntry : logicalTable
            .getTableRuleFunctions().entrySet()) {
            Set<String> referColumnNameSet = ruleSimpleFuncEntry.getKey();
            DistributeRuleSimpleFunc referFunc = ruleSimpleFuncEntry.getValue();
            if (givingColumnNames.containsAll(referColumnNameSet)) {
                // when the rule contains multi columns , it is hard to decide which is proper .
                // We suppose that it will return the same result when the rule knock against other , so that we just use the first matched rule.
                tableRuleFunc = referFunc;
                break;
            }
        }

        if (tableRuleFunc == null) {
            logger.error(LCD.convert("02-00002"), logicalTable.getLogicalTableName(),
                givingColumnNames);
            throw new DistributeDispatchException(
                "OBKV-ROUTER Logical table [" + logicalTable.getLogicalTableName()
                        + "] 02-00002: unable to match table rule from giving columns ["
                        + givingColumnNames + "]");
        }

        return tableRuleFunc;
    }

    public LogicalTable findLogicalTable(String tableName) {
        if (tableName == null) {
            return null;
        }
        // 1、Trim the table name to avoid the unexpected char
        // 2. Upper the table name to unify the usage
        tableName = tableName.trim().toUpperCase();

        return logicalTables.get(tableName);
    }

    public Map<String, LogicalTable> getLogicalTables() {
        return logicalTables;
    }

    public void setLogicalTables(Map<String, LogicalTable> logicalTables) {
        this.logicalTables = logicalTables;
    }

    @Override
    public void close() throws Exception {

    }
}
