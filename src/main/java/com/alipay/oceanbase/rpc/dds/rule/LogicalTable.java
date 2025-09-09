/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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
import com.alipay.oceanbase.rpc.dds.parser.DistributeRuleExpressParser;
import com.alipay.oceanbase.rpc.dds.parser.DistributeRuleSimpleFunc;

import java.util.*;

/**
* @author zhiqi.zzq
* @since 2021/7/9 下午2:57
*/
public class LogicalTable implements Lifecycle {

    private final String                               logicalTableName;
    private final List<String>                         tableRules;
    private final List<String>                         dbRules;
    private final List<String>                         elasticRules;
    private final String                               tableNamePatternStr;
    private final String                               rowKeyColumnStr;

    private NamePattern                                tableNamePattern;
    private String[]                                   rowKeyColumns;
    private Map<Set<String>, DistributeRuleSimpleFunc> tableRuleFunctions;
    private Map<Set<String>, DistributeRuleSimpleFunc> dbRuleFunctions;
    private Map<Set<String>, DistributeRuleSimpleFunc> elasticRuleFunctions;

    public LogicalTable(String logicalTableName, List<String> tableRules, List<String> dbRules,
                        List<String> elasticRules, String tableNamePatternStr,
                        String rowKeyColumnStr) {
        this.logicalTableName = logicalTableName;
        this.tableRules = tableRules;
        this.dbRules = dbRules;
        this.elasticRules = elasticRules;
        this.tableNamePatternStr = tableNamePatternStr;
        this.rowKeyColumnStr = rowKeyColumnStr;
    }

    @Override
    public void init() throws Exception {
        tableNamePattern = new NamePattern(tableNamePatternStr);
        tableRuleFunctions = new HashMap<Set<String>, DistributeRuleSimpleFunc>();
        if (tableRules != null) {
            for (String tableRule : tableRules) {
                DistributeRuleSimpleFunc tableRuleFunc = new DistributeRuleExpressParser(tableRule)
                    .parse();
                Set<String> referColumnNameSet = new HashSet<String>(
                    tableRuleFunc.getRefColumnNames());
                tableRuleFunctions.put(referColumnNameSet, tableRuleFunc);
            }
        }

        if (dbRules != null) {
            dbRuleFunctions = new HashMap<Set<String>, DistributeRuleSimpleFunc>();
            for (String dbRule : dbRules) {
                DistributeRuleSimpleFunc dbRuleFunc = new DistributeRuleExpressParser(dbRule)
                    .parse();
                Set<String> referColumnNameSet = new HashSet<String>(dbRuleFunc.getRefColumnNames());
                dbRuleFunctions.put(referColumnNameSet, dbRuleFunc);
            }
        }

        if (elasticRules != null) {
            elasticRuleFunctions = new HashMap<Set<String>, DistributeRuleSimpleFunc>();
            for (String elasticRule : elasticRules) {
                DistributeRuleSimpleFunc elasticRuleFunc = new DistributeRuleExpressParser(
                    elasticRule).parse();
                Set<String> referColumnNameSet = new HashSet<String>(
                    elasticRuleFunc.getRefColumnNames());
                dbRuleFunctions.put(referColumnNameSet, elasticRuleFunc);
            }
        }

        rowKeyColumns = rowKeyColumnStr.split(",");
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public Map<Set<String>, DistributeRuleSimpleFunc> getTableRuleFunctions() {
        return tableRuleFunctions;
    }

    public void setTableRuleFunctions(Map<Set<String>, DistributeRuleSimpleFunc> tableRuleFunctions) {
        this.tableRuleFunctions = tableRuleFunctions;
    }

    public Map<Set<String>, DistributeRuleSimpleFunc> getDbRuleFunctions() {
        return dbRuleFunctions;
    }

    public void setDbRuleFunctions(Map<Set<String>, DistributeRuleSimpleFunc> dbRuleFunctions) {
        this.dbRuleFunctions = dbRuleFunctions;
    }

    public List<String> getElasticRules() {
        return elasticRules;
    }

    public Map<Set<String>, DistributeRuleSimpleFunc> getElasticRuleFunctions() {
        return elasticRuleFunctions;
    }

    public void setElasticRuleFunctions(Map<Set<String>, DistributeRuleSimpleFunc> elasticRuleFunctions) {
        this.elasticRuleFunctions = elasticRuleFunctions;
    }

    public String getTableNamePatternStr() {
        return tableNamePatternStr;
    }

    public String getRowKeyColumnStr() {
        return rowKeyColumnStr;
    }

    public NamePattern getTableNamePattern() {
        return tableNamePattern;
    }

    public void setTableNamePattern(NamePattern tableNamePattern) {
        this.tableNamePattern = tableNamePattern;
    }

    public String[] getRowKeyColumns() {
        return rowKeyColumns;
    }

    public void setRowKeyColumns(String[] rowKeyColumns) {
        this.rowKeyColumns = rowKeyColumns;
    }

    public List<String> getTableRules() {
        return tableRules;
    }

    public List<String> getDbRules() {
        return dbRules;
    }

    @Override
    public void close() throws Exception {

    }

}
