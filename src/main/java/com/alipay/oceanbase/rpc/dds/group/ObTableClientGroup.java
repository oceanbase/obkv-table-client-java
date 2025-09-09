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

package com.alipay.oceanbase.rpc.dds.group;

import com.alipay.oceanbase.rpc.Lifecycle;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.util.StringUtil;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;

import java.util.*;

/**
* @author zhiqi.zzq
* @since 2021/7/8 上午11:16
*/
public class ObTableClientGroup implements Lifecycle {
    private final String                       groupKey;
    private final String                       weightString;
    private final GroupDataSourceWeight        groupDataSourceWeight;
    private final Map<String, ObTableClient>   atomDataSourceInGroup;
    private final Random                       random = new Random();

    private Integer                            groupIndex;
    private List<AtomDataSourceWeight>         readWeightRange;
    private List<AtomDataSourceWeight>         writeWeightRange;
    private Map<Integer, AtomDataSourceWeight> elasticIndexRange;

    public ObTableClientGroup(String groupKey, String weightString,
                              GroupDataSourceWeight groupDataSourceWeight,
                              Map<String, ObTableClient> atomDataSourceInGroup) {
        this.groupKey = groupKey;
        this.weightString = weightString;
        this.groupDataSourceWeight = groupDataSourceWeight;
        this.atomDataSourceInGroup = atomDataSourceInGroup;
    }

    @Override
    public void init() throws Exception {

        // 1. Parsing group index from group key
        try {
            groupIndex = StringUtil.indexOf(groupKey, "_");
            if (groupIndex < 0)
                throw new IllegalArgumentException(
                    "The group data source id must named as xxx_00 and the 00 indecated the index of current group data source.");
            groupIndex = Integer.parseInt(groupKey.substring(groupIndex + 1));
            if (groupIndex < 0)
                throw new IllegalArgumentException(
                    "The group data source id must named as xxx_00 and the 00 indecated the index of current group data source.");
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "The group data source id must named as xxx_00 and the 00 indecated the index of current group data source.");
        }

        // 2. Generate weight calculator
        readWeightRange = new ArrayList<AtomDataSourceWeight>(10);
        writeWeightRange = new ArrayList<AtomDataSourceWeight>(10);
        elasticIndexRange = new HashMap<Integer, AtomDataSourceWeight>();
        for (AtomDataSourceWeight weight : groupDataSourceWeight.getDataSourceReadWriteWeights()) {
            int elasticIndex = weight.getIndex();
            elasticIndexRange.put(elasticIndex, weight);
            for (int i = 0; i < weight.getReadWeight(); i++) {
                readWeightRange.add(weight);
            }

            for (int i = 0; i < weight.getWriteWeight(); i++) {
                writeWeightRange.add(weight);
            }
        }

    }

    public ObTableClient select(boolean write) {
        return select(write, -1);
    }

    public ObTableClient select(boolean write, Integer elasticIndex) {
        if (elasticIndex != null && elasticIndex > -1) {
            AtomDataSourceWeight weight = elasticIndexRange.get(elasticIndex);
            if (weight == null) {
                throw new IllegalArgumentException("Group DataSource group key [" + groupKey
                                                   + "] found unavailable elastic index ["
                                                   + elasticIndex + "]");
            }

            return atomDataSourceInGroup.get(weight.getDbkey());
        }

        if (write) {
            AtomDataSourceWeight weight = writeWeightRange.get(random.nextInt(writeWeightRange
                .size() - 1));
            return atomDataSourceInGroup.get(weight.getDbkey());
        } else {
            AtomDataSourceWeight weight = readWeightRange
                .get(random.nextInt(readWeightRange.size() - 1));
            return atomDataSourceInGroup.get(weight.getDbkey());
        }
    }

    public String getGroupKey() {
        return groupKey;
    }

    public String getWeightString() {
        return weightString;
    }

    public GroupDataSourceWeight getGroupDataSourceWeight() {
        return groupDataSourceWeight;
    }

    public Integer getGroupIndex() {
        return groupIndex;
    }

    public void setGroupIndex(Integer groupIndex) {
        this.groupIndex = groupIndex;
    }

    @Override
    public void close() throws Exception {

    }
}
