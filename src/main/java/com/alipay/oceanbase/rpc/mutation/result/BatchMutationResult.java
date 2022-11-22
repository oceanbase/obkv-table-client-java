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

package com.alipay.oceanbase.rpc.mutation.result;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.Mutation;

import java.util.ArrayList;
import java.util.List;

public class BatchMutationResult {

    private List<Object> results;

    /*
     * construct with List of Object
     */
    public BatchMutationResult(List<Object> results) {
        this.results = results;
    }

    /*
     * get result
     */
    public MutationResult get(int pos) {
        if (pos >= results.size()) {
            throw new IllegalArgumentException("Invalid pos: " + pos + ", while size of results is: " + results.size());
        }
        return (MutationResult) results.get(pos);
    }

    /*
     * get wrong count in result
     */
    public long getWrongCount() {
        long wrongCount = 0;
        for (Object item : results) {
            if (item instanceof ObTableException) {
                ++wrongCount;
            }
        }
        return wrongCount;
    }

    /*
     * get correct count in result
     */
    public long getCorrectCount() {
        long correctCount = 0;
        for (Object item : results) {
            if (!(item instanceof ObTableException)) {
                ++correctCount;
            }
        }
        return correctCount;
    }

    /*
     * get wrong idx in result
     */
    public int[] getWrongIdx() {
        List<Integer> wrongIdx = new ArrayList<Integer>();
        Integer i = 0;
        for (Object item : results) {
            if (item instanceof ObTableException) {
                wrongIdx.add(i);
            }
            ++i;
        }
        return wrongIdx.stream().mapToInt(Integer::intValue).toArray();
    }

    /*
     * get wrong count in result
     */
    public int[] getCorrectIdx() {
        List<Integer> correctIdx = new ArrayList<Integer>();
        Integer i = 0;
        for (Object item : results) {
            if (!(item instanceof ObTableException)) {
                correctIdx.add(i);
            }
            ++i;
        }
        return correctIdx.stream().mapToInt(Integer::intValue).toArray();
    }
}
