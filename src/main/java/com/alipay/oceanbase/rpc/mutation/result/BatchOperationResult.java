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
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;

import java.util.ArrayList;
import java.util.List;

public class BatchOperationResult {

    private List<Object> results;

    boolean              hasError = false;

    /*
     * construct with List of Object
     */
    public BatchOperationResult(List<Object> results) {
        this.results = results;
    }

    public List<Object> getResults() {
        return results;
    }

    public List<Integer> getErrorCodeList() {
        List<Integer> errorCodeList = new ArrayList<Integer>();
        for (Object item : results) {
            int errorCode = ResultCodes.OB_SUCCESS.errorCode;
            if (item instanceof ObTableException) {
                errorCode = ((ObTableException) item).getErrorCode();
                hasError = true;
            }
            errorCodeList.add(errorCode);
        }
        return errorCodeList;
    }

    public boolean hasError() {
        if (!hasError) {
            for (Object item : results) {
                if (item instanceof ObTableException) {
                    hasError = true;
                    break;
                }
            }
        }
        return hasError;
    }

    public ObTableException getFirstException() {
        ObTableException exception = null;
        for (Object item : results) {
            if (item instanceof ObTableException) {
                exception = (ObTableException) item;
                hasError = true;
                break;
            }
        }
        return exception;
    }

    /*
     * get result
     */
    public OperationResult get(int pos) {
        if (pos >= results.size()) {
            throw new IllegalArgumentException("Invalid pos: " + pos
                                               + ", while size of results is: " + results.size());
        }
        return (MutationResult) results.get(pos);
    }

    /*
     * get size
     */
    public int size() {
        return results.size();
    }

    /*
     * get wrong count in result
     */
    public long getWrongCount() {
        long wrongCount = 0;
        for (Object item : results) {
            if (item instanceof ObTableException) {
                ++wrongCount;
                hasError = true;
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
                hasError = true;
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
