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

package com.alipay.oceanbase.rpc.stream;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.QueryStreamResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryResultSet {
    private final QueryStreamResult queryStreamResult;
    private boolean                 hasMore;
    private long                    sessionId;

    /*
     * Query result set.
     */
    public QueryResultSet(QueryStreamResult queryStreamResult) {
        this.queryStreamResult = queryStreamResult;
    }

    public QueryStreamResult getQueryStreamResult() {
        return queryStreamResult;
    }

    /*
     * Next.
     */
    public boolean next() throws Exception {
        return queryStreamResult.next();
    }

    public Map<String, Object> getRow() {
        List<String> propertiesNames = queryStreamResult.getCacheProperties();
        List<ObObj> row = queryStreamResult.getRow();
        // TODO check row.size == propertiesNames.size()
        Map<String, Object> rowValue = new HashMap<String, Object>();
        for (int i = 0; i < row.size(); i++) {
            rowValue.put(propertiesNames.get(i), row.get(i).getValue());
        }
        return rowValue;
    }

    /*
     * Cache size.
     */
    public int cacheSize() {
        return queryStreamResult.getCacheRows().size();
    }

    /*
     * Close.
     */
    public void close() throws Exception {
        queryStreamResult.close();
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }
}
