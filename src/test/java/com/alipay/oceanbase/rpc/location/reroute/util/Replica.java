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

package com.alipay.oceanbase.rpc.location.reroute.util;

import com.alipay.oceanbase.rpc.location.model.ObServerRole;

public class Replica {
    long tableId;
    long partId;
    String ip;
    int port;
    ObServerRole role;

    public Replica(long tableId, long partId, String ip, int port, ObServerRole role ) {
        this.tableId = tableId;
        this.partId = partId;
        this.ip = ip;
        this.port = port;
        this.role = role;
    }
    public ObServerRole getRole(){
        return role;
    }
}
