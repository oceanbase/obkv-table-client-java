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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacket;
import com.alipay.oceanbase.rpc.bolt.transport.ObTableConnection;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.table.ObTable;

public class TraceUtil {

    /*
     * Trace format from connection and payload.
     */
    public static String formatTraceMessage(final ObTableConnection conn, final ObPayload payload) {
        return formatTraceMessage(conn, payload, "");
    }

    /*
     * Trace format from connection and payload.
     */
    public static String formatTraceMessage(final ObTableConnection conn, final ObPayload payload,
                                            final String msg) {
        return String.format("[Y%X-%016X] server [%s:%d] %s", payload.getSequence(),
            payload.getUniqueId(), conn.getObTable().getIp(), conn.getObTable().getPort(), msg);
    }

    /*
     * Trace format from connection and packet, used when the packet isn't decoded yet.
     */
    public static String formatTraceMessage(final ObTableConnection conn, final ObTablePacket packet) {
        return formatTraceMessage(conn, packet, "");
    }

    /*
     * Trace format from connection and packet, used when the packet isn't decoded yet.
     */
    public static String formatTraceMessage(final ObTableConnection conn,
                                            final ObTablePacket packet, final String msg) {
        return String.format("[Y%X-%016X] server [%s:%d] %s", packet.getHeader().getTraceId0(),
            packet.getHeader().getTraceId1(), conn.getObTable().getIp(), conn.getObTable()
                .getPort(), msg);
    }

    /*
     * Format IP:port from ObTable.
     */
    public static String formatIpPort(final ObTable obTable) {
        return String.format("server [%s:%d]", obTable.getIp(), obTable.getPort());
    }
}
