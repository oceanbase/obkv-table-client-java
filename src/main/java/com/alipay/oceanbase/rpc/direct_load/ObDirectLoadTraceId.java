/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.direct_load;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;

public class ObDirectLoadTraceId {

    private final long uniqueId;
    private final long sequence;

    public ObDirectLoadTraceId(long uniqueId, long sequence) {
        this.uniqueId = uniqueId;
        this.sequence = sequence;
    }

    public String toString() {
        return String.format("Y%X-%016X", uniqueId, sequence);
    }

    public long getUniqueId() {
        return uniqueId;
    }

    public long getSequence() {
        return sequence;
    }

    public static final ObDirectLoadTraceId DEFAULT_TRACE_ID;
    public static TraceIdGenerator          traceIdGenerator;

    static {
        DEFAULT_TRACE_ID = new ObDirectLoadTraceId(0, 0);
        traceIdGenerator = new TraceIdGenerator();
    }

    public static ObDirectLoadTraceId generateTraceId() {
        return traceIdGenerator.generate();
    }

    public static class TraceIdGenerator {

        private final ObDirectLoadLogger logger = ObDirectLoadLogger.getLogger();

        private final long               uniqueId;
        private AtomicLong               sequence;

        public TraceIdGenerator() {
            long ip = 0;
            try {
                ip = ipToLong(InetAddress.getLocalHost().getHostAddress());
            } catch (Exception e) {
                logger.warn("get local host address failed", e);
            }
            long port = (long) (Math.random() % 65536) << 32;
            long isUserRequest = (1l << (32 + 16));
            long reserved = 0;
            uniqueId = ip | port | isUserRequest | reserved;
            sequence = new AtomicLong(0);
        }

        private static long ipToLong(String strIp) {
            String[] ip = strIp.split("\\.");
            return (Long.parseLong(ip[0]) << 24) + (Long.parseLong(ip[1]) << 16)
                   + (Long.parseLong(ip[2]) << 8) + (Long.parseLong(ip[3]));
        }

        public ObDirectLoadTraceId generate() {
            long newSequence = System.currentTimeMillis() * 1000 + sequence.incrementAndGet()
                               % 1000;
            return new ObDirectLoadTraceId(uniqueId, newSequence);
        }

    };

}
