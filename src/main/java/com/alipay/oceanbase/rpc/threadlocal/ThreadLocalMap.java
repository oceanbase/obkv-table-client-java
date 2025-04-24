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

package com.alipay.oceanbase.rpc.threadlocal;

import com.alipay.oceanbase.rpc.location.model.ObReadConsistency;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ThreadLocalMap {

    private static final Logger                           logger               = TableClientLoggerFactory
                                                                                   .getLogger(ThreadLocalMap.class);

    private final static ThreadLocal<Map<Object, Object>> THREAD_LOCAL_CONTEXT = new MapThreadLocal();

    public static final String                            PROCESS_PRIORITY     = "PROCESS_PRIORITY";
    public static final String                            READ_CONSISTENCY     = "READ_CONSISTENCY";

    /*
     * Set process high priority.
     */
    public static void setProcessHighPriority() {
        getContextMap().put(PROCESS_PRIORITY, (short) 4);
    }

    /*
     * Set process normal priority.
     */
    public static void setProcessNormalPriority() {
        getContextMap().put(PROCESS_PRIORITY, (short) 5);
    }

    /*
     * Set process low priority.
     */
    public static void setProcessLowPriority() {
        getContextMap().put(PROCESS_PRIORITY, (short) 6);
    }

    /*
     * Get process priority.
     */
    public static short getProcessPriority() {

        if (getContextMap() == null || getContextMap().get(PROCESS_PRIORITY) == null) {
            return (short) 5;
        }

        return (Short) getContextMap().get(PROCESS_PRIORITY);
    }

    /*
     * Set read consistency.
     */
    public static void setReadConsistency(ObReadConsistency readConsistency) {
        getContextMap().put(READ_CONSISTENCY, readConsistency);
    }

    /*
     * Get read consistency.
     */
    public static ObReadConsistency getReadConsistency() {
        return (ObReadConsistency) getContextMap().get(READ_CONSISTENCY);
    }

    /*
     * Clear read consistency.
     */
    public static void clearReadConsistency() {
        if (getContextMap().containsKey(READ_CONSISTENCY)) {
            getContextMap().remove(READ_CONSISTENCY);
        }
    }

    public static Map<Object, Object> getContextMap() {
        return THREAD_LOCAL_CONTEXT.get();
    }

    /*
     * Set context map.
     */
    public static void setContextMap(Map<Object, Object> context) {
        THREAD_LOCAL_CONTEXT.set(context);
    }

    /*
     * Transmit context map.
     */
    public static void transmitContextMap(Map<Object, Object> context) {
        for (Map.Entry<Object, Object> entry : context.entrySet()) {
            getContextMap().put(entry.getKey(), entry.getValue());
        }
    }

    private static class MapThreadLocal extends ThreadLocal<Map<Object, Object>> {
        protected Map<Object, Object> initialValue() {
            return new ConcurrentHashMap<Object, Object>() {

                private static final long serialVersionUID = 3637958959138295593L;

                /*
                 * put
                 */
                public Object put(Object key, Object value) {
                    if (logger.isDebugEnabled()) {
                        if (containsKey(key)) {
                            logger.debug("Overwritten attribute to thread context: " + key + " = "
                                         + value);
                        } else {
                            logger.debug("Added attribute to thread context: " + key + " = "
                                         + value);
                        }
                    }

                    return super.put(key, value);
                }
            };
        }
    }

    /*
     * Reset.
     */
    public static void reset() {
        getContextMap().clear();
    }
}
