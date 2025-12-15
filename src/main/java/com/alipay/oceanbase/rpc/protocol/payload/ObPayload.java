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

package com.alipay.oceanbase.rpc.protocol.payload;

import io.netty.buffer.ByteBuf;

public interface ObPayload extends ObUnisVersion {

    /*
     * @return channel id
     */
    int getChannelId();

    /*
     * @return tenant id
     */
    long getTenantId();

    /*
     * @return protocol code
     */
    int getPcode();

    /*
     * @return timeout
     */
    long getTimeout();

    boolean isRoutingWrong();

    void setIsRoutingWrong(boolean isRoutingWrong);

    boolean isNeedRefreshMeta();

    void setIsNeedRefreshMeta(boolean isNeedRefreshMeta);

    /*
     * set sequence
     */
    void setSequence(long sequence);

    /*
     * @return sequence
     */
    long getSequence();

    /*
     * set unique id
     */
    void setUniqueId(long uniqueId);

    /*
     * @return unique id
     */
    long getUniqueId();

    /*
     * @return group id
     */
    int getGroupId();

    /*
     * @return encoded payload content
     */
    byte[] encode();

    /*
     * Decode payload from byte buffer
     *
     * @param buf buf from net framework
     * @return this payload
     */
    Object decode(ByteBuf buf);

    /*
     * @return payload size, include header bytes
     */
    long getPayloadSize();

    /*
     * @return payload content size, without header bytes
     */
    long getPayloadContentSize();

    void resetPayloadContentSize();
}
