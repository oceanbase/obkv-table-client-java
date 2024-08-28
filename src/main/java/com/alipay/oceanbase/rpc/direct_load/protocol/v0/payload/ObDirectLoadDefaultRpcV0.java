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

package com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadLogger;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadRpc;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadResult;
import com.alipay.oceanbase.rpc.util.ObBytesString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public abstract class ObDirectLoadDefaultRpcV0 implements ObDirectLoadRpc {

    protected final ObDirectLoadLogger       logger;

    protected final ObTableDirectLoadRequest request;
    protected ObTableDirectLoadResult        result;

    public ObDirectLoadDefaultRpcV0(ObDirectLoadTraceId traceId) {
        logger = ObDirectLoadLogger.getLogger(traceId);
        request = new ObTableDirectLoadRequest();
        request.setTraceId(traceId.getUniqueId(), traceId.getSequence());
    }

    @Override
    public ObPayload getRequest() {
        ObTableDirectLoadRequest.Header header = request.getHeader();
        header.setOperationType(getOpType());
        request.setArgContent(new ObBytesString(getArg().encode()));
        return request;
    }

    @Override
    public void setResult(ObPayload result) throws ObDirectLoadException {
        if (result instanceof ObTableDirectLoadResult) {
            this.result = (ObTableDirectLoadResult) result;
        } else {
            throw new ObDirectLoadUnexpectedException("unexpected result type, result:" + result);
        }

        ObTableDirectLoadResult.Header header = this.result.getHeader();
        ObBytesString resContent = this.result.getResContent();
        if (header.getOperationType() != getOpType()) {
            logger.warn("operation type in result header is error, type:"
                        + header.getOperationType());
            throw new ObDirectLoadUnexpectedException(
                "operation type in result header is error, type:" + header.getOperationType());
        }

        ObSimplePayload res = getRes();
        if (res == null) {
            if (resContent.length() != 0) {
                logger.warn("res content must be empty");
                throw new ObDirectLoadUnexpectedException("res content must be empty");
            }
        } else {
            if (resContent.length() == 0) {
                logger.warn("res content cannot be empty");
                throw new ObDirectLoadUnexpectedException("res content cannot be empty");
            }

            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(this.result.getResContent().length());
            try {
                buf.writeBytes(resContent.bytes);
                res.decode(buf);
            } catch (Exception e) {
                throw ObDirectLoadExceptionUtil.convertException(e);
            } finally {
                buf.release();
            }
        }
    }

    @Override
    public void setRpcTimeout(long timeoutMillis) {
        request.setTimeout(timeoutMillis * 1000);
    }
}
