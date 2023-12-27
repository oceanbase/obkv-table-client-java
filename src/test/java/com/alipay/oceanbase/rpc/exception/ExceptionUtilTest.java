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

package com.alipay.oceanbase.rpc.exception;

import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

import static com.alipay.oceanbase.rpc.protocol.payload.ResultCodes.*;

public class ExceptionUtilTest {
    @Test
    public void testThrowException() {
        try {
            ExceptionUtil.throwObTableException("0.0.0.0", 0, 0L, 0L, -4001, "");
        } catch (ObTableException e) {
            Assert.assertTrue(e.getMessage().contains("OB_OBJ_TYPE_ERROR"));
        }

        try {
            ExceptionUtil.throwObTableException("0.0.0.0", 0, 0L, 0L, -9013, "");
        } catch (ObTableException e) {
            Assert.assertTrue(e instanceof ObTableException);
            Assert.assertTrue(e.getMessage().contains("OB_OSS_WRITE_ERROR"));
        }

        // throwObTableException
        try {
            ExceptionUtil.throwObTableException(-4001);
        } catch (ObTableException e) {
            Assert.assertTrue(e.getMessage().contains("OB_OBJ_TYPE_ERROR"));
        }

        try {
            ExceptionUtil.throwObTableException(0);
        } catch (ObTableException e) {
            Assert.fail("expect OB_SUCCESS");
        }

        // convertToObTableException
        try {
            ExceptionUtil.convertToObTableException("0.0.0.0", 0, 0L, 0L,
                OB_OSS_WRITE_ERROR.errorCode, "");
        } catch (ObTableException e) {
            Assert.assertTrue(e instanceof ObTableUnexpectedException);
            Assert.assertTrue(e.getMessage().contains("OB_OSS_WRITE_ERROR"));
        }

        ResultCodes[] resultCodes = { OB_INVALID_ARGUMENT, OB_DESERIALIZE_ERROR,
                OB_LOCATION_LEADER_NOT_EXIST, OB_RPC_CONNECT_ERROR, OB_PARTITION_NOT_EXIST,
                OB_SERVER_IS_STOPPING, OB_TENANT_NOT_IN_SERVER, OB_TRANS_RPC_TIMEOUT,
                OB_NO_READABLE_REPLICA, OB_TRANS_TIMEOUT, OB_ERR_PRIMARY_KEY_DUPLICATE,
                OB_ERR_UNKNOWN_TABLE, OB_ERR_COLUMN_NOT_FOUND, OB_BAD_NULL_ERROR,
                OB_PASSWORD_WRONG,

        };
        for (ResultCodes c : resultCodes) {
            try {
                ExceptionUtil.throwObTableException("0.0.0.0", 0, 0L, 0L, c.errorCode, "");
            } catch (ObTableException e) {
                Assert.assertFalse(e instanceof ObTableUnexpectedException);
                Assert.assertTrue(e instanceof ObTableException);
            }
        }

        // throwObTableTransportException
        int[] transportCodes = { TransportCodes.BOLT_TIMEOUT, TransportCodes.BOLT_SEND_FAILED,
                TransportCodes.BOLT_RESPONSE_NULL, TransportCodes.BOLT_CHECKSUM_ERR, };
        for (int c : transportCodes) {
            try {
                ExceptionUtil.throwObTableTransportException("hit transport error", c);
            } catch (ObTableException e) {
                Assert.assertTrue(e instanceof ObTableTransportException);
                Assert.assertTrue(e.getMessage().contains("hit transport error"));
            }
        }
        try {
            ExceptionUtil.throwObTableTransportException("hit transport error", -1000);
        } catch (ObTableException e) {
            Assert.assertTrue(e instanceof ObTableTransportException);
            Assert.assertTrue(e.getMessage().contains("unexpected"));
        }
        ExceptionUtil.throwObTableTransportException("no transport error", 0);
    }

    @Test
    public void testObTableException() {
        Class[] exceptions = { ObTableAuthException.class, ObTableCloseException.class,
                ObTableDuplicateKeyException.class, ObTableEntryRefreshException.class,
                ObTableException.class, ObTableGetException.class, ObTableLoginException.class,
                ObTableMasterChangeException.class, ObTableNoMasterException.class,
                ObTableNoReadableReplicaException.class, ObTableNotExistException.class,
                ObTablePartitionChangeException.class, ObTablePartitionInfoRefreshException.class,
                ObTablePartitionLocationRefreshException.class,
                ObTablePartitionNoMasterException.class, ObTablePartitionNotExistException.class,
                ObTableRetryExhaustedException.class, ObTableServerConnectException.class,
                ObTableServerDownException.class, ObTableServerStatusChangeException.class,
                ObTableServerTimeoutException.class, ObTableTimeoutExcetion.class,
                ObTableTransactionRpcTimeout.class, ObTableTransportException.class,
                ObTableUnexpectedException.class, ObTableUnitMigrateException.class,
                ObTableConnectionStatusException.class, ObTableConnectionUnWritableException.class,
                ObTableServerCacheExpiredException.class, ObTableRoutingWrongException.class,
                ObTableReplicaNotReadableException.class, };
        for (Class c : exceptions) {
            try {
                String msg = "ObException";
                int errorCode = 256;
                Exception ex = new Exception("ObTableException");
                Constructor constructor = c.getConstructor();
                Object o = constructor.newInstance();
                Assert.assertTrue(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(String.class);
                o = constructor.newInstance(msg);
                Assert.assertTrue(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(int.class);
                o = constructor.newInstance(errorCode);
                Assert.assertTrue(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(String.class, int.class);
                o = constructor.newInstance(msg, errorCode);
                Assert.assertTrue(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(Throwable.class);
                o = constructor.newInstance(ex);
                Assert.assertTrue(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(String.class, Throwable.class);
                o = constructor.newInstance(msg, ex);
                Assert.assertTrue(c.getName(), o instanceof ObTableException);

                ((ObTableException) o).isNeedRefreshTableEntry();

            } catch (Exception e) {
                Assert.fail("failed to construct ObTableException");
            }
        }

        Class[] runtimeExceptions = { FeatureNotSupportedException.class,
                GenerateColumnParseException.class, ObShardTableException.class,
                ObTablePartitionConsistentException.class, };

        for (Class c : runtimeExceptions) {
            try {
                String msg = "ObException";
                int errorCode = 256;
                Exception ex = new Exception("ObException");
                Constructor constructor = c.getConstructor();
                Object o = constructor.newInstance();
                Assert.assertFalse(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(String.class);
                o = constructor.newInstance(msg);
                Assert.assertFalse(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(int.class);
                o = constructor.newInstance(errorCode);
                Assert.assertFalse(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(String.class, int.class);
                o = constructor.newInstance(msg, errorCode);
                Assert.assertFalse(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(Throwable.class);
                o = constructor.newInstance(ex);
                Assert.assertFalse(c.getName(), o instanceof ObTableException);

                constructor = c.getConstructor(String.class, Throwable.class);
                o = constructor.newInstance(msg, ex);
                Assert.assertFalse(c.getName(), o instanceof ObTableException);
            } catch (Exception e) {
                Assert.fail("failed to construct ObException");
            }
        }
    }

    @Test
    public void testRefreshObTableException() {
        ObTableException e1 = new ObTableException("Not Refresh");
        Assert.assertFalse(e1.isNeedRefreshTableEntry());

        ObTableException e2 = new ObTableEntryRefreshException("Need Refresh");
        Assert.assertTrue(e2.isNeedRefreshTableEntry());

        ObTableException e3 = new ObTableException("Need Refresh", e2);
        Assert.assertTrue(e3.isNeedRefreshTableEntry());

        ObTableException e4 = new ObTableRetryExhaustedException("Caused Exception Need Refresh",
            e3);
        Assert.assertTrue(e4.isNeedRefreshTableEntry());

        ObTableException e5 = new ObTableRetryExhaustedException(
            "Caused Exception Don't Need Refresh", e1);
        Assert.assertFalse(e5.isNeedRefreshTableEntry());
    }
}
