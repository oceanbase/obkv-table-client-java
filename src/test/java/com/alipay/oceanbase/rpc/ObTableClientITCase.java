/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.containerBase.ContainerTestBase;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObTableClientITCase extends ContainerTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ObTableClientITCase.class);

    public Table                client;

    @Test
    public void testAll() throws Exception {
        if (!ObTableClientTestUtil.FULL_USER_NAME.equals(TEST_USERNAME)) {
            return;
        }
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();

        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.RPC_LOGIN_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), "1");
        obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "3000");
        obTableClient.addProperty(Property.RUNTIME_BATCH_MAX_WAIT.getKey(), "3000");
        obTableClient.addProperty(Property.RUNTIME_BATCH_EXECUTOR.getKey(), "32");
        obTableClient.addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), "3000");
        obTableClient.addProperty(Property.SERVER_ENABLE_REROUTING.getKey(), "False");
        obTableClient.init();

        logger.info("obTableClient init success");

        // ObTableClientTest
        ObTableClientTest obTableClientTest = new ObTableClientTest();
        obTableClientTest.setClient(obTableClient);
        obTableClientTest.test_batch();
        obTableClientTest.testBatchMutation();
        obTableClientTest.testMultiThreadBatchOperation();
        obTableClientTest.testCompareWithNull();
        obTableClientTest.testQueryFilterLimit();
        // Todo: add more test
        logger.info("ObTableClientTest success");

        logger.info("testAll success");
    }
}
