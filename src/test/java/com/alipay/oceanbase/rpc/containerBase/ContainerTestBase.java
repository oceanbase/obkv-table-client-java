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

/*
 * Copyright (c) 2023 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alipay.oceanbase.rpc.containerBase;

import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.Arrays;

public class ContainerTestBase {

    private static final Logger             logger        = LoggerFactory
                                                              .getLogger(ContainerTestBase.class);

    protected static final Network          NETWORK       = Network.newNetwork();
    protected static final String           CLUSTER_NAME  = "obkv-table-client-java";
    protected static final String           SYS_PASSWORD  = "OB_ROOT_PASSWORD";
    protected static final String           TEST_USERNAME = "root@test#" + CLUSTER_NAME;

    @ClassRule
    public static final GenericContainer<?> OB_SERVER     = createOceanBaseCEContainer();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static GenericContainer createOceanBaseCEContainer() {
        GenericContainer container = new GenericContainer("oceanbase/oceanbase-ce:4.2.1_bp2")
            .withNetwork(NETWORK)
            .withExposedPorts(2881, 2882, 8080)
            .withEnv("MODE", "slim")
            .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
            .withEnv("OB_ROOT_PASSWORD", SYS_PASSWORD)
            .withCopyFileToContainer(MountableFile.forClasspathResource("ci.sql"),
                "/root/boot/init.d/init.sql")
            .waitingFor(
                Wait.forLogMessage(".*boot success!.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(5)))
            .withLogConsumer(new Slf4jLogConsumer(logger));
        container.setPortBindings(Arrays.asList("2881:2881", "2882:2882", "8080:8080"));
        return container;
    }

    @BeforeClass
    public static void before() {
        if (!ObTableClientTestUtil.FULL_USER_NAME.equals("full-user-name")) {
            return;
        }

        // Set config
        ObTableClientTestUtil.PARAM_URL = "http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster="
                                          + CLUSTER_NAME + "&database=test";
        ObTableClientTestUtil.FULL_USER_NAME = TEST_USERNAME;
        ObTableClientTestUtil.PASSWORD = "";
        ObTableClientTestUtil.PROXY_SYS_USER_NAME = "root";
        ObTableClientTestUtil.PROXY_SYS_USER_PASSWORD = SYS_PASSWORD;
    }
}
