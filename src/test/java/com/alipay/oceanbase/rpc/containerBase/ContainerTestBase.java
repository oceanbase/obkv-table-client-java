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
import org.junit.After;
import org.junit.Before;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

public class ContainerTestBase {

    protected OceanBaseContainer obServer;
    protected String             imageTag = "4.2.1_bp2";

    @Before
    public void before() {
        if (!ObTableClientTestUtil.FULL_USER_NAME.equals("full-user-name")) {
            return;
        }

        obServer = new OceanBaseContainer(OceanBaseContainer.DOCKER_IMAGE_NAME + ":" + imageTag)
            .withNetworkMode("host")
            .withSysPassword("OB_ROOT_PASSWORD")
            .withCopyFileToContainer(MountableFile.forClasspathResource("ci.sql"),
                "/root/boot/init.d/init.sql");

        Startables.deepStart(obServer).join();

        System.out.println("observer start success");

        // Set config
        ObTableClientTestUtil.PARAM_URL = "http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster&database=test";
        ObTableClientTestUtil.FULL_USER_NAME = "root@test#obcluster";
        ObTableClientTestUtil.PASSWORD = "";
        ObTableClientTestUtil.PROXY_SYS_USER_NAME = "root";
        ObTableClientTestUtil.PROXY_SYS_USER_PASSWORD = "OB_ROOT_PASSWORD";
    }

    @After
    public void after() {
        if (obServer != null) {
            obServer.close();
        }
    }
}
