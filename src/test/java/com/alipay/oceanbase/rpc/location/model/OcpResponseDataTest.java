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

package com.alipay.oceanbase.rpc.location.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OcpResponseDataTest {
    @Test
    public void testOcpResponseDataEntity() {
        OcpResponseData ocpResponseData = new OcpResponseData();
        Assert.assertEquals(-1, ocpResponseData.getObRegionId());

        ocpResponseData.setObRegion("test");
        ocpResponseData.setObRegionId(1);
        Assert.assertEquals("test", ocpResponseData.getObRegion());
        Assert.assertEquals(1, ocpResponseData.getObRegionId());

        List<OcpResponseDataRs> rsList = new ArrayList<OcpResponseDataRs>();

        OcpResponseDataRs ocpResponseDataRs1 = new OcpResponseDataRs();

        ocpResponseDataRs1.setAddress("1.1.1.1");
        ocpResponseDataRs1.setRole("LEADER");
        ocpResponseDataRs1.setSql_port(60);

        rsList.add(ocpResponseDataRs1);

        ocpResponseData.setRsList(rsList);

        List<OcpResponseDataIDC> idcList = new ArrayList<OcpResponseDataIDC>();
        OcpResponseDataIDC idc1 = new OcpResponseDataIDC();
        idc1.setIdc("em14");
        idc1.setRegion("HANGZHOU");
        idcList.add(idc1);
        ocpResponseData.setIDCList(idcList);

        Assert.assertEquals(rsList, ocpResponseData.getRsList());

        Assert.assertEquals(ocpResponseDataRs1, ocpResponseData.getRsList().get(0));
        Assert.assertEquals(ocpResponseDataRs1.getAddress(), ocpResponseData.getRsList().get(0)
            .getAddress());
        Assert.assertEquals(ocpResponseDataRs1.getSql_port(), ocpResponseData.getRsList().get(0)
            .getSql_port());
        Assert.assertEquals(ocpResponseDataRs1.getRole(), ocpResponseData.getRsList().get(0)
            .getRole());
        Assert.assertEquals(idc1.getIdc(), ocpResponseData.getIDCList().get(0).getIdc());
        Assert.assertEquals(idc1.getRegion(), ocpResponseData.getIDCList().get(0).getRegion());

        Assert.assertTrue(ocpResponseData.validate());
        Assert.assertTrue(ocpResponseData.toString().contains(idc1.getRegion()));

    }
}
