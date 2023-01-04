/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2022 OceanBase
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

import com.alipay.oceanbase.rpc.util.ObTableHotkeyThrottleUtil;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;

public class ObTableHotkeyThrottleTest {
    public int threadNum = 64;
    public int repeatKeyNum = 4;

    @Test
    public void testThrottle() throws Exception {
        try {
            List<ObTableHotkeyThrottleUtil> allWorkers = new ArrayList<>();
            for (int i = 0; i < threadNum; ++i) {
                ObTableHotkeyThrottleUtil worker = new ObTableHotkeyThrottleUtil();
                if (i < threadNum/4) {
                    worker.init(ObTableHotkeyThrottleUtil.TestType.random, ObTableHotkeyThrottleUtil.OperationType.insertOrUpdate);
                } else if (i < threadNum/3) {
                    worker.init(ObTableHotkeyThrottleUtil.TestType.random, ObTableHotkeyThrottleUtil.OperationType.query);
                } else {
                    long rowKeyNum = i % repeatKeyNum;
                    String rowkeyString = "Test" + rowKeyNum;
                    worker.init(ObTableHotkeyThrottleUtil.TestType.specifiedKey, ObTableHotkeyThrottleUtil.OperationType.query,
                            colVal("c1", rowKeyNum), colVal("c2", rowkeyString));
                }
                allWorkers.add(worker);
                worker.start();
            }
            for (int i = 0; i < threadNum; ++i) {
                allWorkers.get(i).join();
                System.out.println("Thread " + i + "has finished");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
