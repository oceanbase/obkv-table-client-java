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
    public int  threadNum    = 32;
    public int  repeatKeyNum = 4;
    public long startTime;

    @Test
    public void testSimpleThrottle() throws Exception {
        try {
            List<ObTableHotkeyThrottleUtil> allWorkers = new ArrayList<>();
            startTime = System.currentTimeMillis();
            for (int i = 0; i < threadNum; ++i) {
                ObTableHotkeyThrottleUtil worker = new ObTableHotkeyThrottleUtil();
                if (i < threadNum/4) {
                    worker.init(threadNum, i, startTime, "test_throttle", new String[] { "c1", "c2" },
                            ObTableHotkeyThrottleUtil.TestType.random, ObTableHotkeyThrottleUtil.OperationType.insertOrUpdate, 800, null, 0);
                } else if (i < threadNum/3) {
                    worker.init(threadNum, i, startTime, "test_throttle", new String[] { "c1", "c2" }, ObTableHotkeyThrottleUtil.TestType.random, ObTableHotkeyThrottleUtil.OperationType.query, 800, null, 0);
                } else {
                    long rowKeyNum = i % repeatKeyNum;
                    String rowkeyString = "Test" + rowKeyNum;
                    worker.init(threadNum, i, startTime, "test_throttle", new String[] { "c1", "c2" },
                            ObTableHotkeyThrottleUtil.TestType.specifiedKey, ObTableHotkeyThrottleUtil.OperationType.query, 800,
                            null, 0, colVal("c1", rowKeyNum), colVal("c2", rowkeyString));
                }
                allWorkers.add(worker);
                worker.start();
            }

            // for each 2s, get unitOperationTimes and unitOperationTimes from ObTableHotkeyThrottleUtil and print
            for (int i = 0; i < 10; ++i) {
                Thread.sleep(2000);
                List<Integer> blockList = allWorkers.get(0).getUnitBlockTimes();
                List<Integer> operationList = allWorkers.get(0).getUnitOperationTimes();
                System.out.print("Block times: ");
                for (int j = 0; j < threadNum; ++j) {
                    System.out.print(blockList.get(j) + " ");
                }
                System.out.print("\nOperation times: ");
                for (int j = 0; j < threadNum; ++j) {
                    System.out.print(operationList.get(j) + " ");
                }
                System.out.print("\n");
            }

            for (int i = 0; i < threadNum; ++i) {
                allWorkers.get(i).join();
                System.out.println("Thread " + i + "has finished");
            }

            // get totalBlockTimes and totalOperationTimes from ObTableHotkeyThrottleUtil and print
            List<Integer> blockList = allWorkers.get(0).getTotalBlockTimes();
            List<Integer> operationList = allWorkers.get(0).getTotalOperationTimes();
            System.out.print("Total Block times: ");
            for (int j = 0; j < threadNum; ++j) {
                System.out.print(blockList.get(j) + " ");
            }
            System.out.print("\nTotal Operation times: ");
            for (int j = 0; j < threadNum; ++j) {
                System.out.print(operationList.get(j) + " ");
            }
            System.out.print("\n");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
