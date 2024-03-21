package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.assertEquals;

public class OpsController {
    private static final AtomicLong operationCount = new AtomicLong(0);
    private static ObTableClient        client;
    private static String tableName = "test_group_commit";
    private void read() {
        try {
            Map<String, Object> res = client.get(tableName, new Object[]{1L}, new String[]{"c1"});
            assertEquals(1L, res.get("c1"));
            operationCount.incrementAndGet();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    public static void put(long rowKeyVal, long PropVal) {
        try {
            client.put(tableName).setRowKey(colVal("c1", rowKeyVal))
                    .addMutateColVal(colVal("c2", PropVal))
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    private void executeWithTargetOps() throws InterruptedException {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(100);
        executorService.scheduleAtFixedRate(this::read, 0, 10, TimeUnit.MICROSECONDS);
        executorService.scheduleAtFixedRate(() -> {
            long ops = operationCount.getAndSet(0); // 获取当前操作计数并重置
            System.out.println("Current OPS: " + ops );
        }, 1, 1, TimeUnit.SECONDS);
        Thread.sleep(10 * 1000);
        executorService.shutdown();
    }

    public void enableGroupCommitWork() throws Exception {
        client = ObTableClientTestUtil.newTestClient();
        client.addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), "10000");
        client.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "10000");
        client.init();
        client.addRowKeyElement(tableName, new String[] { "c1" });
        put(1L, 1L);
        executeWithTargetOps();
    }
}
