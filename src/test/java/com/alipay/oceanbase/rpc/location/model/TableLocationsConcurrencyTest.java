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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableGetException;
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import com.alipay.oceanbase.rpc.exception.ObTableTimeoutExcetion;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;

/** Deterministic races through the real refresh lock and shared location publication. */
public class TableLocationsConcurrencyTest {
    private static TableEntry entry(long id) {
        TableEntry result = new TableEntry();
        result.setTableId(id);
        result.setRefreshMetaTimeMills(System.currentTimeMillis());
        return result;
    }

    @Test
    public void forceBypassesThrottleWithoutAnEmptyWindowAndFailureKeepsOldEntry() throws Exception {
        ObTableClient client = new ObTableClient() {
            @Override public com.alipay.oceanbase.rpc.table.ObTableServerCapacity getServerCapacity() {
                return new com.alipay.oceanbase.rpc.table.ObTableServerCapacity();
            }
        };
        TableLocations locations = new TableLocations(client);
        TableEntry old = entry(11);
        locations.getLocations().put("t", old);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ObTableNotExistException expected = new ObTableNotExistException("metadata load failed");
        ServerRoster roster = new ServerRoster() {
            @Override public ObServerAddr getServer(long timeout) {
                loading.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) { throw new AssertionError(e); }
                throw expected;
            }
        };
        // Normal refresh is throttled; the forced overload must still enter the loader.
        assertSame(old, locations.refreshMeta("t", roster, new ObUserAuth()));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TableEntry> refresh = executor.submit(() -> locations.refreshMeta(
                "t", roster, new ObUserAuth(), old));
            assertTrue(loading.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 1000; i++) { assertSame(old, locations.getTableEntry("t")); }
            release.countDown();
            try {
                refresh.get(5, TimeUnit.SECONDS);
                fail("load failure must propagate");
            } catch (ExecutionException e) { assertSame(expected, e.getCause()); }
            assertSame(old, locations.getTableEntry("t"));
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void waitingForceReusesIdentityPublishedByAnotherThread() throws Exception {
        CountDownLatch waiting = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger lockLookups = new java.util.concurrent.atomic.AtomicInteger();
        TableLocations locations = new TableLocations(new ObTableClient()) {
            @Override public Lock getMetaRefreshLock(String name) {
                if (lockLookups.incrementAndGet() > 1) { waiting.countDown(); }
                return super.getMetaRefreshLock(name);
            }
        };
        TableEntry old = entry(11), current = entry(22);
        locations.getLocations().put("t", old);
        Lock lock = locations.getMetaRefreshLock("t");
        lock.lock();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TableEntry> refresh = executor.submit(() -> locations.refreshMeta(
                "t", new ServerRoster() {
                    @Override public ObServerAddr getServer(long timeout) {
                        throw new AssertionError("new identity must be reused without loading");
                    }
                }, new ObUserAuth(), old));
            assertTrue(waiting.await(5, TimeUnit.SECONDS));
            locations.getLocations().put("t", current);
            lock.unlock();
            assertSame(current, refresh.get(5, TimeUnit.SECONDS));
            assertSame(current, locations.getTableEntry("t"));
        } finally {
            if (((java.util.concurrent.locks.ReentrantLock) lock).isHeldByCurrentThread()) { lock.unlock(); }
            executor.shutdownNow();
        }
    }

    @Test
    public void sameIdReplacementBeforeLockStillForcesRefresh() throws Exception {
        TableEntry old = entry(500208), current = entry(500208);
        assertNotSame(old.getTableId(), current.getTableId());
        assertForceLoads(old, current, false);
    }

    @Test
    public void sameIdReplacementWhileWaitingStillForcesRefresh() throws Exception {
        assertForceLoads(entry(500208), entry(500208), true);
    }

    @Test
    public void missingExpectedEntryStillForcesRefresh() throws Exception {
        assertForceLoads(null, entry(500208), false);
    }

    @Test
    public void missingCachedEntryStillForcesRefresh() throws Exception {
        assertForceLoads(entry(500208), null, false);
    }

    @Test
    public void changedIdBeforeLockIsReusedWithoutLoading() throws Exception {
        TableLocations locations = new TableLocations(new ObTableClient());
        TableEntry old = entry(500208), current = entry(500212);
        locations.getLocations().put("t", current);
        assertSame(current, locations.refreshMeta("t", new ServerRoster() {
            @Override public ObServerAddr getServer(long timeout) {
                throw new AssertionError("changed identity must be reused");
            }
        }, new ObUserAuth(), old));
    }

    private void assertForceLoads(TableEntry expectedEntry, TableEntry currentEntry,
                                   boolean replaceWhileWaiting) throws Exception {
        ObTableClient client = new ObTableClient() {
            @Override public com.alipay.oceanbase.rpc.table.ObTableServerCapacity getServerCapacity() {
                return new com.alipay.oceanbase.rpc.table.ObTableServerCapacity();
            }
        };
        CountDownLatch waiting = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger lockLookups = new java.util.concurrent.atomic.AtomicInteger();
        TableLocations locations = new TableLocations(client) {
            @Override public Lock getMetaRefreshLock(String name) {
                if (lockLookups.incrementAndGet() > 1) { waiting.countDown(); }
                return super.getMetaRefreshLock(name);
            }
        };
        TableEntry initial = replaceWhileWaiting ? expectedEntry : currentEntry;
        if (initial != null) { locations.getLocations().put("t", initial); }
        ObTableNotExistException loadFailure = new ObTableNotExistException("remote load reached");
        java.util.concurrent.atomic.AtomicInteger loads = new java.util.concurrent.atomic.AtomicInteger();
        ServerRoster roster = new ServerRoster() {
            @Override public ObServerAddr getServer(long timeout) {
                assertSame(currentEntry, locations.getTableEntry("t"));
                loads.incrementAndGet();
                throw loadFailure;
            }
        };
        Lock lock = locations.getMetaRefreshLock("t");
        if (replaceWhileWaiting) { lock.lock(); }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TableEntry> refresh = executor.submit(() -> locations.refreshMeta(
                "t", roster, new ObUserAuth(), expectedEntry));
            if (replaceWhileWaiting) {
                assertTrue(waiting.await(5, TimeUnit.SECONDS));
                locations.getLocations().put("t", currentEntry);
                lock.unlock();
            }
            try {
                refresh.get(5, TimeUnit.SECONDS);
                fail("must attempt a full metadata load instead of returning cached identity");
            } catch (ExecutionException e) { assertSame(loadFailure, e.getCause()); }
            assertEquals(1, loads.get());
            assertSame(currentEntry, locations.getTableEntry("t"));
        } finally {
            if (((java.util.concurrent.locks.ReentrantLock) lock).isHeldByCurrentThread()) { lock.unlock(); }
            executor.shutdownNow();
        }
    }

    @Test
    public void forcedRefreshStillHonorsMetadataLockTimeoutBudget() throws Exception {
        ObTableClient client = new ObTableClient();
        client.setRuntimeMaxWait(50);
        client.setTableEntryRefreshLockTimeout(10);
        TableLocations locations = new TableLocations(client);
        TableEntry old = entry(11);
        locations.getLocations().put("t", old);
        Lock lock = locations.getMetaRefreshLock("t");
        lock.lock();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TableEntry> refresh = executor.submit(() -> locations.refreshMeta(
                "t", new ServerRoster(), new ObUserAuth(), old));
            try {
                refresh.get(5, TimeUnit.SECONDS);
                fail("forced refresh must time out");
            } catch (ExecutionException e) { assertTrue(e.getCause() instanceof ObTableTimeoutExcetion); }
            assertSame(old, locations.getTableEntry("t"));
        } finally {
            lock.unlock();
            executor.shutdownNow();
        }
    }

    @Test
    public void lateLocationWriterReturnsNewIdentityWithoutOverwritingIt() throws Exception {
        TableLocations locations = new TableLocations(new ObTableClient());
        TableEntry old = entry(11), staleResult = entry(11), current = entry(22);
        locations.getLocations().put("t", old);
        CountDownLatch captured = new CountDownLatch(1), publish = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TableEntry> writer = executor.submit(() -> {
                TableEntry snapshot = locations.getTableEntry("t");
                captured.countDown();
                assertTrue(publish.await(5, TimeUnit.SECONDS));
                return locations.cacheRefreshedLocation("t", snapshot, staleResult);
            });
            assertTrue(captured.await(5, TimeUnit.SECONDS));
            locations.getLocations().put("t", current);
            publish.countDown();
            assertSame(current, writer.get(5, TimeUnit.SECONDS));
            assertSame(current, locations.getTableEntry("t"));
        } finally {
            publish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void removedEntryIsNotResurrectedByLocationRefresh() {
        TableLocations locations = new TableLocations(new ObTableClient());
        TableEntry old = entry(11);
        locations.getLocations().put("t", old);
        locations.eraseTableEntry("t");
        try {
            locations.cacheRefreshedLocation("t", old, old);
            fail("removed identity must require a metadata fetch");
        } catch (ObTableGetException e) { assertTrue(e.getMessage().contains("Need to fetch meta")); }
        assertNull(locations.getTableEntry("t"));
    }

    @Test
    public void unchangedIdentityAcceptsRefreshedLocation() {
        TableLocations locations = new TableLocations(new ObTableClient());
        TableEntry old = entry(11), refreshed = entry(11);
        locations.getLocations().put("t", old);
        assertSame(refreshed, locations.cacheRefreshedLocation("t", old, refreshed));
        assertSame(refreshed, locations.getTableEntry("t"));
    }
}
