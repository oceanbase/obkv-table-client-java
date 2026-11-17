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

package com.alipay.oceanbase.rpc.location;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableEntryRefreshException;
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import com.alipay.oceanbase.rpc.location.model.*;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObIndexType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/** Exercises the actual JDBC empty-result wrapping, index cache and public routing entry. */
public class IndexMetadataRefreshTest {
    private final MetadataDriver driver = new MetadataDriver();
    private List<Driver> originalDrivers;

    @Before
    public void installDriver() throws Exception {
        Class.forName(LocationUtil.class.getName());
        originalDrivers = Collections.list(DriverManager.getDrivers());
        for (Driver original : originalDrivers) {
            DriverManager.deregisterDriver(original);
        }
        DriverManager.registerDriver(driver);
    }

    @After
    public void restoreDrivers() throws Exception {
        DriverManager.deregisterDriver(driver);
        for (Driver original : originalDrivers) {
            DriverManager.registerDriver(original);
        }
    }

    @Test
    public void emptyJdbcResultRetainsMissingIndexCodeAndClosesResources() throws Exception {
        Fixture f = new Fixture();
        try {
            LocationUtil.getIndexInfoFromRemote(f.address, f.auth, 100, 100, "__idx_11_i");
            fail("missing index must fail");
        } catch (ObTableEntryRefreshException e) {
            assertEquals(ResultCodes.OB_WRONG_NAME_FOR_INDEX.errorCode, e.getErrorCode());
            assertEquals(e.getErrorCode(), ((ObTableEntryRefreshException) e.getCause()).getErrorCode());
            assertFalse(e.isConnectInactive());
        }
        assertTrue(driver.resultClosed);
        assertTrue(driver.statementClosed);
    }

    @Test
    public void replacementRecoversGlobalAndLocalIndexes() throws Exception {
        for (ObIndexType type : Arrays.asList(ObIndexType.IndexTypeNormalGlobal,
            ObIndexType.IndexTypeNormalLocal)) {
            Fixture f = new Fixture();
            driver.queries.clear();
            driver.indexes.put("__idx_22_i", type);
            String result = f.route.getIndexTableName("t", "i", Collections.singletonList("c"), false);
            assertEquals(type.isGlobalIndex() ? "__idx_22_i" : "t", result);
            assertEquals(Arrays.asList("__idx_11_i", "__idx_22_i"), driver.queries);
            assertEquals(1, f.locations.refreshes);
            assertSame(f.current, f.route.getTableEntry("t"));
            if (type.isGlobalIndex()) {
                assertEquals(Integer.valueOf(0), f.client.getRowKeyElement(result).get("c"));
            }
        }
    }

    @Test
    public void retryForcesRemoteLookupEvenWhenNewIndexWasCached() throws Exception {
        Fixture f = new Fixture();
        driver.indexes.put("__idx_22_i", ObIndexType.IndexTypeNormalGlobal);
        f.locations.getLocations().put("t", f.current);
        f.route.getIndexTableName("t", "i", Collections.singletonList("c"), false);
        f.locations.getLocations().put("t", f.old);
        driver.queries.clear();
        assertEquals("__idx_22_i", f.route.getIndexTableName("t", "i", Collections.singletonList("c"), false));
        assertEquals(Arrays.asList("__idx_11_i", "__idx_22_i"), driver.queries);
    }

    @Test
    public void sameIdAndMissingIndexStopsAfterOneRetry() throws Exception {
        Fixture f = new Fixture();
        f.current.setTableId(11L);
        assertMissing(f);
        assertEquals(Arrays.asList("__idx_11_i", "__idx_11_i"), driver.queries);
        assertEquals(1, f.locations.refreshes);
    }

    @Test
    public void normalCacheAndForceAndPrimarySemanticsArePreserved() throws Exception {
        Fixture f = new Fixture();
        driver.indexes.put("__idx_11_i", ObIndexType.IndexTypeNormalLocal);
        for (String index : Arrays.asList(null, "", "PRIMARY", "primary")) {
            assertEquals("t", f.route.getIndexTableName("t", index, Collections.emptyList(), false));
        }
        assertTrue(driver.queries.isEmpty());
        f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
        f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
        assertEquals(1, driver.queries.size());
        f.route.getIndexTableName("t", "i", Collections.emptyList(), true);
        assertEquals(2, driver.queries.size());
        assertEquals(0, f.locations.refreshes);
        assertSame(f.old, f.locations.getTableEntry("t"));
    }

    @Test
    public void jdbcFailuresDoNotInvalidateDataTable() throws Exception {
        for (SQLException failure : Arrays.asList(new SQLException("permission denied"),
            new SQLTimeoutException("query timeout"))) {
            Fixture f = new Fixture();
            driver.queryFailure = failure;
            try {
                f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
                fail("JDBC failure must propagate");
            } catch (ObTableEntryRefreshException e) {
                assertEquals(-1, e.getErrorCode());
                assertSame(failure, e.getCause());
            }
            assertSame(f.old, f.locations.getTableEntry("t"));
            assertEquals(0, f.locations.refreshes);
        }
    }

    @Test
    public void connectionFailureIsNotDetectedByMessageText() throws Exception {
        Fixture f = new Fixture();
        driver.connectFailure = new SQLException("[-5224] connection failed");
        try {
            f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
            fail("connection failure must propagate");
        } catch (ObTableEntryRefreshException e) {
            assertEquals(-1, e.getErrorCode());
            assertTrue(e.isConnectInactive());
        }
        assertSame(f.old, f.locations.getTableEntry("t"));
        assertEquals(0, f.locations.refreshes);
    }

    @Test
    public void refreshFailurePropagatesWithoutAnotherIndexLookup() throws Exception {
        Fixture f = new Fixture();
        ObTableNotExistException expected = new ObTableNotExistException("table dropped");
        f.locations.refreshFailure = expected;
        try {
            f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
            fail("refresh failure must propagate");
        } catch (ObTableNotExistException actual) {
            assertSame(expected, actual);
        }
        assertEquals(Collections.singletonList("__idx_11_i"), driver.queries);
    }

    @Test
    public void retryJdbcFailurePropagatesWithoutAThirdLookup() throws Exception {
        Fixture f = new Fixture();
        SQLException expected = new SQLTimeoutException("new index lookup timeout");
        driver.beforeQuery = name -> {
            if (name.equals("__idx_22_i")) { driver.queryFailure = expected; }
        };
        try {
            f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
            fail("retry failure must propagate");
        } catch (ObTableEntryRefreshException e) {
            assertSame(expected, e.getCause());
            assertEquals(-1, e.getErrorCode());
        }
        assertEquals(Arrays.asList("__idx_11_i", "__idx_22_i"), driver.queries);
        assertEquals(1, f.locations.refreshes);
    }

    @Test
    public void indexLockTimeoutDoesNotInvalidateDataTable() throws Exception {
        Fixture f = new Fixture();
        f.client.setTableEntryRefreshLockTimeout(20);
        Field locksField = IndexLocations.class.getDeclaredField("locks");
        locksField.setAccessible(true);
        Field indexesField = TableRoute.class.getDeclaredField("indexLocations");
        indexesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, java.util.concurrent.locks.Lock> locks =
            (Map<String, java.util.concurrent.locks.Lock>) locksField.get(indexesField.get(f.route));
        java.util.concurrent.locks.Lock lock = new java.util.concurrent.locks.ReentrantLock();
        locks.put("__idx_11_i", lock);
        lock.lock();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ObTableEntryRefreshException> query = executor.submit(() -> {
                try {
                    f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
                    throw new AssertionError("lock timeout must fail");
                } catch (ObTableEntryRefreshException e) { return e; }
            });
            assertEquals(-1, query.get(5, TimeUnit.SECONDS).getErrorCode());
            assertSame(f.old, f.locations.getTableEntry("t"));
            assertEquals(0, f.locations.refreshes);
            assertTrue(driver.queries.isEmpty());
        } finally {
            lock.unlock();
            executor.shutdownNow();
        }
    }

    @Test
    public void concurrentRefreshDoesNotDeleteNewEntry() throws Exception {
        Fixture f = new Fixture();
        driver.indexes.put("__idx_22_i", ObIndexType.IndexTypeNormalLocal);
        CountDownLatch queriedOld = new CountDownLatch(1);
        CountDownLatch newEntryReady = new CountDownLatch(1);
        driver.beforeQuery = name -> {
            if (name.equals("__idx_11_i")) {
                queriedOld.countDown();
                try {
                    assertTrue(newEntryReady.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> query = executor.submit(() -> f.route.getIndexTableName(
                "t", "i", Collections.emptyList(), false));
            assertTrue(queriedOld.await(5, TimeUnit.SECONDS));
            f.locations.getLocations().put("t", f.current);
            newEntryReady.countDown();
            assertEquals("t", query.get(5, TimeUnit.SECONDS));
            assertSame(f.current, f.locations.getTableEntry("t"));
            assertEquals(0, f.locations.refreshes);
        } finally {
            newEntryReady.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void sameIdReplacementStillRefreshesThroughIndexEntry() throws Exception {
        Fixture f = new Fixture();
        f.old.setTableId(500208L);
        f.current.setTableId(500212L);
        TableEntry sameId = entry(500208);
        driver.indexes.put("__idx_500212_i", ObIndexType.IndexTypeNormalGlobal);
        // The request captured the old entry before another refresh replaced its object.
        driver.beforeQuery = name -> {
            if (name.equals("__idx_500208_i")) {
                f.locations.getLocations().put("t", sameId);
            }
        };
        assertEquals("__idx_500212_i", f.route.getIndexTableName(
            "t", "i", Collections.singletonList("c"), false));
        assertEquals(Arrays.asList("__idx_500208_i", "__idx_500212_i"), driver.queries);
        assertEquals(1, f.locations.refreshes);
        assertSame(f.current, f.locations.getTableEntry("t"));
    }

    @Test
    public void constructIndexNameRetainsRemoteFallbackWhenEntryIsNull() throws Exception {
        Fixture f = new Fixture();
        driver.indexes.put("__idx_11_i", ObIndexType.IndexTypeNormalLocal);
        TableRoute noEntryRoute = new TableRoute(f.client, f.auth) {
            @Override public TableEntry getTableEntry(String name) { return null; }
        };
        field(noEntryRoute, "tableLocations", f.locations);
        field(noEntryRoute, "indexLocations", new IndexLocations(f.client));
        field(noEntryRoute, "serverRoster", f.roster);
        assertEquals("t", noEntryRoute.getIndexTableName("t", "i", Collections.emptyList(), false));
        assertEquals(1, driver.tableIdQueries);
        assertEquals(Collections.singletonList("__idx_11_i"), driver.queries);
        assertEquals(0, f.locations.refreshes);
    }

    private void assertMissing(Fixture f) throws Exception {
        try {
            f.route.getIndexTableName("t", "i", Collections.emptyList(), false);
            fail("missing index must fail after one retry");
        } catch (ObTableEntryRefreshException e) {
            assertEquals(-5224, e.getErrorCode());
        }
    }

    private static void field(Object target, String name, Object value) throws Exception {
        Field field = TableRoute.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static TableEntry entry(long id) {
        TableEntry entry = new TableEntry();
        entry.setTableId(id);
        return entry;
    }

    private static class RefreshLocations extends TableLocations {
        TableEntry current;
        int refreshes;
        Exception refreshFailure;
        RefreshLocations(ObTableClient client) { super(client); }
        @Override public TableEntry refreshMeta(String name, ServerRoster roster,
                                                ObUserAuth auth, TableEntry expected) throws Exception {
            TableEntry cached = getTableEntry(name);
            assertNotNull("identity refresh must not remove the cached entry", cached);
            if (cached != null && expected != null
                && !Objects.equals(cached.getTableId(), expected.getTableId())) {
                return cached;
            }
            refreshes++;
            if (refreshFailure != null) { throw refreshFailure; }
            getLocations().put(name, current);
            return current;
        }
    }

    private class Fixture {
        final ObTableClient client = new ObTableClient();
        final ObUserAuth auth = new ObUserAuth("unit", "unit");
        final ObServerAddr address = new ObServerAddr("127.0.0.1", 19999, 19998);
        final RefreshLocations locations = new RefreshLocations(client);
        final TableEntry old = entry(11);
        final TableEntry current = entry(22);
        final TableRoute route = new TableRoute(client, auth);
        final ServerRoster roster = new ServerRoster();
        Fixture() throws Exception {
            locations.current = current;
            locations.getLocations().put("t", old);
            roster.reset(Collections.singletonList(address));
            field(route, "tableLocations", locations);
            field(route, "indexLocations", new IndexLocations(client));
            field(route, "serverRoster", roster);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, handler);
    }

    private static class MetadataDriver implements Driver {
        final Map<String, ObIndexType> indexes = new HashMap<>();
        final List<String> queries = new ArrayList<>();
        SQLException queryFailure;
        SQLException connectFailure;
        Consumer<String> beforeQuery = name -> { };
        boolean resultClosed;
        boolean statementClosed;
        int tableIdQueries;
        @Override public Connection connect(String url, Properties properties) throws SQLException {
            if (connectFailure != null) { throw connectFailure; }
            return proxy(Connection.class, (p, method, args) -> {
                if (method.getName().equals("prepareStatement")) {
                    boolean tableIdLookup = ((String) args[0]).contains("__all_virtual_proxy_schema");
                    String[] name = { null };
                    return proxy(PreparedStatement.class, (s, call, values) -> {
                        if (call.getName().equals("setString")) { name[0] = (String) values[1]; }
                        if (call.getName().equals("close")) { statementClosed = true; }
                        if (call.getName().equals("executeQuery")) {
                            if (tableIdLookup) {
                                tableIdQueries++;
                            } else {
                                queries.add(name[0]);
                                beforeQuery.accept(name[0]);
                            }
                            if (queryFailure != null) { throw queryFailure; }
                            return proxy(ResultSet.class, (r, read, columns) -> {
                                if (read.getName().equals("close")) { resultClosed = true; }
                                if (read.getName().equals("next")) { return tableIdLookup || indexes.containsKey(name[0]); }
                                if (read.getName().equals("getInt")) { return indexes.get(name[0]).getValue(); }
                                if (read.getName().equals("getLong")) {
                                    if (tableIdLookup) { return 11L; }
                                    return Long.parseLong(name[0].substring(6, name[0].lastIndexOf('_')));
                                }
                                return null;
                            });
                        }
                        return null;
                    });
                }
                return null;
            });
        }
        @Override public boolean acceptsURL(String url) { return url.startsWith("jdbc:mysql:"); }
        @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) { return new DriverPropertyInfo[0]; }
        @Override public int getMajorVersion() { return 1; }
        @Override public int getMinorVersion() { return 0; }
        @Override public boolean jdbcCompliant() { return false; }
        @Override public Logger getParentLogger() { return Logger.getGlobal(); }
    }
}
