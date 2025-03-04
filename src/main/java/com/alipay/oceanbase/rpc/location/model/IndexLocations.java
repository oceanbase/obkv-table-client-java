package com.alipay.oceanbase.rpc.location.model;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableEntryRefreshException;
import com.alipay.oceanbase.rpc.location.LocationUtil;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;

public class IndexLocations {
    private static final Logger      logger    = getLogger(IndexLocations.class);
    private final ObTableClient      tableClient;
    // String means indexName，one index is bound to one lock
    private Map<String, Lock>        locks     = new ConcurrentHashMap<String, Lock>();
    /*
     * indexTableName -> ObIndexInfo, containing index information
     */
    private Map<String, ObIndexInfo> locations = new ConcurrentHashMap<String, ObIndexInfo>();

    public IndexLocations(ObTableClient tableClient) {
        this.tableClient = tableClient;
    }

    public ObIndexInfo getOrRefreshIndexInfo(final String indexTableName, boolean forceRefresh,
                                             final ServerRoster serverRoster, final ObUserAuth sysUA)
                                                                                                     throws Exception {
        long tableEntryRefreshLockTimeout = tableClient.getTableEntryRefreshLockTimeout();
        int tableEntryRefreshTryTimes = tableClient.getTableEntryRefreshTryTimes();
        ObIndexInfo indexInfo = locations.get(indexTableName);
        if (!forceRefresh && indexInfo != null) {
            return indexInfo;
        }
        Lock tempLock = new ReentrantLock();
        Lock lock = locks.putIfAbsent(indexTableName, tempLock);
        lock = (lock == null) ? tempLock : lock;
        boolean acquired = lock.tryLock(tableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);
        if (!acquired) {
            String errMsg = "try to lock index infos refreshing timeout " + "dataSource:"
                            + tableClient.getDataSourceName() + " ,indexTableName:"
                            + indexTableName + " , timeout:" + tableEntryRefreshLockTimeout + ".";
            RUNTIME.error(errMsg);
            throw new ObTableEntryRefreshException(errMsg);
        }
        try {
            indexInfo = locations.get(indexTableName);
            if (!forceRefresh && indexInfo != null) {
                return indexInfo;
            } else {
                logger.info("index info is not exist, create new index info, indexTableName: {}",
                    indexTableName);
                int serverSize = serverRoster.getMembers().size();
                int refreshTryTimes = tableEntryRefreshTryTimes > serverSize ? serverSize
                    : tableEntryRefreshTryTimes;
                for (int i = 0; i < refreshTryTimes; i++) {
                    ObServerAddr serverAddr = serverRoster.getServer(tableClient
                        .getServerAddressPriorityTimeout());
                    indexInfo = LocationUtil.getIndexInfoFromRemote(serverAddr, sysUA,
                        tableClient.getTableEntryAcquireConnectTimeout(),
                        tableClient.getTableEntryAcquireSocketTimeout(), indexTableName);
                    if (indexInfo != null) {
                        locations.put(indexTableName, indexInfo);
                        break;
                    } else {
                        RUNTIME.error("get index info from remote is null, indexTableName: {}",
                            indexTableName);
                    }
                }
                return indexInfo;
            }
        } catch (Exception e) {
            RUNTIME.error("getOrRefresh index info meet exception", e);
            throw e;
        } finally {
            lock.unlock();
        }
    }
}
