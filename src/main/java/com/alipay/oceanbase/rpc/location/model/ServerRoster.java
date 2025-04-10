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

import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ServerRoster {

    private static final Logger                  logger      = TableClientLoggerFactory
                                                                 .getLogger(ServerRoster.class);

    private AtomicInteger                        maxPriority = new AtomicInteger(0);

    private AtomicReference<List<ObServerAddr>>  roster      = new AtomicReference<List<ObServerAddr>>(
                                                                 new ArrayList<ObServerAddr>());

    private AtomicReference<ObServerLdcLocation> serverLdc   = new AtomicReference<ObServerLdcLocation>();
    private AtomicLong                           turn        = new AtomicLong(0);

    /*
     * Reset.
     */
    public void reset(List<ObServerAddr> members) {
        this.maxPriority.set(0);
        this.roster.set(members);
    }

    /*
     * Reset Server LDC.
     */
    public void resetServerLdc(ObServerLdcLocation serverLdc) {
        this.serverLdc.set(serverLdc);
    }

    /*
     * Upgrade max priority.
     */
    public void upgradeMaxPriority(long priority) {

        if (maxPriority.get() >= priority) {
            return;
        }

        if (priority == 0) {
            this.maxPriority.set(0);
            return;
        }

        resetMaxPriority();
    }

    /*
     * Downgrade max priority.
     */
    public void downgradeMaxPriority(long priority) {
        if (maxPriority.get() <= priority) {
            return;
        }
        resetMaxPriority();
    }

    /*
     * Reset max priority.
     */
    public void resetMaxPriority() {
        if (roster.get().size() == 0) {
            this.maxPriority.set(0);
        }
        int priority = Integer.MIN_VALUE;
        for (ObServerAddr obServerAddr : roster.get()) {
            if (obServerAddr.getPriority().get() > priority) { //priority below maxPriority)
                priority = obServerAddr.getPriority().get();
            }
        }
        this.maxPriority.set(priority);

    }

    /*
     * Get max priority.
     */
    public int getMaxPriority() {
        return maxPriority.get();
    }

    /*
     * Get members.
     */
    public List<ObServerAddr> getMembers() {
        return roster.get();
    }

    public AtomicReference<List<ObServerAddr>> getRoster() {
        return roster;
    }

    /*
     * Get ServerLdcLocation.
     */
    public ObServerLdcLocation getServerLdcLocation() {
        return serverLdc.get();
    }

    /*
     * Choose a server by random.
     *
     * @param priorityTimeout
     * @param cachingTimeout
     * @return
     */
    public ObServerAddr getServer(long priorityTimeout) {
        long gradeTime = System.currentTimeMillis();
        List<ObServerAddr> avaliableList = new ArrayList<ObServerAddr>();
        int maxPriority = getMaxPriority();
        for (ObServerAddr obServerAddr : getMembers()) {
            if (obServerAddr.getPriority().get() == maxPriority || //max priority
                gradeTime - obServerAddr.getGrantPriorityTime() > priorityTimeout) { //last grant priority timeout
                avaliableList.add(obServerAddr);
            }
        }
        // round-robin get server address
        long idx = turn.getAndIncrement();
        if (idx == Integer.MAX_VALUE) {
            turn.set(0);
        }
        ObServerAddr addr = avaliableList.get((int) (idx % avaliableList.size()));
        addr.recordAccess();
        return addr;
    }

    /*
     * Reset priority and upgrade priority.
     *
     * @param addr
     */
    public void resetPriority(ObServerAddr addr) {
        if (addr.getPriority().get() != 0) {
            long grantPriorityTime = System.currentTimeMillis();
            addr.setGrantPriorityTime(grantPriorityTime);
            // reset the priority
            addr.getPriority().set(0);
            // has no effect on the order of server roster
            this.upgradeMaxPriority(0);
        }
    }

    /*
     * Down grade priority and reorder the servers.
     *
     * @param addr
     */
    public void downgradePriority(ObServerAddr addr) {
        // must reorder the serverRoster when priority change
        long grantPriorityTime = System.currentTimeMillis();
        addr.setGrantPriorityTime(grantPriorityTime);
        this.downgradeMaxPriority(addr.getPriority().decrementAndGet());

    }

    /*
     * To String.
     */
    @Override
    public String toString() {
        return "ServerRoster{" + "maxPriority=" + maxPriority + ", roster=" + roster
               + ", serverLdc=" + serverLdc + '}';
    }
}
