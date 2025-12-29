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

package com.alipay.oceanbase.rpc.bolt.transport;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Request time trace for analyzing timeout issues.
 * <p>
 * This class tracks various timestamps during the lifecycle of an RPC request:
 * <ul>
 *   <li>startTime: when the request starts</li>
 *   <li>serializeEndTime: when serialization completes</li>
 *   <li>writeToNettyTime: when request is submitted to Netty</li>
 *   <li>nettyEncodeStartTime: when EventLoop starts encoding (reflects queue wait)</li>
 *   <li>nettyEncodeEndTime: when EventLoop finishes encoding</li>
 *   <li>responseReceivedTime: when response is received</li>
 *   <li>endTime: when request ends (success or failure)</li>
 * </ul>
 * <p>
 * The time trace report is generated only when an error/timeout occurs,
 * so there is minimal performance overhead during normal operations.
 */
public class ObTableTimeTrace {

    // Timestamps at various stages (milliseconds)
    private long startTime;              // Request start time
    private long serializeEndTime;       // Serialization complete time
    private long waitWritableStartTime;  // Time when started waiting for channel writable
    private long waitWritableEndTime;    // Time when channel became writable (or timeout)
    private long writeToNettyTime;       // Time when submitted to Netty (writeAndFlush called)
    private long nettyEncodeStartTime;   // Time when EventLoop starts encoding (reflects queue wait)
    private long nettyEncodeEndTime;     // Time when EventLoop finishes encoding
    private long responseReceivedTime;   // Time when response is received
    private long endTime;                // Request end time (success or failure)
    
    // Additional info
    private int channelId;
    private String traceId;              // Lazy: only formatted when report is generated
    private long uniqueId;               // For lazy traceId formatting
    private long sequence;               // For lazy traceId formatting
    private int payloadSize;             // Request payload size in bytes

    public ObTableTimeTrace() {
        this.startTime = System.currentTimeMillis();
    }

    public void markSerializeEnd() {
        this.serializeEndTime = System.currentTimeMillis();
    }

    public void markWaitWritableStart() {
        this.waitWritableStartTime = System.currentTimeMillis();
    }

    public void markWaitWritableEnd() {
        this.waitWritableEndTime = System.currentTimeMillis();
    }

    public void markWriteToNetty() {
        this.writeToNettyTime = System.currentTimeMillis();
    }

    public void markNettyEncodeStart() {
        this.nettyEncodeStartTime = System.currentTimeMillis();
    }

    public void markNettyEncodeEnd() {
        this.nettyEncodeEndTime = System.currentTimeMillis();
    }

    public void markResponseReceived() {
        this.responseReceivedTime = System.currentTimeMillis();
    }

    public void markEnd() {
        this.endTime = System.currentTimeMillis();
    }

    public void setChannelId(int channelId) {
        this.channelId = channelId;
    }

    /**
     * Set trace id directly (for cases where it's already formatted).
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * Set trace id components for lazy formatting.
     * TraceId will only be formatted when generateReport() is called,
     * avoiding String.format overhead in the hot path.
     */
    public void setTraceIdComponents(long uniqueId, long sequence) {
        this.uniqueId = uniqueId;
        this.sequence = sequence;
    }

    /**
     * Get formatted traceId (lazy formatting).
     */
    private String getFormattedTraceId() {
        if (traceId != null) {
            return traceId;
        }
        if (uniqueId != 0 || sequence != 0) {
            return String.format("Y%X-%016X", uniqueId, sequence);
        }
        return "N/A";
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getTotalCost() {
        return endTime > 0 ? endTime - startTime : System.currentTimeMillis() - startTime;
    }

    /**
     * 生成时间追踪报告
     */
    public String generateReport() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        StringBuilder sb = new StringBuilder();
        
        sb.append("\n========== 时间追踪报告 ==========\n");
        sb.append("TraceId: ").append(getFormattedTraceId()).append("\n");
        sb.append("ChannelId: ").append(channelId).append("\n");
        sb.append("PayloadSize: ").append(payloadSize / 1024).append(" KB\n");
        sb.append("\n时间线:\n");
        
        sb.append(String.format("  [%s] 请求开始\n", formatTime(sdf, startTime)));
        
        if (serializeEndTime > 0) {
            sb.append(String.format("  [%s] 序列化完成 (+%d ms)\n", 
                formatTime(sdf, serializeEndTime), serializeEndTime - startTime));
        }
        
        if (waitWritableStartTime > 0) {
            sb.append(String.format("  [%s] 开始等待Channel可写 (+%d ms)\n", 
                formatTime(sdf, waitWritableStartTime), waitWritableStartTime - startTime));
            if (waitWritableEndTime > 0) {
                sb.append(String.format("  [%s] Channel变为可写 (+%d ms, 等待了 %d ms)\n", 
                    formatTime(sdf, waitWritableEndTime), waitWritableEndTime - startTime,
                    waitWritableEndTime - waitWritableStartTime));
            } else {
                sb.append("  [--:--:--.---] Channel变为可写 (超时! Channel一直不可写)\n");
            }
        }
        
        if (writeToNettyTime > 0) {
            sb.append(String.format("  [%s] 提交到Netty (+%d ms)\n", 
                formatTime(sdf, writeToNettyTime), writeToNettyTime - startTime));
        }
        
        if (nettyEncodeStartTime > 0) {
            sb.append(String.format("  [%s] EventLoop开始编码 (+%d ms)\n", 
                formatTime(sdf, nettyEncodeStartTime), nettyEncodeStartTime - startTime));
        } else if (writeToNettyTime > 0) {
            sb.append("  [--:--:--.---] EventLoop开始编码 (未执行! 可能队列积压)\n");
        }
        
        if (nettyEncodeEndTime > 0) {
            sb.append(String.format("  [%s] EventLoop编码完成 (+%d ms)\n", 
                formatTime(sdf, nettyEncodeEndTime), nettyEncodeEndTime - startTime));
        }
        
        if (responseReceivedTime > 0) {
            sb.append(String.format("  [%s] 收到响应 (+%d ms)\n", 
                formatTime(sdf, responseReceivedTime), responseReceivedTime - startTime));
        } else {
            sb.append("  [--:--:--.---] 收到响应 (未收到! 超时)\n");
        }
        
        if (endTime > 0) {
            sb.append(String.format("  [%s] 请求结束 (总耗时: %d ms)\n", 
                formatTime(sdf, endTime), endTime - startTime));
        }
        
        // 分析各阶段耗时
        sb.append("\n耗时分析:\n");
        if (serializeEndTime > 0) {
            sb.append(String.format("  用户线程序列化: %d ms\n", serializeEndTime - startTime));
        }
        if (waitWritableStartTime > 0 && serializeEndTime > 0) {
            sb.append(String.format("  准备发送: %d ms\n", waitWritableStartTime - serializeEndTime));
        }
        if (waitWritableStartTime > 0) {
            if (waitWritableEndTime > 0) {
                long waitTime = waitWritableEndTime - waitWritableStartTime;
                sb.append(String.format("  ★ 等待Channel可写: %d ms%s\n", 
                    waitTime, waitTime > 100 ? " [!!!发送缓冲区满!!!]" : ""));
            } else {
                long waitTime = endTime > 0 ? endTime - waitWritableStartTime : System.currentTimeMillis() - waitWritableStartTime;
                sb.append(String.format("  ★ 等待Channel可写: %d ms [!!!超时! 发送缓冲区一直满!!!]\n", waitTime));
            }
        }
        if (writeToNettyTime > 0 && waitWritableEndTime > 0) {
            sb.append(String.format("  提交到Netty: %d ms\n", writeToNettyTime - waitWritableEndTime));
        } else if (writeToNettyTime > 0 && serializeEndTime > 0) {
            sb.append(String.format("  提交到Netty: %d ms\n", writeToNettyTime - serializeEndTime));
        }
        if (nettyEncodeStartTime > 0 && writeToNettyTime > 0) {
            long queueWait = nettyEncodeStartTime - writeToNettyTime;
            sb.append(String.format("  ★ Netty队列等待: %d ms%s\n", 
                queueWait, queueWait > 100 ? " [!!!可能阻塞!!!]" : ""));
        }
        if (nettyEncodeEndTime > 0 && nettyEncodeStartTime > 0) {
            sb.append(String.format("  EventLoop编码耗时: %d ms\n", nettyEncodeEndTime - nettyEncodeStartTime));
        }
        if (responseReceivedTime > 0 && nettyEncodeEndTime > 0) {
            sb.append(String.format("  网络+服务端处理: %d ms\n", responseReceivedTime - nettyEncodeEndTime));
        } else if (nettyEncodeEndTime > 0) {
            sb.append(String.format("  网络+服务端处理: 超时 (编码完成后等待 %d ms 未收到响应)\n", 
                System.currentTimeMillis() - nettyEncodeEndTime));
        }
        
        sb.append("==================================\n");
        
        return sb.toString();
    }

    private String formatTime(SimpleDateFormat sdf, long time) {
        return sdf.format(new Date(time));
    }

    @Override
    public String toString() {
        return generateReport();
    }
}

