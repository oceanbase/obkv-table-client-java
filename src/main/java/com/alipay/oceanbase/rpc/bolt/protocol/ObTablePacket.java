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

package com.alipay.oceanbase.rpc.bolt.protocol;

import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.remoting.CommandCode;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.ProtocolCode;
import com.alipay.remoting.RemotingCommand;
import com.alipay.remoting.config.switches.ProtocolSwitch;
import com.alipay.remoting.exception.DeserializationException;
import com.alipay.remoting.exception.SerializationException;
import io.netty.buffer.ByteBuf;

public class ObTablePacket implements RemotingCommand {

    private static ProtocolCode OB_RPC_MAGIC_CODE = ProtocolCode
                                                      .fromBytes(ObTableProtocol.MAGIC_HEADER_FLAG);
    private CommandCode         commandCode;
    // channel id
    private int                 id;

    private byte[]              packetContent;
    private ByteBuf             packetContentBuf;

    private ObRpcPacketHeader   header;

    private int                 transportCode;
    private String              message;
    private Throwable           cause;

    /**
     * Decode packet header.
     */
    public void decodePacketHeader() {
        header = new ObRpcPacketHeader();
        header.decode(packetContentBuf);

        this.setCmdCode(ObTablePacketCode.valueOf((short) header.getPcode()));
    }

    /*
     * Create transport error packet.
     */
    public static ObTablePacket createTransportErrorPacket(int transportCode, String message,
                                                           Throwable cause) {
        ObTablePacket tablePacket = new ObTablePacket();
        tablePacket.transportCode = transportCode;
        tablePacket.message = message;
        tablePacket.cause = cause;

        return tablePacket;
    }

    /*
     * Get protocol code.
     */
    @Override
    public ProtocolCode getProtocolCode() {
        return OB_RPC_MAGIC_CODE;
    }

    /*
     * Get cmd code.
     */
    @Override
    public CommandCode getCmdCode() {
        return commandCode;
    }

    /*
     * Set cmd code.
     */
    public void setCmdCode(CommandCode cmdCode) {
        this.commandCode = cmdCode;
    }

    /*
     * Get id.
     */
    @Override
    public int getId() {
        return id;
    }

    /*
     * Set id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /*
     * Get packet content.
     */
    public byte[] getPacketContent() {
        return packetContent;
    }

    /*
     * Set packet content.
     */
    public void setPacketContent(byte[] packetContent) {
        this.packetContent = packetContent;
    }

    /*
     * Set packet content buf.
     */
    public void setPacketContentBuf(ByteBuf packetContent) {
        this.packetContentBuf = packetContent;
    }

    /*
     * Get packet content buf.
     */
    public ByteBuf getPacketContentBuf() {
        return packetContentBuf;
    }

    /*
     * Release byte buf.
     */
    public void releaseByteBuf() {
        // http://netty.io/wiki/reference-counted-objects.html
        if (packetContentBuf != null) {
            packetContentBuf.release();
        }
    }

    /*
     * Get header.
     */
    public ObRpcPacketHeader getHeader() {
        return header;
    }

    /*
     * Set header.
     */
    public void setHeader(ObRpcPacketHeader header) {
        this.header = header;
    }

    /*
     * Is success.
     */
    public boolean isSuccess() {
        return transportCode == 0;
    }

    /*
     * Get transport code.
     */
    public int getTransportCode() {
        return transportCode;
    }

    /*
     * Set transport code.
     */
    public void setTransportCode(int transportCode) {
        this.transportCode = transportCode;
    }

    /*
     * Get message.
     */
    public String getMessage() {
        return message;
    }

    /*
     * Set message.
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /*
     * Get cause.
     */
    public Throwable getCause() {
        return cause;
    }

    /*
     * Set cause.
     */
    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    // TODO useless for now

    /*
     * Get invoke context.
     */
    @Override
    public InvokeContext getInvokeContext() {
        return null;
    }

    /*
     * Get serializer.
     */
    @Override
    public byte getSerializer() {
        return 0;
    }

    /*
     * Get protocol switch.
     */
    @Override
    public ProtocolSwitch getProtocolSwitch() {
        return null;
    }

    /*
     * Serialize.
     */
    @Override
    public void serialize() throws SerializationException {

    }

    /*
     * Deserialize.
     */
    @Override
    public void deserialize() throws DeserializationException {

    }

    /*
     * Serialize content.
     */
    @Override
    public void serializeContent(InvokeContext invokeContext) throws SerializationException {

    }

    /*
     * Deserialize content.
     */
    @Override
    public void deserializeContent(InvokeContext invokeContext) throws DeserializationException {

    }
}
