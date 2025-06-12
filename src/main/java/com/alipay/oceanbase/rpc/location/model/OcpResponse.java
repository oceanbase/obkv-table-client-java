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

public class OcpResponse {
    private int             Code;
    private String          Message;
    private boolean         Success;
    private OcpResponseData Data;
    public OcpResponse() {}

    /*
     * Get code.
     */
    public int getCode() {
        return Code;
    }

    /*
     * Set code.
     */
    public void setCode(int code) {
        Code = code;
    }

    /*
     * Get message.
     */
    public String getMessage() {
        return Message;
    }

    /*
     * Set message.
     */
    public void setMessage(String message) {
        Message = message;
    }

    /*
     * Is success.
     */
    public boolean isSuccess() {
        return Success;
    }

    /*
     * Set success.
     */
    public void setSuccess(boolean success) {
        Success = success;
    }

    /*
     * Get data.
     */
    public OcpResponseData getData() {
        return Data;
    }

    /*
     * Validate.
     */
    public boolean validate() {
        return isSuccess() && getCode() == 200 && Data != null && Data.validate();
    }

    /*
     * Set data.
     */
    public void setData(OcpResponseData data) {
        Data = data;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "OcpResponse{" + "Code=" + Code + ", Message='" + Message + '\'' + ", Success="
               + Success + ", Data=" + Data + '}';
    }
}
