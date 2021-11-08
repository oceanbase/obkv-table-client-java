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

import com.alipay.oceanbase.rpc.location.SecureIdentityLoginModule;
import com.google.common.base.Strings;

/**
 * OceanBase User Authentication.
 *
 */
public class ObUserAuth {
    private String userName;
    private String password;

    private String encPassword; // encrypted password, set by user.

    /**
     * Default constructor.
     */
    public ObUserAuth() {
    }

    /**
     * Constructor with user/password.
     */
    public ObUserAuth(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    /**
     * Set User Name
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Set password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Set encrypted password. Decrypt it and set the password.
     */
    public void setEncPassword(String encPassword) throws Exception {
        this.encPassword = encPassword;
        if (!Strings.isNullOrEmpty(encPassword)) {
            this.password = SecureIdentityLoginModule.decode(encPassword);
        }
    }

    /**
     * Get password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Get user name.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Get encrypted password set by user.
     * <p>
     * Attention: you won't get encrypted password if you haven't set it, even if you have set a password.
     */
    public String getEncPassword() {
        return encPassword;
    }
}
