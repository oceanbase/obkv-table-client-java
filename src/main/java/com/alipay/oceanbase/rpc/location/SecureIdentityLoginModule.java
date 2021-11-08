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

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class SecureIdentityLoginModule {

    private static byte[] ENC_KEY_BYTES_PROD = "gQzLk5tTcGYlQ47GG29xQxfbHIURCheJ".getBytes();

    /**
     * Decode.
     */
    public static String decode(String secret) throws NoSuchPaddingException,
                                              NoSuchAlgorithmException, InvalidKeyException,
                                              BadPaddingException, IllegalBlockSizeException {
        return decode(null, secret);
    }

    /**
     * Decode.
     */
    public static String decode(String encKey, String secret) throws NoSuchPaddingException,
                                                             NoSuchAlgorithmException,
                                                             InvalidKeyException,
                                                             BadPaddingException,
                                                             IllegalBlockSizeException {
        byte[] kbytes = ENC_KEY_BYTES_PROD;
        if (isNotBlank(encKey)) {
            kbytes = encKey.getBytes();
        }
        return iniDecode(kbytes, secret);
    }

    static final String iniDecode(byte[] kbytes, String secret) throws NoSuchPaddingException,
                                                               NoSuchAlgorithmException,
                                                               InvalidKeyException,
                                                               BadPaddingException,
                                                               IllegalBlockSizeException {
        SecretKeySpec key = new SecretKeySpec(kbytes, "Blowfish");
        BigInteger n = new BigInteger(secret, 16);
        byte[] encoding = n.toByteArray();
        // SECURITY-344: fix leading zeros
        if (encoding.length % 8 != 0) {
            int length = encoding.length;
            int newLength = ((length / 8) + 1) * 8;
            int pad = newLength - length; //number of leading zeros
            byte[] old = encoding;
            encoding = new byte[newLength];
            for (int i = old.length - 1; i >= 0; i--) {
                encoding[i + pad] = old[i];
            }
            encoding[0] = (byte) n.signum();
        }
        Cipher cipher = Cipher.getInstance("Blowfish");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] decode = cipher.doFinal(encoding);
        return new String(decode);
    }

    static final boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    static final boolean isBlank(String str) {
        int strLen = 0;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

}
