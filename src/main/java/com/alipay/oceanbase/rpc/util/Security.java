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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.exception.ObTableAuthException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.alipay.oceanbase.rpc.util.StringUtil.isNotBlank;

/**
 * {@link com.mysql.jdbc.MysqlIO#secureAuth}
 *
 */
public class Security {

    private static final byte[] bytes          = { '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h',
            'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U',
            'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V', 'B',
            'N', 'M'                          };
    private static final long   multiplier     = 0x5DEECE66DL;
    private static final long   addend         = 0xBL;
    private static final long   mask           = (1L << 48) - 1;
    private static final long   integerMask    = (1L << 33) - 1;
    private static final long   seedUniquifier = 8682522807148012L;

    private static long         seed;
    static {
        long s = seedUniquifier + System.nanoTime();
        s = (s ^ multiplier) & mask;
        seed = s;
    }

    /**
     * SHA1(passwd) ^ SHA1(scramble + SHA1(SHA1(passwd)))
     *
     * @return scramble password by scramble salt
     */
    public static final ObBytesString scramblePassword(String password, ObBytesString scrambleSalt) {
        if (password == null || password.isEmpty()) {
            return new ObBytesString();
        }
        return scramblePassword(password.getBytes(), scrambleSalt.bytes);
    }

    public static final ObBytesString scramblePassword(byte[] password, byte[] seed) {
        if (password == null || password.length == 0) {
            return new ObBytesString();
        }

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] pass1 = md.digest(password);
            md.reset();
            byte[] pass2 = md.digest(pass1);
            md.reset();
            md.update(seed);
            byte[] pass3 = md.digest(pass2);
            for (int i = 0; i < pass3.length; i++) {
                pass3[i] = (byte) (pass3[i] ^ pass1[i]);
            }
            return new ObBytesString(pass3);
        } catch (Throwable e) {
            throw new ObTableAuthException("password scramble failed", e);
        }
    }

    /**
     * @return password scramble salt
     */
    public static final ObBytesString getPasswordScramble(int size) {
        byte[] bb = bytes;
        byte[] ab = new byte[size];
        for (int i = 0; i < size; i++) {
            ab[i] = randomByte(bb);
        }
        return new ObBytesString(ab);
    }

    private static byte randomByte(byte[] b) {
        int ran = (int) ((random() & integerMask) >>> 16);
        return b[ran % b.length];
    }

    private static long random() {
        long oldSeed = seed;
        long nextSeed = 0L;
        do {
            nextSeed = (oldSeed * multiplier + addend) & mask;
        } while (oldSeed == nextSeed);
        seed = nextSeed;
        return nextSeed;
    }

    private static String ENC_KEY_BYTES_STR      = "jaas is the way";
    private static byte[] ENC_KEY_BYTES          = ENC_KEY_BYTES_STR.getBytes();

    private static String ENC_KEY_BYTES_PROD_STR = "gQzLk5tTcGYlQ47GG29xQxfbHIURCheJ";
    private static byte[] ENC_KEY_BYTES_PROD     = ENC_KEY_BYTES_PROD_STR.getBytes();

    /**
     * Encode.
     */
    public static String encode(String secret) throws NoSuchPaddingException,
                                              NoSuchAlgorithmException, InvalidKeyException,
                                              BadPaddingException, IllegalBlockSizeException {
        return encode(null, secret);
    }

    /**
     * Encode.
     */
    public static String encode(String encKey, String secret) throws InvalidKeyException,
                                                             NoSuchAlgorithmException,
                                                             NoSuchPaddingException,
                                                             IllegalBlockSizeException,
                                                             BadPaddingException {
        byte[] kbytes = ENC_KEY_BYTES_PROD;
        if (isNotBlank(encKey)) {
            kbytes = encKey.getBytes();
        }

        // 默认采用prod key加密与解密,线下环境会异常;
        try {
            return initEncode(kbytes, secret);
        } catch (InvalidKeyException e) {
            kbytes = ENC_KEY_BYTES;
        } catch (NoSuchAlgorithmException e) {
            kbytes = ENC_KEY_BYTES;
        } catch (NoSuchPaddingException e) {
            kbytes = ENC_KEY_BYTES;
        } catch (IllegalBlockSizeException e) {
            kbytes = ENC_KEY_BYTES;
        } catch (BadPaddingException e) {
            kbytes = ENC_KEY_BYTES;
        }

        return initEncode(kbytes, secret);
    }

    static final String initEncode(byte[] kbytes, String secret) throws NoSuchAlgorithmException,
                                                                NoSuchPaddingException,
                                                                InvalidKeyException,
                                                                IllegalBlockSizeException,
                                                                BadPaddingException {
        SecretKeySpec key = new SecretKeySpec(kbytes, "Blowfish");
        Cipher cipher = Cipher.getInstance("Blowfish");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] encoding = cipher.doFinal(secret.getBytes());
        BigInteger n = new BigInteger(encoding);
        return n.toString(16);
    }

    /**
     * Decode.
     */
    public static char[] decode(String secret) throws NoSuchPaddingException,
                                              NoSuchAlgorithmException, InvalidKeyException,
                                              BadPaddingException, IllegalBlockSizeException {
        return decode(null, secret).toCharArray();
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

        try {
            return iniDecode(kbytes, secret);
        } catch (InvalidKeyException e) {
            kbytes = ENC_KEY_BYTES;
        } catch (BadPaddingException e) {
            kbytes = ENC_KEY_BYTES;
        } catch (IllegalBlockSizeException e) {
            kbytes = ENC_KEY_BYTES;
        }
        return iniDecode(kbytes, secret);
    }

    private static String iniDecode(byte[] kbytes, String secret) throws NoSuchPaddingException,
                                                                 NoSuchAlgorithmException,
                                                                 InvalidKeyException,
                                                                 BadPaddingException,
                                                                 IllegalBlockSizeException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(kbytes, "Blowfish");
        BigInteger bigInteger = new BigInteger(secret, 16);
        byte[] encoding = bigInteger.toByteArray();
        // SECURITY-344: fix leading zeros
        if (encoding.length % 8 != 0) {
            int len = encoding.length;
            int nLen = ((len / 8) + 1) * 8;
            int pad = nLen - len; //number of leading zeros
            byte[] old = encoding;
            encoding = new byte[nLen];
            for (int i = old.length - 1; i >= 0; i--) {
                encoding[i + pad] = old[i];
            }
        }
        Cipher cipher = Cipher.getInstance("Blowfish");
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
        byte[] decode = cipher.doFinal(encoding);
        return new String(decode);
    }

}
