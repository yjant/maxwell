package com.zendesk.maxwell.util;


import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Base64;


public class AesEncode {

    private final String AES = "AES";
    private  String ENCODE_SEED ;


	public AesEncode(String ENCODE_SEED) {
		this.ENCODE_SEED = ENCODE_SEED;
	}

	/**
     * 加密
     * 1.构造密钥生成器
     * 2.根据ecnodeRules规则初始化密钥生成器
     * 3.产生密钥
     * 4.创建和初始化密码器
     * 5.内容加密
     * 6.返回字符串
     */
    public String encode(String content) {
        try {
            Key key = getCodeKey();
            Cipher cipher = Cipher.getInstance(AES);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            byte[] byteEncode = content.getBytes(StandardCharsets.UTF_8);
            byte[] byteAES = cipher.doFinal(byteEncode);
            return Base64.getEncoder().encodeToString(byteAES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }




    /**
     * 解密
     * 解密过程：
     * 1.同加密1-4步
     * 2.将加密后的字符串反纺成byte[]数组
     * 3.将加密内容解密
     */
    public String decode(String content) {
        try {

            Key key = getCodeKey();
            Cipher cipher = Cipher.getInstance(AES);
            cipher.init(Cipher.DECRYPT_MODE, key);
            byte[] byteContent = new BASE64Decoder().decodeBuffer(content);
            byte[] byteDecode = cipher.doFinal(byteContent);
            return new String(byteDecode, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private Key getCodeKey() throws Exception{
        //第一步： 生成KEY
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        //设置加密用的种子，密钥
        random.setSeed(ENCODE_SEED.getBytes());
        keyGenerator.init(128,random);
        //第二步： 产生密钥
        SecretKey secretKey = keyGenerator.generateKey();
        //第三步： 获取密钥
        byte[] keyBytes = secretKey.getEncoded();
        //第四步： KEY转换
        return new SecretKeySpec(keyBytes, AES);
    }

}
