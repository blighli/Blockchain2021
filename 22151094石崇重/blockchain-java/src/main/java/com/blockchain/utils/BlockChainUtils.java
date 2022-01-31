package com.blockchain.utils;

import javax.xml.bind.DatatypeConverter;
import java.math.BigInteger;
import java.security.*;
import java.security.interfaces.ECPublicKey;
import java.security.spec.*;
import java.util.Base64;
import java.util.UUID;

public class BlockChainUtils {

    public static String getSHA256Hash(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data.getBytes("UTF-8"));

            return DatatypeConverter.printHexBinary(hash);
        } catch (Exception NoSuchAlgorithmException) {
            throw new RuntimeException(NoSuchAlgorithmException);
        }
    }


    public static String generateTransactionId() {
        return UUID.randomUUID().toString();
    }



    public static byte[] applySignature(PrivateKey privateKey, String data) {
        Signature signature;
        byte[] realSig;
        try {
            signature = Signature.getInstance("ECDSA", "BC");
            signature.initSign(privateKey);
            byte[] strByte = data.getBytes();
            signature.update(strByte);
            realSig = signature.sign();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return realSig;
    }



    public static boolean verifySignature(PublicKey publicKey, String data, byte[] signature) {
        try {
            Signature ecdsaVerify = Signature.getInstance("ECDSA", "BC");
            ecdsaVerify.initVerify(publicKey);
            ecdsaVerify.update(data.getBytes());
            return ecdsaVerify.verify(signature);
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static String getStringFromKey(Key key) {

        return Base64.getEncoder().encodeToString(key.getEncoded());
    }


    public static String[] convertKeytoString(PublicKey publicKey){
        ECPublicKey ecPublicKey= (ECPublicKey)publicKey;
        byte[] publicKeyX = ecPublicKey.getW().getAffineX().toByteArray();
        byte[] publicKeyY = ecPublicKey.getW().getAffineY().toByteArray();
        String[] res = new String[2];
        res[0] = Base64.getEncoder().encodeToString(publicKeyX);
        res[1]= Base64.getEncoder().encodeToString(publicKeyY);
        return res;
    }


    public static PublicKey convertStringtoKey(String[] encodedPublicKey) {
        PublicKey publicKey = null;
        try {
            String encodedPublicKeyX = encodedPublicKey[0];
            String encodedPublicKeyY = encodedPublicKey[1];
            byte[] pkX = Base64.getDecoder().decode(encodedPublicKeyX);
            byte[] pkY = Base64.getDecoder().decode(encodedPublicKeyY);
            ECPoint pubPoint = new ECPoint(new BigInteger(1, pkX), new BigInteger(1, pkY));
            AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
            parameters.init(new ECGenParameterSpec("secp192k1"));
            ECParameterSpec ecParameters = parameters.getParameterSpec(ECParameterSpec.class);
            ECPublicKeySpec pubSpec = new ECPublicKeySpec(pubPoint, ecParameters);
            KeyFactory kf = KeyFactory.getInstance("EC");
            publicKey = kf.generatePublic(pubSpec);
            return publicKey;
        } catch (Exception e) {
            return publicKey;
        }
    }
}
