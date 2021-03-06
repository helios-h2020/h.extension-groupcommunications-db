package eu.h2020.helios_social.modules.groupcommunications.db.crypto.security;

import com.google.gson.Gson;

import org.spongycastle.crypto.CryptoException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.GCMParameterSpec;
import javax.inject.Inject;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;
import eu.h2020.helios_social.modules.groupcommunications.api.messaging.AbstractMessage;
import eu.h2020.helios_social.modules.groupcommunications.api.messaging.MessageAndKey;

/**
 * Singleton class to implement HELIOS cryptographic interface {@link HeliosCrypto}
 */
public class HeliosCryptoManager implements HeliosCrypto {
    private static final Logger LOG =
            Logger.getLogger(HeliosCryptoManager.class.getName());
    private static final HeliosCryptoManager ourInstance = new HeliosCryptoManager();
    private static final String TAG = "HeliosCryptoManager";

    /**
     * Get the singleton instance of this Manager.
     *
     * @return {@link HeliosCryptoManager}
     */
    public static HeliosCryptoManager getInstance() {
        return ourInstance;
    }

    private HeliosCryptoManager() {
    }

    @Override
    public SecretKey generateAESKey() {
        SecureRandom secureRandom = new SecureRandom();
        byte[] seed = new byte[16];
        secureRandom.nextBytes(seed);
        return new SecretKeySpec(seed, "AES");
    }

    @Override
    public KeyPair generateRSAKeyPair() {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048);
            return keyPairGenerator.genKeyPair();
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "RSA algorithm not found!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] encryptAESKey(PublicKey encryptionRSAKey, SecretKey AESKey) throws  CryptoException{
        try {
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA1AndMGF1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, encryptionRSAKey);
            return cipher.doFinal(AESKey.getEncoded());
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            LOG.info( "Padding not supported!");
            e.printStackTrace();
        } catch (BadPaddingException e) {
            LOG.info( "Bad padding!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            LOG.info( "Illegal block size!");
            e.printStackTrace();
        }
        return null;
    }



    @Override
    public SecretKey decryptAESKey(PrivateKey decryptionRSAKey, byte[] encryptedAESKey) throws  CryptoException{
        try {
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA1AndMGF1Padding");
            cipher.init(Cipher.DECRYPT_MODE, decryptionRSAKey);
            byte[] decryptedAESKeyBytes = cipher.doFinal(encryptedAESKey);
            return new SecretKeySpec(decryptedAESKeyBytes, 0, decryptedAESKeyBytes.length, "AES");
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            LOG.info( "Padding not supported!");
            e.printStackTrace();
        } catch (BadPaddingException e) {
            LOG.info( "Bad padding!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            LOG.info( "Illegal block size!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] encryptBytes(byte[] plaintextBytes, SecretKey AESKey, byte[] iv) throws  CryptoException{
        try {
            SecureRandom secureRandom = new SecureRandom();
            secureRandom.nextBytes(iv);
            final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv);
            cipher.init(Cipher.ENCRYPT_MODE, AESKey, parameterSpec);
            return cipher.doFinal(plaintextBytes);
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            LOG.info( "Padding not supported!");
            e.printStackTrace();
        } catch (BadPaddingException e) {
            LOG.info( "Bad padding!");
            e.printStackTrace();
        } catch (InvalidAlgorithmParameterException e) {
            LOG.info( "Invalid algorithm parameter!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            LOG.info( "Illegal block size!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] decryptBytes(byte[] cryptotextBytes, SecretKey AESKey, byte[] iv) throws  CryptoException{
        try {
            final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv);
            cipher.init(Cipher.DECRYPT_MODE, AESKey, parameterSpec);
            return cipher.doFinal(cryptotextBytes);
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            LOG.info( "Padding not supported!");
            e.printStackTrace();
        } catch (BadPaddingException e) {
            LOG.info( "Bad padding!");
            e.printStackTrace();
        } catch (InvalidAlgorithmParameterException e) {
            LOG.info( "Invalid algorithm parameter!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            LOG.info( "Illegal block size!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] signBytes(PrivateKey signingRSAKey, byte[] bytesToSign) {
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(signingRSAKey);
            signature.update(bytesToSign);
            return signature.sign();
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        } catch (SignatureException e) {
            LOG.info( "Signature exception");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean verifyBytes(PublicKey verificationRSAKey, byte[] bytesToSign, byte[] signatureToVerify) {
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(verificationRSAKey);
            signature.update(bytesToSign);
            return signature.verify(signatureToVerify);
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        } catch (SignatureException e) {
            LOG.info( "Signature exception");
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public byte[] createHMAC(byte[] data, byte[] secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec key = new SecretKeySpec(secret, mac.getAlgorithm());
            mac.init(key);
            byte[] calc = mac.doFinal(data);
            return calc;
        } catch (NoSuchAlgorithmException e) {
            LOG.info( "Algorithm not supported!");
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            LOG.info( "Invalid key!");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean verifyHMAC(byte[] hmac, byte[] data, byte[] secret) {
        byte[] calc = this.createHMAC(data, secret);
        return Arrays.equals(hmac, calc);
    }

    @Override
    public byte[] decryptMessage(byte[] data, PrivateKey privateKey) throws CryptoException {
        try {
            String initialStringMessage = new String(data, StandardCharsets.UTF_8);
            MessageAndKey messageAndKey =
                    new Gson().fromJson(initialStringMessage, MessageAndKey.class);
            byte[] messageAndKeyBytes = messageAndKey.getMessageAndKeyBytes();

            // split message and key
            LOG.info("messageAndKeyBytes" + messageAndKeyBytes.length);
            ByteBuffer bb = ByteBuffer.wrap(messageAndKeyBytes);
            byte[] encryptedMessage = new byte[messageAndKeyBytes.length - 256 - 12];
            byte[] encryptedAESKey = new byte[256];
            byte[] iv = new byte[12];
            bb.get(encryptedMessage, 0, encryptedMessage.length);
            bb.get(encryptedAESKey, 0, encryptedAESKey.length);
            bb.get(iv, 0, iv.length);
            LOG.info("encryptedAESKey" + encryptedAESKey.length);

            //decrypting AES key
            SecretKey messageEncryptionKey = decryptAESKey(privateKey, encryptedAESKey);
            // decrypt message
            return decryptBytes(encryptedMessage, messageEncryptionKey, iv);
        } catch (Exception e){
            throw new CryptoException();
        }
    }

    @Override
    public MessageAndKey encryptMessage(PublicKey publicKey, AbstractMessage message) throws  CryptoException{
        try{
        SecretKey messageEncryptionKey = generateAESKey();
        byte[] iv = new byte[12];
        byte[] encryptedByteArray = encryptBytes(message.toJson().getBytes(), messageEncryptionKey, iv);
        byte[] encryptedAESKey = encryptAESKey(publicKey, messageEncryptionKey);
        byte[] messageAndKey = ByteBuffer.allocate(encryptedByteArray.length + encryptedAESKey.length + iv.length)
                .put(encryptedByteArray)
                .put(encryptedAESKey)
                .put(iv)
                .array();
        return  new MessageAndKey(messageAndKey);
        } catch (Exception e){
            throw new CryptoException();
        }
    }
}
