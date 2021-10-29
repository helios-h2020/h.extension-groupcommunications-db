package eu.h2020.helios_social.modules.groupcommunications.db.crypto.security;

import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class RSAKeyPair {

    private KeyPair keyPair;
    private byte[] privateKeyBytes;
    private byte[] publicKeyBytes;



    public RSAKeyPair(KeyPair keyPair) {
        this.keyPair = keyPair;
        // Get bytes of the public and private keys
        this.privateKeyBytes = keyPair.getPrivate().getEncoded();
        this.publicKeyBytes = keyPair.getPublic().getEncoded();
    }

    public RSAKeyPair(byte[] privateKeyBytes, byte[] publicKeyBytes){
        // Get key pair Objects from their respective byte arrays
        // We initialize encoded key specifications based on the encoding formats
        EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey newPrivateKey = keyFactory.generatePrivate(privateKeySpec);
            PublicKey newPublicKey = keyFactory.generatePublic(publicKeySpec);
            this.keyPair = new KeyPair(newPublicKey,newPrivateKey);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            e.printStackTrace();
        }

    }

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public byte[] getPrivateKeyBytes() {
        return privateKeyBytes;
    }

    public byte[] getPublicKeyBytes() {
        return publicKeyBytes;
    }
}
