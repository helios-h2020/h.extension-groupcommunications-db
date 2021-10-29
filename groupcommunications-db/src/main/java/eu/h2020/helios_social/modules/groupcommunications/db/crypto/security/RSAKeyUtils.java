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

public class RSAKeyUtils {

    public static PublicKey getPublicKeyFromBytes(byte[] publicKeyBytes){

            // Get key pair Objects from their respective byte arrays
            // We initialize encoded key specifications based on the encoding formats
            EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
            try {
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                return keyFactory.generatePublic(publicKeySpec);
            } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                e.printStackTrace();
            }
            return null;
    }

    public PrivateKey getPrivateKeyFromBytes(byte[] privateKeyBytes){

        // Get key pair Objects from their respective byte arrays
        // We initialize encoded key specifications based on the encoding formats
        EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(privateKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            e.printStackTrace();
        }
        return null;
    }
}
