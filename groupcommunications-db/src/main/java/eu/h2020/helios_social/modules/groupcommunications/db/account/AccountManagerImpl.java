package eu.h2020.helios_social.modules.groupcommunications.db.account;

import eu.h2020.helios_social.modules.groupcommunications_utils.account.AccountManager;
import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.CryptoComponent;
import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.DecryptionException;
import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.KeyStrengthener;
import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.SecretKey;
import eu.h2020.helios_social.modules.groupcommunications_utils.db.DatabaseConfig;
import eu.h2020.helios_social.modules.groupcommunications_utils.identity.Identity;
import eu.h2020.helios_social.modules.groupcommunications_utils.identity.IdentityManager;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.MethodsNotNullByDefault;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.ParametersNotNullByDefault;
import eu.h2020.helios_social.modules.groupcommunications_utils.util.IoUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import static java.util.logging.Level.WARNING;
import static eu.h2020.helios_social.modules.groupcommunications_utils.crypto.DecryptionResult.INVALID_CIPHERTEXT;
import static eu.h2020.helios_social.modules.groupcommunications_utils.util.LogUtils.logException;
import static eu.h2020.helios_social.modules.groupcommunications_utils.util.StringUtils.fromHexString;
import static eu.h2020.helios_social.modules.groupcommunications_utils.util.StringUtils.toHexString;

@MethodsNotNullByDefault
@ParametersNotNullByDefault
public class AccountManagerImpl implements AccountManager {

	private static final Logger LOG =
			Logger.getLogger(AccountManagerImpl.class.getName());

	private static final String DB_KEY_FILENAME = "db.key";
	private static final String DB_KEY_BACKUP_FILENAME = "db.key.bak";

	private final DatabaseConfig databaseConfig;
	private final CryptoComponent crypto;
	private final IdentityManager identityManager;
	private final File dbKeyFile, dbKeyBackupFile;

	protected final Object stateChangeLock = new Object();
	protected String userPassword;
	@Nullable
	private volatile SecretKey databaseKey = null;

	@Inject
	public AccountManagerImpl(DatabaseConfig databaseConfig,
			CryptoComponent crypto,
			IdentityManager identityManager) {
		this.databaseConfig = databaseConfig;
		this.crypto = crypto;
		this.identityManager = identityManager;
		File keyDir = databaseConfig.getDatabaseKeyDirectory();
		dbKeyFile = new File(keyDir, DB_KEY_FILENAME);
		dbKeyBackupFile = new File(keyDir, DB_KEY_BACKUP_FILENAME);
	}

	@Override
	public boolean hasDatabaseKey() {
		return databaseKey != null;
	}

	@Override
	@Nullable
	public SecretKey getDatabaseKey() {
		return databaseKey;
	}

	// Package access for testing
	@GuardedBy("stateChangeLock")
	@Nullable
	String loadEncryptedDatabaseKey() {
		String key = readDbKeyFromFile(dbKeyFile);
		if (key == null) {
			LOG.info("No database key in primary file");
			key = readDbKeyFromFile(dbKeyBackupFile);
			if (key == null) LOG.info("No database key in backup file");
			else LOG.warning("Found database key in backup file");
		} else {
			LOG.info("Found database key in primary file");
		}
		return key;
	}

	@GuardedBy("stateChangeLock")
	@Nullable
	private String readDbKeyFromFile(File f) {
		if (!f.exists()) {
			LOG.info("Key file does not exist");
			return null;
		}
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(f), Charset.forName("UTF-8")));
			String key = reader.readLine();
			reader.close();
			return key;
		} catch (IOException e) {
			logException(LOG, WARNING, e);
			return null;
		}
	}

	// Package access for testing
	@GuardedBy("stateChangeLock")
	boolean storeEncryptedDatabaseKey(String hex) {
		LOG.info("Storing database key in file");
		// Create the directory if necessary
		if (databaseConfig.getDatabaseKeyDirectory().mkdirs())
			LOG.info("Created database key directory");
		// If only the backup file exists, rename it so we don't overwrite it
		if (dbKeyBackupFile.exists() && !dbKeyFile.exists()) {
			if (dbKeyBackupFile.renameTo(dbKeyFile))
				LOG.info("Renamed old backup");
			else LOG.warning("Failed to rename old backup");
		}
		try {
			// Write to the backup file
			writeDbKeyToFile(hex, dbKeyBackupFile);
			LOG.info("Stored database key in backup file");
			// Delete the old primary file, if it exists
			if (dbKeyFile.exists()) {
				if (dbKeyFile.delete()) LOG.info("Deleted primary file");
				else LOG.warning("Failed to delete primary file");
			}
			// The backup file becomes the new primary
			if (dbKeyBackupFile.renameTo(dbKeyFile)) {
				LOG.info("Renamed backup file to primary");
			} else {
				LOG.warning("Failed to rename backup file to primary");
				return false; // Don't overwrite our only copy
			}
			// Write a second copy to the backup file
			writeDbKeyToFile(hex, dbKeyBackupFile);
			LOG.info("Stored second copy of database key in backup file");
			return true;
		} catch (IOException e) {
			logException(LOG, WARNING, e);
			return false;
		}
	}

	@GuardedBy("stateChangeLock")
	private void writeDbKeyToFile(String key, File f) throws IOException {
		FileOutputStream out = new FileOutputStream(f);
		out.write(key.getBytes(Charset.forName("UTF-8")));
		out.flush();
		out.close();
	}

	@Override
	public boolean accountExists() {
		synchronized (stateChangeLock) {
			return loadEncryptedDatabaseKey() != null;
		}
	}

	@Override
	public boolean createAccount(String name, String password) {
		synchronized (stateChangeLock) {
			if (hasDatabaseKey())
				throw new AssertionError("Already have a database key");
			Identity identity =
					identityManager
							.createIdentity(UUID.randomUUID().toString(), name);
			identityManager.registerIdentity(identity);
			SecretKey key = crypto.generateSecretKey();
			if (!encryptAndStoreDatabaseKey(key, password)) return false;
			databaseKey = key;
			return true;
		}
	}

	@GuardedBy("stateChangeLock")
	private boolean encryptAndStoreDatabaseKey(SecretKey key, String password) {
		byte[] plaintext = key.getBytes();
		byte[] ciphertext = crypto.encryptWithPassword(plaintext, password,
				databaseConfig.getKeyStrengthener());
		return storeEncryptedDatabaseKey(toHexString(ciphertext));
	}

	@Override
	public void deleteAccount() {
		synchronized (stateChangeLock) {
			LOG.info("Deleting account");
			IoUtils.deleteFileOrDir(databaseConfig.getDatabaseKeyDirectory());
			IoUtils.deleteFileOrDir(databaseConfig.getDatabaseDirectory());
			databaseKey = null;
		}
	}

	@Override
	public void signIn(String password) throws DecryptionException {
		synchronized (stateChangeLock) {
			userPassword = password;
			databaseKey = loadAndDecryptDatabaseKey(password);
			LOG.info("databasekey"+databaseKey);
		}
	}

	@GuardedBy("stateChangeLock")
	private SecretKey loadAndDecryptDatabaseKey(String password)
			throws DecryptionException {
		String hex = loadEncryptedDatabaseKey();
		if (hex == null) {
			LOG.warning("Failed to load encrypted database key");
			throw new DecryptionException(INVALID_CIPHERTEXT);
		}
		byte[] ciphertext = fromHexString(hex);
		KeyStrengthener keyStrengthener = databaseConfig.getKeyStrengthener();
		byte[] plaintext = crypto.decryptWithPassword(ciphertext, password,
				keyStrengthener);
		SecretKey key = new SecretKey(plaintext);
		// If the DB key was encrypted with a weak key and a key strengthener
		// is now available, re-encrypt the DB key with a strengthened key
		if (keyStrengthener != null &&
				!crypto.isEncryptedWithStrengthenedKey(ciphertext)) {
			LOG.info("Re-encrypting database key with strengthened key");
			encryptAndStoreDatabaseKey(key, password);
		}
		return key;
	}

	@Override
	public void changePassword(String oldPassword, String newPassword)
			throws DecryptionException {
		synchronized (stateChangeLock) {
			SecretKey key = loadAndDecryptDatabaseKey(oldPassword);
			encryptAndStoreDatabaseKey(key, newPassword);
		}
	}

	@Override
	public String getUserPassword() {
		return userPassword;
	}

	@Override
	public void setUserPassword(String password) {
		userPassword = password;
	}
}
