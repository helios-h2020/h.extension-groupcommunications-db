package eu.h2020.helios_social.happ.helios.talk.db.settings;

import eu.h2020.helios_social.modules.groupcommunications_utils.db.DatabaseComponent;
import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;
import eu.h2020.helios_social.modules.groupcommunications_utils.db.Transaction;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.NotNullByDefault;
import eu.h2020.helios_social.modules.groupcommunications_utils.settings.Settings;
import eu.h2020.helios_social.modules.groupcommunications_utils.settings.SettingsManager;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

@Immutable
@NotNullByDefault
class SettingsManagerImpl implements SettingsManager {

	private final DatabaseComponent db;

	@Inject
	SettingsManagerImpl(DatabaseComponent db) {
		this.db = db;
	}

	@Override
	public Settings getSettings(String namespace) throws DbException {
		return db.transactionWithResult(true, txn ->
				db.getSettings(txn, namespace));
	}

	@Override
	public Settings getSettings(Transaction txn, String namespace)
			throws DbException {
		return db.getSettings(txn, namespace);
	}

	@Override
	public void mergeSettings(Settings s, String namespace) throws DbException {
		db.transaction(false, txn -> db.mergeSettings(txn, s, namespace));
	}
}
