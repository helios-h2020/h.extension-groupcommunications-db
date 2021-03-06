package eu.h2020.helios_social.modules.groupcommunications.db.account;

import eu.h2020.helios_social.modules.groupcommunications_utils.account.AccountManager;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;

@Module
public class AccountModule {

	@Provides
	@Singleton
	AccountManager provideAccountManager(AccountManagerImpl accountManager) {
		return accountManager;
	}
}
