package eu.h2020.helios_social.modules.groupcommunications.db.database;

import eu.h2020.helios_social.modules.groupcommunications_utils.db.DatabaseComponent;
import eu.h2020.helios_social.modules.groupcommunications_utils.db.DatabaseConfig;
import eu.h2020.helios_social.modules.groupcommunications_utils.db.TransactionManager;
import eu.h2020.helios_social.modules.groupcommunications_utils.sync.event.EventBus;
import eu.h2020.helios_social.modules.groupcommunications_utils.sync.event.EventExecutor;
import eu.h2020.helios_social.modules.groupcommunications_utils.lifecycle.ShutdownManager;
import eu.h2020.helios_social.modules.groupcommunications_utils.system.Clock;

import java.sql.Connection;
import java.util.concurrent.Executor;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;

@Module
public class DatabaseModule {

	@Provides
	@Singleton
	Database<Connection> provideDatabase(DatabaseConfig config,
			Clock clock) {
		return new H2Database(config, clock);
	}

	@Provides
	@Singleton
	DatabaseComponent provideDatabaseComponent(Database<Connection> db,
			EventBus eventBus, @EventExecutor Executor eventExecutor,
			ShutdownManager shutdownManager) {
		return new DatabaseComponentImpl<>(db, Connection.class, eventBus,
				eventExecutor, shutdownManager);
	}

	@Provides
	TransactionManager provideTransactionManager(DatabaseComponent db) {
		return db;
	}
}
