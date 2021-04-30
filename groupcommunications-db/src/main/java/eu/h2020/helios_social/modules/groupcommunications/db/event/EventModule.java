package eu.h2020.helios_social.modules.groupcommunications.db.event;

import eu.h2020.helios_social.modules.groupcommunications_utils.sync.event.EventBus;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;

@Module
public class EventModule {

	@Provides
	@Singleton
	EventBus provideEventBus(EventBusImpl eventBus) {
		return eventBus;
	}
}
