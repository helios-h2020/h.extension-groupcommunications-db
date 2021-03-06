package eu.h2020.helios_social.modules.groupcommunications.db.system;

import eu.h2020.helios_social.modules.groupcommunications_utils.lifecycle.LifecycleManager;
import eu.h2020.helios_social.modules.groupcommunications_utils.system.Clock;
import eu.h2020.helios_social.modules.groupcommunications_utils.system.Scheduler;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.inject.Inject;
import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;

@Module
public class SystemModule {

	public static class EagerSingletons {
		@Inject
		@Scheduler
		ScheduledExecutorService scheduledExecutorService;
	}

	private final ScheduledExecutorService scheduler;

	public SystemModule() {
		// Discard tasks that are submitted during shutdown
		RejectedExecutionHandler policy =
				new ScheduledThreadPoolExecutor.DiscardPolicy();
		scheduler = new ScheduledThreadPoolExecutor(1, policy);
	}

	@Provides
	Clock provideClock() {
		return new SystemClock();
	}

	@Provides
	@Singleton
	@Scheduler
	ScheduledExecutorService provideScheduledExecutorService(
			LifecycleManager lifecycleManager) {
		lifecycleManager.registerForShutdown(scheduler);
		return scheduler;
	}
}
