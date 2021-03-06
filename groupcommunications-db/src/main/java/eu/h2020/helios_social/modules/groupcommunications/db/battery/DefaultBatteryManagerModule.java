package eu.h2020.helios_social.modules.groupcommunications.db.battery;

import eu.h2020.helios_social.modules.groupcommunications_utils.battery.BatteryManager;

import dagger.Module;
import dagger.Provides;

/**
 * Provides a default implementation of {@link BatteryManager} for systems
 * without batteries.
 */
@Module
public class DefaultBatteryManagerModule {

	@Provides
	BatteryManager provideBatteryManager() {
		return () -> false;
	}
}
