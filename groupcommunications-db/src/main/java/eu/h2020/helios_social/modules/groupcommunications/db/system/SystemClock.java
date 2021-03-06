package eu.h2020.helios_social.modules.groupcommunications.db.system;

import eu.h2020.helios_social.modules.groupcommunications_utils.system.Clock;

/**
 * Default clock implementation.
 */
public class SystemClock implements Clock {

	@Override
	public long currentTimeMillis() {
		return System.currentTimeMillis();
	}

	@Override
	public void sleep(long milliseconds) throws InterruptedException {
		Thread.sleep(milliseconds);
	}
}
