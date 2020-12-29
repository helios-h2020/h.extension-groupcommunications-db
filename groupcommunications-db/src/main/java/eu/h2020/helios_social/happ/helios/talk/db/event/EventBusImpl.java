package eu.h2020.helios_social.happ.helios.talk.db.event;

import eu.h2020.helios_social.happ.helios.talk.api.event.Event;
import eu.h2020.helios_social.happ.helios.talk.api.event.EventBus;
import eu.h2020.helios_social.happ.helios.talk.api.event.EventExecutor;
import eu.h2020.helios_social.happ.helios.talk.api.event.EventListener;
import eu.h2020.helios_social.happ.helios.talk.api.nullsafety.NotNullByDefault;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
@NotNullByDefault
class EventBusImpl implements EventBus {

	private final Collection<EventListener> listeners =
			new CopyOnWriteArrayList<>();
	private final Executor eventExecutor;

	@Inject
	EventBusImpl(@EventExecutor Executor eventExecutor) {
		this.eventExecutor = eventExecutor;
	}

	@Override
	public void addListener(EventListener l) {
		listeners.add(l);
	}

	@Override
	public void removeListener(EventListener l) {
		listeners.remove(l);
	}

	@Override
	public void broadcast(Event e) {
		eventExecutor.execute(() -> {
			for (EventListener l : listeners) l.eventOccurred(e);
		});
	}
}
