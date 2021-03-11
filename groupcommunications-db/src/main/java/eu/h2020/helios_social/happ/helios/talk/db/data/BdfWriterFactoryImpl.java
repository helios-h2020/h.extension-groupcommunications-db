package eu.h2020.helios_social.happ.helios.talk.db.data;

import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfWriter;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfWriterFactory;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.NotNullByDefault;

import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

@Immutable
@NotNullByDefault
class BdfWriterFactoryImpl implements BdfWriterFactory {

	@Override
	public BdfWriter createWriter(OutputStream out) {
		return new BdfWriterImpl(out);
	}
}
