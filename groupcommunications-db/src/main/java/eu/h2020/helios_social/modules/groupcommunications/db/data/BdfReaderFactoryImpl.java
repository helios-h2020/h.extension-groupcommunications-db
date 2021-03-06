package eu.h2020.helios_social.modules.groupcommunications.db.data;

import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfReader;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfReaderFactory;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.NotNullByDefault;

import java.io.InputStream;

import javax.annotation.concurrent.Immutable;

import static eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfReader.DEFAULT_MAX_BUFFER_SIZE;
import static eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfReader.DEFAULT_NESTED_LIMIT;

@Immutable
@NotNullByDefault
class BdfReaderFactoryImpl implements BdfReaderFactory {

	@Override
	public BdfReader createReader(InputStream in) {
		return new BdfReaderImpl(in, DEFAULT_NESTED_LIMIT,
				DEFAULT_MAX_BUFFER_SIZE);
	}

	@Override
	public BdfReader createReader(InputStream in, int nestedLimit,
			int maxBufferSize) {
		return new BdfReaderImpl(in, nestedLimit, maxBufferSize);
	}
}
