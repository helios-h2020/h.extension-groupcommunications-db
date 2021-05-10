package eu.h2020.helios_social.modules.groupcommunications.db.data;

import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfReaderFactory;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfWriterFactory;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.Encoder;

import dagger.Module;
import dagger.Provides;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.Parser;

@Module
public class DataModule {

	@Provides
	BdfReaderFactory provideBdfReaderFactory() {
		return new BdfReaderFactoryImpl();
	}

	@Provides
	BdfWriterFactory provideBdfWriterFactory() {
		return new BdfWriterFactoryImpl();
	}

	@Provides
	Parser provideMetaDataParser(BdfReaderFactory bdfReaderFactory) {
		return new ParserImpl(bdfReaderFactory);
	}

	@Provides
	Encoder provideMetaDataEncoder(BdfWriterFactory bdfWriterFactory) {
		return new EncoderImpl(bdfWriterFactory);
	}

}
