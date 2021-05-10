package eu.h2020.helios_social.modules.groupcommunications.db.crypto;

import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.CryptoComponent;
import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.PasswordStrengthEstimator;
import eu.h2020.helios_social.modules.groupcommunications_utils.system.SecureRandomProvider;

import java.security.SecureRandom;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;

@Module
public class CryptoModule {

	@Provides
	AuthenticatedCipher provideAuthenticatedCipher() {
		return new XSalsa20Poly1305AuthenticatedCipher();
	}

	@Provides
	@Singleton
	CryptoComponent provideCryptoComponent(
			SecureRandomProvider secureRandomProvider,
			ScryptKdf passwordBasedKdf) {
		return new CryptoComponentImpl(secureRandomProvider, passwordBasedKdf);
	}

	@Provides
	PasswordStrengthEstimator providePasswordStrengthEstimator() {
		return new PasswordStrengthEstimatorImpl();
	}

	@Provides
	SecureRandom getSecureRandom(CryptoComponent crypto) {
		return crypto.getSecureRandom();
	}

}
