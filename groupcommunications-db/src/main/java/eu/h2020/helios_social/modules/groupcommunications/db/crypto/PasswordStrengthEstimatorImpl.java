package eu.h2020.helios_social.modules.groupcommunications.db.crypto;

import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.PasswordStrengthEstimator;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.NotNullByDefault;

import java.util.HashSet;

import javax.annotation.concurrent.Immutable;

@Immutable
@NotNullByDefault
class PasswordStrengthEstimatorImpl implements PasswordStrengthEstimator {

	// The minimum number of unique characters in a strong password
	private static final int STRONG_UNIQUE_CHARS = 12;

	@Override
	public float estimateStrength(String password) {
		HashSet<Character> unique = new HashSet<>();
		int length = password.length();
		for (int i = 0; i < length; i++) unique.add(password.charAt(i));
		return Math.min(1, (float) unique.size() / STRONG_UNIQUE_CHARS);
	}
}
