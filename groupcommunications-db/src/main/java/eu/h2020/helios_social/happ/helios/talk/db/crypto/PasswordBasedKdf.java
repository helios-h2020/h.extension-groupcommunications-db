package eu.h2020.helios_social.happ.helios.talk.db.crypto;

import eu.h2020.helios_social.modules.groupcommunications_utils.crypto.SecretKey;

interface PasswordBasedKdf {

	int chooseCostParameter();

	SecretKey deriveKey(String password, byte[] salt, int cost);
}
