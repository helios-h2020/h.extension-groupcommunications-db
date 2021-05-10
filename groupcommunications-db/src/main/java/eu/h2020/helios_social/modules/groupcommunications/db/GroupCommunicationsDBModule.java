package eu.h2020.helios_social.modules.groupcommunications.db;

import dagger.Module;
import eu.h2020.helios_social.modules.groupcommunications.db.crypto.CryptoExecutorModule;
import eu.h2020.helios_social.modules.groupcommunications.db.crypto.CryptoModule;
import eu.h2020.helios_social.modules.groupcommunications.db.data.DataModule;
import eu.h2020.helios_social.modules.groupcommunications.db.database.DatabaseExecutorModule;
import eu.h2020.helios_social.modules.groupcommunications.db.database.DatabaseModule;
import eu.h2020.helios_social.modules.groupcommunications.db.event.EventModule;
import eu.h2020.helios_social.modules.groupcommunications.db.identity.IdentityModule;
import eu.h2020.helios_social.modules.groupcommunications.db.lifecycle.LifecycleModule;
import eu.h2020.helios_social.modules.groupcommunications.db.settings.SettingsModule;
import eu.h2020.helios_social.modules.groupcommunications.db.system.SystemModule;

@Module(includes = {
        CryptoModule.class,
        CryptoExecutorModule.class,
        DataModule.class,
        DatabaseModule.class,
        DatabaseExecutorModule.class,
        EventModule.class,
        IdentityModule.class,
        LifecycleModule.class,
        SettingsModule.class,
        SystemModule.class,
})
public class GroupCommunicationsDBModule {
}
