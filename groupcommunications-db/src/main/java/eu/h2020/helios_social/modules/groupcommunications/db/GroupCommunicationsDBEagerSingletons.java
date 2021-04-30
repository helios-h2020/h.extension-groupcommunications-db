package eu.h2020.helios_social.modules.groupcommunications.db;

import eu.h2020.helios_social.modules.groupcommunications.db.crypto.CryptoExecutorModule;
import eu.h2020.helios_social.modules.groupcommunications.db.database.DatabaseExecutorModule;
import eu.h2020.helios_social.modules.groupcommunications.db.identity.IdentityModule;
import eu.h2020.helios_social.modules.groupcommunications.db.lifecycle.LifecycleModule;
import eu.h2020.helios_social.modules.groupcommunications.db.system.SystemModule;

public interface GroupCommunicationsDBEagerSingletons {


    void inject(CryptoExecutorModule.EagerSingletons init);

    void inject(DatabaseExecutorModule.EagerSingletons init);

    void inject(IdentityModule.EagerSingletons init);

    void inject(LifecycleModule.EagerSingletons init);

    void inject(SystemModule.EagerSingletons init);


    class Helper {

        public static void injectEagerSingletons(GroupCommunicationsDBEagerSingletons c) {
            c.inject(new CryptoExecutorModule.EagerSingletons());
            c.inject(new DatabaseExecutorModule.EagerSingletons());
            c.inject(new IdentityModule.EagerSingletons());
            c.inject(new LifecycleModule.EagerSingletons());
            c.inject(new SystemModule.EagerSingletons());
        }
    }
}
