package eu.h2020.helios_social.modules.groupcommunications.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.modules.groupcommunications.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration8_9 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration8_9.class.getName());

    private final DatabaseTypes dbTypes;

    Migration8_9(DatabaseTypes dbTypes) {
        this.dbTypes = dbTypes;
    }

    @Override
    public int getStartVersion() {
        return 8;
    }

    @Override
    public int getEndVersion() {
        return 9;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute(dbTypes.replaceTypes("CREATE TABLE crypto_keys"
                    + " (privateKey BLOB,"
                    + " publicKey BLOB)"));

            s.execute(dbTypes.replaceTypes("ALTER TABLE contacts ADD COLUMN publicKey BLOB"));

            s.execute(dbTypes.replaceTypes("ALTER TABLE pendingContacts ADD COLUMN publicKey BLOB"));



        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
