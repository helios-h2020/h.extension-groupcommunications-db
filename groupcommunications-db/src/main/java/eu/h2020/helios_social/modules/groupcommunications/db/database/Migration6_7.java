package eu.h2020.helios_social.modules.groupcommunications.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.modules.groupcommunications.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration6_7 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration6_7.class.getName());

    private final DatabaseTypes dbTypes;

    Migration6_7(DatabaseTypes dbTypes) {
        this.dbTypes = dbTypes;
    }

    @Override
    public int getStartVersion() {
        return 6;
    }

    @Override
    public int getEndVersion() {
        return 7;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute(dbTypes.replaceTypes("CREATE TABLE groupAccessRequests"
                    + " (contactId _STRING NOT NULL,"
                    + " contextId _STRING NOT NULL,"
                    + " pendingGroupId _STRING NOT NULL,"
                    + " name _STRING NOT NULL,"
                    + " type INT NOT NULL,"
                    + " incoming BOOLEAN NOT NULL,"
                    + " timestamp BIGINT NOT NULL,"
                    + " peerName _STRING NOT NULL,"
                    + " PRIMARY KEY (contactId, pendingGroupId),"
                    + " FOREIGN KEY (contextId)"
                    + " REFERENCES contexts (contextId)"
                    + " ON DELETE CASCADE)"));
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
