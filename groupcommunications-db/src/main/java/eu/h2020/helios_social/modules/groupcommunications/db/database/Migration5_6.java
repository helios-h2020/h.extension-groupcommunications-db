package eu.h2020.helios_social.modules.groupcommunications.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.modules.groupcommunications.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration5_6 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration5_6.class.getName());

    private final DatabaseTypes dbTypes;

    Migration5_6(DatabaseTypes dbTypes) {
        this.dbTypes = dbTypes;
    }

    @Override
    public int getStartVersion() {
        return 5;
    }

    @Override
    public int getEndVersion() {
        return 6;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute(dbTypes.replaceTypes("CREATE TABLE groupMembers"
                    + " (peerId _STRING NOT NULL,"
                    + " profilePicture BLOB,"
                    + " alias _STRING NOT NULL,"
                    + " groupId _STRING NOT NULL,"
                    + " PRIMARY KEY (peerId, groupId))"));
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
