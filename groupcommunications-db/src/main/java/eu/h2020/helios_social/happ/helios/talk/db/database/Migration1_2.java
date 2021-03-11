package eu.h2020.helios_social.happ.helios.talk.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.happ.helios.talk.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

class Migration1_2 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration1_2.class.getName());

    private final DatabaseTypes dbTypes;

    Migration1_2(DatabaseTypes dbTypes) {
        this.dbTypes = dbTypes;
    }

    @Override
    public int getStartVersion() {
        return 1;
    }

    @Override
    public int getEndVersion() {
        return 2;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute(dbTypes.replaceTypes("CREATE TABLE inverted_index"
                    + " (entity _STRING NOT NULL,"
                    + " key _STRING NOT NULL,"
                    + " value _BINARY NOT NULL,"
                    + " PRIMARY KEY (entity, key))"));
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }

}
