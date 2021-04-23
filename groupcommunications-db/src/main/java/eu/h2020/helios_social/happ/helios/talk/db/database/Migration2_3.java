package eu.h2020.helios_social.happ.helios.talk.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.happ.helios.talk.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration2_3 implements Migration<Connection> {
    private static final Logger LOG = getLogger(Migration2_3.class.getName());

    private final DatabaseTypes dbTypes;

    Migration2_3(DatabaseTypes dbTypes) {
        this.dbTypes = dbTypes;
    }

    @Override
    public int getStartVersion() {
        return 2;
    }

    @Override
    public int getEndVersion() {
        return 3;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute("DROP TABLE inverted_index");
            s.execute(dbTypes.replaceTypes("CREATE TABLE inverted_index"
                    + " (entity _STRING NOT NULL,"
                    + " contextId _STRING NOT NULL,"
                    + " key _STRING NOT NULL,"
                    + " value _BINARY NOT NULL,"
                    + " PRIMARY KEY (entity, contextId, key),"
                    + " FOREIGN KEY (contextId)"
                    + " REFERENCES contexts (contextId)"
                    + " ON DELETE CASCADE)"));
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
