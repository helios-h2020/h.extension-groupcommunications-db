package eu.h2020.helios_social.modules.groupcommunications.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.modules.groupcommunications.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration4_5 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration4_5.class.getName());

    Migration4_5() {

    }

    @Override
    public int getStartVersion() {
        return 4;
    }

    @Override
    public int getEndVersion() {
        return 5;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute("ALTER TABLE messages"
                              + " ADD CONSTRAINT contextId"
                              + " FOREIGN KEY (contextId)"
                              + " REFERENCES contexts (contextId)"
                              + " ON DELETE CASCADE");
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
