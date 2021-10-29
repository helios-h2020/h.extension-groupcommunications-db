package eu.h2020.helios_social.modules.groupcommunications.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.modules.groupcommunications.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration7_8 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration7_8.class.getName());

    Migration7_8() {

    }

    @Override
    public int getStartVersion() {
        return 7;
    }

    @Override
    public int getEndVersion() {
        return 8;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute("DELETE FROM groupMembers WHERE NOT EXISTS ( SELECT * FROM groups AS T1 WHERE T1.groupId = groupMembers.groupId)");
            s.execute("ALTER TABLE groupMembers"
                    + " ADD CONSTRAINT groupId"
                    + " FOREIGN KEY (groupId)"
                    + " REFERENCES groups (groupId)"
                    + " ON DELETE CASCADE");
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
