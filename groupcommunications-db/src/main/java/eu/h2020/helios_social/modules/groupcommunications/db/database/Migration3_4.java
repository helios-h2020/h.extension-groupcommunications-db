package eu.h2020.helios_social.modules.groupcommunications.db.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import eu.h2020.helios_social.modules.groupcommunications.api.exception.DbException;

import static eu.h2020.helios_social.modules.groupcommunications.db.database.JdbcUtils.tryToClose;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Logger.getLogger;

public class Migration3_4 implements Migration<Connection> {

    private static final Logger LOG = getLogger(Migration3_4.class.getName());

    private final DatabaseTypes dbTypes;

    Migration3_4(DatabaseTypes dbTypes) {
        this.dbTypes = dbTypes;
    }

    @Override
    public int getStartVersion() {
        return 3;
    }

    @Override
    public int getEndVersion() {
        return 4;
    }

    @Override
    public void migrate(Connection txn) throws DbException {
        Statement s = null;
        try {
            s = txn.createStatement();
            s.execute("DROP TABLE forumMemberList");
            s.execute(dbTypes.replaceTypes("CREATE TABLE forumMemberList"
                    + " (groupId _STRING NOT NULL,"
                    + " peerId _STRING NOT NULL,"
                    + " fakeId _STRING,"
                    + " fakename _STRING,"
                    + " alias _STRING,"
                    + " role INT NOT NULL,"
                    + " timestamp BIGINT NOT NULL,"
                    + " PRIMARY KEY (groupId, fakeId),"
                    + " FOREIGN KEY (groupId)"
                    + " REFERENCES groups (groupId)"
                    + " ON DELETE CASCADE)"));
        } catch (SQLException e) {
            tryToClose(s, LOG, WARNING);
            throw new DbException(e);
        }
    }
}
