package de.bwaldvogel.mongo.session;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.oplog.Oplog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class Session {
    private static final Logger log = LoggerFactory.getLogger(Session.class);
    private UUID sessionId;
    private boolean autocommit = true;
    private final Map<String, MongoDatabase> databases = new ConcurrentHashMap<>();
    private Oplog oplog;

    public Session(UUID sessionId, Oplog oplog) {
        this.sessionId = sessionId;
        this.oplog = oplog;
        if (sessionId == null) {
            this.sessionId = UUID.randomUUID();
        }
    }

    public MongoDatabase resolveDatabase(String databaseName, Function<String, MongoDatabase> openOrCreateDatabase) {
        return databases.computeIfAbsent(databaseName, name -> {
            MongoDatabase database = openOrCreateDatabase.apply(databaseName);
            log.info("created database {}", database.getDatabaseName());
            return database;
        });
    }

    public void setOplog(Oplog oplog) {
        this.oplog = oplog;
    }

    public Oplog getOplog() {
        return this.oplog;
    }

    public void startTransaction() {}

    public void commitTransaction() {}

    public void endSession() {}

    public boolean autocommit() {
        return autocommit;
    }

    public UUID sessionId() {
        return sessionId;
    }

    public Session withAutocommit(boolean value) {
        this.autocommit = value;
        return this;
    }

    public void dropDatabase(String databaseName) {
        databases.remove(databaseName);
    }
}
