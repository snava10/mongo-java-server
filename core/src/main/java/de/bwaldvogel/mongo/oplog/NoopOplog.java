package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public final class NoopOplog extends AbstractOplog {

    private static final NoopOplog INSTANCE = new NoopOplog(Clock.systemDefaultZone());

    public static NoopOplog get() {
        return INSTANCE;
    }

    private NoopOplog(Clock clock) {
        super(clock);
    }

    @Override
    public void handleInsert(String databaseName, Document query) {
    }

    @Override
    public void handleUpdate(String databaseName, Document query, List<Object> updatedIds) {
    }

    @Override
    public void handleDelete(String databaseName, Document query, List<Object> deletedIds) {
    }

}
