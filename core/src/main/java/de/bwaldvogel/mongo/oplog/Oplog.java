package de.bwaldvogel.mongo.oplog;

import java.util.List;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public interface Oplog {

    void handleCommand(String databaseName, String command, Document query, List<Object> modifiedIds);
    void handleInsert(String databaseName, Document query);
    void handleUpdate(String databaseName, Document query, List<Object> updatedIds);
    void handleDelete(String databaseName, Document query, List<Object> deletedIds);
}
