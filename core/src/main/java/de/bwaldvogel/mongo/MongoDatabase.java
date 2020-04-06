package de.bwaldvogel.mongo;

import java.util.Map;

import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoDatabase {

    String getDatabaseName();

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String command, Document query);

    QueryResult<Document> handleQuery(MongoQuery query);

    QueryResult<Document> handleGetMore(MongoGetMore getMore);

    void handleInsert(MongoInsert insert);

    void handleDelete(MongoDelete delete);

    void handleUpdate(MongoUpdate update);

    boolean isEmpty();

    MongoCollection<?> resolveCollection(String collectionName, boolean throwIfNotFound);

    void drop();

    void dropCollection(String collectionName);

    void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName);

    void unregisterCollection(String collectionName);
//
//    Cursor getCursorById(long cursorId);
//
//    void saveCursor(Cursor cursor);

}
