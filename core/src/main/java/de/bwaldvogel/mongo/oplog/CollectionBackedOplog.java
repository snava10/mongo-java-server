package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class CollectionBackedOplog extends AbstractOplog {

    private final MongoCollection<Document> collection;
    public CollectionBackedOplog(Clock clock, MongoCollection<Document> collection) {
        super(clock);
        this.collection = collection;
    }

    @Override
    public void handleInsert(String databaseName, Document query) {
        Instant instant = clock.instant();
        List<Document> documents = (List<Document>) query.get("documents");
        List<Document> oplogDocuments = documents.stream().map(d ->
            new OplogDocument()
                .withTimestamp(new BsonTimestamp(instant.toEpochMilli()))
                .withWall(instant)
                .withOperationType(OperationType.INSERT)
                .withOperationDocument(d.clone())
                .withNamespace(String.format("%s.%s", databaseName, query.get("insert")))
                .asDocument()).collect(Collectors.toList());
        collection.insertDocuments(oplogDocuments);
    }

    @Override
    public void handleUpdate(String databaseName, Document query, List<Object> updatedIds) {
        Instant instant = clock.instant();
        Document updateDoc = ((List<Document>) query.get("updates")).get(0);
        List<Document> oplogDocuments = updatedIds.stream().map(id ->
            new OplogDocument()
                .withTimestamp(new BsonTimestamp(instant.toEpochMilli()))
                .withWall(instant)
                .withOperationType(OperationType.UPDATE)
                .withOperationDocument(buildUpdateOperationDocument(updateDoc))
                .withNamespace(String.format("%s.%s", databaseName, query.get("update")))
                .withAdditionalOperationalDocument(new Document("_id", id))
                .asDocument()).collect(Collectors.toList());
        collection.insertDocuments(oplogDocuments);
    }

    @Override
    public void handleDelete(String databaseName, Document query, List<Object> deletedIds) {
    }

    private Document buildUpdateOperationDocument(Document document) {
        Document mergedDoc = Utils.mergeUpdateDocuments(document);
        if (mergedDoc.containsKey("_id")) {
            // This is a result of a replace one
            mergedDoc.remove("_id");
            return new Document("$set", mergedDoc);
        }

        if (mergedDoc.containsKey("$unset")) {
            if (mergedDoc.get("$unset").getClass().equals(Document.class)) {
                Document doc = (Document)mergedDoc.get("$unset");
                doc.keySet().forEach(k -> doc.put(k, true));
            }
        }
        return mergedDoc;
    }
}
