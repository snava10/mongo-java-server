package de.bwaldvogel.mongo.oplog;

import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.ADDITIONAL_OPERATION_DOCUMENT;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.HASH;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.NAMESPACE;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.OPERATION_DOCUMENT;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.OPERATION_TYPE;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.PROTOCOL_VERSION;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.T;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.TIMESTAMP;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.WALL;

import java.time.Instant;
import java.util.UUID;

import de.bwaldvogel.mongo.bson.Bson;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class OplogDocument implements Bson {
    private final Document document;

    public OplogDocument() {
        document = new Document();
        withProtocolVersion(2L);
    }

    public OplogDocument(Document document) {
        this.document = document.clone();
    }

    public Document asDocument() {
        return document;
    }

    public BsonTimestamp getTimestamp() {
        return (BsonTimestamp) document.get(TIMESTAMP.getCode());
    }

    public OplogDocument withTimestamp(BsonTimestamp timestamp) {
        document.put(TIMESTAMP.getCode(), timestamp);
        return this;
    }

    public long getT() {
        return (Long) document.get(T.getCode());
    }

    public OplogDocument withT(long t) {
        document.put(T.getCode(), t);
        return this;
    }

    public long getHash() {
        return (Long) document.get(HASH.getCode());
    }

    public OplogDocument withHash(long h) {
        document.put(HASH.getCode(), h);
        return this;
    }

    public long getProtocolVersion() {
        return (Long) document.get(PROTOCOL_VERSION.getCode());
    }

    public OplogDocument withProtocolVersion(long protocolVersion) {
        document.put(PROTOCOL_VERSION.getCode(), protocolVersion);
        return this;
    }

    public OperationType getOperationType() {
        return OperationType.valueOf((String) document.get(OPERATION_TYPE.getCode()));
    }

    public OplogDocument withOperationType(OperationType operationType) {
        document.put(OPERATION_TYPE.getCode(), operationType.getCode());
        return this;
    }

    public String getNamespace() {
        return (String) document.get(NAMESPACE.getCode());
    }

    public OplogDocument withNamespace(String namespace) {
        document.put(NAMESPACE.getCode(), namespace);
        return this;
    }

    public UUID getUUID() {
        return (UUID) document.get(OplogDocumentFieldName.UUID.getCode());
    }

    public OplogDocument withUUID(UUID uuid) {
        document.put(OplogDocumentFieldName.UUID.getCode(), uuid);
        return this;
    }

    public Instant getWall() {
        return (Instant) document.get(WALL.getCode());
    }

    public OplogDocument withWall(Instant wall) {
        document.put(WALL.getCode(), wall);
        return this;
    }

    public Document getOperationDocument() {
        return (Document) document.get(OPERATION_DOCUMENT.getCode());
    }

    public OplogDocument withOperationDocument(Document operationDocument) {
        document.put(OPERATION_DOCUMENT.getCode(), operationDocument);
        return this;
    }

    public Document getAdditionalOperationDocument() {
        return (Document) document.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
    }

    public OplogDocument withAdditionalOperationalDocument(Document operationalDocument) {
        document.put(ADDITIONAL_OPERATION_DOCUMENT.getCode(), operationalDocument);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof OplogDocument)) {
            return false;
        }
        OplogDocument other = (OplogDocument)o;
        return asDocument().equals(other.asDocument());
    }

    @Override
    public int hashCode() {
        return asDocument().hashCode();
    }
}
