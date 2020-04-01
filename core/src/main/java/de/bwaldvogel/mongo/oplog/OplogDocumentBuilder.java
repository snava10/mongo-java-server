package de.bwaldvogel.mongo.oplog;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

public class OplogDocumentBuilder {

    private OplogDocument oplogDocument;
    OplogDocumentBuilder() {
        this.oplogDocument = new OplogDocument();
    }

    public OplogDocumentBuilder timestamp(BsonTimestamp timestamp) {
        oplogDocument.setTs(timestamp);
        return this;
    }

    public OplogDocumentBuilder time(long t) {
        oplogDocument.setT(t);
        return this;
    }

    public OplogDocumentBuilder documentHash(long hash) {
        oplogDocument.setH(hash);
        return this;
    }

    public OplogDocumentBuilder version(long version) {
        oplogDocument.setV(version);
        return this;
    }

    public OplogDocumentBuilder operationType(OperationType operationType) {
        oplogDocument.setOp(operationType);
        return this;
    }

    public OplogDocumentBuilder namespace(String namespace) {
        oplogDocument.setNs(namespace);
        return this;
    }

    public OplogDocumentBuilder uuid(UUID uuid) {
        oplogDocument.setUi(uuid);
        return this;
    }

    public OplogDocumentBuilder wall(LocalDate wall) {
        oplogDocument.setWall(wall);
        return this;
    }

    public OplogDocumentBuilder document(Document doc) {
        oplogDocument.setO(doc);
        return this;
    }

    public OplogDocumentBuilder updatedDocument(Document doc) {
        oplogDocument.setO2(doc);
        return this;
    }

    public OplogDocument build() {
        return oplogDocument;
    }

    public OplogDocumentBuilder oplogDocument(Document doc) {
        oplogDocument = new OplogDocument();
        oplogDocument.setTs(Optional.ofNullable((BsonTimestamp)doc.get("ts")).orElse(null));
        oplogDocument.setT(Optional.ofNullable((Long)doc.get("l")).orElse(0L));
        oplogDocument.setH(Optional.ofNullable((Long)doc.get("h")).orElse(0L));
        oplogDocument.setV(Optional.ofNullable((Long)doc.get("v")).orElse(0L));
        oplogDocument.setOp(Optional.ofNullable((String)doc.get("op")).map(OperationType::valueOf).orElse(null));
        oplogDocument.setNs(Optional.ofNullable((String)doc.get("ns")).orElse(null));
        oplogDocument.setUi(Optional.ofNullable((UUID)doc.get("ui")).orElse(null));
        oplogDocument.setWall(Optional.ofNullable((LocalDate)doc.get("wall")).orElse(null));
        oplogDocument.setO2(Optional.ofNullable((Document)doc.get("o2")).orElse(null));
        oplogDocument.setO(Optional.ofNullable((Document)doc.get("o")).orElse(null));
        return this;
    }
}
