package de.bwaldvogel.mongo.backend;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.ADDITIONAL_OPERATION_DOCUMENT;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.NAMESPACE;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.OPERATION_DOCUMENT;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.OPERATION_TYPE;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.PROTOCOL_VERSION;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.TIMESTAMP;
import static de.bwaldvogel.mongo.oplog.OplogDocumentFieldName.WALL;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.oplog.OperationType;

public abstract class AbstractBackendOplogEnabledTest extends AbstractBackendTest {

    @Override
    protected void setUpBackend() throws Exception {
        MongoBackend backend = createBackend();
        backend.setClock(TEST_CLOCK);
        mongoServer = new MongoServer(backend).withOplogEnabled();
        serverAddress = mongoServer.bind();
    }

    @Test
    public void testSimpleOplogInsert() {
        Document doc = new Document("name", "testUser1");
        collection.insertOne(doc);
        Document oplogDoc = oplogCollection.find().first();
        assertThat(oplogDoc).isNotNull();
        assertThat(oplogDoc.get(TIMESTAMP.getCode())).isNotNull();
        assertThat(oplogDoc.get(WALL.getCode())).isNotNull();
        assertThat(oplogDoc.get(ADDITIONAL_OPERATION_DOCUMENT.getCode())).isNull();
        assertThat(oplogDoc.get(PROTOCOL_VERSION.getCode())).isEqualTo(2L);
        assertThat(oplogDoc.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().getFullName());
        assertThat(oplogDoc.get(OPERATION_TYPE.getCode())).isEqualTo(OperationType.INSERT.getCode());
        assertThat(oplogDoc.get(OPERATION_DOCUMENT.getCode())).isEqualTo(doc);
    }

    @Test
    public void testSetOplogReplaceOneById() {
        collection.insertOne(json("_id: 1, b: 6"));
        Document updatedDocument = json("a: 5, b: 7");
        collection.replaceOne(json("_id: 1"), updatedDocument);
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());
        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testSetOplogUpdateOneById() {
        collection.insertOne(json("_id: 34, b: 6"));
        Document updatedDocument = json("a: 6");
        collection.updateOne(eq("_id", 34), set("a", 6));

        List<Document> oplogs = new ArrayList<>();

        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());

        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(34);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testSetOplogUpdateOneManyFieldsUsingDriverHelpers() {
        collection.insertOne(json("_id: 1, b: 6"));
        Document updatedDocument = json("a: 7, b: 7");
        collection.updateOne(eq("_id", 1), Arrays.asList(set("a", 7), set("b", 7)));

        List<Document> oplogs = new ArrayList<>();

        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());

        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testSetOplogUpdateOneFilteringByOtherThanId() {
        collection.insertOne(json("_id: 37, b: 6"));
        Document updatedDocument = json("a: 7, b: 7");
        collection.updateOne(eq("b", 6), Arrays.asList(set("a", 7), set("b", 7)));

        List<Document> oplogs = new ArrayList<>();

        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());

        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(37);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testSetUpdateManyUpdatedIdsShouldBeReflectedInOplog() {
        collection.insertMany(Arrays.asList(json("_id: 37, b: 6"), json("_id: 41, b: 7")));
        Document updatedDocument = json("a: 7, b: 7");
        collection.updateMany(or(eq("b", 6), eq("b", 7)), Arrays.asList(set("a", 7), set("b", 7)));
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(4);

        List<Object> updatedIds = oplogs.stream().skip(2).map(d -> ((Document) d.get(ADDITIONAL_OPERATION_DOCUMENT.getCode())).get("_id"))
            .collect(Collectors.toList());

        assertThat(updatedIds).containsExactly(37, 41);
    }

    @Test
    public void testSetMultipleUpdatesInARow() {
        Document doc = json("_id: 34, b: 6");
        collection.insertOne(doc);
        collection.updateOne(eq("_id", 34), set("a", 6));
        collection.updateOne(eq("_id", 34), set("b", 7));

        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);

        assertThat(oplogs.size()).isEqualTo(3);
        assertThat(oplogs.get(0).get(OPERATION_TYPE.getCode())).isEqualTo("i");
        assertThat(oplogs.get(0).get(OPERATION_DOCUMENT.getCode())).isEqualTo(doc);
        assertThat(oplogs.get(1).get(OPERATION_TYPE.getCode())).isEqualTo("u");
        assertThat(oplogs.get(1).get(OPERATION_DOCUMENT.getCode())).isEqualTo(json("$set: {a: 6}"));
        assertThat(oplogs.get(2).get(OPERATION_TYPE.getCode())).isEqualTo("u");
        assertThat(oplogs.get(2).get(OPERATION_DOCUMENT.getCode())).isEqualTo(json("$set: {b: 7}"));
    }

    @Test
    public void testUnsetOplogById() {
        collection.insertOne(json("_id: 1, b: 6"));
        collection.updateOne(json("_id: 1"), unset("b"));
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);

        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);

        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());
        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$unset")).isEqualTo(json("b: true"));

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testMultipleUnsetOplogById() {
        collection.insertOne(json("_id: 1, b: 6, a: 4"));
        collection.updateOne(json("_id: 1"), combine(unset("b"), unset("a")));
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);

        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);

        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());
        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$unset")).isEqualTo(json("b: true, a: true"));

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testMultipleSetUnsetOplogById() {
        collection.insertOne(json("_id: 1, b: 6, a: 4, c: 7, d: 8"));
        collection.updateOne(json("_id: 1"), combine(set("b", 5), set("a",5), unset("c"), unset("d")));
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);

        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);

        OperationType op = OperationType.fromCode(updateOplogEntry.get(OPERATION_TYPE.getCode()).toString());
        Document o2 = (Document) updateOplogEntry.get(ADDITIONAL_OPERATION_DOCUMENT.getCode());
        Document o = (Document) updateOplogEntry.get(OPERATION_DOCUMENT.getCode());
        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$set")).isEqualTo(json("b: 5, a: 5"));
        assertThat(o.get("$unset")).isEqualTo(json("c: true, d: true"));

        assertThat(updateOplogEntry.get(NAMESPACE.getCode())).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get(TIMESTAMP.getCode());
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get(WALL.getCode());
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    @Disabled
    public void testFindOneAndReplace() {
        collection.insertMany(Arrays.asList(json("_id: 37, b: 6"), json("_id: 41, b: 7")));
        collection.findOneAndReplace(or(eq("b", 6), eq("b", 7)), json("_id: 37, b: 123"));
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        // Todo Assertions
    }

}
