package de.bwaldvogel.mongo.backend;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.set;
import static de.bwaldvogel.mongo.backend.TestUtils.json;

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
        assertThat(oplogDoc.get("ts")).isNotNull();
        assertThat(oplogDoc.get("wall")).isNotNull();
        assertThat(oplogDoc.get("o2")).isNull();
        assertThat(oplogDoc.get("v")).isEqualTo(2L);
        assertThat(oplogDoc.get("ns")).isEqualTo(collection.getNamespace().getFullName());
        assertThat(oplogDoc.get("op")).isEqualTo(OperationType.INSERT.getCode());
        assertThat(oplogDoc.get("o")).isEqualTo(doc);
    }

    @Test
    public void testOplogReplaceOneById() {
        collection.insertOne(json("_id: 1, b: 6"));
        Document updatedDocument = json("a: 5, b: 7");
        collection.replaceOne(json("_id: 1"), updatedDocument);
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get("op").toString());
        Document o2 = (Document) updateOplogEntry.get("o2");
        Document o = (Document) updateOplogEntry.get("o");
        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get("ns")).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get("ts");
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get("wall");
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testOplogUpdateOneById() {
        collection.insertOne(json("_id: 34, b: 6"));
        Document updatedDocument = json("a: 6");
        collection.updateOne(eq("_id", 34), set("a", 6));

        List<Document> oplogs = new ArrayList<>();

        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get("op").toString());
        Document o2 = (Document) updateOplogEntry.get("o2");
        Document o = (Document) updateOplogEntry.get("o");

        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(34);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get("ns")).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get("ts");
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get("wall");
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testOplogUpdateOneManyFieldsUsingDriverHelpers() {
        collection.insertOne(json("_id: 1, b: 6"));
        Document updatedDocument = json("a: 7, b: 7");
        collection.updateOne(eq("_id", 1), Arrays.asList(set("a", 7), set("b", 7)));

        List<Document> oplogs = new ArrayList<>();

        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get("op").toString());
        Document o2 = (Document) updateOplogEntry.get("o2");
        Document o = (Document) updateOplogEntry.get("o");

        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(1);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get("ns")).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get("ts");
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get("wall");
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testOplogUpdateOneFilteringByOtherThanId() {
        collection.insertOne(json("_id: 37, b: 6"));
        Document updatedDocument = json("a: 7, b: 7");
        collection.updateOne(eq("b", 6), Arrays.asList(set("a", 7), set("b", 7)));

        List<Document> oplogs = new ArrayList<>();

        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(2);
        Document updateOplogEntry = oplogs.get(1);
        OperationType op = OperationType.fromCode(updateOplogEntry.get("op").toString());
        Document o2 = (Document) updateOplogEntry.get("o2");
        Document o = (Document) updateOplogEntry.get("o");

        assertThat(op).isEqualTo(OperationType.UPDATE);
        assertThat(o2.get("_id")).isEqualTo(37);
        assertThat(o.get("$set")).isEqualTo(updatedDocument);

        assertThat(updateOplogEntry.get("ns")).isEqualTo(collection.getNamespace().toString());
        BsonTimestamp ts = (BsonTimestamp) updateOplogEntry.get("ts");
        Instant instant = TEST_CLOCK.instant();
        BsonTimestamp expectedTs = new BsonTimestamp(instant.toEpochMilli());
        assertThat(ts).isEqualTo(expectedTs);

        Date wall = (Date) updateOplogEntry.get("wall");
        assertThat(wall).isEqualTo(Date.from(instant));
    }

    @Test
    public void testUpdateManyUpdatedIdsShouldBeReflectedInOplog() {
        collection.insertMany(Arrays.asList(json("_id: 37, b: 6"), json("_id: 41, b: 7")));
        Document updatedDocument = json("a: 7, b: 7");
        collection.updateMany(or(eq("b", 6), eq("b", 7)), Arrays.asList(set("a", 7), set("b", 7)));
        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);
        assertThat(oplogs.size()).isEqualTo(4);

        List<Object> updatedIds = oplogs.stream().skip(2).map(d -> ((Document) d.get("o2")).get("_id"))
            .collect(Collectors.toList());

        assertThat(updatedIds).containsExactly(37, 41);
    }

    @Test
    public void testMultipleUpdatesInARow() {
        Document doc = json("_id: 34, b: 6");
        collection.insertOne(doc);
        collection.updateOne(eq("_id", 34), set("a", 6));
        collection.updateOne(eq("_id", 34), set("b", 7));

        List<Document> oplogs = new ArrayList<>();
        oplogCollection.find().forEach((Consumer<Document>) oplogs::add);

        assertThat(oplogs.size()).isEqualTo(3);
        assertThat(oplogs.get(0).get("op")).isEqualTo("i");
        assertThat(oplogs.get(0).get("o")).isEqualTo(doc);
        assertThat(oplogs.get(1).get("op")).isEqualTo("u");
        assertThat(oplogs.get(1).get("o")).isEqualTo(json("$set: {a: 6}"));
        assertThat(oplogs.get(2).get("op")).isEqualTo("u");
        assertThat(oplogs.get(2).get("o")).isEqualTo(json("$set: {b: 7}"));
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
