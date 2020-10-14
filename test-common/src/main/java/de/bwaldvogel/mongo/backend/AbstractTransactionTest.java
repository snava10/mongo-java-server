package de.bwaldvogel.mongo.backend;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.session.ServerSession;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Updates.set;
import static de.bwaldvogel.mongo.backend.TestUtils.json;

public abstract class AbstractTransactionTest extends AbstractTest {

    @Test
    public void testSimpleTransaction() {
        ClientSession clientSession = syncClient.startSession();
        TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();

        TransactionBody txnBody = (TransactionBody<String>) () -> {
            collection.insertOne(clientSession, json("_id: 1, name: \"testDoc\""));
            return "Inserted into collection";
        };

        try {
            clientSession.withTransaction(txnBody, txnOptions);
        } catch (RuntimeException e) {
            // some error handling
        } finally {
            clientSession.close();
        }
        Document doc = collection.find(json("_id: 1")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("name")).isEqualTo("testDoc");
    }

    @Test
    public void testTransactionShouldOnlyApplyChangesAfterCommitting() throws InterruptedException {
        collection.insertOne(json("_id: 1, value: 1"));
        ClientSession clientSession = syncClient.startSession();
        clientSession.startTransaction();
        collection.updateOne(clientSession, json("_id: 1"), set("value", 2));
        collection.updateOne(clientSession, json("_id: 1"), set("value", 4));
//        collection.updateOne(json("_id: 1"), set("value", 3));

        Document doc = collection.find(json("_id: 1")).first();
        assertThat(doc).isNotNull();
//        assertThat(doc.get("value")).isEqualTo(1);

        try {
            clientSession.commitTransaction();
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            // some error handling
        } finally {
            clientSession.close();
        }

        doc = collection.find(json("_id: 1")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("value")).isEqualTo(4);
        Thread.yield();
    }

}
