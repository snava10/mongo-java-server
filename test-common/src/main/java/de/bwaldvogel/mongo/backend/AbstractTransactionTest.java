package de.bwaldvogel.mongo.backend;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static de.bwaldvogel.mongo.backend.TestUtils.json;

public abstract class AbstractTransactionTest extends AbstractTest {

    @Test
    public void testTransaction() {
        collection.insertOne(new Document());
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
}
