package de.bwaldvogel.mongo.backend;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

import de.bwaldvogel.mongo.bson.Document;

public class Cursor implements Iterable<Document> {

    private final long cursorId;
    private final Queue<Document> documents;
    private final String collectionName;

    public Cursor(Iterable<Document> documents, String collectionName) {
        this.cursorId = Cursor.generateCursorId();
        this.documents = new LinkedList<>();
        for(Document doc: documents) {
            this.documents.add(doc);
        }
        this.collectionName = collectionName;
    }

    public int documentsCount() {
        return documents.size();
    }

    public boolean isEmpty() {
        return documents.isEmpty();
    }

    public Document toDocument() {
        return null;
    }

    public long getCursorId() {
        return cursorId;
    }

    public Queue<Document> getDocuments() {
        return documents;
    }

    @Override
    public Iterator<Document> iterator() {
        return documents.iterator();
    }

    public static long generateCursorId() {
        return ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
    }

    public String getCollectionName() {
        return collectionName;
    }
}
