package de.bwaldvogel.mongo.wire.message;

import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.ReplyFlag;

public class MongoReply {
    private final MessageHeader header;
    private final List<? extends Document> documents;
    private long cursorID;
    private int startingFrom;
    private int flags;

    public MongoReply(MessageHeader header, Document document, ReplyFlag... replyFlags) {
        this(header, Collections.singletonList(document), 0, replyFlags);
    }

    public MongoReply(MessageHeader header, Document document, long cursorID, ReplyFlag... replyFlags) {
        this(header, document, replyFlags);
        this.cursorID = cursorID;
    }

    public MongoReply(MessageHeader header, List<? extends Document> documents, long cursorID, ReplyFlag... replyFlags) {
        this.cursorID = cursorID;
        this.header = header;
        this.documents = documents;
        for (ReplyFlag replyFlag : replyFlags) {
            flags = replyFlag.addTo(flags);
        }
    }

    public MessageHeader getHeader() {
        return header;
    }

    public List<Document> getDocuments() {
        return Collections.unmodifiableList(documents);
    }


    public long getCursorID() {
        return cursorID;
    }

    public int getStartingFrom() {
        return startingFrom;
    }

    public int getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("(");
        sb.append(String.format("header: %s, responseFlags: %d, cursorID: %d, startingFrom: %d, numberReturned: %d, ",
            header, flags, cursorID, startingFrom, documents.size()));
        sb.append("documents: ").append(getDocuments());
        sb.append(")");
        return sb.toString();
    }
}
