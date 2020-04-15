package de.bwaldvogel.mongo.oplog;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Bson;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import jdk.nashorn.internal.objects.annotations.Getter;

public class OplogDocument implements Bson {
    private BsonTimestamp ts;
    private long t;
    private long h;
    private long v = 1L;
    private OperationType op;
    private String ns;
    private UUID ui = UUID.randomUUID();
    private Instant wall;
    private Document o;
    private Document o2;

    public Document toDocument() {
        Document doc = new Document();
        Stream.of(this.getClass().getMethods()).filter(m -> m.isAnnotationPresent(Getter.class)).forEach(
            m -> {
                try {
                    if (m.getReturnType().equals(OperationType.class)) {
                        doc.put(m.getName().substring(3).toLowerCase(), m.invoke(this).toString());
                    } else {
                        doc.put(m.getName().substring(3).toLowerCase(), m.invoke(this));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        return doc;
    }

    public Document toChangeStreamDocument() {
        Document doc = new Document();
        Stream.of(this.getClass().getMethods()).filter(m -> m.isAnnotationPresent(Getter.class)).forEach(
            m -> {
                try {
                    if (m.getReturnType().equals(OperationType.class)) {
                        doc.put(m.getAnnotation(Getter.class).name(), m.invoke(this).toString());
                    } else {
                        doc.put(m.getAnnotation(Getter.class).name(), m.invoke(this));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        return doc;
    }

    public static OplogDocumentBuilder builder() {
        return new OplogDocumentBuilder();
    }

    @Getter
    public BsonTimestamp getTs() {
        return ts;
    }

    public void setTs(BsonTimestamp ts) {
        this.ts = ts;
    }

    @Getter
    public long getT() {
        return t;
    }

    public void setT(long t) {
        this.t = t;
    }

    @Getter
    public long getH() {
        return h;
    }

    public void setH(long h) {
        this.h = h;
    }

    @Getter
    public long getV() {
        return v;
    }

    public void setV(long v) {
        this.v = v;
    }

    @Getter
    public OperationType getOp() {
        return op;
    }

    public void setOp(OperationType op) {
        this.op = op;
    }

    @Getter(name="namespaceDocument")
    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    @Getter
    public UUID getUi() {
        return ui;
    }

    public void setUi(UUID ui) {
        this.ui = ui;
    }

    @Getter
    public Instant getWall() {
        return wall;
    }

    public void setWall(Instant wall) {
        this.wall = wall;
    }

    @Getter(name="fullDocument")
    public Document getO() {
        return o;
    }

    public void setO(Document o) {
        this.o = o;
    }

    @Getter
    public Document getO2() {
        return o2;
    }

    public void setO2(Document o2) {
        this.o2 = o2;
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
        return toDocument().equals(((OplogDocument)o).toDocument());
    }

    @Override
    public int hashCode() {
        return toDocument().hashCode();
    }
}
