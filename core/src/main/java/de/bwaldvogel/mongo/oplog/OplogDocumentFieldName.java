package de.bwaldvogel.mongo.oplog;

import java.util.HashMap;
import java.util.Map;

public enum OplogDocumentFieldName {
    TIMESTAMP("ts"),
    T("t"),
    HASH("h"),
    PROTOCOL_VERSION("v"),
    OPERATION_TYPE("op"),
    NAMESPACE("ns"),
    UUID("ui"),
    WALL("wall"),
    OPERATION_DOCUMENT("o"),
    ADDITIONAL_OPERATION_DOCUMENT("o2");

    OplogDocumentFieldName(String code) { this.code = code; }
    public String getCode() { return code; }

    private String code;

    private static final Map<String, OplogDocumentFieldName> MAP = new HashMap<>();

    static {
        for (OplogDocumentFieldName oplogDocumentFieldName : OplogDocumentFieldName.values()) {
            MAP.put(oplogDocumentFieldName.getCode(), oplogDocumentFieldName);
        }
    }

    public static OplogDocumentFieldName fromCode(String code) {
        OplogDocumentFieldName oplogDocumentFieldName = MAP.get(code);
        if (oplogDocumentFieldName == null) {
            throw new IllegalArgumentException("unknown operation type: " + code);
        }
        return oplogDocumentFieldName;
    }

}
