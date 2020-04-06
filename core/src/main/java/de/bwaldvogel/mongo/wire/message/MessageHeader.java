package de.bwaldvogel.mongo.wire.message;

public class MessageHeader {

    private final int requestID;
    private final int responseTo;
    private int opCode;

    public MessageHeader(int requestID, int responseTo, int opCode) {
        this.requestID = requestID;
        this.responseTo = responseTo;
        this.opCode = opCode;
    }

    public int getRequestID() {
        return requestID;
    }

    public int getResponseTo() {
        return responseTo;
    }

    public int opCode() { return opCode; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("(");
        sb.append("request: ").append(requestID);
        sb.append(", responseTo: ").append(responseTo);
        sb.append(")");
        return sb.toString();
    }
}
