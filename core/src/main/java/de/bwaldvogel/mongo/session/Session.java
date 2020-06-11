package de.bwaldvogel.mongo.session;

import java.util.UUID;

public class Session {
    private UUID sessionId;

    public Session(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public void startTransaction() {}
}
