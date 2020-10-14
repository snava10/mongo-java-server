package de.bwaldvogel.mongo.session;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class SessionRegistry {
    private ConcurrentMap<UUID, Session> sessionMap = new ConcurrentHashMap<>();
    public CountDownLatch latch = new CountDownLatch(0);

    public void endSessions(List<UUID> sessionIds) {
        sessionIds.forEach(id -> sessionMap.remove(id));
    }

    public boolean containsSession(UUID sessionId) {
        return sessionMap.containsKey(sessionId);
    }

    public boolean hasActiveSession() {
        return !sessionMap.isEmpty();
    }

}
