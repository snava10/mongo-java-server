package de.bwaldvogel.mongo.session;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SessionRegistry {
    private ConcurrentMap<UUID, Session> sessionMap = new ConcurrentHashMap<>();

    public Session resolveSession(UUID sessionId) {
        if (sessionId == null) {
            return resolveSession(UUID.randomUUID());
        }
        return sessionMap.putIfAbsent(sessionId, new Session(sessionId));
    }

    public void endSessions(List<UUID> sessionIds) {
        sessionIds.forEach(id -> sessionMap.remove(id));
    }
}
