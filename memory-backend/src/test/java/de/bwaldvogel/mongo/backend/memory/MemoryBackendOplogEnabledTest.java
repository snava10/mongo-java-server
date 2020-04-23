package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendOplogEnabledTest;

public class MemoryBackendOplogEnabledTest extends AbstractBackendOplogEnabledTest {
    @Override
    protected MongoBackend createBackend() {
        return new MemoryBackend();
    }
}
