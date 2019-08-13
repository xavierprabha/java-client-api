package com.marklogic.client.dataservices;

import com.marklogic.client.datamovement.JobTicket;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public interface BulkCaller {
    int       getThreadCount();
    JobTicket start();
    boolean   awaitCompletion();
    boolean   awaitCompletion(long timeout, TimeUnit unit);
    void      stop();

    interface BuilderBase {
    }
    interface ArgsBase {
        Long    getCallNumber();
        Instant getCallTime();
        int     getRetryNumber();
    }
}
