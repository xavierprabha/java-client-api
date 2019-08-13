package com.marklogic.client.dataservices;

public interface BatchingCaller<T> extends BulkCaller {
    void flushAndWait();
    interface BuilderBase extends BulkCaller.BuilderBase {
        BuilderBase batchSize(int batchSize);
    }
    interface ArgsBase extends BulkCaller.ArgsBase {
    }
}
