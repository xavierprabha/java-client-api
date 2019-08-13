package com.marklogic.client.dataservices;

public interface GeneratingCaller<T> extends BulkCaller {
    interface BuilderBase extends BulkCaller.BuilderBase {
    }
    interface ArgsBase extends BulkCaller.ArgsBase {
    }
}
