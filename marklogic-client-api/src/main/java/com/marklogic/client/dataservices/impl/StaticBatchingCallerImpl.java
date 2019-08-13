/*
 * Copyright 2019 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.client.dataservices.impl;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.dataservices.BatchingCaller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

public abstract class StaticBatchingCallerImpl<T, A extends StaticBulkCallerImpl.StaticArgsImpl, R extends StaticBulkCallerImpl.StaticResultImpl>
        extends StaticBulkCallerImpl<A, R>
        implements BatchingCaller<T> {
// TODO: start generify, push into abstract base, and delete
    private LinkedBlockingQueue<T> queue;
// TODO: end generify, push into abstract base, and delete


    public StaticBatchingCallerImpl(DatabaseClient client, StaticBuilderImpl builder) {
        super(client, builder);
        this.queue = new LinkedBlockingQueue<>();
        super.withBatchSize(builder.getBatchSize());
    }

    @Override
    public StaticBatchingCallerImpl withBatchSize(int batchSize) {
        super.withBatchSize(batchSize);
        return this;
    }

/* TODO
        if (getBatchSize() <= 0) {
            withBatchSize(1);
            logger.warn("batchSize should be 1 or greater -- setting batchSize to 1");
        }
 */

    protected void addValue(T value) {
        if (value == null) return;

        queue.add(value);

        boolean timeToCallBatch = (queue.size() % getBatchSize()) == 0;
        if (!timeToCallBatch) return;

        List<T> batch = new ArrayList<>();
        int batchSize = queue.drainTo(batch, getBatchSize());
        if (batchSize < 1) return;

        submitArgsForBatch(batch.stream());
    }
    protected void addAllValues(Stream<T> values) {
        if (values == null) return;
        values.forEach(this::addValue);
    }

    @Override
    public void flushAndWait() {
        for (
            List<T> batch = new ArrayList<>();
            queue.drainTo(batch, getBatchSize()) > 0;
            batch = new ArrayList<>()
        ) {
            submitArgsForBatch(batch.stream());
        }

        awaitCompletion();
    }

    abstract protected void submitArgsForBatch(Stream<T> values);

    static abstract public class StaticArgsImpl extends StaticBulkCallerImpl.StaticArgsImpl implements BatchingCaller.ArgsBase {
    }
    static abstract public class StaticResultImpl extends StaticBulkCallerImpl.StaticResultImpl {
    }
    static abstract public class StaticBuilderImpl extends StaticBulkCallerImpl.StaticBuilderImpl implements BatchingCaller.BuilderBase {
        private int batchSize = 10;
        public int getBatchSize() {
            return batchSize;
        }
        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }
}
