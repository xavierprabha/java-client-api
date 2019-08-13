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
import com.marklogic.client.MarkLogicInternalException;
import com.marklogic.client.datamovement.DataMovementException;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.datamovement.ForestConfiguration;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.impl.BatcherImpl;
import com.marklogic.client.dataservices.BulkCaller;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StaticBulkCallerImpl<A extends StaticBulkCallerImpl.StaticArgsImpl, R extends StaticBulkCallerImpl.StaticResultImpl>
        extends BatcherImpl
        implements BulkCaller {
    private static Logger logger = LoggerFactory.getLogger(StaticBulkCallerImpl.class);

// TODO: start push into abstract base and delete
    private JobTicket jobTicket;
    private CallingThreadPoolExecutor threadPool;
    private List<DatabaseClient> clients;
    private Calendar jobStartTime;
    private Calendar jobEndTime;
    private final AtomicLong    callCount   = new AtomicLong(0);
    private final AtomicLong    taskCount   = new AtomicLong(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean stopped     = new AtomicBoolean(false);
    private final AtomicBoolean started     = new AtomicBoolean(false);
// TODO: end push into abstract base and delete

    private ThreadUnit threadUnit;

    public StaticBulkCallerImpl(DatabaseClient client, StaticBuilderImpl builder) {
        super(client.newDataMovementManager());
        this.threadUnit      = builder.threadUnit;
        withBatchSize(1);
        ForestConfiguration forestConfig = getMoveMgr().readForestConfig();
        withForestConfig(forestConfig);
        switch(this.threadUnit) {
            case FOREST:
                int forestCount = forestConfig.listForests().length;
                super.withThreadCount((forestCount == 0) ? 1 : (forestCount * builder.threadCount));
                break;
            default:
                super.withThreadCount(builder.threadCount);
                break;
        }
    }

    @Override
    public StaticBulkCallerImpl<A, R> withForestConfig(ForestConfiguration forestConfig) {
        super.withForestConfig(forestConfig);
        Forest[] forests = forests(forestConfig);
        Set<String> hosts = hosts(forests);
        clients = clients(hosts);
// TODO fire host unavailable listeners if host list changed
        return this;
    }

    @Override
    public void start(JobTicket ticket) {
        jobTicket = ticket;
        initialize();
    }
    @Override
    public JobTicket start() {
        return getMoveMgr().startJob(this);
    }
    @Override
    public JobTicket getJobTicket() {
        return jobTicket;
    }
    public void stopJob() {
        getMoveMgr().stopJob(this);
    }
    @Override
    public void stop() {
        jobEndTime = Calendar.getInstance();
        stopped.set(true);
        if (threadPool != null) {
            try {
                threadPool.shutdown();
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                logger.warn("interrupted while awaiting termination", e);
                threadPool.shutdownNow();
            }
        }
    }
    @Override
    public boolean isStarted() {
        return started.get();
    }
    @Override
    public boolean isStopped() {
        return stopped.get();
    }
    @Override
    public Calendar getJobStartTime() {
        if (this.isStarted()) {
            return jobStartTime;
        }
        return null;
    }
    @Override
    public Calendar getJobEndTime() {
        if (this.isStopped()) {
            return jobEndTime;
        }
        return null;
    }
    @Override
    public StaticBulkCallerImpl<A, R> withJobId(String jobId) {
        setJobId(jobId);
        return this;
    }

    @Override
    public StaticBulkCallerImpl<A, R> withJobName(String jobName) {
        super.withJobName(jobName);
        return this;
    }

    private void initialize() {
        if (initialized.getAndSet(true)) return;

        threadPool = new CallingThreadPoolExecutor(this, getThreadCount());

        jobStartTime = Calendar.getInstance();
        started.set(true);
    }

    private Forest getForest(Long callNumber) {
        Forest[] forests = getForestConfig().listForests();
        if (forests == null || forests.length == 0) return null;
        int forestNumber = (int) (callNumber % forests.length);
        return forests[forestNumber];
    }
    protected String getForestName(Long callNumber) {
        Forest forest = getForest(callNumber);
        return (forest == null)? null : forest.getForestName();
    }

    DatabaseClient getClient(Long callNumber) {
        int clientSize = (clients == null) ? 0 : clients.size();
/* TODO
        if (clientSize < 2 || getDataMovementManager().getConnectionType() == DatabaseClient.ConnectionType.GATEWAY) {
 */
        if (clientSize < 2) {
            return getPrimaryClient();
        }
        int clientNumber = (int) (callNumber % clientSize);
        return clients.get(clientNumber);
    }

    abstract protected R call(DatabaseClient db, A args);
    abstract protected A notifySuccess(A args, R result);
    abstract protected A notifyFailure(A args, Throwable error);
    abstract protected void prepare(Long taskNumber, A args);

    @Override
    public boolean awaitCompletion() {
        return awaitCompletion(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    @Override
    public boolean awaitCompletion(long timeout, TimeUnit unit) {
/* TODO:
        requireNotStopped();
        requireInitialized(true);
 */
        return threadPool.awaitCompletion(timeout, unit);
    }

    protected void submitArgs(A args) {
        args.setCallNumber(callCount.incrementAndGet());
        threadPool.execute(new CallTask(this, args));
    }
    protected BlockingQueue<Runnable> makeWorkQueue() {
        return new LinkedBlockingQueue<>(getThreadCount() * 15);
    }

// TODO: move out of base interface
    @Override
    public BatcherImpl withThreadCount(int threadCount) {
        throw new UnsupportedOperationException("cannot change thread count for bulk caller after building");
    }

    public enum ThreadUnit {
        CLUSTER, FOREST;
    }

    static class CallingThreadPoolExecutor<A extends StaticArgsImpl, R extends StaticBulkCallerImpl.StaticResultImpl>
            extends ThreadPoolExecutor {
        private StaticBulkCallerImpl<A, R> bulkCaller;
        private Set<CallTask<A, R>>        queuedAndExecutingTasks;
        private CountDownLatch             idleLatch;
        CallingThreadPoolExecutor(StaticBulkCallerImpl<A, R> bulkCaller, int threadCount) {
            super(threadCount, threadCount, 1, TimeUnit.MINUTES,
                    bulkCaller.makeWorkQueue(), new ThreadPoolExecutor.CallerRunsPolicy()
            );
            this.bulkCaller = bulkCaller;
            // reserve capacity for all executing threads as well as executor queue
            this.queuedAndExecutingTasks = ConcurrentHashMap.newKeySet(getQueue().size() + threadCount);
            this.idleLatch = new CountDownLatch(threadCount);
        }
        @Override
        public <T> Future<T> submit(Callable<T> callable) {
            throw new MarkLogicInternalException("submit of callable not supported");
        }
        @Override
        public Future<Boolean> submit(Runnable command) {
            throw new MarkLogicInternalException("submit of task not supported");
        }
        @Override
        public <T> Future<T> submit(Runnable command, T result) {
            throw new MarkLogicInternalException("submit of task with default result not supported");
        }
        @Override
        public void execute(Runnable command) {
            if (!(command instanceof CallTask<?,?>)) {
                throw new MarkLogicInternalException("submitted unknown implementation of task");
            }
            queuedAndExecutingTasks.add((CallTask<A,R>) command);
            super.execute(command);
        }
        @Override
        protected void afterExecute(Runnable command, Throwable t) {
            queuedAndExecutingTasks.remove(command);
            super.afterExecute(command, t);
        }

// TODO: unneeded?
        void threadIdling() {
            idleLatch.countDown();
        }

        boolean awaitCompletion(long timeout, TimeUnit unit) {
            try {
                if (isTerminated()) return true;

                // take a snapshot of the queue at the current time
                Set<CallTask<A,R>> queue = new HashSet<>();
                queue.addAll(queuedAndExecutingTasks);

                if (queue.isEmpty())
                    return true;
                // wait for the future of every queued or executing task
                for (CallTask<A,R> task : queue) {
                    if (task.isCancelled() || task.isDone())
                        continue;
                    task.get(timeout, unit);
                }

                return true;
            } catch (InterruptedException e) {
                logger.warn("interrupted while awaiting completion", e);
            } catch (ExecutionException e) {
                logger.warn("access exception while awaiting completion", e);
            } catch (TimeoutException e) {
                throw new DataMovementException("timed out while awaiting completion", e);
            }
            return false;
        }
    }

    static class CallTask<A extends StaticArgsImpl, R extends StaticBulkCallerImpl.StaticResultImpl> extends FutureTask<Boolean> {
        CallTask(StaticBulkCallerImpl<A, R> batcher, A args) {
            super(new CallMaker(batcher, args));
        }

    }

    static class CallMaker<A extends StaticArgsImpl, R extends StaticBulkCallerImpl.StaticResultImpl> implements Callable<Boolean> {
        private StaticBulkCallerImpl<A, R> batcher;
        private boolean                    fireFailureListeners = true;
        private Long                       taskNumber;
        private A                          args;
        CallMaker(StaticBulkCallerImpl<A, R> batcher, A args) {
            this.batcher = batcher;
            this.args    = args;
            this.taskNumber = batcher.taskCount.incrementAndGet();
        }

        public Long getTaskNumber() {
            return taskNumber;
        }
        void setFailureListeners(boolean enable) {
            fireFailureListeners = enable;
        }

        public StaticBulkCallerImpl<A, R> getBatcher() {
            return this.batcher;
        }
        public A getArgs() {
            return args;
        }
        public boolean isFireFailureListeners() {
            return fireFailureListeners;
        }

        @Override
        public Boolean call() {
            StaticBulkCallerImpl<A, R> batcher = getBatcher();

            long taskNumber = getTaskNumber();
            DatabaseClient client = batcher.getClient(taskNumber);

            A args = getArgs();

            Boolean succeeded = null;
            while (args != null) {
                batcher.prepare(taskNumber, args);

                logger.info("call {} in task {} for {} retry", taskNumber, args.getCallNumber(), args.getRetryNumber());

                args.setCallTime(Instant.now());

                R result = null;
                Throwable failure = null;
                try {
                    result = batcher.call(client, args);
                } catch (Throwable throwable) {
                    if (isFireFailureListeners()) {
                        failure = throwable;
                    } else if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    } else {
                        throw new DataMovementException("Failed to retry call", throwable);
                    }
                }
                succeeded = (failure == null);

                A nextArgs = succeeded ?
                        batcher.notifySuccess(args, result) :
                        batcher.notifyFailure(args, failure);
                if (nextArgs != null) {
                    nextArgs.setRetryNumber(!succeeded ? 0 : (args.getRetryNumber() + 1));
                    if (nextArgs != args)
                        nextArgs.setCallNumber(batcher.callCount.incrementAndGet());
                }
                args = nextArgs;
            }

            return succeeded;
        }
    }

    static abstract public class StaticArgsImpl implements BulkCaller.ArgsBase {
        private Long    callNumber;
        private Instant callTime;
        private int     retryNumber = 0;

        @Override
        public Long getCallNumber() {
            return callNumber;
        }
        void setCallNumber(Long callNumber) {
            this.callNumber = callNumber;
        }
        @Override
        public Instant getCallTime() {
            return callTime;
        }
        void setCallTime(Instant callTime) {
            this.callTime = callTime;
        }
        public int getRetryNumber() {
            return retryNumber;
        }
        void setRetryNumber(int retryNumber) {
            this.retryNumber = retryNumber;
        }
    }
    static abstract public class StaticResultImpl {
    }
    static abstract public class StaticBuilderImpl implements BulkCaller.BuilderBase {
        private ThreadUnit threadUnit;
        private int        threadCount;
        protected StaticBuilderImpl() {
            setThreadUnit(ThreadUnit.FOREST);
            setThreadCount(1);
        }
        protected void setThreadUnit(ThreadUnit unit) {
            switch(unit) {
                case CLUSTER:
                case FOREST:
                    threadUnit = unit;
                    break;
                default:
                    throw new IllegalArgumentException(
                            (unit == null) ? "null thread unit" : "unknown thread unit: "+unit.name()
                    );
            }
        }
        protected int getThreadCount() {
            return this.threadCount;
        }
        protected void setThreadCount(int threadCount) {
            if (threadCount < 1) {
                throw new IllegalArgumentException("thread count must be one or more: "+threadCount);
            }
            this.threadCount = threadCount;
        }
    }
}
