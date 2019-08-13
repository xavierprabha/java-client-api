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
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.dataservices.GeneratingCaller;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public abstract class StaticGeneratingCallerImpl<A extends StaticBulkCallerImpl.StaticArgsImpl, R extends StaticBulkCallerImpl.StaticResultImpl>
        extends StaticBulkCallerImpl<A, R>
        implements GeneratingCaller {

    public StaticGeneratingCallerImpl(DatabaseClient client, StaticBuilderImpl builder) {
        super(client, builder);
    }

    @Override
    public void start(JobTicket ticket) {
        super.start(ticket);
/* TODO: support for forest name parameter
            String[] forestNames = null;
            if(forestParamName!=null) {
                Forest[] forests;
                forests = super.getForestConfig().listForests();
                forestNames = new String[forests.length];
                int j=0;
                for(Forest i:forests) {
                    forestNames[j] = i.getForestName();
                    j++;
                }
            }

 */
        for (int i=0; i < getThreadCount(); i++) {
            submitArgs(newArgs());
        }
    }

    abstract protected A newArgs();

    protected BlockingQueue<Runnable> makeWorkQueue() {
        return new SynchronousQueue<>();
    }

    static abstract public class StaticArgsImpl extends StaticBulkCallerImpl.StaticArgsImpl implements GeneratingCaller.ArgsBase {
    }
    static abstract public class StaticResultImpl extends StaticBulkCallerImpl.StaticResultImpl {
    }
    static abstract public class StaticBuilderImpl extends StaticBulkCallerImpl.StaticBuilderImpl implements GeneratingCaller.BuilderBase {
    }
}
