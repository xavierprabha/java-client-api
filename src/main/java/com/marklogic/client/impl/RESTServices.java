/*
 * Copyright 2012 MarkLogic Corporation
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
package com.marklogic.client.impl;

import java.util.Map;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import com.marklogic.client.AbstractDocumentManager.Metadata;
import com.marklogic.client.BadRequestException;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.DocumentIdentifier;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.RequestLogger;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.config.QueryDefinition;

public interface RESTServices {
	public void connect(String host, int port, String user, String password, Authentication type, SSLContext context, HostnameVerifier verifier);
	public void release();

	public void deleteDocument(RequestLogger logger, DocumentIdentifier docId, String transactionId, Set<Metadata> categories)
		throws ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public <T> T getDocument(RequestLogger logger, DocumentIdentifier docId, String transactionId, Set<Metadata> categories, Map<String,String> extraParams, String mimetype, Class<T> as)
		throws ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public Object[] getDocument(RequestLogger logger, DocumentIdentifier docId, String transactionId, Set<Metadata> categories, Map<String,String> extraParams, String[] mimetypes, Class[] as)
		throws BadRequestException, ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public boolean head(RequestLogger logger, DocumentIdentifier docId, String transactionId)
		throws ForbiddenUserException, FailedRequestException;
	public void putDocument(RequestLogger logger, DocumentIdentifier docId, String transactionId, Set<Metadata> categories, Map<String,String> extraParams, String mimetype, Object value)
		throws ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public void putDocument(RequestLogger logger, DocumentIdentifier docId, String transactionId, Set<Metadata> categories, Map<String,String> extraParams, String[] mimetypes, Object[] values)
		throws BadRequestException, ResourceNotFoundException, ForbiddenUserException, FailedRequestException;

    public <T> T search(Class <T> as, QueryDefinition queryDef, String mimetype, long start, String transactionId)
    	throws ForbiddenUserException, FailedRequestException;

	public String openTransaction() throws ForbiddenUserException, FailedRequestException;
	public void commitTransaction(String transactionId) throws ForbiddenUserException, FailedRequestException;
	public void rollbackTransaction(String transactionId) throws ForbiddenUserException, FailedRequestException;

	// namespaces, etc.
	public <T> T getValue(RequestLogger logger, String type, String key, String mimetype, Class<T> as)
		throws ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public <T> T getValues(RequestLogger logger, String type, String mimetype, Class<T> as)
		throws ForbiddenUserException, FailedRequestException;
	public void postValues(RequestLogger logger, String type, String mimetype, Object value)
	throws ForbiddenUserException, FailedRequestException;
	public void postValue(RequestLogger logger, String type, String key, String mimetype, Object value)
		throws ForbiddenUserException, FailedRequestException;
	public void putValue(RequestLogger logger, String type, String key, String mimetype, Object value)
		throws ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public void deleteValue(RequestLogger logger, String type, String key)
		throws ResourceNotFoundException, ForbiddenUserException, FailedRequestException;
	public void deleteValues(RequestLogger logger, String type)
		throws ForbiddenUserException, FailedRequestException;
}
