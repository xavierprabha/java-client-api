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

import com.marklogic.client.Format;
import com.marklogic.client.MarkLogicInternalException;
import com.marklogic.client.RequestParameters;
import com.marklogic.client.XMLDocumentManager;
import com.marklogic.client.io.marker.XMLReadHandle;
import com.marklogic.client.io.marker.XMLWriteHandle;

class XMLDocumentImpl
    extends AbstractDocumentImpl<XMLReadHandle, XMLWriteHandle>
    implements XMLDocumentManager
{
	private DocumentRepair repair;

	XMLDocumentImpl(RESTServices services) {
		super(services, Format.XML);
	}

	public DocumentRepair getDocumentRepair() {
		return repair;
	}
	public void setDocumentRepair(DocumentRepair policy) {
		repair = policy;
	}

	protected RequestParameters getWriteParams() {
		if (repair == null)
			return null;

		RequestParameters params = new RequestParameters();
		if (repair == DocumentRepair.FULL)
			params.put("repair", "full");
		else if (repair == DocumentRepair.NONE)
			params.put("repair", "none");
		else
			throw new MarkLogicInternalException("Internal error - unknown repair policy: "+repair.name());

		return params;
	}
}
