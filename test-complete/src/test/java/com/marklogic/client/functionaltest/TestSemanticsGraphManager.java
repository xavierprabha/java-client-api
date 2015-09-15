package com.marklogic.client.functionaltest;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;

import org.junit.*;
import org.junit.runners.MethodSorters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.Transaction;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.ReaderHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.RDFMimeTypes;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSemanticsGraphManager extends BasicJavaClientREST {

	// private static final String DEFAULT_GRAPH =
	// "http://replace.defaultGraphValue.here/";
	private static GraphManager gmWriter;
	private static GraphManager gmReader;
	private static GraphManager gmAdmin;
	private static String dbName = "SemanticsDB-JavaAPI";
	private static String[] fNames = { "SemanticsDB-JavaAPI-1" };
	private static String restServerName = "REST-Java-Client-API-Server";
	private static int restPort = 8011;
	private static int uberPort = 8000;
	private DatabaseClient adminClient = null;
	private DatabaseClient writerClient = null;
	private DatabaseClient readerClient = null;
	private DatabaseClient evalClient = null;
	private static String datasource = "src/test/java/com/marklogic/client/functionaltest/data/semantics/";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("In setup");
		setupJavaRESTServer(dbName, fNames[0], restServerName, 8011);
		setupAppServicesConstraint(dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("In tear down");
		// Delete database first. Otherwise axis and collection cannot be
		// deleted
		tearDownJavaRESTServer(dbName, fNames, restServerName);
		deleteRESTUser("eval-user");
		deleteUserRole("test-eval");
	}

	@After
	public void testCleanUp() throws Exception {
		clearDB(restPort);
		adminClient.release();
		writerClient.release();
		readerClient.release();
		System.out.println("Running clear script");
	}

	@Before
	public void setUp() throws Exception {
		createUserRolesWithPrevilages("test-eval", "xdbc:eval", "xdbc:eval-in", "xdmp:eval-in", "any-uri", "xdbc:invoke");
		createRESTUser("eval-user", "x", "test-eval", "rest-admin", "rest-writer", "rest-reader");
		adminClient = DatabaseClientFactory.newClient("localhost", restPort, dbName, "rest-admin", "x", Authentication.DIGEST);
		writerClient = DatabaseClientFactory.newClient("localhost", restPort, dbName, "rest-writer", "x", Authentication.DIGEST);
		readerClient = DatabaseClientFactory.newClient("localhost", restPort, dbName, "rest-reader", "x", Authentication.DIGEST);
		evalClient = DatabaseClientFactory.newClient("localhost", uberPort, dbName, "eval-user", "x", Authentication.DIGEST);
		gmWriter = writerClient.newGraphManager();
		gmReader = readerClient.newGraphManager();
		gmAdmin = adminClient.newGraphManager();
	}

	/*
	 * Write Triples using Write user & StringHandle with mime-Type = n-triples
	 * Get the list of Graphs from the DB with Read User Iterate through the
	 * list of graphs to validate the Graph Delete Graph using URI Validate the
	 * graph doesn't exist in DB
	 */
	@Test
	public void testListUris_readUser() throws Exception {
		Exception exp = null;
		String ntriple5 = "<http://example.org/s5> <http://example.com/p2> <http://example.org/o2> .";
		gmWriter.write("htp://test.sem.graph/G1", new StringHandle(ntriple5).withMimetype("application/n-triples"));
		Iterator<String> iter = gmReader.listGraphUris();
		while (iter.hasNext()) {
			String uri = iter.next();
			assertNotNull(uri);
			assertNotEquals("", uri);
			System.out.println("DEBUG: [GraphsTest] uri =[" + uri + "]");
		}
		try {
			gmWriter.delete("htp://test.sem.graph/G1");
			gmWriter.read("htp://test.sem.graph/G1", new StringHandle());
		} catch (Exception e) {
			exp = e;
		}
		assertTrue("Read after detele did not throw expected Exception, Received ::" + exp,
				exp.toString().contains("Could not read resource at graphs."));

		// Delete non existing graph

		try {
			gmWriter.delete("htp://test.sem.graph/G1");
		} catch (Exception e) {
			exp = e;
		}
		assertTrue(
				"Deleting non-existing Graph did not throw expected Exception:: http://bugtrack.marklogic.com/35064 , Received ::" + exp,
				exp.toString().contains("Could not delete resource at graphs"));
	}

	/*
	 * Write Triples using Write user & StringHandle with mime-Type = n-triples
	 * Get the list of Graphs from the DB with Write User Iterate through the
	 * list of graphs to validate the Graph
	 */

	@Test
	public void testListUris_writeUser() throws Exception {
		String ntriple5 = "<http://example.org/s22> <http://example.com/p22> <http://example.org/o22> .";

		gmWriter.write("htp://test.sem.graph/G2", new StringHandle(ntriple5)

		.withMimetype("application/n-triples"));
		try {
			Iterator<String> iter = gmWriter.listGraphUris();
			while (iter.hasNext()) {
				String uri = iter.next();
				assertNotNull(uri);
				assertNotEquals("", uri);
				System.out.println("DEBUG: [GraphsTest] uri =[" + uri + "]");
			}
		} catch (Exception e) {
		}
	}

	/*
	 * Write Triples using Read user & StringHandle with mime-Type = n-triples
	 * Catch the Exception and validate ForbiddenUser Exception with the
	 * Message.
	 */
	@Test
	public void testWriteTriples_ReadUser() throws Exception {
		Exception exp = null;
		String ntriple5 = "<http://example.org/s33> <http://example.com/p33> <http://example.org/o33> .";

		try {
			gmReader.write("htp://test.sem.graph/G3", new StringHandle(ntriple5).withMimetype("application/n-triples"));

		} catch (Exception e) {
			exp = e;
		}
		assertTrue(
				"Unexpected Error Message, \n Expecting:: com.marklogic.client.ForbiddenUserException: Local message: User is not allowed to write resource at graphs. Server Message: "
						+ "You do not have permission to this method and URL \n BUT Received :: " + exp.toString(), exp.toString()
						.contains(
								"com.marklogic.client.ForbiddenUserException: Local message: User is not allowed to write resource at graphs. Server Message: "
										+ "You do not have permission to this method and URL"));

	}

	/*
	 * Write Triples using Write user,Delete Graph using Read User Catch
	 * exception and validate the correct Error message can also use
	 * RDFMimeTypes.NTRIPLES
	 */
	@Test
	public void testWriteTriples_DeletereadUser() throws Exception {
		Exception exp = null;
		boolean exceptionThrown = false;
		String ntriple5 = "<http://example.org/s44> <http://example.com/p44> <http://example.org/o44> .";

		try {
			gmWriter.write("htp://test.sem.graph/G4", new StringHandle(ntriple5).withMimetype("application/n-triples"));
			gmReader.delete("htp://test.sem.graph/G4");

		} catch (Exception e) {
			exp = e;
			exceptionThrown = true;
		}
		if (exceptionThrown)
			assertTrue(
					"Unexpected Error Message, \n Expecting:: com.marklogic.client.ForbiddenUserException: Local message: User is not allowed to delete resource at graphs. Server Message: "
							+ "You do not have permission to this method and URL \n BUT Received :: " + exp.toString(), exp.toString()
							.contains(
									"com.marklogic.client.ForbiddenUserException: Local message: User is not allowed to delete resource at graphs. Server Message: "
											+ "You do not have permission to this method and URL"));
		assertTrue("Expected Exception but Received NullPointer", exceptionThrown);

	}

	/*
	 * Write & Read tirples of type N-Triples using File & String handles
	 */

	@Test
	public void testWrite_nTriples_FileHandle() throws Exception {
		File file = new File(datasource + "5triples.nt");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/n", filehandle.withMimetype("application/n-triples"));
		StringHandle handle = gmWriter.read("htp://test.sem.graph/n", new StringHandle().withMimetype(RDFMimeTypes.NTRIPLES));
		assertTrue("Did not insert document or inserted empty doc", handle.toString().contains("<http://jibbering.com/foaf/santa.rdf#bod>"));

	}

	/*
	 * Write & Read rdf using ReaderHandle Validate the content in DB by
	 * converting into String.
	 */

	@Test
	public void testWrite_rdfxml_FileHandle() throws Exception {
		BufferedReader buffreader = new BufferedReader(new FileReader(datasource + "semantics.rdf"));
		ReaderHandle handle = new ReaderHandle();
		handle.set(buffreader);
		gmWriter.write("http://test.reader.handle/bufferreadtrig", handle.withMimetype(RDFMimeTypes.RDFXML));
		buffreader.close();
		ReaderHandle read = gmWriter.read("http://test.reader.handle/bufferreadtrig", new ReaderHandle().withMimetype(RDFMimeTypes.RDFXML));
		Reader readFile = read.get();
		String readContent = convertReaderToString(readFile);
		assertTrue("Did not get receive expected string content, Received:: " + readContent,
				readContent.contains("http://www.daml.org/2001/12/factbook/vi#A113932"));

	}

	/*
	 * Write & Read triples of type ttl using bytehandle
	 */
	@Test
	public void testWrite_ttl_FileHandle() throws Exception {
		File file = new File(datasource + "relative3.ttl");
		FileInputStream fileinputstream = new FileInputStream(file);
		ByteArrayOutputStream byteoutputstream = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		// Get triples into bytes from file
		for (int readNum; (readNum = fileinputstream.read(buf)) != -1;) {
			byteoutputstream.write(buf, 0, readNum);
		}
		byte[] bytes = byteoutputstream.toByteArray();
		fileinputstream.close();
		byteoutputstream.close();
		BytesHandle contentHandle = new BytesHandle();
		contentHandle.set(bytes);
		// write triples in bytes to DB
		gmWriter.write("http://test.turtle.com/byteHandle", contentHandle.withMimetype(RDFMimeTypes.TURTLE));
		BytesHandle byteHandle = gmWriter.read("http://test.turtle.com/byteHandle", new BytesHandle().withMimetype(RDFMimeTypes.TURTLE));
		byte[] readInBytes = byteHandle.get();
		String readInString = new String(readInBytes);
		assertTrue("Did not insert document or inserted empty doc", readInString.contains("#relativeIRI"));
	}

	/*
	 * Read & Write triples using File Handle and parse the read results into
	 * json using Jackson json node.
	 */
	@Test
	public void testWrite_rdfjson_FileHandle() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		new ObjectMapper();
		File file = new File(datasource + "relative6.json");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/rdfjson", filehandle.withMimetype("application/rdf+json"));
		FileHandle handle = new FileHandle();
		handle.setMimetype(RDFMimeTypes.RDFJSON);
		handle = gmWriter.read("htp://test.sem.graph/rdfjson", handle);
		File readFile = handle.get();
		JsonNode readContent = mapper.readTree(readFile);
		// Verify read content with inserted content
		assertTrue("Did not insert document or inserted empty doc", readContent.toString()
				.contains("http://purl.org/dc/elements/1.1/title"));
	}

	// Write & Read triples of type N3 using ByteHandle
	@Test
	public void testWrite_n3_BytesHandle() throws Exception {
		File file = new File(datasource + "relative7.n3");
		FileInputStream fileinputstream = new FileInputStream(file);
		ByteArrayOutputStream byteoutputstream = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		// Get triples into bytes from file
		for (int readNum; (readNum = fileinputstream.read(buf)) != -1;) {
			byteoutputstream.write(buf, 0, readNum);
		}
		byte[] bytes = byteoutputstream.toByteArray();
		fileinputstream.close();
		byteoutputstream.close();
		BytesHandle contentHandle = new BytesHandle();
		contentHandle.set(bytes);
		// write triples in bytes to DB
		gmWriter.write("http://test.n3.com/byte", contentHandle.withMimetype(RDFMimeTypes.N3));
		InputStreamHandle read = gmWriter.read("http://test.n3.com/byte", new InputStreamHandle().withMimetype(RDFMimeTypes.N3));
		InputStream fileRead = read.get();
		String readContent = convertInputStreamToString(fileRead);
		assertTrue("Did not find expected content after inserting the triples:: Found: " + readContent,
				readContent.contains("http://localhost/publications/journals/Journal1/1940"));

	}

	@Test
	public void testWrite_nquads_FileHandle() throws Exception {
		File file = new File(datasource + "relative2.nq");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/nquads", filehandle.withMimetype("application/n-quads"));
		StringHandle handle = gmWriter.read("htp://test.sem.graph/nquads", new StringHandle().withMimetype(RDFMimeTypes.NQUADS));
		assertTrue("Did not insert document or inserted empty doc", handle.toString().contains("<#electricVehicle2>"));
	}

	/*
	 * Write same Triples into same graph twice & Read Triples of type Trig
	 * using FileHandle Should not insert duplicate and Read should not result
	 * in duplicates
	 */

	@Test
	public void testWrite_trig_FileHandle() throws Exception {
		File file = new File(datasource + "semantics.trig");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/trig", filehandle.withMimetype("application/trig"));
		gmWriter.write("htp://test.sem.graph/trig", filehandle.withMimetype("application/trig"));
		FileHandle handle = gmWriter.read("htp://test.sem.graph/trig", new FileHandle());
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		assertTrue("Did not insert document or inserted empty doc",
				expectedContent.contains("http://www.example.org/exampleDocument#Monica"));

	}

	/*
	 * Open a Transaction Write tripleXML into DB using FileHandle & rest-writer
	 * using the transaction Read the Graph and validate Graph exists in DB
	 * using the same transaction Delete the Graph witin the same transaction
	 * Read and validate the delete by catching the exception Finally if the
	 * Transaction is open Perform Rollback
	 */

	@Test
	public void testWrite_triplexml_FileHandle() throws Exception {
		Transaction trx = writerClient.openTransaction();
		GraphManager gmWriter = writerClient.newGraphManager();
		File file = new File(datasource + "relative5.xml");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		// Write Graph using transaction
		gmWriter.write("htp://test.sem.graph/tripxml", filehandle.withMimetype(RDFMimeTypes.TRIPLEXML), trx);
		// Validate graph written to DB by reading within the same transaction
		StringHandle handle = gmWriter.read("htp://test.sem.graph/tripxml", new StringHandle().withMimetype(RDFMimeTypes.TRIPLEXML), trx);
		assertTrue("Did not insert document or inserted empty doc", handle.toString().contains("Anna's Homepage"));
		// Delete Graph in the same transaction
		gmWriter.delete("htp://test.sem.graph/tripxml", trx);
		// Validate Graph is deleted
		try {
			gmWriter.read("htp://test.sem.graph/tripxml", new StringHandle(), trx);
			trx.commit();
			trx = null;
		} catch (ResourceNotFoundException e) {
			System.out.println(e);
		} finally {
			if (trx != null) {
				trx.rollback();
				trx = null;
			}

		}

	}

	/*
	 * Insert rdf triples intoDB which does not have base URI defined validate
	 * Undeclared base URI exception is thrown
	 */

	@Test
	// GitIssue #319
	public void testWrite_rdfxml_WrongTriplesSchema() throws Exception {
		Exception exp = null;
		File file = new File(datasource + "relative4.rdf");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		try {
			gmWriter.write("htp://test.sem.graph/rdf", filehandle.withMimetype(RDFMimeTypes.RDFXML));
		} catch (Exception e) {
			exp = e;
		}
		assertTrue("Did not throw expected Exception, Expecting ::XDMP-BASEURI (err:FOER0000): Undeclared base URI " + "\n Received::"
				+ exp, exp.toString().contains("XDMP-BASEURI (err:FOER0000): Undeclared base URI"));

	}

	/*
	 * -ve case to validate non-iri triples insert and read should throw
	 * SEM-NOTRDF exception Note:: In RDF, the subject and predicate can only be
	 * of type IRI
	 */
	// Git issue #322 , server returns 500 error code
	@Test
	public void testRead_nonirirdfxml_FileHandle() throws Exception {
		StringHandle handle = new StringHandle();
		File file = new File(datasource + "non-iri.xml");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/tripxmlnoniri", filehandle.withMimetype(RDFMimeTypes.TRIPLEXML));
		try {

			handle = gmWriter.read("htp://test.sem.graph/tripxmlnoniri", new StringHandle());

		} catch (Exception e) {
			assertTrue("Did not receive expected Error message, Expecting ::SEM-NOTRDF \n  Received::" + e,
					e.toString().contains("SEM-NOTRDF") && e != null);

		}
		assertTrue("Read content has un-expected content ", handle.get().toString().contains("5.11"));
	}

	/*
	 * Write & Read NON -RDF format triples
	 */
	@Test
	public void testRead_nonirin3_FileHandle() throws Exception {
		StringHandle handle = new StringHandle();
		File file = new File(datasource + "non-iri.n3");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/n3noniri", filehandle.withMimetype(RDFMimeTypes.N3));
		try {

			handle = gmWriter.read("htp://test.sem.graph/n3noniri", new StringHandle().withMimetype(RDFMimeTypes.N3));
		} catch (Exception e) {
			assertTrue("Did not receive expected Error message, Expecting ::SEM-NOTRDF \n  Received::" + e,
					e.toString().contains("SEM-NOTRDF") && e != null);

		}
		assertTrue("Read content has un-expected content ", handle.get().toString().contains("p0:named_graph"));
	}

	/*
	 * ReadAs & WriteAs N-Triples using File Type
	 */
	@Test
	public void testReadAs_WriteAs() throws Exception {
		File file = new File(datasource + "semantics.nt");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.writeAs("htp://test.sem.graph/ntAs", filehandle.withMimetype(RDFMimeTypes.NTRIPLES));
		File read = gmWriter.readAs("htp://test.sem.graph/ntAs", File.class);
		String expectedContent = convertFileToString(read);
		assertTrue("writeAs & readAs  test did not return expected content",
				expectedContent.contains("http://www.w3.org/2001/sw/RDFCore/ntriples/"));
	}

	/*
	 * ReadAs & WriteAs N-Triples using File Type within a Transaction
	 */
	@Test
	public void testWriteAs_ReadAs_FileHandle() throws Exception {
		Transaction trx = writerClient.openTransaction();
		GraphManager gmWriter = writerClient.newGraphManager();
		File file = new File(datasource + "semantics.ttl");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		// Write Graph using transaction
		gmWriter.writeAs("htp://test.sem.graph/ttlas", filehandle.withMimetype(RDFMimeTypes.TURTLE), trx);
		// Validate graph written to DB by reading within the same transaction
		File readFile = gmWriter.readAs("htp://test.sem.graph/ttlas", File.class, trx);
		String expectedContent = convertFileToString(readFile);
		assertTrue("Did not insert document or inserted empty doc", expectedContent.contains("http://www.w3.org/2004/02/skos/core#Concept"));
		// Delete Graph in the same transaction
		gmWriter.delete("htp://test.sem.graph/ttlas", trx);
		trx.commit();
		// Validate Graph is deleted
		try {
			trx = writerClient.openTransaction();
			String readContent = gmWriter.readAs("htp://test.sem.graph/ttlas", String.class, trx);
			trx.commit();
			trx = null;
			assertTrue("Unexpected content read, expecting Resource not found exception", readContent == null);
		} catch (ResourceNotFoundException e) {
			System.out.println(e);
		} finally {
			if (trx != null) {
				trx.rollback();
				trx = null;
			}

		}

	}

	/*
	 * Merge Triples of same mime type Validate the triples are inserted
	 * correctly & MergeAs & QuadsWriteHandle
	 */
	@Test
	public void testMerge_trig_FileHandle() throws Exception {
		File file = new File(datasource + "trig.trig");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		File file2 = new File(datasource + "semantics.trig");
		FileHandle filehandle2 = new FileHandle();
		filehandle2.set(file2);
		gmWriter.write("htp://test.sem.graph/trigMerge", filehandle.withMimetype(RDFMimeTypes.TRIG));
		gmWriter.merge("htp://test.sem.graph/trigMerge", filehandle2.withMimetype(RDFMimeTypes.TRIG));
		FileHandle handle = gmWriter.read("htp://test.sem.graph/trigMerge", new FileHandle());
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		assertTrue(
				"Did not insert document or inserted empty doc",
				expectedContent.contains("http://www.example.org/exampleDocument#Monica")
						&& expectedContent.contains("http://purl.org/dc/elements/1.1/publisher"));
	}

	/*
	 * Merge Triples of different mime type's Validate the triples are inserted
	 * correctly
	 */

	@Test
	public void testMerge_jsonxml_FileHandle() throws Exception {
		File file = new File(datasource + "bug25348.json");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		File file2 = new File(datasource + "relative5.xml");
		FileHandle filehandle2 = new FileHandle();
		filehandle2.set(file2);
		gmWriter.write("htp://test.sem.graph/trigM", filehandle.withMimetype(RDFMimeTypes.RDFJSON));
		gmWriter.merge("htp://test.sem.graph/trigM", filehandle2.withMimetype(RDFMimeTypes.TRIPLEXML));
		FileHandle handle = gmWriter.read("htp://test.sem.graph/trigM", new FileHandle().withMimetype(RDFMimeTypes.RDFJSON));
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		assertTrue("Did not insert document or inserted empty doc", expectedContent.contains("http://example.com/ns/person#firstName")
				&& expectedContent.contains("Anna's Homepage"));
	}

	/*
	 * WriteAs,MergeAs & ReadAs Triples of different mimetypes
	 */
	@Test
	public void testMergeAs_jsonxml_FileHandle() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.RDFJSON);
		File file = new File(datasource + "bug25348.json");
		FileInputStream fis = new FileInputStream(file);
		Object graphData = convertInputStreamToString(fis);
		fis.close();
		gmWriter.writeAs("htp://test.sem.graph/jsonMergeAs", graphData);
		gmWriter.setDefaultMimetype(RDFMimeTypes.TRIPLEXML);
		File file2 = new File(datasource + "relative5.xml");
		Object graphData2 = convertFileToString(file2);
		gmWriter.mergeAs("htp://test.sem.graph/jsonMergeAs", graphData2);
		File readFile = gmWriter.readAs("htp://test.sem.graph/jsonMergeAs", File.class);
		String expectedContent = convertFileToString(readFile);
		assertTrue("Did not insert document or inserted empty doc", expectedContent.contains("http://example.com/ns/person#firstName")
				&& expectedContent.contains("Anna's Homepage"));
	}

	/*
	 * WriteAs,MergeAs & ReadAs Triples of different mimetypes within a
	 * Transaction & QuadsWriteHandle
	 */

	@Test
	public void testMergeAs_jsonxml_Trx_FileHandle() throws Exception {
		Transaction trx = writerClient.openTransaction();
		GraphManager gmWriter = writerClient.newGraphManager();
		gmWriter.setDefaultMimetype(RDFMimeTypes.RDFJSON);
		File file = new File(datasource + "bug25348.json");
		FileInputStream fis = new FileInputStream(file);
		Object graphData = convertInputStreamToString(fis);
		fis.close();
		gmWriter.writeAs("htp://test.sem.graph/jsonMergeAsTrx", graphData, trx);
		File file2 = new File(datasource + "relative6.json");
		Object graphData2 = convertFileToString(file2);
		gmWriter.mergeAs("htp://test.sem.graph/jsonMergeAsTrx", graphData2, trx);
		File readFile = gmWriter.readAs("htp://test.sem.graph/jsonMergeAsTrx", File.class, trx);
		String expectedContent = convertFileToString(readFile);
		assertTrue("Did not insert document or inserted empty doc", expectedContent.contains("http://example.com/ns/person#firstName")
				&& expectedContent.contains("Anna's Homepage"));
		try {
			trx.commit();
			trx = null;
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			if (trx != null) {
				trx.rollback();
				trx = null;
			}
		}
	}

	/*
	 * Merge Triples into Graph which already has the same triples Inserts two
	 * documents into DB Read the graph , should not contain duplicate triples
	 */

	@Test
	public void testMergeSameDoc_jsonxml_FileHandle() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.RDFJSON);
		File file = new File(datasource + "bug25348.json");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("htp://test.sem.graph/trigMSameDoc", filehandle);
		gmWriter.merge("htp://test.sem.graph/trigMSameDoc", filehandle);
		FileHandle handle = gmWriter.read("htp://test.sem.graph/trigMSameDoc", new FileHandle());
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		assertTrue(
				"Did not insert document or inserted empty doc",
				expectedContent
						.equals("{\"http://example.com/ns/directory#m\":{\"http://example.com/ns/person#firstName\":[{\"value\":\"Michelle\", \""
								+ "type\":\"literal\", \"datatype\":\"http://www.w3.org/2001/XMLSchema#string\"}]}}"));
	}

	/*
	 * Merge & Delete With in Transaction
	 */
	@Test
	public void testWriteMergeDelete_Trx() throws Exception {
		Transaction trx = writerClient.openTransaction();
		GraphManager gmWriter = writerClient.newGraphManager();
		File file = new File(datasource + "triplexml1.xml");
		gmWriter.write("htp://test.sem.graph/mergetrx", new FileHandle(file).withMimetype(RDFMimeTypes.TRIPLEXML), trx);
		file = new File(datasource + "bug25348.json");
		gmWriter.merge("htp://test.sem.graph/mergetrx", new FileHandle(file).withMimetype(RDFMimeTypes.RDFJSON), trx);
		FileHandle handle = gmWriter.read("htp://test.sem.graph/mergetrx", new FileHandle(), trx);
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		try {
			assertTrue("Did not Merge document or inserted empty doc",
					expectedContent.contains("Michelle") && expectedContent.contains("Anna's Homepage"));
			gmWriter.delete("htp://test.sem.graph/mergetrx", trx);
			trx.commit();
			trx = null;
			StringHandle readContent = gmWriter.read("htp://test.sem.graph/mergetrx", new StringHandle(), trx);
			assertTrue("Unexpected Content from read, expecting null", readContent == null);
		} catch (Exception e) {
			assertTrue("Unexpected Exception Thrown", e.toString().contains("ResourceNotFoundException"));
		} finally {
			if (trx != null)
				trx.commit();
			trx = null;
		}
	}

	/*
	 * Write and read triples into ML default graph
	 */
	@Test
	public void testWriteRead_defaultGraph() throws FileNotFoundException {
		File file = new File(datasource + "semantics.trig");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write(gmWriter.DEFAULT_GRAPH, filehandle.withMimetype(RDFMimeTypes.TRIG));
		FileHandle handle = gmWriter.read(gmWriter.DEFAULT_GRAPH, new FileHandle());
		GraphPermissions permissions = gmWriter.getPermissions(gmWriter.DEFAULT_GRAPH);
		System.out.println(permissions);
		assertEquals(Capability.UPDATE, permissions.get("rest-writer").iterator().next());
		assertEquals(Capability.READ, permissions.get("rest-reader").iterator().next());
		gmWriter.deletePermissions(gmWriter.DEFAULT_GRAPH);
		permissions = gmWriter.getPermissions(gmWriter.DEFAULT_GRAPH);
		System.out.println(permissions);
		assertEquals(Capability.UPDATE, permissions.get("rest-writer").iterator().next());
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		System.out.println(gmWriter.listGraphUris().next().toString());
		assertTrue("" + gmWriter.listGraphUris().next().toString(),
				gmWriter.listGraphUris().next().toString().equals("http://marklogic.com/semantics#default-graph"));
		assertTrue("Did not insert document or inserted empty doc",
				expectedContent.contains("http://www.example.org/exampleDocument#Monica"));
	}

	/*
	 * Write Triples of Type JSON Merge NTriples into the same graph and
	 * validate ReplaceGraphs with File handle & Nquads mime-type and validate &
	 * DeleteGraphs and validate ResourceNotFound Exception
	 */
	@Test
	public void test001MergeReplace_quads() throws FileNotFoundException, InterruptedException {
		String uri = "http://test.sem.quads/json-quads";
		String ntriple6 = "<http://example.org/s6> <http://example.com/mergeQuadP> <http://example.org/o2> <http://test.sem.quads/json-quads>.";
		File file = new File(datasource + "bug25348.json");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write(uri, filehandle.withMimetype(RDFMimeTypes.RDFJSON));
		gmWriter.mergeGraphs(new StringHandle(ntriple6).withMimetype(RDFMimeTypes.NQUADS));
		FileHandle handle = gmWriter.read(uri, new FileHandle());
		File readFile = handle.get();
		String expectedContent = convertFileToString(readFile);
		assertTrue("Did not merge Quad", expectedContent.contains("<http://example.com/mergeQuadP"));

		file = new File(datasource + "relative2.nq");
		gmWriter.replaceGraphs(new FileHandle(file).withMimetype(RDFMimeTypes.NQUADS));
		uri = "http://originalGraph";
		StringHandle readQuads = gmWriter.read(uri, new StringHandle());
		assertTrue("Did not Replace Quads", readQuads.toString().contains("#electricVehicle2"));
		gmWriter.deleteGraphs();

		try {
			StringHandle readContent = gmWriter.read(uri, new StringHandle());
			assertTrue("Unexpected content read, expecting Resource not found exception", readContent.get() == null);

		} catch (Exception e) {
			assertTrue("Unexpected Exception Thrown", e.toString().contains("ResourceNotFoundException"));
		}
	}

	/*
	 * Replace with Large Number(600+) of Graphs of type nquads and validate
	 * Merge different quads with different graph and validate deletegraphs and
	 * validate resourcenotfound exception
	 */

	@Test
	public void test002MergeReplaceAs_Quads() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.NQUADS);
		File file = new File(datasource + "semantics.nq");
		gmWriter.replaceGraphsAs(file);
		String uri = "http://en.wikipedia.org/wiki/Alexander_I_of_Serbia?oldid=492189987#absolute-line=1";
		StringHandle readQuads = gmWriter.read(uri, new StringHandle());
		assertTrue("Did not Replace Quads", readQuads.toString().contains("http://dbpedia.org/ontology/Monarch"));

		file = new File(datasource + "relative2.nq");
		gmWriter.mergeGraphsAs(file);
		uri = "http://originalGraph";
		readQuads = gmWriter.read(uri, new StringHandle());
		assertTrue("Did not Replace Quads", readQuads.toString().contains("#electricVehicle2"));

		gmWriter.deleteGraphs();
		try {
			StringHandle readContent = gmWriter.read(uri, new StringHandle());
			assertTrue("Unexpected content read, expecting Resource not found exception", readContent.get() == null);

		} catch (Exception e) {

			assertTrue("Unexpected Exception Thrown", e.toString().contains("ResourceNotFoundException"));
		}
	}

	@Test
	public void testThingsAs() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.NTRIPLES);
		File file = new File(datasource + "relative1.nt");
		FileHandle filehandle = new FileHandle();
		filehandle.set(file);
		gmWriter.write("http://test.things.com/", filehandle);
		String things = gmWriter.thingsAs(String.class, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		assertTrue(
				"Did not return Expected graph Uri's",
				things.equals("<#electricVehicle2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://people.aifb.kit.edu/awa/2011/smartgrid/schema/smartgrid#ElectricVehicle> ."));
	}

	@Test
	public void testThings_file() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.TRIPLEXML);
		String tripleGraphUri = "http://test.things.com/file";
		File file = new File(datasource + "relative5.xml");
		gmWriter.write(tripleGraphUri, new FileHandle(file));
		StringHandle things = gmWriter.things(new StringHandle(), "about");
		assertTrue("Things did not return expected Uri's",
				things.get().equals("<about> <http://purl.org/dc/elements/1.1/title> \"Anna's Homepage\" ."));
		gmWriter.delete(tripleGraphUri);
	}

	@Test
	public void testThings_fileNomatch() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.TRIPLEXML);
		String tripleGraphUri = "http://test.things.com/file";
		File file = new File(datasource + "relative5.xml");
		gmWriter.write(tripleGraphUri, new FileHandle(file));
		Exception exp = null;
		try {
			StringHandle things = gmWriter.things(new StringHandle(), "noMatch");
			assertTrue("Things did not return expected Uri's", things == null);
		} catch (Exception e) {
			exp = e;
		}
		assertTrue(exp.toString().contains("ResourceNotFoundException"));
		gmWriter.delete(tripleGraphUri);
	}

	@Test
	public void testThings_Differentmimetypes() throws Exception {
		gmWriter.setDefaultMimetype(RDFMimeTypes.TRIPLEXML);
		String tripleGraphUri = "http://test.things.com/multiple";
		File file = new File(datasource + "relative5.xml");
		gmWriter.write(tripleGraphUri, new FileHandle(file));
		file = new File(datasource + "semantics.ttl");
		gmWriter.merge(tripleGraphUri, new FileHandle(file).withMimetype(RDFMimeTypes.TURTLE));
		StringHandle things = gmWriter.things(new StringHandle(), "http://dbpedia.org/resource/Hadoop");
		assertTrue("Things did not return expected Uri's", things.get().contains("Apache Hadoop"));
		gmWriter.delete(tripleGraphUri);
	}

	// TODO:: Re-write this Method into multiple tests after 8.0-4 release
	@Test
	public void testPermissions_noTrx() throws Exception {
		File file = new File(datasource + "semantics.rdf");
		FileHandle handle = new FileHandle();
		handle.set(file);
		String uri = "test_permissions11";
		// Create Role
		createUserRolesWithPrevilages("test-perm");
		// Create User with Above Role
		createRESTUser("perm-user", "x", "test-perm");
		// Create Client with above User
		DatabaseClient permUser = DatabaseClientFactory.newClient("localhost", restPort, dbName, "perm-user", "x", Authentication.DIGEST);
		// Create GraphManager with Above client
		GraphManager gmTestPerm = permUser.newGraphManager();
		// Set Update Capability for the Created User
		GraphPermissions perms = gmTestPerm.permission("test-perm", Capability.UPDATE);
		// Write Graph with triples into DB
		gmTestPerm.write(uri, handle.withMimetype(RDFMimeTypes.RDFXML), perms);
		// Get PErmissions for the User and Validate
		System.out.println("Permissions after create , Shold not see Execute" + gmTestPerm.getPermissions(uri));
		perms = gmTestPerm.getPermissions(uri);
		for (Capability capability : perms.get("test-perm")) {
			assertTrue("capability should be UPDATE, not [" + capability + "]", capability == Capability.UPDATE);
		}

		// Set Capability for the User
		perms = gmTestPerm.permission("test-perm", Capability.EXECUTE);
		// Merge Permissions
		gmTestPerm.mergePermissions(uri, perms);
		// gmTestPerm.writePermissions(uri, perms);
		// Get Permissions And Validate
		perms = gmTestPerm.getPermissions(uri);
		System.out.println("Permissions after setting execute , Should  see Execute & Update" + gmTestPerm.getPermissions(uri));
		for (Capability capability : perms.get("test-perm")) {
			assertTrue("capability should be UPDATE && Execute, not [" + capability + "]", capability == Capability.UPDATE
					|| capability == Capability.EXECUTE);
		}
		assertTrue("Did not have expected capabilities", perms.get("test-perm").size() == 2);

		// Write Read permission to uri and validate permissions are overwritten
		// with write
		perms = gmTestPerm.permission("test-perm", Capability.READ);
		gmTestPerm.write(uri, handle.withMimetype(RDFMimeTypes.RDFXML), perms);
		for (Capability capability : perms.get("test-perm")) {
			assertTrue("capability should be READ, not [" + capability + "]", capability == Capability.READ);
		}
		assertTrue("Did not have expected capabilities", perms.get("test-perm").size() == 1);

		// Delete Permissions and Validate
		gmTestPerm.deletePermissions(uri);
		perms = gmTestPerm.getPermissions(uri);
		assertNull(perms.get("test-perm"));

		// Set and Write Execute Permission
		perms = gmTestPerm.permission("test-perm", Capability.EXECUTE);

		gmTestPerm.writePermissions(uri, perms);
		// Read and Validate triples
		try {
			gmTestPerm.read(uri, handle.withMimetype(RDFMimeTypes.RDFXML));
			ReaderHandle read = gmTestPerm.read(uri, new ReaderHandle());
			Reader readFile = read.get();
			String readContent = convertReaderToString(readFile);
			assertTrue("Did not get receive expected string content, Received:: " + readContent,
					readContent.contains("http://www.daml.org/2001/12/factbook/vi#A113932"));
		} catch (Exception e) {
			System.out.println("Tried to Read  and validate triples, shold  see this exception ::" + e);
		}
		System.out.println("Permissions after setting execute , Should  see Execute & Update & Read " + gmTestPerm.getPermissions(uri));

		// delete all capabilities
		gmTestPerm.deletePermissions(uri);

		System.out.println("Capabilities after delete , SHold not see Execute" + gmTestPerm.getPermissions(uri));

		// set Update and perform Read
		perms = gmTestPerm.permission("test-perm", Capability.UPDATE);
		gmTestPerm.mergePermissions(uri, perms);
		try {
			gmTestPerm.read(uri, new ReaderHandle());
		} catch (Exception e) {
			System.out.println(" Should receive unauthorized exception ::" + e);
		}

		// Delete
		gmTestPerm.deletePermissions(uri);

		// Set to Read and Perform Merge
		perms = gmTestPerm.permission("test-perm", Capability.READ);
		gmTestPerm.mergePermissions(uri, perms);
		try {
			gmTestPerm.merge(uri, handle);
		} catch (Exception e) {
			System.out.println(" Should receive unauthorized exception ::" + e);
		}
		// Read and validate triples
		ReaderHandle read = gmTestPerm.read(uri, new ReaderHandle());
		Reader readFile = read.get();
		String readContent = convertReaderToString(readFile);
		assertTrue("Did not get receive expected string content, Received:: " + readContent,
				readContent.contains("http://www.daml.org/2001/12/factbook/vi#A113932"));

	}

	/*
	 * -ve cases for permission outside transaction and after rollback of
	 * transaction
	 */
	@Test
	public void testPermissions_withtrxNeg() throws Exception {
		File file = new File(datasource + "semantics.rdf");
		FileHandle handle = new FileHandle();
		handle.set(file);
		String uri = "test_permissions12";
		// Create Role
		createUserRolesWithPrevilages("test-perm");
		// Create User with Above Role
		createRESTUser("perm-user", "x", "test-perm");
		// Create Client with above User
		DatabaseClient permUser = DatabaseClientFactory.newClient("localhost", restPort, dbName, "perm-user", "x", Authentication.DIGEST);
		// Create GraphManager with Above client

		Transaction trx = permUser.openTransaction();
		try {
			GraphManager gmTestPerm = permUser.newGraphManager();
			// Set Update Capability for the Created User
			GraphPermissions perms = gmTestPerm.permission("test-perm", Capability.UPDATE);
			gmTestPerm.write(uri, handle.withMimetype(RDFMimeTypes.RDFXML), trx);
			trx.commit();

			trx = permUser.openTransaction();
			gmTestPerm.mergePermissions(uri, perms, trx);
			// Validate test-perm role not available outside transaction
			GraphPermissions perm = gmTestPerm.getPermissions(uri);
			System.out.println("OUTSIDE TRX , SHOULD NOT SEE test-perm EXECUTE" + perm);
			assertNull(perm.get("test-perm"));
			perms = gmTestPerm.getPermissions(uri, trx);
			assertTrue("Permission within trx should have Update capability", perms.get("test-perm").contains(Capability.UPDATE));
			trx.rollback();
			trx = null;
			perms = gmTestPerm.getPermissions(uri);
			assertNull(perm.get("test-perm"));
		} catch (Exception e) {

		} finally {
			if (trx != null) {
				trx.commit();
				trx = null;
			}
		}

	}

	// TODO:: Re-write this Method into multiple tests after 8.0-4 release
	@Test
	public void testPermissions_withTrx() throws Exception {
		File file = new File(datasource + "semantics.rdf");
		FileHandle handle = new FileHandle();
		handle.set(file);
		String uri = "test_permissions12";
		// Create Role
		createUserRolesWithPrevilages("test-perm");
		// Create User with Above Role
		createRESTUser("perm-user", "x", "test-perm");
		// Create Client with above User
		DatabaseClient permUser = DatabaseClientFactory.newClient("localhost", restPort, dbName, "perm-user", "x", Authentication.DIGEST);

		// Create GraphManager with Above client
		Transaction trx = permUser.openTransaction();
		GraphManager gmTestPerm = permUser.newGraphManager();
		// Set Update Capability for the Created User
		GraphPermissions perms = gmTestPerm.permission("test-perm", Capability.UPDATE);
		// Write Graph with triples into DB
		try {
			gmTestPerm.write(uri, handle.withMimetype(RDFMimeTypes.RDFXML), perms, trx);
			// Get PErmissions for the User and Validate
			System.out.println("Permissions after create , Shold not see Execute" + gmTestPerm.getPermissions(uri, trx));
			perms = gmTestPerm.getPermissions(uri, trx);
			for (Capability capability : perms.get("test-perm")) {
				assertTrue("capability should be UPDATE, not [" + capability + "]", capability == Capability.UPDATE);
			}

			// Set Capability for the User
			perms = gmTestPerm.permission("test-perm", Capability.EXECUTE);
			// Merge Permissions
			gmTestPerm.mergePermissions(uri, perms, trx);
			// Get Permissions And Validate
			perms = gmTestPerm.getPermissions(uri, trx);
			System.out.println("Permissions after setting execute , Should  see Execute & Update" + gmTestPerm.getPermissions(uri, trx));
			for (Capability capability : perms.get("test-perm")) {
				assertTrue("capability should be UPDATE && Execute, not [" + capability + "]", capability == Capability.UPDATE
						|| capability == Capability.EXECUTE);
			}

			// Validate write with Update and Execute permissions
			try {
				gmTestPerm.write(uri, handle.withMimetype(RDFMimeTypes.RDFXML), perms, trx);
			} catch (Exception e) {
				System.out.println("Tried to Write Here ::" + e);//
			}
			System.out.println("Permissions after setting execute , Should  see Execute & Update" + gmTestPerm.getPermissions(uri, trx));

			// Delete Permissions and Validate
			gmTestPerm.deletePermissions(uri, trx);
			try {
				perms = gmTestPerm.getPermissions(uri, trx);
			} catch (Exception e) {
				System.out
						.println("Permissions after setting execute , Should  see Execute & Update" + gmTestPerm.getPermissions(uri, trx));
			}
			assertNull(perms.get("test-perm"));

			// Set and Write Execute Permission
			perms = gmTestPerm.permission("test-perm", Capability.EXECUTE);

			gmTestPerm.writePermissions(uri, perms, trx);

			// Read and Validate triples
			try {
				ReaderHandle read = gmTestPerm.read(uri, new ReaderHandle(), trx);
				Reader readFile = read.get();
				String readContent = convertReaderToString(readFile);
				assertTrue("Did not get receive expected string content, Received:: " + readContent,
						readContent.contains("http://www.daml.org/2001/12/factbook/vi#A113932"));
			} catch (Exception e) {
				System.out.println("Tried to Read  and validate triples, shold not see this exception ::" + e);
			}
			System.out.println("Permissions after setting execute , Should  see Execute & Update & Read "
					+ gmTestPerm.getPermissions(uri, trx));

			// delete all capabilities
			gmTestPerm.deletePermissions(uri, trx);

			System.out.println("Capabilities after delete , SHold not see Execute" + gmTestPerm.getPermissions(uri, trx));

			// set Update and perform Read
			perms = gmTestPerm.permission("test-perm", Capability.UPDATE);
			gmTestPerm.mergePermissions(uri, perms, trx);
			try {
				gmTestPerm.read(uri, new ReaderHandle(), trx);
			} catch (Exception e) {
				System.out.println(" Should receive unauthorized exception ::" + e);
			}

			// Delete
			gmTestPerm.deletePermissions(uri, trx);

			// Set to Read and Perform Merge
			perms = gmTestPerm.permission("test-perm", Capability.READ);

			gmTestPerm.mergePermissions(uri, perms, trx);
			perms = gmTestPerm.permission("test-perm", Capability.READ);
			gmTestPerm.mergePermissions(uri, perms, trx);
			try {
				gmTestPerm.merge(uri, handle, trx);
			} catch (Exception e) {
				System.out.println(" Should receive unauthorized exception ::" + e);
			}
			// Read and validate triples
			ReaderHandle read = gmTestPerm.read(uri, new ReaderHandle(), trx);
			Reader readFile = read.get();
			String readContent = convertReaderToString(readFile);
			assertTrue("Did not get receive expected string content, Received:: " + readContent,
					readContent.contains("http://www.daml.org/2001/12/factbook/vi#A113932"));
			trx.commit();
			trx = null;

		} catch (Exception e) {

		} finally {
			if (trx != null) {
				trx.commit();
				trx = null;
			}
		}
	}
}