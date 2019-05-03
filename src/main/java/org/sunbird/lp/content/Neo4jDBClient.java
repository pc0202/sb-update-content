package org.sunbird.lp.content;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author pritha
 *
 */
public class Neo4jDBClient {

	/**
	 * Holds mapping for N contentId and it's content
	 */
	private Map<String, Content> contentMap = new HashMap<String, Content>();
	private final Driver driver;
	private StatementResult result;

	/**
	 * @param uri
	 * @param user
	 * @param password
	 */
	public Neo4jDBClient(String uri, String user, String password) {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}

	public void close() throws Exception {
		driver.close();
	}

	/**
	 * fetches the all content's unique id, downloadUrl, artifactUrl where size is null  
	 * @param withLimit    to limit the number of records gets fetched
	 */
	private void fetchContents(final int withLimit) {
		try (Session session = driver.session()) {
			result = session.readTransaction(new TransactionWork<StatementResult>() {
				@Override
				public StatementResult execute(Transaction tx) {
					return tx.run(
							"match (n) where n.contentType=\"Asset\" and not exists(n.size) return n.IL_UNIQUE_ID as contentId,"
									+ " n.size as contentSize, n.downloadUrl as downloadUrl, n.artifactUrl as artifactUrl limit "
									+ withLimit);
				}
			});
		}
	}

	/**
	 * Updates the size value of a given content
	 * @param content  
	 */
	public void updateContentSize(final Content content) {
		try (Session session = driver.session()) {
			String trxWork = session.writeTransaction(new TransactionWork<String>() {
				@Override
				public String execute(Transaction tx) {
					StatementResult result = tx.run(
							"MATCH (n) where n.contentType=\"Asset\" and n.IL_UNIQUE_ID= $id " + "SET n.size = $size "
									+ "RETURN n.IL_UNIQUE_ID, n.size",
							parameters("id", content.getContentId(), "size", content.getContentSize()));
					return result.single().get(0).asString();
				}
			});
			System.out.println(content.getContentId() +"content size updated to "+content.getContentSize());
		}
	}

	/**
	 * populates the content map.
	 * 
	 */
	private void populateContents() {
		while (result.hasNext()) {
			Record record = result.next();
			String cId = record.get("contentId").asString();
			String dUrl = record.get("downloadUrl").asString();
			String aUrl = record.get("artifactUrl").asString();

			Content content = new Content(cId, aUrl);
			content.setDownloadUrl(dUrl);
			//System.out.println("Content fetched" + content.toString());
			contentMap.put(cId, content);

		}
	}

	/**
	 * Execute fetching the contents for DB and populating to Content model
	 * @param withLimit
	 * @throws Exception
	 */
	public void run(final int withLimit) throws Exception {
		fetchContents(withLimit);
		populateContents();
    	System.out.println("Number of content's fetched to model, "+contentMap.size());

	}

	public Map<String, Content> getContentMap() {
		return contentMap;
	}
}
