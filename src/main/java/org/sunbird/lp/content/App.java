package org.sunbird.lp.content;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.io.IOException;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;

/**
 * Connects to Neo4j db, finds contents with size value not present and updates the size for each such content
 * if the artifactUrl or downloadUrl is present 
 * 
 * @author pritha
 *
 */
public class App {
	public static void main(String[] args) throws Exception {

		// Connect to a neo4j instance
		Neo4jDBClient dbclient = new Neo4jDBClient("bolt://localhost:7687", "", "");

		// fetches 100 content which does not have size defined
		dbclient.run(100);

		// Loop through each content (whose size not set)
		for (Entry<String, Content> entry : dbclient.getContentMap().entrySet()) {
			Content content = entry.getValue();

			// if content artifactUrl is null then get content downloadUrl
			String artifactUrl = !content.getArifactUrl().equals("null") ? content.getArifactUrl()
					: content.getDownloadUrl();

			try {
				if (artifactUrl != null && !artifactUrl.isEmpty() && !artifactUrl.equals("null")) {
					long size = getContentSize(artifactUrl);  // gets content's size from header
					content.setContentSize(size);					 
					dbclient.updateContentSize(content); // updating the size of content
				} else {
					System.out.println(content.getContentId() + ": artifactUrl, downloadUrl both are empty, so size could not be updated ");
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Exception occured for Content-" + content.getContentId() + ": " + e.getMessage());
			}
		}
				
		//process completed, closing connection 
		dbclient.close();

	}

	public static long getContentSize(String artifactUrl)
			throws JsonGenerationException, JsonMappingException, IOException, UnirestException {
		HttpResponse<String> response = Unirest.head(artifactUrl).asString();
		System.out.println(new ObjectMapper().writeValueAsString(response));
		//System.out.println(response.getHeaders().get("Content-Length"));
		String contentLength = response.getHeaders().get("Content-Length").iterator().next();
		return new Long(contentLength);
	}

}
