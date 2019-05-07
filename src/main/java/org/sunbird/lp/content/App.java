package org.sunbird.lp.content;

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

		int number = 100;
		try {
			number = new Integer(args[0]);
		} catch (Exception e) {
			System.out.println("WARN: invalid commandline argument, default limit is 100!!");
		}
		
		while(dbclient.run(number) > 0 ) {
			long starts = System.currentTimeMillis();
			dbclient.updateAllContentSize();
			long ends = System.currentTimeMillis();
			
			System.out.println(number +" records update in time: "+ (ends-starts)/1000 +"s");
		}

		//process completed, closing connection 
		dbclient.close();

	}

}
