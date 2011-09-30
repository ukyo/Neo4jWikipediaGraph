package example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Example1 {
	private GraphDatabaseService databaseService;
	public Example1() {
		databaseService = new EmbeddedGraphDatabase("DB");
	}
	
	public void setNode(String s) {
		Transaction tx = databaseService.beginTx();
		
		try {
			Node firstNode = databaseService.createNode();
			firstNode.setProperty("name", s);
			
			tx.success();
		} catch(Exception e) {
			tx.failure();
		} finally {
			tx.finish();
			databaseService.shutdown();
		}
	}
	
	public void viewNodes() {
		for(Node node: databaseService.getAllNodes()) {
			System.out.println("ID=" + node.getId());
			for(String key: node.getPropertyKeys()) {
				System.out.println(key + "=" + node.getProperty(key));
			}
			System.out.println("--");
		}
		databaseService.shutdown();
	}
	
	public static void main(String[] args) {
		Example1 example1 = new Example1();
//		example1.setNode("ukyo");
		example1.viewNodes();
	}
}
