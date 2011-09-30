package wikipediagraph.neo4j;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import wikipediagraph.util.MySqlDumpReader;

public class GraphBuilder {

	public static void createPages() {
		GraphDatabaseService graphDB = new EmbeddedGraphDatabase("D:\\neo4j\\wikipedia");
		IndexManager index = graphDB.index();
		Index<Node> pages = index.forNodes("pages");
		int counter = 0;
		Transaction tx = graphDB.beginTx();
		for(String[] s: new MySqlDumpReader("D:\\jawiki-latest-page.sql")) {
			if(Integer.parseInt(s[1]) != 0) continue;
			if(s[2].length() <= 2) continue;
			Node page = graphDB.createNode();
			page.setProperty("page_id", Integer.parseInt(s[0]));
			page.setProperty("page_title", s[2].substring(1, s[2].length() - 1));
			pages.add(page, "page_id", page.getProperty("page_id"));
			pages.add(page, "page_title", page.getProperty("page_title"));
			
			//こうしないとメモリが・・・
			if (++counter % 100000 == 0) {
				tx.success();
				tx.finish();
				System.gc();
				tx = graphDB.beginTx();
			}
		}
		tx.success();
		tx.finish();
		graphDB.shutdown();
	}
	
	public static void createRelationShip() {
		GraphDatabaseService graphDB = new EmbeddedGraphDatabase("D:\\neo4j\\wikipedia");
		IndexManager index = graphDB.index();
		Index<Node> pages = index.forNodes("pages");
		DynamicRelationshipType HYPER_LINK = DynamicRelationshipType.withName("HYPER_LINK");
		int counter = 0;
		Transaction tx = graphDB.beginTx();
		Node from = null, to = null;
		try {
			for(String[] s: new MySqlDumpReader("D:\\jawiki-latest-pagelinks.sql")) {
				if(Integer.parseInt(s[1]) != 0) continue;
				if(s[2].length() <= 2) continue;
				try {
					if (from == null || (Integer)from.getProperty("page_id") != Integer.parseInt(s[0])) {
						from = pages.get("page_id", Integer.parseInt(s[0])).getSingle();
						if(from == null) continue;
					}
					to = pages.get("page_title", s[2].substring(1, s[2].length() - 1)).getSingle();
					if(to == null) continue;
					Relationship link = from.createRelationshipTo(to, HYPER_LINK);

					//こうしないとメモリが・・・
					if (++counter % 100000 == 0) {
						tx.success();
						tx.finish();
						System.gc();
						tx = graphDB.beginTx();
					}
				} catch (Exception e) {
					System.out.println(from);
					System.out.println(to);
					System.out.println(s[0]+" "+s[1]+" "+s[2]);
				}
			}
			tx.success();
		} catch (Exception e) {
			tx.failure();
			
			System.out.println("error");
		} finally {
			tx.finish();
			graphDB.shutdown();
		}
		
	}
	
	public static void view() {
		GraphDatabaseService graphDB = new EmbeddedGraphDatabase("D:\\neo4j\\wikipedia");
		try {
			IndexManager index = graphDB.index();
			Index<Node> pages = index.forNodes("pages");
			for (Node page: pages.query("page_title", "セロリ*")) {
				System.out.println(page.getProperty("page_id"));
				System.out.println(page.getProperty("page_title"));
			}
		} catch(Exception e) {
			System.out.println(e.getMessage());
		} finally {
			graphDB.shutdown();
		}
		
	}
	
	public static void main(String[] args) {
		GraphBuilder.createPages();
		GraphBuilder.createRelationShip();
//		Main.view();
	}

}
