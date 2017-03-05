package edu.ufl.ctsi.rts.persist.neo4j.entity;

import java.util.HashMap;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

public abstract class EntityNodePersister {
	
	static HashMap<String, Node> uiNode = new HashMap<String, Node>();
	
	GraphDatabaseService graphDb;

	
	public EntityNodePersister(GraphDatabaseService db) {
		this.graphDb = db;
	}

	public Node persistEntity(String ui) {
		//check the cache first
		Node n = getFromCache(ui);
		if (n!=null) return n;
		
		//Each subclass will have slightly different query to run
		String query = setupQuery();
		System.out.println("entity node persister: query = " + query);
		System.out.println("ui = " + ui);
		
		/*Set up parameters of query.  Each subclass will require that we
		   set the "value" parameter of the unique identifier property to
		   the unique identifier sent here
		   */
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", ui);
		//parameters.put("label", getLabel());
		
		
		//run the query.
		Result er = graphDb.execute( query, parameters );
		System.out.println(er.toString());
		List<String> cs = er.columns();
		for (String c : cs) {
			System.out.println(c);
		}
		//ResourceIterator<Node> resultIterator = ee.execute( query, parameters ).columnAs( "n" );
	    //n = resultIterator.next();
	    n = (Node) er.columnAs("n").next();
	    
	    //add node to cache. TODO: if overall transaction fails, then we need to clear this cache 
	    uiNode.put(ui, n);
	    
	    return n;
	}
	
	protected Node getFromCache(String ui) {
		Node n = (uiNode.containsKey(ui)) ? uiNode.get(ui) : null;
		return n;
	}
	
	public static void clearCache() {
		uiNode.clear();
	}
	
	protected abstract String setupQuery();
}
