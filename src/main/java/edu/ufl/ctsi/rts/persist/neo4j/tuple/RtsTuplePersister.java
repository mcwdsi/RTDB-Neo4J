package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.tuple.RtsTuple;
import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TupleNodeCreator;

public abstract class RtsTuplePersister {
	
	static final String TUPLE_BY_IUI_QUERY = "MATCH (n:" + RtsNodeLabel.TUPLE.getLabelText() + " { iui : {value} }) return n";
	
	static final String TUPLE_TYPE_PROPERTY_NAME = "type";
	
	GraphDatabaseService graphDb;
	
	RtsTuple tupleToPersist;
	
	TupleNodeCreator tnc;
	
	Node n;
	
	public RtsTuplePersister(GraphDatabaseService db) {
		graphDb = db;

		tnc = new TupleNodeCreator(db);
	}
	
	public void persistTuple(RtsTuple t) {
		//check to see if tuple exists already, if so, then done
		if (existsInDb(t)) return;
		
		/* 
		 * for future reference, so we don't have to keep passing it around as a
		 * 	parameter 
		 */
		tupleToPersist = t;
		
		//if not in database already, then create the tuple node
		n = tnc.persistEntity(t.getTupleIui().toString());
				
		//set the type of the tuple - each non-abstract subclass will know its type
		setTupleTypeProperty();
		
		//then call abstract method that subclasses must implement to handle
		// specifics of different tuples
		completeTuple(t);
	}
	
	protected boolean existsInDb(RtsTuple t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTupleIui().toString());
		return graphDb.execute(TUPLE_BY_IUI_QUERY, parameters).hasNext();
	}

	protected abstract void completeTuple(RtsTuple t);
	protected abstract void setTupleTypeProperty(); 
	
}
