package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import edu.uams.dbmi.rts.tuple.RtsTuple;
import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TupleNodeCreator;

public abstract class RtsTuplePersister {
	
	static final String TUPLE_BY_IUI_QUERY = "MATCH (n:" + RtsNodeLabel.TUPLE.getLabelText() + " { iui : $value }) return n";
	
	//static final String TUPLE_TYPE_PROPERTY_NAME = "type";
	
	GraphDatabaseService graphDb;
	
	RtsTuple tupleToPersist;
	
	TupleNodeCreator tnc;
	
	Node n;
	
	public RtsTuplePersister(GraphDatabaseService db) {
		graphDb = db;

		tnc = new TupleNodeCreator(db);
	}
	
	public void persistTuple(RtsTuple t, Transaction tx) {
		//check to see if tuple exists already, if so, then done
		if (existsInDb(t, tx)) return;
		
		/* 
		 * for future reference, so we don't have to keep passing it around as a
		 * 	parameter 
		 */
		tupleToPersist = t;
		
		//if not in database already, then create the tuple node
		n = tnc.persistEntity(t.getTupleIui().toString(), tx);
				
		//set the type of the tuple - each non-abstract subclass will know its type
		setTupleTypeProperty();
		
		//then call abstract method that subclasses must implement to handle
		// specifics of different tuples
		completeTuple(t, tx);
	}
	
	protected boolean existsInDb(RtsTuple t, Transaction tx) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTupleIui().toString());
	
		Result er = tx.execute( TUPLE_BY_IUI_QUERY, parameters );
		boolean exists = er.hasNext();
			
		return exists;		
		//return graphDb.executeTransactionally(TUPLE_BY_IUI_QUERY, parameters).hasNext();
	}

	protected abstract void completeTuple(RtsTuple t, Transaction tx);
	protected abstract void setTupleTypeProperty(); 
	
}
