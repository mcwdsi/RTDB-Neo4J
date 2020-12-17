package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import edu.uams.dbmi.rts.tuple.RtsTuple;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public abstract class RepresentationalTuplePersister extends
		RtsTuplePersister {
	
	InstanceNodeCreator inc;

	public RepresentationalTuplePersister(GraphDatabaseService db) {
		super(db);
		inc = new InstanceNodeCreator(this.graphDb);
	}

	@Override
	protected void completeTuple(RtsTuple t, Transaction tx) {
		//connect to referent
		connectToReferent(tx);
		//connect tuple to author
		connectToAuthor(tx);
		//handle tuple specific parameters
		handleTupleSpecificParameters(tx);
	}
	
	protected abstract void connectToReferent(Transaction tx);// {
		//InstanceNodeCreator inc = new InstanceNodeCreator(ee);
		//Node referentNode = inc.persistEntity(tupleToPersist.getReferent().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		//referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	//}
	
	protected void connectToAuthor(Transaction tx) {
		Node authorNode = inc.persistEntity(tupleToPersist.getAuthorIui().toString(), tx);
		n.createRelationshipTo(authorNode, RtsRelationshipType.iuia);
	}
	
	public abstract void handleTupleSpecificParameters(Transaction tx);
}
