package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import edu.uams.dbmi.rts.tuple.PtoLackUTuple;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoLackUTuplePersister extends AssertionalTuplePersister {

	UniversalNodeCreator unc;
	/*
	 * Here's an interesting question.  Right now we assume that we will only say
	 *   that an instance lacks a relation to some universal for instances that
	 *   are not temporal regions.  But will we ever say that a temporal region
	 *   lacks a relationship to some universal?  I doubt it, but you never know...
	 */

	public PtoLackUTuplePersister(GraphDatabaseService db) {
		super(db);
		unc = new UniversalNodeCreator(this.graphDb);
	}

	@Override
	protected void setTupleTypeProperty() {
		n.addLabel(RtsTupleNodeLabel.L);
	}
	
	@Override
	public void handleTupleSpecificParameters(Transaction tx) {
		super.handleTupleSpecificParameters(tx);
		/* by now, we've already handled iuit, iuia, iuip, ta, tr, and r, which 
		 *   leaves ptou, and should we choose to do something with it someday,
		 *   iuio. 
		 */
		connectToUniversalNode(tx);
	}

	private void connectToUniversalNode(Transaction tx) {
		PtoLackUTuple ptolacku = (PtoLackUTuple)tupleToPersist;
		Node target = unc.persistEntity(ptolacku.getRelationshipURI().toString(), tx);
		n.createRelationshipTo(target, RtsRelationshipType.uui);
		
		Node ontology = inc.persistEntity(ontologyForUui.toString(), tx);
		Iterable<Relationship> ontologyRels = target.getRelationships(RtsRelationshipType.iuio);
		boolean hasOntologyTarget = false;
		for (Relationship rel : ontologyRels) {
			Node relTarget = rel.getEndNode();
			hasOntologyTarget = relTarget.equals(ontology);
		}
		if (!hasOntologyTarget)
			target.createRelationshipTo(ontology, RtsRelationshipType.iuio);	
	}
	
	@Override
	protected void connectToReferent(Transaction tx) {
		InstanceNodeCreator inc = new InstanceNodeCreator(graphDb);
		Node referentNode = inc.persistEntity(
			((PtoLackUTuple)tupleToPersist).getReferentIui().toString(), tx);
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
