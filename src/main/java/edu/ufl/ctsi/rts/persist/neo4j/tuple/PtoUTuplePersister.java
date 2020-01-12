package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.uams.dbmi.rts.tuple.PtoUTuple;
import edu.uams.dbmi.rts.tuple.component.RelationshipPolarity;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoUTuplePersister extends AssertionalTuplePersister {

	UniversalNodeCreator unc;
	
	public PtoUTuplePersister(GraphDatabaseService db) {
		super(db);
		unc = new UniversalNodeCreator(this.graphDb);
	}


	@Override
	protected void setTupleTypeProperty() {
		PtoUTuple ptou = (PtoUTuple)tupleToPersist;
		if (ptou.getRelationshipPolarity().equals(RelationshipPolarity.AFFIRMATIVE))
			n.addLabel(RtsTupleNodeLabel.U);
		else 
			n.addLabel(RtsTupleNodeLabel.U_);
	}
	
	@Override
	public void handleTupleSpecificParameters() {
		super.handleTupleSpecificParameters();
		/* by now, we've already handled iuit, iuia, iuip, ta, tr, and r, which 
		 *   leaves ptou, and should we choose to do something with it someday,
		 *   iuio. 
		 */
		connectToUniversalNode();
	}

	private void connectToUniversalNode() {
		PtoUTuple ptou = (PtoUTuple)tupleToPersist;
		Node target = unc.persistEntity(ptou.getUniversalUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
		Node ontology = inc.persistEntity(ontologyForUui.toString());
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
	protected void connectToReferent() {
		InstanceNodeCreator inc = new InstanceNodeCreator(this.graphDb);
		Node referentNode = inc.persistEntity(((PtoUTuple)tupleToPersist).getReferentIui().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
