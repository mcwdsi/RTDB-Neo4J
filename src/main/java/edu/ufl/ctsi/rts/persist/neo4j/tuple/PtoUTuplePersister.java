package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.tuple.PtoUTuple;
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
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptou");
		n.addLabel(RtsTupleNodeLabel.ptou);
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		super.handleTemplateSpecificParameters();
		/* by now, we've already handled iuit, iuia, iuip, ta, tr, and r, which 
		 *   leaves ptou, and should we choose to do something with it someday,
		 *   iuio. 
		 */
		connectToUniversalNode();
	}

	private void connectToUniversalNode() {
		PtoUTuple ptou = (PtoUTuple)templateToPersist;
		Node target = unc.persistEntity(ptou.getUniversalUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}
	
	@Override
	protected void connectToReferent() {
		InstanceNodeCreator inc = new InstanceNodeCreator(this.graphDb);
		Node referentNode = inc.persistEntity(((PtoUTuple)templateToPersist).getReferentIui().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
