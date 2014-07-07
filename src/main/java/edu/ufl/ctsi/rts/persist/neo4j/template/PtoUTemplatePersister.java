package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoUTemplatePersister extends AssertionalTemplatePersister {

	UniversalNodeCreator unc;
	
	public PtoUTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
		unc = new UniversalNodeCreator(this.ee);
	}


	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptou");
		n.addLabel(RtsTemplateNodeLabel.ptou);
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
		PtoUTemplate ptou = (PtoUTemplate)templateToPersist;
		Node target = unc.persistEntity(ptou.getUniversalUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}
}
