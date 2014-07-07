package edu.ufl.ctsi.rts.persist.neo4j.template;

import java.nio.charset.Charset;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.DataNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoDETemplatePersister extends AssertionalTemplatePersister {

	UniversalNodeCreator unc;
	DataNodeCreator dnc;
	
	public PtoDETemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
		unc = new UniversalNodeCreator(this.ee);
		dnc = new DataNodeCreator(this.ee);
	}

	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptodr");
		n.addLabel(RtsTemplateNodeLabel.ptode);
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		super.handleTemplateSpecificParameters();
		/*
		 * At this point, we've already handled iuit, iuip, iuia, ta, tr, and r.
		 */
		
		//connect to universal for data
		connectToUniversalNode();
		
		//connect to data
		connectToDataNode();
	}

	private void connectToUniversalNode() {
		PtoDETemplate ptodr = (PtoDETemplate)templateToPersist;
		Node target = unc.persistEntity(ptodr.getDatatypeUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}

	private void connectToDataNode() {
		PtoDETemplate ptodr = (PtoDETemplate)templateToPersist;
		String dataAsString = new String(ptodr.getData(), Charset.forName("UTF-8"));
		Node target = dnc.persistEntity(dataAsString);
		n.createRelationshipTo(target, RtsRelationshipType.dr);
	}
}
