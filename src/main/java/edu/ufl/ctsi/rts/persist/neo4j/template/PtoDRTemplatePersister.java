package edu.ufl.ctsi.rts.persist.neo4j.template;

import java.nio.charset.Charset;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.PtoDRTemplate;
import edu.ufl.ctsi.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.DataNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoDRTemplatePersister extends AssertionalTemplatePersister {

	InstanceNodeCreator inc;
	UniversalNodeCreator unc;
	DataNodeCreator dnc;
	
	public PtoDRTemplatePersister(GraphDatabaseService db, ExecutionEngine ee,
			RtsNodeLabel referentLabel) {
		super(db, ee, referentLabel);
		inc = new InstanceNodeCreator(this.ee);
		unc = new UniversalNodeCreator(this.ee);
		dnc = new DataNodeCreator(this.ee);
	}

	@Override
	public EntityNodePersister getReferentNodeCreator() {
		return inc;
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptodr");
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
		PtoDRTemplate ptodr = (PtoDRTemplate)templateToPersist;
		Node target = unc.persistEntity(ptodr.getDatatypeUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}
	
	
	private void connectToDataNode() {
		PtoDRTemplate ptodr = (PtoDRTemplate)templateToPersist;
		String dataAsString = new String(ptodr.getData(), Charset.forName("UTF-16"));
		Node target = unc.persistEntity(dataAsString);
		n.createRelationshipTo(target, RtsRelationshipType.dr);
	}
}
