package edu.ufl.ctsi.rts.persist.neo4j.template;


import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;

public class PtoPTemplatePersister extends AssertionalTemplatePersister {
	
	public PtoPTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptop");
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		// superclass handles iuit, iuip, iuia, ta, tr, and r
		super.handleTemplateSpecificParameters();
		
		/*
		 * Connect node to each particular node in p parameter
		 */
		connectToParticulars();
	}

	private void connectToParticulars() {
		int order = 1;
		PtoPTemplate ptop = (PtoPTemplate)templateToPersist;
		Iterable<Iui> p = ptop.getParticulars();
		for (Iui i : p) {
			Node target = inc.persistEntity(i.toString());
			Relationship r = n.createRelationshipTo(target, RtsRelationshipType.p);
			r.setProperty("relation order", Integer.toString(order));
			order++;
		}
	}

}
