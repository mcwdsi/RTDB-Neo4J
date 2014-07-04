package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.cui.Cui;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.PtoCTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.ConceptNodeCreator;

public class PtoCTemplatePersister extends AssertionalTemplatePersister {
	
	ConceptNodeCreator cnc;
	
	Cui cui;
	Iui conceptSystemIui;
	
	public PtoCTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
		cnc = new ConceptNodeCreator(this.ee);
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptoc");
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		/* 
		 * superclass handles iuit, iuip, iuia, ta, tr
		 */
		super.handleTemplateSpecificParameters();
		
		getParametersFromTemplate();
		
		connectToConceptNode();
		
		connectToConceptSystemNode();
	}

	private void getParametersFromTemplate() {
		PtoCTemplate ptoc = (PtoCTemplate)templateToPersist;
		cui = ptoc.getConceptCui();
		conceptSystemIui = ptoc.getConceptSystemIui();
	}

	private void connectToConceptNode() {
		Node target = cnc.persistEntity(cui.toString());
		n.createRelationshipTo(target, RtsRelationshipType.co);
	}

	private void connectToConceptSystemNode() {
		Node target = inc.persistEntity(conceptSystemIui.toString());
		n.createRelationshipTo(target, RtsRelationshipType.cs);
	}
}
