package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.TenTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;

@Deprecated
public class TenTemplatePersister extends RepresentationalTemplatePersister {
	
	String taRef;
	String nsIui;
	String name;
	String teRef;
	
	public TenTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
	}

	@Override
	public void handleTemplateSpecificParameters() {
		// TODO Auto-generated method stub
		/*
		 * iuit, iuip, iuia already handled, which leaves iuite, iuins, name,
		 *   and ta  
		 */
		getParametersFromTemplate();
		
		connectToTimeOfAssertion();
		
		connectToTemporalEntity();
		
		connectToNamespace();
		
		addNameAsProperty();
	}

	private void getParametersFromTemplate() {
		TenTemplate ten = (TenTemplate)templateToPersist;
		//taIui = ten.getAuthoringTimeIui().toString();
		taRef = ten.getAuthoringTimeReference().toString();
		nsIui = ten.getNamingSystemIui().toString();
		//teIui = ten.getTemporalEntityIui().toString();
		teRef = ten.getTemporalEntityReference().toString();
		name = ten.getName();
	}

	private void connectToTimeOfAssertion() {
		Node target = inc.persistEntity(taRef);
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}
	
	private void connectToTemporalEntity() {
		Node target = inc.persistEntity(teRef);
		n.createRelationshipTo(target, RtsRelationshipType.iuite);
	}
	
	private void connectToNamespace() {
		Node target = inc.persistEntity(nsIui);
		n.createRelationshipTo(target, RtsRelationshipType.iuins);
	}

	private void addNameAsProperty() {
		n.setProperty("name", name);
	}

	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ten");
		n.addLabel(RtsTemplateNodeLabel.ten);
	}

	@Override
	protected void connectToReferent() {
		// TODO Auto-generated method stub
		throw new IllegalStateException("TenTemplatePersister is deprecated.");
	}

}
