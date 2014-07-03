package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.TenTemplate;
import edu.ufl.ctsi.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalRegionNodeCreator;

public class TenTemplatePersister extends RepresentationalTemplatePersister {

	InstanceNodeCreator inc;
	TemporalRegionNodeCreator trnc;
	
	String taIui;
	String nsIui;
	String name;
	String teIui;
	
	public TenTemplatePersister(GraphDatabaseService db, ExecutionEngine ee,
			RtsNodeLabel referentLabel) {
		super(db, ee, referentLabel);
		inc = new InstanceNodeCreator(this.ee);
		trnc = new TemporalRegionNodeCreator(this.ee);
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
		taIui = ten.getAuthoringTimeIui().toString();
		nsIui = ten.getNamingSystemIui().toString();
		teIui = ten.getTemporalEntityIui().toString();
		
	}

	private void connectToTimeOfAssertion() {
		Node target = trnc.persistEntity(taIui);
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}
	
	private void connectToTemporalEntity() {
		Node target = trnc.persistEntity(teIui);
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
	public EntityNodePersister getReferentNodeCreator() {
		return inc;
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ten");
	}

}
