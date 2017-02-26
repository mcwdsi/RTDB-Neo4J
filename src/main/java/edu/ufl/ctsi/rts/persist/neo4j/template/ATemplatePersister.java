package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public class ATemplatePersister extends RepresentationalTemplatePersister {

	Iso8601DateTimeFormatter dtf;
	
	public ATemplatePersister(GraphDatabaseService db, ExecutionEngine ee)
			 {
		super(db, ee);
		dtf = new Iso8601DateTimeFormatter();
	}

	@Override
	public void handleTemplateSpecificParameters() {
		//superclass handles iuit, iuip, and iuia, so only one left is tap
		setTapProperty();
	}
	
	protected void setTapProperty() {
		ATemplate a = (ATemplate)templateToPersist;
		n.setProperty("tap", dtf.format(a.getAuthoringTimestamp()));
	}

	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "a");
		n.addLabel(RtsTemplateNodeLabel.a);
	}

	@Override
	protected void connectToReferent() {
		InstanceNodeCreator inc = new InstanceNodeCreator(ee);
		Node referentNode = inc.persistEntity(((ATemplate)templateToPersist).getReferent().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
