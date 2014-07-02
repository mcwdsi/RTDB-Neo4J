package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.ufl.ctsi.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public class ATemplatePersister extends RepresentationalTemplatePersister {

	InstanceNodeCreator inc;
	Iso8601DateTimeFormatter dtf;
	
	public ATemplatePersister(GraphDatabaseService db, ExecutionEngine ee)
			 {
		super(db, ee, RtsNodeLabel.INSTANCE);
		inc = new InstanceNodeCreator(this.ee);
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
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "a");
	}

	@Override
	public EntityNodePersister getReferentNodeCreator() {
		return inc;
	}

}
