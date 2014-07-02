package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.TeTemplate;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.ufl.ctsi.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalRegionNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class TeTemplatePersister extends RepresentationalTemplatePersister {

	TemporalRegionNodeCreator trnc;
	UniversalNodeCreator unc;
	
	Iso8601DateTimeFormatter dtf;
	
	public TeTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee, RtsNodeLabel.TEMPORAL_REGION);
		trnc = new TemporalRegionNodeCreator(this.ee);
		unc = new UniversalNodeCreator(this.ee);
		dtf = new Iso8601DateTimeFormatter();
	}

	@Override
	public void handleTemplateSpecificParameters() {
		//superclass handles iuit, iuip, and iuia
		
		setTapProperty();
		connectToUuiNode();
	}

	protected void setTapProperty() {
		TeTemplate te = (TeTemplate)templateToPersist;
		n.setProperty("tap", dtf.format(te.getAuthoringTimestamp()));
	}
	
	protected void connectToUuiNode() {
		TeTemplate te = (TeTemplate)templateToPersist;
		Node target = unc.persistEntity(te.getUniversalUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}

	@Override
	public EntityNodePersister getReferentNodeCreator() {
		return trnc;
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "te");		
	}

}
