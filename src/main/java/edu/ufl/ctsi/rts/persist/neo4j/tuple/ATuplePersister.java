package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.tuple.ATuple;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public class ATuplePersister extends RepresentationalTuplePersister {

	Iso8601DateTimeFormatter dtf;
	
	public ATuplePersister(GraphDatabaseService db) {
		super(db);
		dtf = new Iso8601DateTimeFormatter();
	}

	@Override
	public void handleTemplateSpecificParameters() {
		//superclass handles iuit, iuip, and iuia, so only one left is tap
		setTapProperty();
	}
	
	protected void setTapProperty() {
		ATuple a = (ATuple)templateToPersist;
		n.setProperty("tap", dtf.format(a.getAuthoringTimestamp()));
	}

	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "a");
		n.addLabel(RtsTupleNodeLabel.a);
	}

	@Override
	protected void connectToReferent() {
		InstanceNodeCreator inc = new InstanceNodeCreator(this.graphDb);
		Node referentNode = inc.persistEntity(((ATuple)templateToPersist).getReferentIui().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
