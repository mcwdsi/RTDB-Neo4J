package edu.ufl.ctsi.rts.persist.neo4j.template;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;

public class PtoPTemplatePersister extends AssertionalTemplatePersister {
	
	TemporalReferencePersister trp;
	
	public PtoPTemplatePersister(GraphDatabaseService db) {
		super(db);
		trp = new TemporalReferencePersister(this.graphDb);
	}

	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptop");
		n.addLabel(RtsTemplateNodeLabel.ptop);
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
		Iterable<ParticularReference> p = ptop.getAllParticulars();
		for (ParticularReference i : p) {
			Node target = null;
			if (i instanceof Iui) {
				target = inc.persistEntity(i.toString());
			} else if (i instanceof TemporalReference) {
				target = trp.persistTemporalReference((TemporalReference)i);
			}
			Relationship r = n.createRelationshipTo(target, RtsRelationshipType.p);
			r.setProperty("relation order", Integer.toString(order));
			order++;
		}
	}

	@Override
	protected void connectToReferent() {
		/* gets connected to referent above in connectToParticulars, so this is a no op */
		
	}

}
