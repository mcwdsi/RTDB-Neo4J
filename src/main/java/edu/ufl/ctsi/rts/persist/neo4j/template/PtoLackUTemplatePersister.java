package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.PtoLackUTemplate;
import edu.ufl.ctsi.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoLackUTemplatePersister extends AssertionalTemplatePersister {

	UniversalNodeCreator unc;
	/*
	 * Here's an interesting question.  Right now we assume that we will only say
	 *   that an instance lacks a relation to some universal for instances that
	 *   are not temporal regions.  But will we ever say that a temporal region
	 *   lacks a relationship to some universal?  I doubt it, but you never know...
	 */
	InstanceNodeCreator inc;
	
	public PtoLackUTemplatePersister(GraphDatabaseService db,
			ExecutionEngine ee, RtsNodeLabel referentLabel) {
		super(db, ee, referentLabel);
		inc = new InstanceNodeCreator(this.ee);
		unc = new UniversalNodeCreator(this.ee);
	}

	@Override
	public EntityNodePersister getReferentNodeCreator() {
		return inc;
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptolacku");
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		super.handleTemplateSpecificParameters();
		/* by now, we've already handled iuit, iuia, iuip, ta, tr, and r, which 
		 *   leaves ptou, and should we choose to do something with it someday,
		 *   iuio. 
		 */
		connectToUniversalNode();
	}

	private void connectToUniversalNode() {
		PtoLackUTemplate ptolacku = (PtoLackUTemplate)templateToPersist;
		Node target = unc.persistEntity(ptolacku.getRelationshipURI().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}
}
