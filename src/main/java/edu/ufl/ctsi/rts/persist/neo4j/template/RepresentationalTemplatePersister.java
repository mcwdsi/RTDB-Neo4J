package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.ufl.ctsi.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public abstract class RepresentationalTemplatePersister extends
		RtsTemplatePersister {
	
	RtsNodeLabel referentLabel;
	
	InstanceNodeCreator inc;

	public RepresentationalTemplatePersister(GraphDatabaseService db,
			ExecutionEngine ee, RtsNodeLabel referentLabel) {
		super(db, ee);
		inc = new InstanceNodeCreator(this.ee);
	}

	@Override
	protected void completeTemplate(RtsTemplate t) {
		//connect to referent
		connectToReferent();
		//connect template to author
		connectToAuthor();
		//handle template specific parameters
		handleTemplateSpecificParameters();
	}
	
	protected void connectToReferent() {
		EntityNodePersister enp = getReferentNodeCreator();
		Node referentNode = enp.persistEntity(templateToPersist.getReferentIui().toString());
		n.createRelationshipTo(referentNode, RtsRelationshipType.iuip);
	}
	
	protected void connectToAuthor() {
		Node authorNode = inc.persistEntity(templateToPersist.getAuthorIui().toString());
		n.createRelationshipTo(authorNode, RtsRelationshipType.iuia);
	}
	
	public abstract void handleTemplateSpecificParameters();
	public abstract EntityNodePersister getReferentNodeCreator();

}
