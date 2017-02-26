package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public abstract class RepresentationalTemplatePersister extends
		RtsTemplatePersister {
	
	InstanceNodeCreator inc;

	public RepresentationalTemplatePersister(GraphDatabaseService db,
			ExecutionEngine ee) {
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
	
	protected abstract void connectToReferent();// {
		//InstanceNodeCreator inc = new InstanceNodeCreator(ee);
		//Node referentNode = inc.persistEntity(templateToPersist.getReferent().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		//referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	//}
	
	protected void connectToAuthor() {
		Node authorNode = inc.persistEntity(templateToPersist.getAuthorIui().toString());
		n.createRelationshipTo(authorNode, RtsRelationshipType.iuia);
	}
	
	public abstract void handleTemplateSpecificParameters();
}
