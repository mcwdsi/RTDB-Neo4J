package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import edu.uams.dbmi.rts.template.RtsTemplate;

public class MetadataTemplatePersister extends RtsTemplatePersister {

	public MetadataTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void completeTemplate(RtsTemplate t) {
		// TODO Auto-generated method stub
		connectToTemplate();
		
		connectToActor();
		
		//change
		
		//change reason
		
		//error code
		
		//replacements (s parameter)
	}

	/* connect the metadata template to the template it is about
	 * 
	 */
	private void connectToTemplate() {
		// TODO Auto-generated method stub
		
	}

	/*
	 * Could use a better name, but basically connect metadata template to the
	 * 	entity that acted on it.  Actions = insert, invalidate, revalidate.
	 */
	private void connectToActor() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void setTemplateTypeProperty() {
		// TODO Auto-generated method stub
		
	}

}
