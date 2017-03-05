package edu.ufl.ctsi.rts.persist.neo4j.template;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemplateNodeCreator;

public abstract class RtsTemplatePersister {
	
	static final String TEMPLATE_BY_IUI_QUERY = "MATCH (n:" + RtsNodeLabel.TEMPLATE.getLabelText() + " { iui : {value} }) return n";
	
	static final String TEMPLATE_TYPE_PROPERTY_NAME = "type";
	
	GraphDatabaseService graphDb;
	
	RtsTemplate templateToPersist;
	
	TemplateNodeCreator tnc;
	
	Node n;
	
	public RtsTemplatePersister(GraphDatabaseService db) {
		graphDb = db;

		tnc = new TemplateNodeCreator(db);
	}
	
	public void persistTemplate(RtsTemplate t) {
		//check to see if template exists already, if so, then done
		if (existsInDb(t)) return;
		
		/* 
		 * for future reference, so we don't have to keep passing it around as a
		 * 	parameter 
		 */
		templateToPersist = t;
		
		//if not in database already, then create the template node
		n = tnc.persistEntity(t.getTemplateIui().toString());
		
		//set the type of the template - each non-abstract subclass will know its type
		setTemplateTypeProperty();
		
		//then call abstract method that subclasses must implement to handle
		// specifics of different templates
		completeTemplate(t);
	}
	
	protected boolean existsInDb(RtsTemplate t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTemplateIui().toString());
		return graphDb.execute(TEMPLATE_BY_IUI_QUERY, parameters).hasNext();
	}

	protected abstract void completeTemplate(RtsTemplate t);
	protected abstract void setTemplateTypeProperty(); 
	
}
