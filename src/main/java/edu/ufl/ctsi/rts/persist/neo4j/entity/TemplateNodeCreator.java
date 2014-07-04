package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class TemplateNodeCreator extends EntityNodePersister {

	public TemplateNodeCreator(ExecutionEngine engine) {
		super(engine);
		// TODO Auto-generated constructor stub
	}
	
	static final String QUERY = "MERGE (n:template { iui: {value} }) return n";

	@Override
	protected String setupQuery() {
		return QUERY;
	}

	@Override
	protected Label getLabel() {
		return DynamicLabel.label(RtsNodeLabel.TEMPLATE.getLabelText());
	}

}
