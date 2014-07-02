package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

import edu.ufl.ctsi.neo4j.RtsNodeLabel;

public class UniversalNodeCreator extends EntityNodePersister {

	static final String QUERY = "MERGE (n:universal { uui: {value} }) return n";

	public UniversalNodeCreator(ExecutionEngine engine) {
		super(engine);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String setupQuery() {
		return QUERY;
	}

	@Override
	protected Label getLabel() {
		return DynamicLabel.label(RtsNodeLabel.TYPE.getLabelText());
	}

}
