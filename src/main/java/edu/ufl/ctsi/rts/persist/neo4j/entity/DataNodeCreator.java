package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class DataNodeCreator extends EntityNodePersister {

	public DataNodeCreator(ExecutionEngine engine) {
		super(engine);
	}

	static final String QUERY = "MERGE (n:data { dr: {value} }) return n";

	@Override
	protected String setupQuery() {
		return QUERY;
	}

	@Override
	protected Label getLabel() {
		return DynamicLabel.label(RtsNodeLabel.DATA.getLabelText());
	}

}
