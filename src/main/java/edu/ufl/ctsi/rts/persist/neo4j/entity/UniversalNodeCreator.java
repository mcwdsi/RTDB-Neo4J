package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.graphdb.GraphDatabaseService;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class UniversalNodeCreator extends EntityNodePersister {

	static final String QUERY = "MERGE (n:" + RtsNodeLabel.TYPE.getLabelText() + " { uui: $value }) return n";

	public UniversalNodeCreator(GraphDatabaseService db) {
		super(db);
	}

	@Override
	protected String setupQuery() {
		return QUERY;
	}
}
