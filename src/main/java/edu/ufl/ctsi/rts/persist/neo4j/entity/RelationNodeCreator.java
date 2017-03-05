package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.graphdb.GraphDatabaseService;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class RelationNodeCreator extends EntityNodePersister {

	static final String QUERY = "MERGE (n:" + RtsNodeLabel.RELATION.getLabelText() + " { rui: $value }) return n";

	public RelationNodeCreator(GraphDatabaseService db) {
		super(db);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String setupQuery() {
		return QUERY;
	}
}
