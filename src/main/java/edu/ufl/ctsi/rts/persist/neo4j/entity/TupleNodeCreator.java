package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.graphdb.GraphDatabaseService;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class TupleNodeCreator extends EntityNodePersister {

	public TupleNodeCreator(GraphDatabaseService db) {
		super(db);
		// TODO Auto-generated constructor stub
	}
	
	static final String QUERY = "MERGE (n:" + RtsNodeLabel.TEMPLATE.getLabelText() + " { iui: $value }) return n";

	@Override
	protected String setupQuery() {
		return QUERY;
	}
}
