package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.graphdb.GraphDatabaseService;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class ConceptNodeCreator extends EntityNodePersister {

	public ConceptNodeCreator(GraphDatabaseService db) {
		super(db);
	}

	static final String QUERY = "MERGE (n:" + RtsNodeLabel.CONCEPT.getLabelText() + " { cui: $value }) return n";

	@Override
	protected String setupQuery() {
		return QUERY;
	}
}
