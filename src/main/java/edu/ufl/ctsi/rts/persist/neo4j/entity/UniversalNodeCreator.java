package edu.ufl.ctsi.rts.persist.neo4j.entity;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class UniversalNodeCreator extends EntityNodePersister {

	static final String QUERY = "MERGE (n:universal { uui: {value} }) return n";

	public UniversalNodeCreator(GraphDatabaseService db) {
		super(db);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String setupQuery() {
		return QUERY;
	}

	@Override
	protected Label getLabel() {
		return Label.label(RtsNodeLabel.TYPE.getLabelText());
	}

}
