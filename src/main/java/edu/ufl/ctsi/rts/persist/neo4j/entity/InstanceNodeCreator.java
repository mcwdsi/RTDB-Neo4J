package edu.ufl.ctsi.rts.persist.neo4j.entity;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;

import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;

public class InstanceNodeCreator extends EntityNodePersister {

	static final String QUERY = "MERGE (n:" + RtsNodeLabel.INSTANCE.getLabelText() + " { iui: $value }) return n";

	public InstanceNodeCreator(GraphDatabaseService db) {
		super(db);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String setupQuery() {
		return QUERY;
	}

	public Iterator<String> getAllIuis() {
		return uiNode.keySet().iterator();
	}
}
