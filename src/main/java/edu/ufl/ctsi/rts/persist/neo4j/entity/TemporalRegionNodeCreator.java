package edu.ufl.ctsi.rts.persist.neo4j.entity;

import java.util.Iterator;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

import edu.ufl.ctsi.neo4j.RtsNodeLabel;

public class TemporalRegionNodeCreator extends EntityNodePersister {

	static final String QUERY = "MERGE (n:temporal_region { iui: {value} }) return n";
	
	public TemporalRegionNodeCreator(ExecutionEngine engine) {
		super(engine);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	protected String setupQuery() {
		return QUERY;
	}

	@Override
	protected Label getLabel() {
		return DynamicLabel.label(RtsNodeLabel.TEMPORAL_REGION.getLabelText());
	}
	
	public Iterator<String> getAllIuis() {
		return uiNode.keySet().iterator();
	}
}
