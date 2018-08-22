package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.time.TemporalRegion;
import edu.uams.dbmi.rts.uui.Uui;
import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class TemporalRegionPersister {

	static final String TEMPORAL_REGION_BY_TEMPORAL_REFERENCE_QUERY = 
			"MATCH (n:" + RtsNodeLabel.TEMPORAL_REGION.getLabelText() + 
				" { tref : {value} })-[:uui]->(n2:universal),"
				+ "n-[:iuins]->(n3:instance) return n";
	
	static final String TEMPORAL_NODE_BY_TEMPORAL_REFERENCE_QUERY = 
			"MATCH (n:" + RtsNodeLabel.TEMPORAL_REGION.getLabelText() + 
			" { tref : {value} }) return n";
	
	GraphDatabaseService graphDb;

	
	TemporalNodeCreator tnc;
	InstanceNodeCreator inc;
	UniversalNodeCreator unc;
	
	Node n;

	protected TemporalRegion temporalReferenceToPersist;
		
	public TemporalRegionPersister(GraphDatabaseService db) {
		this.graphDb = db;
		tnc = new TemporalNodeCreator(this.graphDb);
		inc = new InstanceNodeCreator(this.graphDb);
		unc = new UniversalNodeCreator(this.graphDb);
	}
	
	public Node persistTemporalRegion(TemporalRegion t) {
		//check to see if temporal reference exists already, if so, then grab the node and return
		if (existsInDb(t)) {
			return n;
		}
		
		/* 
		 * for future reference, so we don't have to keep passing it around as a
		 * 	parameter 
		 */
		temporalReferenceToPersist = t;
		
		//if not in database already, then create the temporal node
		n = tnc.persistEntity(t.getTemporalReference().toString());
		
		//connect temporal entity to its type (0D vs. 1D)
		connectToType(t.getTemporalType());
		
		//connect temporal entity to calendaring system
		connectToCalendaringSystem(t.getCalendarSystemIui());
		
		//add isIso flag
		n.setProperty("isIso", t.isISO());
		
		return n;
				
	}
	
	private void connectToType(Uui temporalType) {
		Node target = unc.persistEntity(temporalType.toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
		
	}

	private void connectToCalendaringSystem(Iui calendarSystemIui) {
		Node target = inc.persistEntity(calendarSystemIui.toString());
		n.createRelationshipTo(target, RtsRelationshipType.iuins);
	}

	protected boolean existsInDb(TemporalRegion t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTemporalReference().toString());
		Result r = 
				graphDb.execute(TEMPORAL_REGION_BY_TEMPORAL_REFERENCE_QUERY, parameters);
		boolean exists = r.hasNext();
		//System.out.println("TemporalReferencePersister: checking to see if node exists already");
		if (exists) {
			Map<String,Object> map = r.next();
			Collection<Object> c = map.values();
			if (c.size() > 1) System.err.println("should have retrieved at most one temporal reference!");
			n = (Node)c.iterator().next();
			//System.out.println("\t" + n);
		}
		//System.out.println(exists);
		return exists;
	}
	
	public Node getNode() {
		return n;
	}
	
}
