package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.rts.time.TemporalRegion;
import edu.uams.dbmi.rts.uui.Uui;
import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class TemporalReferencePersister {

	static final String TEMPORAL_REGION_BY_TEMPORAL_REFERENCE_QUERY = 
			"MATCH (n:" + RtsNodeLabel.TEMPORAL_REGION.getLabelText() + 
				" { tref : $value })-[:uui]->(n2:universal),"
				+ "(n)-[:iuins]->(n3:instance) return n";
	
	static final String TEMPORAL_NODE_BY_TEMPORAL_REFERENCE_QUERY = 
			"MATCH (n:" + RtsNodeLabel.TEMPORAL_REGION.getLabelText() + 
			" { tref : $value }) return n";
	
	GraphDatabaseService graphDb;

	
	TemporalNodeCreator tnc;
	InstanceNodeCreator inc;
	UniversalNodeCreator unc;
	
	Node n;

	protected TemporalReference temporalReferenceToPersist;
	protected Transaction tx;
		
	public TemporalReferencePersister(GraphDatabaseService db) {
		this.graphDb = db;
		tnc = new TemporalNodeCreator(this.graphDb);
		inc = new InstanceNodeCreator(this.graphDb);
		unc = new UniversalNodeCreator(this.graphDb);
	}
	
	public Node persistTemporalReference(TemporalReference t, Transaction tx) {
		//check to see if temporal reference exists already, if so, then grab the node and return
		if (existsInDb(t, tx)) {
			if (t instanceof TemporalRegion) {
				if (!n.hasRelationship(RtsRelationshipType.uui)) {
					TemporalRegion tr = (TemporalRegion)t;
					connectToType(tr.getTemporalType(), tx);
				}
			}
			return n;
		}
		/* 
		 * for future reference, so we don't have to keep passing it around as a
		 * 	parameter 
		 */
		temporalReferenceToPersist = t;
			
		//if not in database already, then create the temporal node
		n = tnc.persistEntity(t.toString(), tx);
			
		//connect temporal entity to its type (0D vs. 1D)
		//System.out.println("Connecting temporal region to its type:");
		if (t instanceof TemporalRegion) {
			connectToType(((TemporalRegion)t).getTemporalType(), tx);
		}
			
		//connect temporal entity to calendaring system
		//System.out.println("Connecting temporal region to its calendaring system:");
		connectToCalendaringSystem(t.getCalendarSystemIui(), tx);
		
		return n;			
	}
	
	private void connectToType(Uui temporalType, Transaction tx) {
		Node target = unc.persistEntity(temporalType.toString(), tx);
		//System.out.println("Universal node for " + temporalType + " has ID: " + target.getId());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
		
	}

	private void connectToCalendaringSystem(Iui calendarSystemIui, Transaction tx) {
		Node target = inc.persistEntity(calendarSystemIui.toString(), tx);
		n.createRelationshipTo(target, RtsRelationshipType.iuins);
	}

	protected boolean existsInDb(TemporalReference t, Transaction tx) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.toString());
	
		Result r = 
			tx.execute(TEMPORAL_REGION_BY_TEMPORAL_REFERENCE_QUERY, parameters);

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
