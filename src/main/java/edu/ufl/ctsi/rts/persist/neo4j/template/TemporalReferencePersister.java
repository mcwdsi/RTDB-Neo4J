package edu.ufl.ctsi.rts.persist.neo4j.template;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.rts.uui.Uui;
import edu.ufl.ctsi.rts.neo4j.RtsNodeLabel;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class TemporalReferencePersister {

	static final String TEMPORAL_NODE_BY_TEMPORAL_REFERENCE_QUERY = 
			"MATCH (n:" + RtsNodeLabel.TEMPORAL_REGION.getLabelText() + 
				" { tref : {value} }) return n";
	
	GraphDatabaseService graphDb;

	
	TemporalNodeCreator tnc;
	InstanceNodeCreator inc;
	UniversalNodeCreator unc;
	
	Node n;

	protected TemporalReference temporalReferenceToPersist;
		
	public TemporalReferencePersister(GraphDatabaseService db) {
		this.graphDb = db;
		tnc = new TemporalNodeCreator(this.graphDb);
		inc = new InstanceNodeCreator(this.graphDb);
		unc = new UniversalNodeCreator(this.graphDb);
	}
	
	public Node persistTemporalReference(TemporalReference t) {
		//check to see if temporal reference exists already, if so, then grab the node and return
		if (existsInDb(t)) {
			return n;
		}
		
		/* 
		 * for future reference, so we don't have to keep passing it around as a
		 * 	parameter 
		 */
		temporalReferenceToPersist = t;
		
		//if not in database already, then create the template node
		n = tnc.persistEntity(t.getIdentifier());
		
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

	protected boolean existsInDb(TemporalReference t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getIdentifier());
		Result r = 
				graphDb.execute(TEMPORAL_NODE_BY_TEMPORAL_REFERENCE_QUERY, parameters);
		boolean exists = r.hasNext();
		if (exists) {
			Map<String,Object> map = r.next();
			Collection<Object> c = map.values();
			if (c.size() > 1) System.err.println("should only have retrieved one temporal reference!");
			n = (Node)c.iterator().next();
			System.out.println(n);
		}
		return exists;
	}
	
	public Node getNode() {
		return n;
	}
	
}