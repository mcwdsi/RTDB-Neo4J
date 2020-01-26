package edu.ufl.ctsi.rts.neo4j;


import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.cui.Cui;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.metadata.RtsChangeReason;
import edu.uams.dbmi.rts.metadata.RtsChangeType;
import edu.uams.dbmi.rts.metadata.RtsErrorCode;
import edu.uams.dbmi.rts.persist.RtsStore;
import edu.uams.dbmi.rts.query.TupleQuery;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.rts.time.TemporalRegion;
import edu.uams.dbmi.rts.tuple.ATuple;
import edu.uams.dbmi.rts.tuple.MetadataTuple;
import edu.uams.dbmi.rts.tuple.PtoCTuple;
import edu.uams.dbmi.rts.tuple.PtoDETuple;
import edu.uams.dbmi.rts.tuple.PtoLackUTuple;
import edu.uams.dbmi.rts.tuple.PtoPTuple;
import edu.uams.dbmi.rts.tuple.PtoUTuple;
import edu.uams.dbmi.rts.tuple.RtsTuple;
import edu.uams.dbmi.rts.tuple.RtsTupleType;
import edu.uams.dbmi.rts.tuple.component.RelationshipPolarity;
import edu.uams.dbmi.rts.uui.Uui;
import edu.uams.dbmi.util.iso8601.Iso8601DateParseException;
import edu.uams.dbmi.util.iso8601.Iso8601DateTime;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeParser;
import edu.uams.dbmi.util.iso8601.Iso8601TimeParseException;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.ATuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.MetadataTuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.PtoCTuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.PtoDETuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.PtoLackUTuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.PtoPTuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.PtoUTuplePersister;
import edu.ufl.ctsi.rts.persist.neo4j.tuple.TemporalRegionPersister;

public class RtsTuplePersistenceManager implements RtsStore {

	static String CNODE_QUERY = "MERGE (n:change_reason { c: {value} }) return n";
	static String CTNODE_QUERY = "MERGE (n:change_type { ct: {value} }) return n";
	
	public GraphDatabaseService graphDb;
	
	Label tupleLabel;
	Label aTupleLabel;
	Label ptouTupleLabel;
	Label ptopTupleLabel;
	Label ptolackuTupleLabel;
	
	Label instanceLabel;
	Label temporalRegionLabel;
	Label typeLabel;
	Label relationLabel;
	Label dataLabel;
	
	Label metadataLabel;

	HashSet<RtsTuple> tuples;
	HashSet<MetadataTuple> metadata;
	HashSet<TemporalReference> tempReferences;
	HashSet<TemporalRegion> tempRegions;
	
	HashMap<Iui, Node> iuiNode;
	HashMap<String, Node> uiNode;
	HashMap<String, RtsTuple> iuiToItsAssignmentTuple;
	HashSet<String> iuisInPtoPTuples;
	HashMap<String, String> iuiToNodeLabel;
	
	Iso8601DateTimeFormatter dttmFormatter;
	
	ATuplePersister atp;
	PtoUTuplePersister pup;
	PtoPTuplePersister ppp;
	PtoLackUTuplePersister plup;
	PtoDETuplePersister pdrp;
	PtoCTuplePersister pcp;
	MetadataTuplePersister mp;
	
	TemporalRegionPersister trp;
	String dbPath;
	
	public RtsTuplePersistenceManager(String dbPath) {
		this.dbPath = dbPath;
		tuples = new HashSet<RtsTuple>();
		metadata = new HashSet<MetadataTuple>();
		tempReferences = new HashSet<TemporalReference>();
		tempRegions = new HashSet<TemporalRegion>();
		iuiNode = new HashMap<Iui, Node>();
		uiNode = new HashMap<String, Node>();
		iuiToItsAssignmentTuple = new HashMap<String, RtsTuple>();
		iuisInPtoPTuples = new HashSet<String>();
		iuiToNodeLabel = new HashMap<String, String>();
		dttmFormatter = new Iso8601DateTimeFormatter();
		createDb();
		setupSchema();
		
		atp = new ATuplePersister(graphDb);
		pup = new PtoUTuplePersister(graphDb);
		ppp = new PtoPTuplePersister(graphDb);
		plup = new PtoLackUTuplePersister(graphDb);
		pdrp = new PtoDETuplePersister(graphDb);
		pcp = new PtoCTuplePersister(graphDb);
		mp = new MetadataTuplePersister(graphDb);
		trp = new TemporalRegionPersister(graphDb);
	}
	
	static final String queryInstanceNode = "match (n) where n.iui={value} return n;";
	
	protected void addTuple(RtsTuple t) {
		if (t instanceof ATuple) {
			ATuple at = (ATuple)t;
			iuiToItsAssignmentTuple.put(at.getReferentIui().toString(), t);
		} else if ( (t instanceof PtoPTuple) ) {
			PtoPTuple ptop = (PtoPTuple)t;
			Iterable<ParticularReference> p = ptop.getAllParticulars();
			for (ParticularReference i : p) {
				if (i instanceof Iui) iuisInPtoPTuples.add(i.toString());
				else if (i instanceof TemporalReference) {
					TemporalReference tr = (TemporalReference)i;
					tempReferences.add(tr);
				}
			}
		} else if ( (t instanceof PtoDETuple) ) {
			PtoDETuple ptode = (PtoDETuple)t;
			ParticularReference pr = ptode.getReferent();
			if (pr instanceof TemporalReference)
				tempReferences.add((TemporalReference)pr);
		}
		if (t instanceof MetadataTuple) {
			metadata.add((MetadataTuple)t);
		} else {
			tuples.add(t);
		}
	} 
	
	public void addTuples(Collection<RtsTuple> t) {
		Iterator<RtsTuple> i = t.iterator();
		while (i.hasNext()) {
			addTuple(i.next());
		}
	}
	
	@Override
	public void commit() {
		commitTuples();
	}
	
	
	public void commitTuples() {
		try (Transaction tx = graphDb.beginTx() ) {
			
			/*
			 * Before we begin, let's be sure that we either have assignment tuples
			 *  for each IUI that a PtoP tuple references or that the IUI node 
			 *  exists in the database already.  
			 */
			checkIuisInPtoP();
			
			Iso8601DateTime dt = new Iso8601DateTime();
			//Iso8601DateTimeFormatter dtf = new Iso8601DateTimeFormatter();
			//String iuid = dtf.format(dt);
			
			for (TemporalRegion r : tempRegions) {
				trp.persistTemporalRegion(r);
			}
			
			for (RtsTuple t : tuples) {
				if (t instanceof ATuple) {
					atp.persistTuple(t);
				} else if (t instanceof PtoUTuple) {
					pup.persistTuple(t);
				} else if (t instanceof PtoLackUTuple) {
					plup.persistTuple(t);
				} else if (t instanceof PtoDETuple) {
					pdrp.persistTuple(t);
				} else if (t instanceof PtoPTuple) {
					ppp.persistTuple(t);
				} else if (t instanceof PtoCTuple) {
					pcp.persistTuple(t);
				} 
			}
			
			for (MetadataTuple d : metadata) {
				d.setAuthoringTimestamp(dt);
				mp.persistTuple(d);
			}
		
			tx.success();
			//tx.close();
			
			/*
			 * We've sent them all to db, so we can clear.  In the future, we will
			 *  likely want to send them to some cache first.  But this class isn't the 
			 *  cache, it is merely the thing that submits a chunk of related 
			 *  tuples as one transaction.
			 */
			tuples.clear();
			metadata.clear();
			tempReferences.clear();
			tempRegions.clear();
			EntityNodePersister.clearCache();
		} catch (Throwable t) {
			if ( t instanceof TransactionFailureException )
	        {
	            TransactionFailureException tfe = (TransactionFailureException)t;
	            System.err.println(tfe.getLocalizedMessage());
	            tfe.printStackTrace();
	        }

		}
	}
	
	private void checkIuisInPtoP() {
		for (String iui : iuisInPtoPTuples) {
			if (!iuiToItsAssignmentTuple.containsKey(iui)) {
				HashMap<String, Object> params = new HashMap<String, Object>();
				params.put("value", iui);
				ResourceIterator<Node> rin = graphDb.execute(queryInstanceNode, params).columnAs("n");
				if (!rin.hasNext()) {
					System.err.println("Iui " + iui + " is referenced in a PtoP tuple but has " +
							"no assignment tuple in the cache and there is no node for it already "
							+ "in the database.");
				}
				/*else {
					Node n = rin.next();
					Iterable<Label> labels = n.getLabels();
					for (Label l : labels) {
						String name = l.name();
						if (name.equals("instance")) {
							iuiToNodeLabel.put(iui, name);
							break;
						} else if (name.equals("temporal_region")) {
							iuiToNodeLabel.put(iui, name);
							break;
						}
					}
				}
				*/
			}
		}
		
	}

	static String createTupleQuery = "CREATE (n:tuple { iui : {value}})";
	
	/*
	 * Above, we made sure that a tuple with this tuple IUI didn't exist already.
	 *  So we're clear to add it de novo without worrying about violating a unique
	 *  constraint on tuple IUIs.
	 */
	@SuppressWarnings("unused")
	private Node createTupleNode(RtsTuple t) {
		Node n = graphDb.createNode(tupleLabel);
		n.setProperty("ui", t.getTupleIui().toString());
		return n;
	}

	public Iterator<RtsTuple> getTupleIterator() {
		return tuples.iterator();
	}
	
	public Iterator<MetadataTuple> getMetadataTupleIterator() {
		return metadata.iterator();
	}
	
	public Stream<RtsTuple> getTupleStream() {
		return tuples.stream();
	}
	
	public Stream<MetadataTuple> getMetadataTupleStream() {
		return metadata.stream();
	}
	
	public Stream<TemporalReference> getTemporalReferenceStream() {
		return tempReferences.stream();
	}
	
	public Stream<TemporalRegion> getTemporalRegionStream() {
		return tempRegions.stream();
	}
	/*
	private void connectToReferentNode(Node tupleNode, RtsTuple t) {
		Node referentNode;
		Iui referentIui = t.getReferentIui();
		if (iuiNode.containsKey(referentIui)) {
			referentNode = iuiNode.get(referentIui);
		} else {
			referentNode = getOrCreateEntityNode(referentIui, RtsNodeLabel.INSTANCE);
		}
		
		tupleNode.createRelationshipTo(referentNode, RtsRelationshipType.iuip);
	}

	private void connectToAuthorNode(Node tupleNode, RtsTuple t) {
		Node authorNode;
		Iui authorIui = t.getAuthorIui();
		if (iuiNode.containsKey(authorIui)) {
			authorNode = iuiNode.get(authorIui);
		} else {
			authorNode = getOrCreateEntityNode(authorIui, RtsNodeLabel.INSTANCE);
		}
		
		tupleNode.createRelationshipTo(authorNode, RtsRelationshipType.iuia);
	}//*/

	/*
	private void completeTeTuple(Node n, TeTuple t) {
		// TODO Auto-generated method stub
		n.setProperty("type", "TE");
		n.setProperty("tap", dttmFormatter.format(t.getAuthoringTimestamp()));
		
		Node typeNode = getOrCreateNode(RtsNodeLabel.TYPE, t.getUniversalUui().toString());
		n.createRelationshipTo(typeNode, RtsRelationshipType.uui);
	}
	*/
	
	static String TYPE_QUERY = "MERGE (n:universal {ui : {value}})"
			//+ "ON CREATE "
			+ "RETURN n";

	@SuppressWarnings("unused")
	private void completeATuple(Node n, ATuple t) {
		n.setProperty("type", "A");
		n.setProperty("tap", dttmFormatter.format(t.getAuthoringTimestamp()));
	}
	

	
	/*
	private void connectToTemporalEntityNode(Node tupleNode, Iui teIui) {
		Node teNode;
		if (iuiNode.containsKey(teIui)) {
			teNode = iuiNode.get(teIui);
		} else {
			teNode = getOrCreateNode(RtsNodeLabel.TEMPORAL_REGION, teIui.toString()); 
					//getOrCreateEntityNode(teIui, RtsNodeLabel.TEMPORAL_REGION);
		}
		
		tupleNode.createRelationshipTo(teNode, RtsRelationshipType.iuite);
	}

	private void connectToNamingSystemNode(Node tupleNode, Iui nsIui) {
		Node teNode;
		if (iuiNode.containsKey(nsIui)) {
			teNode = iuiNode.get(nsIui);
		} else {
			teNode = getOrCreateEntityNode(nsIui, RtsNodeLabel.INSTANCE);
		}
		
		tupleNode.createRelationshipTo(teNode, RtsRelationshipType.ns);
	}//*/
	
	@SuppressWarnings("unused")
	private void connectNodeToNode(Node sourceNode, RtsRelationshipType relType, 
			RtsNodeLabel targetNodeLabel, String targetNodeUi) {
		Node target;
		if (uiNode.containsKey(targetNodeUi)) {
			target = uiNode.get(targetNodeUi);
		} else {
			target = getOrCreateNode(targetNodeLabel, targetNodeUi);
		}
		
		sourceNode.createRelationshipTo(target, relType);
	}
	
	static String nodeQueryBase = "MERGE (n:[label] { ui : {value} }) return n";
	
	private Node getOrCreateNode(RtsNodeLabel targetNodeLabel,
			String targetNodeUi) {
		//build the query and parameters
		String query = nodeQueryBase.replace("[label]", targetNodeLabel.getLabelText());
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", targetNodeUi);
	    
		//run the query.
	    ResourceIterator<Node> resultIterator = graphDb.execute( query, parameters ).columnAs( "n" );
	    Node n = resultIterator.next();
	    
	    //add node to cache
	    uiNode.put(targetNodeUi, n);
	    
	    //TODO change this to also throw a new type of exception, and transaction 
	    //	should roll back
	    if ( targetNodeLabel.equals(RtsNodeLabel.INSTANCE) ) {
	    	if ( !iuiToItsAssignmentTuple.containsKey(targetNodeUi) ) {
		    	System.err.println("ERROR: creating new entity with IUI " + targetNodeUi +
		    			" but this IUI has no corresponding assignment tuple!");
	    	}
	    }
		
		return n;
	}

	//static String tupleByIuiQuery = "START n=node:nodes(iui = {value}) RETURN n";
	static String tupleByIuiQuery = "MATCH (n:tuple { iui : {value} }) return n, labels(n)";
	
	boolean isTupleInDb(RtsTuple t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTupleIui().toString());
		return graphDb.execute(tupleByIuiQuery, parameters).hasNext();
	}
	
	/*
	static String INST_QUERY = "MERGE (n:instance {ui : {value}})"
			//+ "ON CREATE "
			+ "RETURN n";
			
	
	static String TR_QUERY = "MERGE (n:temporal_region {ui : {value}})"
			//+ "ON CREATE "
			+ "RETURN n";
			//*/
	
	static String ENTITY_QUERY = "MERGE (n:[label] {ui : {value}})"
			//+ "ON CREATE "
			+ "RETURN n";
	
	/*Node getOrCreateEntityNode(Iui iui, RtsNodeLabel label) {
		//setup the query with the proper node label and iui property
		String query = ENTITY_QUERY.replace("[label]", label.toString());
	    HashMap<String, Object> parameters = new HashMap<>();
	    parameters.put( "value", iui.toString() );
	    
	    //run the query.
	    ResourceIterator<Node> resultIterator = ee.execute( query, parameters ).columnAs( "n" );
	    Node result = resultIterator.next();
	    
	    //add the node to the cache
	    iuiNode.put(iui, result);
	    

	    if (!iuiToAssignmentTuple.containsKey(iui)) {

	    }
	    return result;
	}
	
	private Node getOrCreateTypeNode(Uui universalUui) {
		//setup the query parameters with the uui
	    HashMap<String, Object> parameters = new HashMap<>();
	    parameters.put( "value", universalUui.toString() );
	    
	    //run the query.
	    ResourceIterator<Node> resultIterator = ee.execute( TYPE_QUERY, parameters ).columnAs( "n" );
	    Node result = resultIterator.next();
	    
	    return result;	
	}
	
	private Node getOrCreateRelationNode(String rui) {
		//TODO
		return null;
	}
	
	private Node getOrCreateDrNode(String data) {
		//TODO
		return null;
	}//*/
	
    void setupSchema() {
    	
    	/*
    	tupleLabel = DynamicLabel.label("tuple");
    	instanceLabel = DynamicLabel.label("instance");
    	typeLabel = DynamicLabel.label("universal");
    	relationLabel = DynamicLabel.label("relation");
    	temporalRegionLabel = DynamicLabel.label("temporal_region");
    	dataLabel = DynamicLabel.label("data");
    	metadataLabel = DynamicLabel.label("metadata");
    	*/
    	
    	tupleLabel = Label.label("tuple");
    	instanceLabel = Label.label("instance");
    	typeLabel = Label.label("universal");
    	relationLabel = Label.label("relation");
    	temporalRegionLabel = Label.label("temporal region");
    	dataLabel = Label.label("data");
    	metadataLabel = Label.label("metadata");
    	
  	
        try ( Transaction tx2 = graphDb.beginTx() )
        {

            graphDb.schema()
                    .constraintFor( tupleLabel )
                    .assertPropertyIsUnique( "iui" )
                    .create();
            
            graphDb.schema()
            		.constraintFor( instanceLabel )
            		.assertPropertyIsUnique( "iui" )
            		.create();
            
            graphDb.schema()
            		.constraintFor( typeLabel )
            		.assertPropertyIsUnique( "uui" )
            		.create();
            
            graphDb.schema()
            		.constraintFor( relationLabel )
            		.assertPropertyIsUnique( "rui" )
            		.create();
            
            graphDb.schema()
    				.constraintFor( dataLabel )
    				.assertPropertyIsUnique( "dr" )
    				.create();
            
            graphDb.schema()
            		.constraintFor( metadataLabel )
            		.assertPropertyIsUnique("c")
            		.create();
            
            graphDb.schema()
    				.constraintFor( metadataLabel )
    				.assertPropertyIsUnique("ct")
    				.create();         
            
            graphDb.schema()
    				.constraintFor( temporalRegionLabel )
    				.assertPropertyIsUnique("tref")
    				.create();   
            
            tx2.success();
        }
    }
    
    //void setupMetadata() {
    	/*
    	 * Experimented with representing change types and reasons as nodes.  Seems like
    	 * too much overhead.
    	 * 
    	 */
    	/*
    	try ( Transaction tx3 = graphDb.beginTx() ) {
    		RtsChangeReason[] reasons = RtsChangeReason.values();
    		for (RtsChangeReason r : reasons) {
    			String value = r.toString();
    			/*Set up parameters of query.  
    			  *//*
    			HashMap<String, Object> parameters = new HashMap<String, Object>();
    			parameters.put("value", value);
    			
    			//run the query.
    			ExecutionResult er = ee.execute( CNODE_QUERY, parameters );
    			System.out.println(er.dumpToString());
    			List<String> cs = er.columns();
    			for (String c : cs) {
    				System.out.println(c);
    			}

    		    //Node n = (Node) er.columnAs("n").next();
    		}
    		
    		RtsChangeType[] types = RtsChangeType.values();
    		for (RtsChangeType t : types) {
    			String value = t.toString();
    			/*Set up parameters of query.  
    			  *//*
    			HashMap<String, Object> parameters = new HashMap<String, Object>();
    			parameters.put("value", value);
    			
    			//run the query.
    			ExecutionResult er = ee.execute( CTNODE_QUERY, parameters );
    			System.out.println(er.dumpToString());
    			List<String> cs = er.columns();
    			for (String c : cs) {
    				System.out.println(c);
    			}

    		    //Node n = (Node) er.columnAs("n").next();
    		}
    		
    		tx3.success();
    	}*/
    //}
    
	public void addTemporalReference(TemporalReference t) {
		tempReferences.add(t);
	}
	
	public void addTemporalRegion(TemporalRegion t) {
		tempRegions.add(t);
	}

	@Override
	public boolean saveTuple(RtsTuple Tuple) {
		addTuple(Tuple);
		return true;
	}

	@Override
	public RtsTuple getTuple(Iui iui) {
		try ( Transaction tx = graphDb.beginTx() ) {
		
			HashMap<String, Object> parameters = new HashMap<String, Object>();
			parameters.put("value", iui.toString().toLowerCase());
			Result r = graphDb.execute(tupleByIuiQuery, parameters);
			//System.out.println(r.resultAsString());
			Node n = null; 
			String label = null;
			while (r.hasNext()) {
				Map<String, Object> rNext = r.next();
				Object nAsO = rNext.get("n");
				n = (Node)nAsO;
				Object labelsAsO = rNext.get("labels(n)");
				@SuppressWarnings("unchecked")
				Iterable<String> labelsAsSet = (Iterable<String>)labelsAsO;
				for (String s : labelsAsSet) {
					if (!s.equals("tuple")) { label = s; break; }
				}
			}
			
			tx.success();
			
			return reconstituteTuple(n, label, iui);
		}
	}

	private RtsTuple reconstituteTuple(Node n, String label, Iui iuit) {
		RtsTuple tuple = null;
		Iui iuip, iuia, iuid, iuics, iuins, iuioU, iuioR, about;
		Set<Iui> s;
		List<ParticularReference> p;
		Uui uui;
		URI r; 
		TemporalRegion ta, tr;
		Iso8601DateTime tap, td;
		Cui co;
		ParticularReference prForDE;
		switch (label) {
			case "A":
				ATuple a = new ATuple();
				a.setTupleIui(iuit);
				iuip = getIuipFromDb(n);
				a.setReferentIui(iuip);
				iuia = getIuiaFromDb(n);
				a.setAuthorIui(iuia);
				tap = getTapFromDb(n);
				a.setAuthoringTimestamp(tap);
				tuple = a;
				break;
			case "U":
			case "U_":
				PtoUTuple ptou = new PtoUTuple();
				ptou.setTupleIui(iuit);
				iuip = getIuipFromDb(n);
				ptou.setReferentIui(iuip);
				iuia = getIuiaFromDb(n);
				ptou.setAuthorIui(iuia);
				uui = getUuiFromDb(n);
				ptou.setUniversalUui(uui);
				iuioU = getIuioForUuiFromDb(n);
				ptou.setUniversalOntologyIui(iuioU);
				ta = getTaFromDb(n);
				tr = getTrFromDb(n);
				ptou.setAuthoringTimeReference(ta.getTemporalReference());
				ptou.setTemporalReference(tr.getTemporalReference());
				r = getRFromDb(n);
				ptou.setRelationshipURI(r);
				iuioR = getIuioForRFromDb(n);
				ptou.setRelationshipOntologyIui(iuioR);
				if (label.equals("U_")) ptou.setRelationshipPolarity(RelationshipPolarity.NEGATED);
				tuple = ptou;
				break;
			case "P":
			case "P_":
				PtoPTuple ptop = new PtoPTuple();
				ptop.setTupleIui(iuit);
				iuia = getIuiaFromDb(n);
				ptop.setAuthorIui(iuia);
				ta = getTaFromDb(n);
				tr = getTrFromDb(n);
				ptop.setAuthoringTimeReference(ta.getTemporalReference());
				ptop.setTemporalReference(tr.getTemporalReference());
				r = getRFromDb(n);
				ptop.setRelationshipURI(r);
				iuioR = getIuioForRFromDb(n);
				ptop.setRelationshipOntologyIui(iuioR);
				p = getPFromDb(n);
				ptop.setParticulars(p);
				if (label.equals("P_")) ptop.setRelationshipPolarity(RelationshipPolarity.NEGATED);
				tuple = ptop;
				break;
			case "L":
				PtoLackUTuple ptolacku = new PtoLackUTuple();
				ptolacku.setTupleIui(iuit);
				iuip = getIuipFromDb(n);
				ptolacku.setReferentIui(iuip);
				iuia = getIuiaFromDb(n);
				ptolacku.setAuthorIui(iuia);
				uui = getUuiFromDb(n);
				ptolacku.setUniversalUui(uui);
				iuioU = getIuioForUuiFromDb(n);
				ptolacku.setUniversalOntologyIui(iuioU);
				ta = getTaFromDb(n);
				tr = getTrFromDb(n);
				ptolacku.setAuthoringTimeReference(ta.getTemporalReference());
				ptolacku.setTemporalReference(tr.getTemporalReference());
				r = getRFromDb(n);
				ptolacku.setRelationshipURI(r);
				iuioR = getIuioForRFromDb(n);
				ptolacku.setRelationshipOntologyIui(iuioR);
				tuple = ptolacku;
				break;
			case "E":
				PtoDETuple ptode = new PtoDETuple();
				ptode.setTupleIui(iuit);
				iuia = getIuiaFromDb(n);
				ptode.setAuthorIui(iuia);
				//no tr, just ta
				ta = getTaFromDb(n);
				ptode.setAuthoringTimeReference(ta.getTemporalReference());
				r = getRFromDb(n);
				ptode.setRelationshipURI(r);
				iuioR = getIuioForRFromDb(n);
				ptode.setRelationshipOntologyIui(iuioR);
				uui = getUuiFromDb(n);
				ptode.setDatatypeUui(uui);
				iuioU = getIuioForUuiFromDb(n);
				ptode.setDatatypeOntologyIui(iuioU);
				prForDE = getPrForDeFromDb(n);
				ptode.setReferent(prForDE);
				byte[] data = getDeDataFromDb(n);
				ptode.setData(data);
				tuple = ptode;
				break;
			case "D":
				MetadataTuple d = new MetadataTuple();
				d.setTupleIui(iuit);
				//iuid
				iuid = getIuidFromDb(n);
				d.setAuthorIui(iuid);
				//td
				td = getTdFromDb(n);
				d.setAuthoringTimestamp(td);
				//about (iuit)
				about = getAboutFromDb(n);
				d.setReferent(about);
				//change reason
				RtsChangeReason cr = getCrFromDb(n);
				d.setChangeReason(cr);
				//change type
				RtsChangeType ct = getCtFromDb(n);
				d.setChangeType(ct);
				//error code
				RtsErrorCode e = getEFromDb(n);
				d.setErrorCode(e);
				//replacement tuple IUI list (if any)
				s = getSFromDb(n);
				if (s!= null) d.setReplacementTupleIuis(s);
				tuple = d;
				break;
			case "C":
				PtoCTuple ptoc = new PtoCTuple();
				ptoc.setTupleIui(iuit);
				iuip = getIuipFromDb(n);
				ptoc.setReferentIui(iuip);
				iuia = getIuiaFromDb(n);
				ptoc.setAuthorIui(iuia);
				co = getCuiFromDb(n);
				ptoc.setConceptCui(co);
				ta = getTaFromDb(n);
				tr = getTrFromDb(n);
				ptoc.setAuthoringTimeReference(ta.getTemporalReference());
				ptoc.setTemporalReference(tr.getTemporalReference());
				iuics = getIuiCsFromDb(n);
				ptoc.setConceptSystemIui(iuics);
				tuple = ptoc;
				break;
			default:
				System.err.println("Unknown tuple type: " + label);
		}
		
		return tuple;
	}

	private Iui getIuipFromDb(Node n) {
		String iuipTxt = (String)n.getRelationships(RtsRelationshipType.iuip).iterator().next().getEndNode().getProperty("iui");
		Iui iuip = Iui.createFromString(iuipTxt);
		return iuip;
	}
	
	private Iui getIuiaFromDb(Node n) {
		String iuipTxt = (String)n.getRelationships(RtsRelationshipType.iuia).iterator().next().getEndNode().getProperty("iui");
		Iui iuip = Iui.createFromString(iuipTxt);
		return iuip;
	}
	
	private Iso8601DateTime getTapFromDb(Node n) {
		String tapTxt = (String)n.getProperty("tap");
		Iso8601DateTimeParser p = new Iso8601DateTimeParser();
		Iso8601DateTime t = null;
		try {
			t = p.parse(tapTxt);
		} catch (Iso8601DateParseException | Iso8601TimeParseException e) {
			e.printStackTrace();
		}
		return t;
	}
	
	private Uui getUuiFromDb(Node n) {
		String uuiTxt = (String)n.getRelationships(RtsRelationshipType.uui).iterator().next().getEndNode().getProperty("uui");
		return Uui.createFromString(uuiTxt);
	}
	
	private Iui getIuioForUuiFromDb(Node n) {
		String iuipTxt = (String)n.getRelationships(RtsRelationshipType.uui).iterator().next().getEndNode(). 
				getRelationships(RtsRelationshipType.iuio).iterator().next().getEndNode().getProperty("iui");
		Iui iuioU = Iui.createFromString(iuipTxt);
		return iuioU;
	}
	
	private TemporalRegion getTaFromDb(Node n) {
		Node ta = n.getRelationships(RtsRelationshipType.ta).iterator().next().getEndNode(); 
		String trefTxt = (String)ta.getProperty("tref");
		boolean isIso = (boolean)ta.getProperty("isIso"); 
		Node nsNode = ta.getRelationships(RtsRelationshipType.iuins).iterator().next().getEndNode();
		String iuinsTxt = (String)nsNode.getProperty("iui");
		Iui iuins = Iui.createFromString(iuinsTxt);
		Node uuiNode = ta.getRelationships(RtsRelationshipType.uui).iterator().next().getEndNode();
		String uuiTxt = (String)uuiNode.getProperty("uui");
		Uui uui = Uui.createFromString(uuiTxt);
		
		TemporalReference tRef = new TemporalReference(trefTxt, isIso);
		TemporalRegion tr = new TemporalRegion(tRef, uui, iuins);
		return tr;
	}

	private TemporalRegion getTrFromDb(Node n) {
		Node ta = n.getRelationships(RtsRelationshipType.tr).iterator().next().getEndNode(); 
		String trefTxt = (String)ta.getProperty("tref");
		boolean isIso = (boolean)ta.getProperty("isIso"); 
		Node nsNode = ta.getRelationships(RtsRelationshipType.iuins).iterator().next().getEndNode();
		String iuinsTxt = (String)nsNode.getProperty("iui");
		Iui iuins = Iui.createFromString(iuinsTxt);
		Node uuiNode = ta.getRelationships(RtsRelationshipType.uui).iterator().next().getEndNode();
		String uuiTxt = (String)uuiNode.getProperty("uui");
		Uui uui = Uui.createFromString(uuiTxt);
		
		TemporalReference tRef = new TemporalReference(trefTxt, isIso);
		TemporalRegion tr = new TemporalRegion(tRef, uui, iuins);
		return tr;		
	}
	
	private URI getRFromDb(Node n) {
		String rTxt = (String)n.getRelationships(RtsRelationshipType.r).iterator().next().getEndNode().getProperty("rui");
		URI r = URI.create(rTxt);
		return r;
	}
	
	private Iui getIuioForRFromDb(Node n) {
		String iuipTxt = (String)n.getRelationships(RtsRelationshipType.r).iterator().next().getEndNode(). 
				getRelationships(RtsRelationshipType.iuio).iterator().next().getEndNode().getProperty("iui");
		Iui iuioR = Iui.createFromString(iuipTxt);
		return iuioR;
	}
	
	private List<ParticularReference> getPFromDb(Node n) {
		Iterable<Relationship> rs = n.getRelationships(RtsRelationshipType.p);
		ArrayList<ParticularReference> pList = new ArrayList<ParticularReference>();
		ParticularReference[] pRefs = new ParticularReference[10];
		int maxOrder = Integer.MIN_VALUE;
		for (Relationship r : rs) {
			Node end = r.getEndNode();
			int rOrder = Integer.parseInt((String)r.getProperty("relation order"));
			if (rOrder > maxOrder) maxOrder = rOrder;
			int index = rOrder - 1;
			if (end.hasProperty("iui")) {
				Iui iui = Iui.createFromString((String)end.getProperty("iui"));
				pRefs[index] = iui;
			} else if (end.hasProperty("tref")) {
				String trefTxt = (String)end.getProperty("tref");
				boolean isIso = (boolean)end.getProperty("isIso");
				TemporalReference tref = new TemporalReference(trefTxt, isIso);
				pRefs[index] = tref;
			} else {
				System.err.println("WTF. Particular reference node should have either iui or tref property!");
			}
		}
		for (int i=0; i<maxOrder; i++)
			pList.add(pRefs[i]);
		
		return pList;
	}
	
	private ParticularReference getPrForDeFromDb(Node n) {
		// We built the iuip relationship as incoming, because the PtoDE tuple says how the 
		//  particular is concretized
		Iterable<Relationship> rs = n.getRelationships(RtsRelationshipType.iuip, Direction.INCOMING);
		int cR = 0;
		Node instNode = null;
		for (Relationship r : rs) {
			cR++;
			instNode = r.getStartNode();  //again, because tuple is EndNode
		}
		if (cR > 1 || cR == 0) {
			System.err.println("PtoDE tuple should have exactly one iuip relationship but instead has " + cR);
		}
		ParticularReference pr = null;
		if (instNode.hasProperty("iui")) {
			Iui iui = Iui.createFromString((String)instNode.getProperty("iui"));
			pr = iui;
		} else if (instNode.hasProperty("tref")) {
			String trefTxt = (String)instNode.getProperty("tref");
			boolean isIso = (boolean)instNode.getProperty("isIso");
			TemporalReference tref = new TemporalReference(trefTxt, isIso);
			pr = tref;
		} else {
			System.err.println("Incompatible node for reference of PtoDE tuple.");
		}
		return pr;
	}
	
	private Cui getCuiFromDb(Node n) {
		String cuiTxt = (String)n.getRelationships(RtsRelationshipType.co).iterator().next().getEndNode().getProperty("cui");
		return new Cui(cuiTxt);
	}
	
	private byte[] getDeDataFromDb(Node n) {
		String dataAsTxt = (String)n.getRelationships(RtsRelationshipType.dr).iterator().next().getEndNode().getProperty("dr");
		return dataAsTxt.getBytes();
	}
	
	private Iui getIuiCsFromDb(Node n) {
		String iuiTxt = (String)n.getRelationships(RtsRelationshipType.cs).iterator().next().getEndNode().getProperty("iui");
		return Iui.createFromString(iuiTxt);
	}
	
	private Iso8601DateTime getTdFromDb(Node n) {
		String tapTxt = (String)n.getProperty("td");
		Iso8601DateTimeParser p = new Iso8601DateTimeParser();
		Iso8601DateTime t = null;
		try {
			t = p.parse(tapTxt);
		} catch (Iso8601DateParseException | Iso8601TimeParseException e) {
			e.printStackTrace();
		}
		return t;
	}
	
	private Iui getIuidFromDb(Node n) {
		String iuipTxt = (String)n.getRelationships(RtsRelationshipType.iuid).iterator().next().getEndNode().getProperty("iui");
		Iui iuip = Iui.createFromString(iuipTxt);
		return iuip;
	}
	
	private Iui getAboutFromDb(Node n) {
		String iuitTxt = (String)n.getRelationships(RtsRelationshipType.about).iterator().next().getEndNode().getProperty("iui");
		Iui iuit = Iui.createFromString(iuitTxt);
		return iuit;
	}
	
	private RtsChangeReason getCrFromDb(Node n) {
		String crTxt = (String)n.getProperty("c");
		RtsChangeReason cr = RtsChangeReason.valueOf(crTxt);
		System.out.println("CHANGE REASON = " + cr);
		return cr;
	}
	
	private RtsChangeType getCtFromDb(Node n) {
		String ctTxt = (String)n.getProperty("ct");
		RtsChangeType ct = RtsChangeType.valueOf(ctTxt);
		System.out.println("CHANGE TYPE = " + ct);
		return ct;
	}
	
	private RtsErrorCode getEFromDb(Node n) {
		String eTxt = (String)n.getProperty("e");
		RtsErrorCode e = RtsErrorCode.valueOf(eTxt);
		System.out.println("ERROR CODE = " + e);
		return e;
	}
	
	private Set<Iui> getSFromDb(Node n) {
		HashSet<Iui> s = null;
		
		Iterable<Relationship> rs = n.getRelationships(RtsRelationshipType.s);
		if (rs.iterator().hasNext()) s = new HashSet<Iui>();
		
		for (Relationship r : rs) {
			String iuiTxt = (String)r.getEndNode().getProperty("iui");
			s.add(Iui.createFromString(iuiTxt));
		}
		
		return s;
	}
	
	@Override
	public Set<RtsTuple> getByReferentIui(Iui iui) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<RtsTuple> getByAuthorIui(Iui iui) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iui getAvailableIui() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<RtsTuple> runQuery(TupleQuery TupleQuery, RtsTupleType TupleType) {
		// TODO Auto-generated method stub
		return null;
	}
	
    public void createDb() {
    	deleteFileOrDirectory( new File(dbPath) );
        // START SNIPPET: startDb
       graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File(dbPath) );
        registerShutdownHook( graphDb );
     // END SNIPPET: startDb
    }
    
    @Override
    public void shutDown() {
    	//graphDb.
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }
    
    private void registerShutdownHook( final GraphDatabaseService graphDb ) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        } );
    }

    private static void deleteFileOrDirectory( File file ) {
        if ( file.exists() ) {
            if ( file.isDirectory() ) {
                for ( File child : file.listFiles() ) {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }
}
