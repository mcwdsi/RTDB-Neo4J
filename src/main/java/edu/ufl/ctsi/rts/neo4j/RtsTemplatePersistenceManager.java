package edu.ufl.ctsi.rts.neo4j;


import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import neo4jtest.test.App;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.MetadataTemplate;
import edu.uams.dbmi.rts.template.PtoCTemplate;
import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.uams.dbmi.rts.template.PtoLackUTemplate;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.util.iso8601.Iso8601DateTime;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.ATemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.MetadataTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoCTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoDETemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoLackUTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoPTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoUTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.TemporalReferencePersister;

public class RtsTemplatePersistenceManager {

	static String CNODE_QUERY = "MERGE (n:change_reason { c: {value} }) return n";
	static String CTNODE_QUERY = "MERGE (n:change_type { ct: {value} }) return n";
	
	public GraphDatabaseService graphDb;
	
	Label templateLabel;
	Label aTemplateLabel;
	Label ptouTemplateLabel;
	Label ptopTemplateLabel;
	Label ptolackuTemplateLabel;
	
	Label instanceLabel;
	Label temporalRegionLabel;
	Label typeLabel;
	Label relationLabel;
	Label dataLabel;
	
	Label metadataLabel;

	HashSet<RtsTemplate> templates;
	HashSet<MetadataTemplate> metadata;
	HashSet<TemporalReference> tempReferences;
	
	HashMap<Iui, Node> iuiNode;
	HashMap<String, Node> uiNode;
	HashMap<String, RtsTemplate> iuiToItsAssignmentTemplate;
	HashSet<String> iuisInPtoPTemplates;
	HashMap<String, String> iuiToNodeLabel;
	
	Iso8601DateTimeFormatter dttmFormatter;
	
	ATemplatePersister atp;
	//TenTemplatePersister tenp;
	PtoUTemplatePersister pup;
	PtoPTemplatePersister ppp;
	PtoLackUTemplatePersister plup;
	PtoDETemplatePersister pdrp;
	PtoCTemplatePersister pcp;
	MetadataTemplatePersister mp;
	
	TemporalReferencePersister trp;
	
	public RtsTemplatePersistenceManager() {
		templates = new HashSet<RtsTemplate>();
		metadata = new HashSet<MetadataTemplate>();
		tempReferences = new HashSet<TemporalReference>();
		iuiNode = new HashMap<Iui, Node>();
		uiNode = new HashMap<String, Node>();
		iuiToItsAssignmentTemplate = new HashMap<String, RtsTemplate>();
		iuisInPtoPTemplates = new HashSet<String>();
		iuiToNodeLabel = new HashMap<String, String>();
		dttmFormatter = new Iso8601DateTimeFormatter();
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File(App.DB_PATH) );
		setupSchema();
		setupMetadata();

		atp = new ATemplatePersister(graphDb);
		//tenp = new TenTemplatePersister(graphDb, ee);
		pup = new PtoUTemplatePersister(graphDb);
		ppp = new PtoPTemplatePersister(graphDb);
		plup = new PtoLackUTemplatePersister(graphDb);
		pdrp = new PtoDETemplatePersister(graphDb);
		pcp = new PtoCTemplatePersister(graphDb);
		mp = new MetadataTemplatePersister(graphDb);
		trp = new TemporalReferencePersister(graphDb);
	}
	
	static final String queryInstanceNode = "match (n) where n.iui={value} return n;";
	
	public void addTemplate(RtsTemplate t) {
		if (t instanceof ATemplate) {
			ATemplate at = (ATemplate)t;
			iuiToItsAssignmentTemplate.put(at.getReferent().toString(), t);
		} else if ( (t instanceof PtoPTemplate) ) {
			PtoPTemplate ptop = (PtoPTemplate)t;
			Iterable<ParticularReference> p = ptop.getAllParticulars();
			for (ParticularReference i : p) {
				if (i instanceof Iui) iuisInPtoPTemplates.add(i.toString());
			}
		}
		if (t instanceof MetadataTemplate) {
			metadata.add((MetadataTemplate)t);
		} else {
			templates.add(t);
		}
	} 
	
	public void addTemplates(Collection<RtsTemplate> t) {
		Iterator<RtsTemplate> i = t.iterator();
		while (i.hasNext()) {
			addTemplate(i.next());
		}
	}
	
	
	public void commitTemplates() {
		try (Transaction tx = graphDb.beginTx() ) {
			
			/*
			 * Before we begin, let's be sure that we either have assignment templates
			 *  for each IUI that a PtoP template references or that the IUI node 
			 *  exists in the database already.  
			 */
			checkIuisInPtoP();
			
			Iso8601DateTime dt = new Iso8601DateTime();
			//Iso8601DateTimeFormatter dtf = new Iso8601DateTimeFormatter();
			//String iuid = dtf.format(dt);
			
			for (RtsTemplate t : templates) {
				if (t instanceof ATemplate) {
					atp.persistTemplate(t);
				//} else if (t instanceof TeTemplate) {
				//	tep.persistTemplate(t);
				//} else if (t instanceof TenTemplate) {
				//	tenp.persistTemplate(t);
				} else if (t instanceof PtoUTemplate) {
					pup.persistTemplate(t);
				} else if (t instanceof PtoLackUTemplate) {
					plup.persistTemplate(t);
				} else if (t instanceof PtoDETemplate) {
					pdrp.persistTemplate(t);
				} else if (t instanceof PtoPTemplate) {
					ppp.persistTemplate(t);
				} else if (t instanceof PtoCTemplate) {
					pcp.persistTemplate(t);
				} 
			}
			
			for (MetadataTemplate d : metadata) {
				d.setAuthoringTimestamp(dt);
				mp.persistTemplate(d);
			}
			
			tx.success();
			
			/*
			 * We've sent them all to db, so we can clear.  In the future, we will
			 *  likely want to send them to some cache first.  But this class isn't the 
			 *  cache, it is merely the thing that submits a chunk of related 
			 *  templates as one transaction.
			 */
			templates.clear();
			EntityNodePersister.clearCache();
		}
	}
	
	private void checkIuisInPtoP() {
		for (String iui : iuisInPtoPTemplates) {
			if (!iuiToItsAssignmentTemplate.containsKey(iui)) {
				HashMap<String, Object> params = new HashMap<String, Object>();
				params.put("value", iui);
				ResourceIterator<Node> rin = graphDb.execute(queryInstanceNode, params).columnAs("n");
				if (!rin.hasNext()) {
					System.err.println("Iui " + iui + " is referenced in a PtoP template but has " +
							"no assignment template in the cache and there is no node for it already "
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

	

	static String createTemplateQuery = "CREATE (n:template { iui : {value}})";
	
	/*
	 * Above, we made sure that a template with this template IUI didn't exist already.
	 *  So we're clear to add it de novo without worrying about violating a unique
	 *  constraint on template IUIs.
	 */
	private Node createTemplateNode(RtsTemplate t) {
		Node n = graphDb.createNode(templateLabel);
		n.setProperty("ui", t.getTemplateIui().toString());
		return n;
	}

	/*
	private void connectToReferentNode(Node templateNode, RtsTemplate t) {
		Node referentNode;
		Iui referentIui = t.getReferentIui();
		if (iuiNode.containsKey(referentIui)) {
			referentNode = iuiNode.get(referentIui);
		} else {
			referentNode = getOrCreateEntityNode(referentIui, RtsNodeLabel.INSTANCE);
		}
		
		templateNode.createRelationshipTo(referentNode, RtsRelationshipType.iuip);
	}

	private void connectToAuthorNode(Node templateNode, RtsTemplate t) {
		Node authorNode;
		Iui authorIui = t.getAuthorIui();
		if (iuiNode.containsKey(authorIui)) {
			authorNode = iuiNode.get(authorIui);
		} else {
			authorNode = getOrCreateEntityNode(authorIui, RtsNodeLabel.INSTANCE);
		}
		
		templateNode.createRelationshipTo(authorNode, RtsRelationshipType.iuia);
	}//*/

	/*
	private void completeTeTemplate(Node n, TeTemplate t) {
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

	private void completeATemplate(Node n, ATemplate t) {
		n.setProperty("type", "A");
		n.setProperty("tap", dttmFormatter.format(t.getAuthoringTimestamp()));
	}
	

	
	/*
	private void connectToTemporalEntityNode(Node templateNode, Iui teIui) {
		Node teNode;
		if (iuiNode.containsKey(teIui)) {
			teNode = iuiNode.get(teIui);
		} else {
			teNode = getOrCreateNode(RtsNodeLabel.TEMPORAL_REGION, teIui.toString()); 
					//getOrCreateEntityNode(teIui, RtsNodeLabel.TEMPORAL_REGION);
		}
		
		templateNode.createRelationshipTo(teNode, RtsRelationshipType.iuite);
	}

	private void connectToNamingSystemNode(Node templateNode, Iui nsIui) {
		Node teNode;
		if (iuiNode.containsKey(nsIui)) {
			teNode = iuiNode.get(nsIui);
		} else {
			teNode = getOrCreateEntityNode(nsIui, RtsNodeLabel.INSTANCE);
		}
		
		templateNode.createRelationshipTo(teNode, RtsRelationshipType.ns);
	}//*/
	
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
	    	if ( !iuiToItsAssignmentTemplate.containsKey(targetNodeUi) ) {
		    	System.err.println("ERROR: creating new entity with IUI " + targetNodeUi +
		    			" but this IUI has no corresponding assignment template!");
	    	}
	    }
		
		return n;
	}

	//static String templateByIuiQuery = "START n=node:nodes(iui = {value}) RETURN n";
	static String templateByIuiQuery = "MATCH (n:template { ui : {value} }) return n";
	
	boolean isTemplateInDb(RtsTemplate t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTemplateIui().toString());
		return graphDb.execute(templateByIuiQuery, parameters).hasNext();
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
	    

	    if (!iuiToAssignmentTemplate.containsKey(iui)) {

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
    	
    	templateLabel = DynamicLabel.label("template");
    	instanceLabel = DynamicLabel.label("instance");
    	typeLabel = DynamicLabel.label("universal");
    	relationLabel = DynamicLabel.label("relation");
    	temporalRegionLabel = DynamicLabel.label("temporal_region");
    	dataLabel = DynamicLabel.label("data");
    	metadataLabel = DynamicLabel.label("metadata");
    	
  	
        try ( Transaction tx2 = graphDb.beginTx() )
        {
            graphDb.schema()
                    .constraintFor( templateLabel )
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
    
    void setupMetadata() {
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
    }
    
	public void addTemporalReference(TemporalReference t) {
		tempReferences.add(t);
	}
}
