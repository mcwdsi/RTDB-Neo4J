package edu.ufl.ctsi.rts.neo4j;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import neo4jtest.test.App;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.MetadataTemplate;
import edu.uams.dbmi.rts.template.PtoCTemplate;
import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.uams.dbmi.rts.template.PtoLackUTemplate;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.rts.template.TeTemplate;
import edu.uams.dbmi.rts.template.TenTemplate;
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
import edu.ufl.ctsi.rts.persist.neo4j.entity.EntityNodePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.ATemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoCTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoDETemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoLackUTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoPTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.PtoUTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.TeTemplatePersister;
import edu.ufl.ctsi.rts.persist.neo4j.template.TenTemplatePersister;

public class RtsTemplatePersistenceManager {
	
	public GraphDatabaseService graphDb;
	ExecutionEngine ee;
	
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
	

	HashSet<RtsTemplate> templates;
	HashSet<MetadataTemplate> metadata;
	
	HashMap<Iui, Node> iuiNode;
	HashMap<String, Node> uiNode;
	HashMap<String, RtsTemplate> iuiToItsAssignmentTemplate;
	HashSet<String> iuisInPtoPTemplates;
	HashMap<String, String> iuiToNodeLabel;
	
	Iso8601DateTimeFormatter dttmFormatter;
	
	ATemplatePersister atp;
	TeTemplatePersister tep;
	TenTemplatePersister tenp;
	PtoUTemplatePersister pup;
	PtoPTemplatePersister ppp;
	PtoLackUTemplatePersister plup;
	PtoDETemplatePersister pdrp;
	PtoCTemplatePersister pcp;
	
	public RtsTemplatePersistenceManager() {
		templates = new HashSet<RtsTemplate>();
		iuiNode = new HashMap<Iui, Node>();
		uiNode = new HashMap<String, Node>();
		iuiToItsAssignmentTemplate = new HashMap<String, RtsTemplate>();
		iuisInPtoPTemplates = new HashSet<String>();
		iuiToNodeLabel = new HashMap<String, String>();
		dttmFormatter = new Iso8601DateTimeFormatter();
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( App.DB_PATH );
		setupSchema();
		setupExecutionEngine();
		atp = new ATemplatePersister(graphDb, ee);
		tep = new TeTemplatePersister(graphDb, ee);
		tep = new TeTemplatePersister(graphDb, ee);
		tenp = new TenTemplatePersister(graphDb, ee);
		pup = new PtoUTemplatePersister(graphDb, ee);
		ppp = new PtoPTemplatePersister(graphDb, ee);
		plup = new PtoLackUTemplatePersister(graphDb, ee);
		pdrp = new PtoDETemplatePersister(graphDb, ee);
		pcp = new PtoCTemplatePersister(graphDb, ee);
	}
	
	static final String queryInstanceNode = "match (n) where n.iui={value} return n;";
	
	public void addTemplate(RtsTemplate t) {
		if (t instanceof ATemplate || t instanceof TeTemplate) {
			iuiToItsAssignmentTemplate.put(t.getReferentIui().toString(), t);
		} else if ( (t instanceof PtoPTemplate) ) {
			PtoPTemplate ptop = (PtoPTemplate)t;
			Iterable<Iui> p = ptop.getParticulars();
			for (Iui i : p) {
				iuisInPtoPTemplates.add(i.toString());
			}
		}
		templates.add(t);
	} 
	
	public void addTemplates(Collection<RtsTemplate> t) {
		Iterator<RtsTemplate> i = t.iterator();
		while (i.hasNext()) {
			addTemplate(i.next());
		}
	}
	
	/*
	 * 
	 * 
	 * Initially I architected it as requiring all entity nodes (instance, temporal_region,
	 *   concretization, relation, universal, etc.) to exist before creating template nodes.
	 *   It quickly became obvious that this solution is too restrictive, and that just 
	 *   creating/retrieving the necessary entity nodes on the fly is more flexible.  The 
	 *   main thing is that it unnecessarily puts sequential ordering constraints on 
	 *   something we may want to do in other/multiple threads, in parallel, etc.
	 *   
	 * We do, however, want to ensure that every instance node that we create here is 
	 *   associated with an ATemplate and that every temporal region node we create 
	 *   here is associated with a TeTemplate.
	 *   
	 */
	public void commitTemplatesOld() {
		try (Transaction tx = graphDb.beginTx() ) {
			
			for (RtsTemplate t : templates) {
				//if a template with the template IUI does not exist already, commit it.
				if (!isTemplateInDb(t)) {
					commitTemplate(t);
				}
			}
			
			tx.success();
			
			/*
			 * We've sent them all to db, so we can clear.  In the future, we will
			 *  likely want to send them to some cache first.  But this class isn't the 
			 *  cache, it is merely the thing that submits a chunk of related 
			 *  templates as one transaction.
			 */
			templates.clear();
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
			
			for (RtsTemplate t : templates) {
				if (t instanceof ATemplate) {
					atp.persistTemplate(t);
				} else if (t instanceof TeTemplate) {
					tep.persistTemplate(t);
				} else if (t instanceof TenTemplate) {
					tenp.persistTemplate(t);
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
				ResourceIterator<Node> rin = ee.execute(queryInstanceNode, params).columnAs("n");
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

	void commitTemplate(RtsTemplate t) {
		/*
		 * First, create all entities (instances, universals, relations, temporal
		 *   regions, data, etc.) that this template references.
		 *   
		 * We'll be caching these along the way in a HashMap, so we'll check
		 *   locally first, and if we can't find it in memory, then we'll 
		 *   "get or create" using MERGE in the graph database and add to 
		 *   the local, in-memory cache for possible future use.
		 */
		Node templateNode = createTemplateNode(t);
		//connectToReferentNode(templateNode, t);
		RtsNodeLabel referentNodeLabel = (t instanceof TeTemplate) ? 
				RtsNodeLabel.TEMPORAL_REGION : RtsNodeLabel.INSTANCE;
		connectNodeToNode(templateNode, RtsRelationshipType.iuip, 
				referentNodeLabel, t.getReferentIui().toString());
		//connectToAuthorNode(templateNode, t);
		connectNodeToNode(templateNode, RtsRelationshipType.iuia, 
				RtsNodeLabel.INSTANCE, t.getAuthorIui().toString());
		
		if (t instanceof ATemplate) {
			completeATemplate(templateNode, (ATemplate)t);
		} else if (t instanceof TeTemplate) {
			completeTeTemplate(templateNode, (TeTemplate)t);
		} else if (t instanceof TenTemplate) {
			completeTenTemplate(templateNode, (TenTemplate)t);
		} else if (t instanceof MetadataTemplate) {
			/*
			 * This is just a reminder. Metadata templates might not go here,
			 *   mainly because we need to connect them to the templates
			 *   they are about, so we need to finish creating templates in 
			 *   general.  Also, there is the issue of time of insertion 
			 *   of the templates.  Ideally setting that time would be the 
			 *   last thing we did before ending the transaction.
			 */
		} else {
			//else is PtoP, PtoU, PtoCo, PtoLackU, PtoDR
			/*
			 * All of these have ta and tr parameters. All but PtoCo have an 'r' 
			 * 	or 'relation' parameter.  We could hard-code all PtoCo r values
			 *  as 'annototed-by'.  But that relation not in IAO, and at this point, 
			 *  I don't feel like updating the RTDB code, although I certainly think longer
			 *  term, being explicit about what is the relation between the particular
			 *  and the concept code is important.  But that could be done by documentation.
			 *  
			 *  It would only be important to code the annotation relation in PtoCo if we
			 *   want to encode different subrelations of 'annotated-by'.
			 */
			if (t instanceof PtoUTemplate) {
				PtoUTemplate p = (PtoUTemplate)t;
				completePtoUTemplate(templateNode, p);
			} else if (t instanceof PtoPTemplate) {
				PtoPTemplate p = (PtoPTemplate)t;
				completePtoPTemplate(templateNode, p);
			} else if (t instanceof PtoLackUTemplate) {
				PtoLackUTemplate p = (PtoLackUTemplate)t;
				completePtoLackUTemplate(templateNode, p);
			} else if (t instanceof PtoDETemplate) {
				PtoDETemplate p = (PtoDETemplate)t;
				completePtoDETemplate(templateNode, p);
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

	private void completeTeTemplate(Node n, TeTemplate t) {
		// TODO Auto-generated method stub
		n.setProperty("type", "TE");
		n.setProperty("tap", dttmFormatter.format(t.getAuthoringTimestamp()));
		
		Node typeNode = getOrCreateNode(RtsNodeLabel.TYPE, t.getUniversalUui().toString());
		n.createRelationshipTo(typeNode, RtsRelationshipType.uui);
	}
	
	static String TYPE_QUERY = "MERGE (n:universal {ui : {value}})"
			//+ "ON CREATE "
			+ "RETURN n";

	private void completeATemplate(Node n, ATemplate t) {
		n.setProperty("type", "A");
		n.setProperty("tap", dttmFormatter.format(t.getAuthoringTimestamp()));
	}
	
	private void completeTenTemplate(Node n, TenTemplate t) {
		// TODO Auto-generated method stub
		n.setProperty("type", "TEN");
		
		//Connect it to the temporal region that it names
		//connectToTemporalEntityNode(n, t.getTemporalEntityIui());
		connectNodeToNode(n, RtsRelationshipType.iuite, RtsNodeLabel.TEMPORAL_REGION,
				t.getTemporalEntityIui().toString());
		
		//Add the name itself 
		n.setProperty("name", t.getName());
		
		//Connect it to the naming system
		//connectToNamingSystemNode(n, t.getNamingSystemIui());
		connectNodeToNode(n, RtsRelationshipType.iuins, RtsNodeLabel.INSTANCE,
				t.getNamingSystemIui().toString());
		
		//Connect it to the temporal region at which the name was asserted to
		// designate the temporal entity
		t.getAuthoringTimeIui();
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
	    ResourceIterator<Node> resultIterator = ee.execute( query, parameters ).columnAs( "n" );
	    Node n = resultIterator.next();
	    
	    //add node to cache
	    uiNode.put(targetNodeUi, n);
	    
	    //TODO change this to also throw a new type of exception, and transaction 
	    //	should roll back
	    if ( targetNodeLabel.equals(RtsNodeLabel.INSTANCE) ||
	    		targetNodeLabel.equals(RtsNodeLabel.TEMPORAL_REGION) ) {
	    	if ( !iuiToItsAssignmentTemplate.containsKey(targetNodeUi) ) {
		    	System.err.println("ERROR: creating new entity with IUI " + targetNodeUi +
		    			" but this IUI has no corresponding assignment template!");
	    	}
	    }
		
		return n;
	}

	private void completePtoUTemplate(Node n, PtoUTemplate p) {
		// TODO Auto-generated method stub
		n.setProperty("type", "PtoU");
	}

	private void completePtoPTemplate(Node n, PtoPTemplate p) {
		// TODO Auto-generated method stub
		n.setProperty("type", "PtoP");
	}

	private void completePtoLackUTemplate(Node n, PtoLackUTemplate p) {
		// TODO Auto-generated method stub
		n.setProperty("type", "PtoLackU");
	}

	private void completePtoDETemplate(Node n, PtoDETemplate p) {
		// TODO Auto-generated method stub
		n.setProperty("type", "PtoDR");
	}

	//static String templateByIuiQuery = "START n=node:nodes(iui = {value}) RETURN n";
	static String templateByIuiQuery = "MATCH (n:template { ui : {value} }) return n";
	
	boolean isTemplateInDb(RtsTemplate t) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("value", t.getTemplateIui().toString());
		return ee.execute(templateByIuiQuery, parameters).iterator().hasNext();
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
                      	
            tx2.success();
        }
    }
    
    void setupExecutionEngine() {
    	ee = new ExecutionEngine(graphDb);
    }
}
