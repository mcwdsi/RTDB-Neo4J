package neo4jtest.test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import edu.uams.dbmi.rts.cui.Cui;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.rts.template.PtoCTemplate;
import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.uams.dbmi.rts.template.TeTemplate;
import edu.uams.dbmi.rts.template.TenTemplate;
import edu.uams.dbmi.rts.uui.Uui;
import edu.uams.dbmi.util.iso8601.Iso8601DateTime;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplatePersistenceManager;

/**
 * Hello world!
 *
 */
public class App 
{
	/**
	 * Licensed to Neo Technology under one or more contributor
	 * license agreements. See the NOTICE file distributed with
	 * this work for additional information regarding copyright
	 * ownership. Neo Technology licenses this file to you under
	 * the Apache License, Version 2.0 (the "License"); you may
	 * not use this file except in compliance with the License.
	 * You may obtain a copy of the License at
	 *
	 * http://www.apache.org/licenses/LICENSE-2.0
	 *
	 * Unless required by applicable law or agreed to in writing,
	 * software distributed under the License is distributed on an
	 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	 * KIND, either express or implied. See the License for the
	 * specific language governing permissions and limitations
	 * under the License.
	 */


	    public static final String DB_PATH = "target/neo4j-hello-db";

	    public String greeting;

	    // START SNIPPET: vars
	    GraphDatabaseService graphDb;
	    Node firstNode;
	    Node secondNode;
	    Relationship relationship;
	    // END SNIPPET: vars

	    // START SNIPPET: createReltype
	    private static enum RelTypes implements RelationshipType
	    {
	        KNOWS
	    }
	    // END SNIPPET: createReltype

	    public static void main( final String[] args )
	    {
	        App hello = new App();
	        hello.createDb();
	        hello.addData();
	        hello.removeData();
	        hello.shutDown();
	        
            
            RtsTemplatePersistenceManager rpm = new RtsTemplatePersistenceManager();
            
            Iui wh = Iui.createRandomIui();
            Iui wh_chair = Iui.createRandomIui();
            Iui wh_name = Iui.createRandomIui();
            
            /*
             * W. Hogan
             */
            ATemplate a1 = new ATemplate();
            a1.setAuthorIui(wh);
            a1.setReferentIui(wh);
            a1.setAuthoringTimestamp(new Iso8601DateTime());
            a1.setTemplateIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's chair
             */
            ATemplate a2 = new ATemplate();
            a2.setAuthorIui(wh);
            a2.setReferentIui(wh_chair);
            a2.setAuthoringTimestamp(new Iso8601DateTime());
            a2.setTemplateIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's full name
             */
            ATemplate a3 = new ATemplate();
            a3.setAuthorIui(wh);
            a3.setReferentIui(wh_name);
            a3.setAuthoringTimestamp(new Iso8601DateTime());
            a3.setTemplateIui(Iui.createRandomIui());
            
            /*
             * Time of assertion of this set of templates
             */
            TeTemplate t = new TeTemplate();
            t.setAuthoringTimestamp(new Iso8601DateTime());
            t.setReferentIui(Iui.createRandomIui());
            t.setAuthorIui(wh);
            t.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInstant"));
            t.setTemplateIui(Iui.createRandomIui());
            
            
            Iui gregorianIui = Iui.createFromString("D4AF5C9A-47BA-4BF4-9BAE-F13A8ED6455E");
            Iui maxTimeIntervalIui = Iui.createFromString("26F1052B-311D-43B1-9ABC-B4E2EDD1B283");
            
            /*
             * Name of time of assertion of this set of templates
             */
            TenTemplate ten = new TenTemplate();
            ten.setTemplateIui(Iui.createRandomIui());
            ten.setAuthorIui(wh);
            ten.setName("2014-07-03T15:49:37.543");
            ten.setNamingSystemIui(gregorianIui);
            ten.setReferentIui(Iui.createRandomIui());
            ten.setTemporalEntityIui(t.getReferentIui());
            ten.setAuthoringTimeIui(t.getReferentIui());   
            
            /*
             * Time during which W. Hogan has been owner of his char
             */
            TeTemplate t2 = new TeTemplate();
            t2.setAuthoringTimestamp(new Iso8601DateTime());
            t2.setReferentIui(Iui.createRandomIui());
            t2.setAuthorIui(wh);
            t2.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInterval"));
            t2.setTemplateIui(Iui.createRandomIui());
            
            /*
             * Time during which W. Hogan has been instance of human being
             */
            TeTemplate t3 = new TeTemplate();
            t3.setAuthoringTimestamp(new Iso8601DateTime());
            t3.setReferentIui(Iui.createRandomIui());
            t3.setAuthorIui(wh);
            t3.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInterval"));
            t3.setTemplateIui(Iui.createRandomIui());
            
            /*
             * Day of W. Hogan's birth
             */
            TeTemplate t4 = new TeTemplate();
            t4.setAuthoringTimestamp(new Iso8601DateTime());
            t4.setReferentIui(Iui.createRandomIui());
            t4.setTemplateIui(Iui.createRandomIui());
            t4.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInterval"));
            t4.setAuthorIui(wh);
            
            /*
             * Name of day of W. Hogan's birth
             */
            TenTemplate ten2 = new TenTemplate();
            ten2.setTemplateIui(Iui.createRandomIui());
            ten2.setTemporalEntityIui(t4.getReferentIui());
            ten2.setAuthoringTimeIui(t.getReferentIui());
            ten2.setAuthorIui(wh);
            ten2.setName("1969-04-20");
            ten2.setNamingSystemIui(gregorianIui);
            ten2.setReferentIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's chair owned by W. Hogan
             */
            PtoPTemplate ptop = new PtoPTemplate();
            ptop.setReferentIui(wh_chair);
            ptop.setAuthorIui(wh);
            try {
				ptop.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/OMIABIS_0000048"));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ptop.setAuthoringTimeIui(t.getReferentIui());
            ptop.addParticular(wh);
            ptop.setTemplateIui(Iui.createRandomIui());
            ptop.setTemporalEntityIui(t2.getReferentIui());
            
            URI instance_of=null;
			try {
				instance_of = new URI("http://purl.obolibrary.org/obo/ro.owl#instance_of");
			} catch (URISyntaxException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            
            /*
             * W. Hogan instance of human being
             */
            PtoUTemplate ptou = new PtoUTemplate();
            ptou.setTemplateIui(Iui.createRandomIui());
            ptou.setReferentIui(wh);
            ptou.setRelationshipURI(instance_of);
            ptou.setAuthoringTimeIui(t.getReferentIui());
            ptou.setAuthorIui(wh);
            ptou.setTemporalEntityIui(t3.getReferentIui());
            ptou.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/NCBITaxon_9606"));
            
            /*
             * W. Hogan's name instance of personal name
             */
            PtoUTemplate ptou3 = new PtoUTemplate();
            ptou3.setTemplateIui(Iui.createRandomIui());
            ptou3.setReferentIui(wh_name);
            ptou3.setRelationshipURI(instance_of);
            ptou3.setAuthoringTimeIui(t.getReferentIui());
            ptou3.setAuthorIui(wh);
            ptou3.setTemporalEntityIui(t3.getReferentIui());
            ptou3.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/IAO_0020015"));
            
            /*
             * W. Hogan's name designates W. Hogan
             */
            PtoPTemplate ptop2 = new PtoPTemplate();
            ptop2.setReferentIui(wh_name);
            ptop2.setAuthorIui(wh);
            try {
				ptop2.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/IAO_0000219"));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ptop2.setAuthoringTimeIui(t.getReferentIui());
            ptop2.addParticular(wh);
            ptop2.setTemplateIui(Iui.createRandomIui());
            ptop2.setTemporalEntityIui(t3.getReferentIui());    
            
            /*
             * W. Hogan's name's digital representation
             */
            PtoDETemplate ptodr = new PtoDETemplate();
            ptodr.setTemplateIui(Iui.createRandomIui());
            ptodr.setAuthorIui(wh);
            ptodr.setAuthoringTimeIui(t.getReferentIui());
            ptodr.setReferentIui(wh_name);
            ptodr.setData("William Hogan".getBytes());
            try {
				ptodr.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/BFO_0000058"));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ptodr.setDatatypeUui(new Uui("http://ctsi.ufl.edu/rts/UTF-16"));
            
            /*
             * PtoP for day of W. Hogan's birth to time during which W. Hogan has been a human being
             */
            PtoPTemplate ptop3 = new PtoPTemplate();
            ptop3.setTemplateIui(Iui.createRandomIui());
            ptop3.setAuthorIui(wh);
            ptop3.setAuthoringTimeIui(t.getReferentIui());
            ptop3.setReferentIui(t4.getReferentIui());
            ptop3.addParticular(t3.getReferentIui());
            try {
				ptop3.setRelationshipURI(new URI("http://ctsi.ufl.edu/rts/overlaps"));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ptop3.setTemporalEntityIui(maxTimeIntervalIui);
            
            Iui snctCsIui = Iui.createFromString("59487A5D-C808-48A4-9DAE-894DCE866A96");        
            /*
             * PtoC that annotates W. Hogan with SNOMED-CT's "Father (person)" concept
             */
            PtoCTemplate ptoc = new PtoCTemplate();
            ptoc.setTemplateIui(Iui.createRandomIui());
            ptoc.setReferentIui(wh);
            ptoc.setAuthoringTimeIui(t.getReferentIui());
            ptoc.setAuthorIui(wh);
            ptoc.setConceptCui(new Cui("66839005"));
            ptoc.setTemporalEntityIui(t.getReferentIui());
            ptoc.setConceptSystemIui(snctCsIui);            
            
            rpm.addTemplate(a1);
            rpm.addTemplate(a2);
            rpm.addTemplate(a3);
            rpm.addTemplate(t);
            rpm.addTemplate(t2);
            rpm.addTemplate(t3);
            rpm.addTemplate(t4);
            rpm.addTemplate(ptop);
            rpm.addTemplate(ptop2);
            rpm.addTemplate(ptop3);
            rpm.addTemplate(ptou);
            rpm.addTemplate(ptou3);
            rpm.addTemplate(ten);
            rpm.addTemplate(ten2);
            rpm.addTemplate(ptodr);
            rpm.addTemplate(ptoc);
            rpm.commitTemplates();
            
            hello.graphDb = rpm.graphDb;
            
            //hello.shutDown();
            
            //hello.createDb();
            hello.queryRts();
            
            Charset c = Charset.forName("UTF-16");
            System.out.println(c + "\t" + c.displayName() + "\t" + c.name() + "\tis registered: " + c.isRegistered() + "\tcan encode: " + c.canEncode());

	    }

	    void createDb()
	    {
	        deleteFileOrDirectory( new File( DB_PATH ) );
	        // START SNIPPET: startDb
	        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
	        registerShutdownHook( graphDb );
	     // END SNIPPET: startDb
	    }
	    
	    void addData() {
	        
	        // START SNIPPET: transaction
	        try ( Transaction tx = graphDb.beginTx() )
	        {
	            // Database operations go here
	            // END SNIPPET: transaction
	            // START SNIPPET: addData
	            firstNode = graphDb.createNode();
	            firstNode.setProperty( "message", "Hello, " );
	            secondNode = graphDb.createNode();
	            secondNode.setProperty( "message", "World!" );

	            relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
	            relationship.setProperty( "message", "brave Neo4j " );
	            // END SNIPPET: addData

	            // START SNIPPET: readData
	            System.out.print( firstNode.getProperty( "message" ) );
	            System.out.print( relationship.getProperty( "message" ) );
	            System.out.print( secondNode.getProperty( "message" ) );
	            // END SNIPPET: readData

	            greeting = ( (String) firstNode.getProperty( "message" ) )
	                       + ( (String) relationship.getProperty( "message" ) )
	                       + ( (String) secondNode.getProperty( "message" ) );
	            
	            System.out.println(greeting);

	            // START SNIPPET: transaction
	            tx.success();

	        }
	        // END SNIPPET: transaction

	    }

	    void removeData()
	    {
	        try ( Transaction tx = graphDb.beginTx() )
	        {
	            // START SNIPPET: removingData
	            // let's remove the data
	            firstNode.getSingleRelationship( RelTypes.KNOWS, Direction.OUTGOING ).delete();
	            firstNode.delete();
	            secondNode.delete();
	            // END SNIPPET: removingData

	            tx.success();
	        }
	    }

	    void shutDown()
	    {
	        System.out.println();
	        System.out.println( "Shutting down database ..." );
	        // START SNIPPET: shutdownServer
	        graphDb.shutdown();
	        // END SNIPPET: shutdownServer
	    }

	    // START SNIPPET: shutdownHook
	    private static void registerShutdownHook( final GraphDatabaseService graphDb )
	    {
	        // Registers a shutdown hook for the Neo4j instance so that it
	        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	        // running application).
	        Runtime.getRuntime().addShutdownHook( new Thread()
	        {
	            @Override
	            public void run()
	            {
	                graphDb.shutdown();
	            }
	        } );
	    }
	    // END SNIPPET: shutdownHook

	    private static void deleteFileOrDirectory( File file )
	    {
	        if ( file.exists() )
	        {
	            if ( file.isDirectory() )
	            {
	                for ( File child : file.listFiles() )
	                {
	                    deleteFileOrDirectory( child );
	                }
	            }
	            file.delete();
	        }
	    }
	    
	    void queryRts() {
            ExecutionEngine ee = new ExecutionEngine(graphDb);
            String query = "START n=node(*) RETURN n";
            
            try ( Transaction tx = graphDb.beginTx() ) {
            	
            	ExecutionResult result = ee.execute(query);
            	ResourceIterator<Map<String, Object>> i = result.iterator();
            	while (i.hasNext()) {
            		Map<String, Object> entry = i.next();
            		System.out.println(entry.size());
            		Set<String> keys = entry.keySet();
            		for (String key : keys) {
            			System.out.print("\t" + key);
            			Object o = entry.get(key);
            			Node n = (Node)o;
            			
            			System.out.print("\t" + n.getId());
            			
            			String label = n.getLabels().iterator().next().toString();
            			System.out.print("\t" + label);
            			if (label.equals("instance") || label.equals("temporal_region") ) {
            				System.out.print("\tiui = "+ n.getProperty("iui"));
            			} else if (label.equals("universal")) {
            				n.addLabel(DynamicLabel.label("universal"));  //what happens if we try to add a label that is already there?
            				System.out.print("\tuui = " + n.getProperty("uui"));
            			} else if (label.equals("relation")) {
            				System.out.print("\trui = " + n.getProperty("rui"));
            			} else if (label.equals("template")) {
            				System.out.print("\tiui = "+ n.getProperty("iui"));
            				String type = (String) n.getProperty("type");
            				System.out.print("\t" + type);
            				
            				if (type.equals("a") || type.equals("te")) {
            					String tap = (String) n.getProperty("tap");
            					System.out.print("\ttap =" + tap);
            				} else if (type.equals("ten")) {
            					String name = (String)n.getProperty("name");
            					System.out.print("\tname = " + name);
            				} else if (type.equals("ptodr")) {
            				
            				}
            			} else if (label.equals("data")) {
        					String data = (String)n.getProperty("dr");
        					System.out.print("\tdr = " + data);
        				} else if (label.equals("concept")) {
        					String code = (String)n.getProperty("cui");
        					System.out.print("\tcui = " + code);
        				}
            			
            			System.out.println();
            			
            			
            			Iterator<Relationship> iRel = n.getRelationships().iterator();
            			while (iRel.hasNext()) {
            				Relationship r = iRel.next();
            				Node en = r.getEndNode();
            				Node sn = r.getStartNode();
            				RelationshipType rt = r.getType();
            				System.out.println("\t\t" + r.getId() + "\t" + sn.getId() + "\t" + rt.name() + "\t" + en.getId());// + "\t" + en.getProperty("type"));
            			}//*/
            			
            		}
            	}
            	
            	tx.success();
            
            }
	    }
	
}
