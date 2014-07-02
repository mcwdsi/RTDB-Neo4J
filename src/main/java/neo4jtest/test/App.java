package neo4jtest.test;

import java.io.File;
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

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.rts.template.TeTemplate;
import edu.uams.dbmi.rts.uui.Uui;
import edu.uams.dbmi.util.iso8601.Iso8601DateTime;
import edu.ufl.ctsi.neo4j.RtsTemplatePersistenceManager;

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
            
            ATemplate a1 = new ATemplate();
            a1.setAuthorIui(wh);
            a1.setReferentIui(wh);
            a1.setAuthoringTimestamp(new Iso8601DateTime());
            a1.setTemplateIui(Iui.createRandomIui());
            
            ATemplate a2 = new ATemplate();
            a2.setAuthorIui(wh);
            a2.setReferentIui(wh_chair);
            a2.setAuthoringTimestamp(new Iso8601DateTime());
            a2.setTemplateIui(Iui.createRandomIui());
            
            TeTemplate t = new TeTemplate();
            t.setAuthoringTimestamp(new Iso8601DateTime());
            t.setReferentIui(Iui.createRandomIui());
            t.setAuthorIui(wh);
            t.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInterval"));
            t.setTemplateIui(Iui.createRandomIui());
            
            rpm.addTemplate(a1);
            rpm.addTemplate(a2);
            rpm.addTemplate(t);
            rpm.commitTemplates();
            
            hello.graphDb = rpm.graphDb;
            
            //hello.shutDown();
            
            //hello.createDb();
            hello.queryRts();
            
            Charset c = Charset.forName("UTF-16");
            System.out.println(c + "\t" + c.displayName() + "\t" + c.name() + "\tis registered: " + c.isRegistered() + "\t:can encode: " + c.canEncode());

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
            			if (label.equals("instance") || label.equals("temporal_region") || label.equals("template")) {
            				System.out.print("\t"+ n.getProperty("iui"));
            			} else if (label.equals("universal")) {
            				System.out.print("\t" + n.getProperty("uui"));
            			}
            			
            			if (n.hasLabel(DynamicLabel.label("template"))) {
            				String type = (String) n.getProperty("type");
            				System.out.print("\t" + type);
            				
            				if (type.equals("a") || type.equals("te")) {
            					String tap = (String) n.getProperty("tap");
            					System.out.print("\t" + tap);
            				}
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
