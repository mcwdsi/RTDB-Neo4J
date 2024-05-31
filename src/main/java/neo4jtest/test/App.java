package neo4jtest.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.cui.Cui;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.metadata.RtsChangeReason;
import edu.uams.dbmi.rts.metadata.RtsChangeType;
import edu.uams.dbmi.rts.metadata.RtsErrorCode;
import edu.uams.dbmi.rts.query.TupleQuery;
import edu.uams.dbmi.rts.time.TemporalRegion;
import edu.uams.dbmi.rts.tuple.ATuple;
import edu.uams.dbmi.rts.tuple.MetadataTuple;
import edu.uams.dbmi.rts.tuple.PtoCTuple;
import edu.uams.dbmi.rts.tuple.PtoDETuple;
import edu.uams.dbmi.rts.tuple.PtoPTuple;
import edu.uams.dbmi.rts.tuple.PtoUTuple;
import edu.uams.dbmi.rts.tuple.RtsTuple;
import edu.uams.dbmi.rts.tuple.RtsTupleType;
import edu.uams.dbmi.rts.uui.Uui;
import edu.uams.dbmi.util.iso8601.Iso8601Date;
import edu.uams.dbmi.util.iso8601.Iso8601Date.DateConfiguration;
import edu.uams.dbmi.util.iso8601.Iso8601DateParseException;
import edu.uams.dbmi.util.iso8601.Iso8601DateParser;
import edu.uams.dbmi.util.iso8601.Iso8601DateTime;
import edu.uams.dbmi.util.iso8601.Iso8601UnitTime;
import edu.uams.dbmi.util.iso8601.TimeUnit;
import edu.ufl.ctsi.rts.neo4j.RtsTuplePersistenceManager;
import edu.ufl.ctsi.rts.text.RtsTupleTextParser;
import edu.ufl.ctsi.rts.text.RtsTupleTextWriter;

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
    private static final Path dbDirectory = Path.of( "target/neo4j-test-db" );

	    //public static final String DB_PATH = "target/neo4j-hello-db";

	    public String greeting;

	    // START SNIPPET: vars
	    GraphDatabaseService graphDb;
        DatabaseManagementService mgmtSvc;
        DatabaseManagementServiceBuilder dbMgmtSvcBuilder;
	    Node firstNode;
	    Node secondNode;
	    Relationship relationship;
        Node n1;
        Node n2;
        Relationship rel;
        
	    // END SNIPPET: vars

	    // START SNIPPET: createReltype
	    private static enum RelTypes implements RelationshipType
	    {
	        KNOWS,
            ISA
	    }
	    // END SNIPPET: createReltype
	    
	    static URI instance_of;
	    static Iui gregorianIui = Iui.createFromString("D4AF5C9A-47BA-4BF4-9BAE-F13A8ED6455E");
	    static Iui bfoIui = Iui.createFromString("E7006DDD-3075-46DB-BEC2-FAD5A5F0B513");
	    static Iui roIui = Iui.createFromString("C8BFD0E2-9CCE-4961-80F4-1290A7767B7C");
	    static Iui ncbiTaxonIui = Iui.createFromString("D7A93FCB-72CA-4D89-A1AE-328F33138FBF");
	    static Iui pnoIui = Iui.createFromString("6C151D9A-6694-4EFD-840F-BC7CBE90DB5E");
	    static Iui uberonIui = Iui.createFromString("F312650F-F05E-4EA7-B04C-AF050409A232");
	    
	    //This one should refer to https://www.iana.org/assignments/character-sets/character-sets.xhtml
	    static Iui characterEncodingsIui = Iui.createFromString("85F850AD-C348-4256-81F0-24DC45B63079");
	    
	    static PtoPTuple designatesPtoP;
	    static PtoPTuple ownsPtoP;
	    static PtoPTuple trefPtop;
	    static PtoDETuple namePtoDE;
	    

	    public static void main( final String[] args )
	    {
	        App hello = new App();
	       //hello.deleteFileOrDirectory( new File(DB_PATH) );
	       //System.exit(1);
	        hello.createDb();
	        hello.addData();
	        hello.removeData();
	        hello.shutDown();

            instance_of = null;
			try {
				instance_of = new URI("http://purl.obolibrary.org/obo/ro.owl#instance_of");
			} catch (URISyntaxException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			/*
			 * This is the name of the time of assignment and assertion we will use for all non-metadata
			 *   tuples
			 */
            String ta_name = "2014-07-03T15:49:37.543-0400";
            Iso8601Date ta_date = new Iso8601Date(DateConfiguration.YEAR_MONTH_DAY, 2014, 7, 3);
            Iso8601UnitTime ta_time = new Iso8601UnitTime(15, 49, 37, TimeUnit.MILLISECOND, 543); 
            
            /*
             * This is the name of the date of birth of the person
             */
            String tb_name = "1970-01-01";  //not my real birth date
            /*
             * This is the name of the person
             */
            String wHoganNameTxt = "William Hogan";
            Iui wh = Iui.createRandomIui();
	       
            RtsTuplePersistenceManager rpm = new RtsTuplePersistenceManager(App.dbDirectory, true);
            
            /*
             * Time of assertion of this set of tuples
             */
            TemporalRegion ta = new TemporalRegion(
            		new Iso8601DateTime(ta_date, ta_time), TimeZone.getDefault());
           rpm.addTemporalRegion(ta);
            
            /*
             * Name of time of assertion of this set of tuples
             */
            PtoDETuple ten = new PtoDETuple();
            ten.setTupleIui(Iui.createRandomIui());
            ten.setAuthorIui(wh);
            ten.setData(ta_name.getBytes());
            ten.setNamingSystem(gregorianIui);
            ten.setReferent(Iui.createRandomIui());
            ten.setAuthoringTimeReference(ta);  
            try {
				ten.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/BFO_0000058"));
				ten.setRelationshipOntologyIui(bfoIui);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ten.setDatatypeUui(new Uui("https://www.ietf.org/rfc/rfc3629.txt"));
            ten.setDatatypeOntologyIui(characterEncodingsIui);
            //ten.setNamingSystem(gregorianIui);
                        
            rpm.addTemporalReference(ta);
            rpm.saveTuple(ten);
            
            //Metadata tuple for ten
         	MetadataTuple d2 = new MetadataTuple();
        	d2.setTupleIui(Iui.createRandomIui());
        	d2.setReferent(ten.getTupleIui());
        	d2.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
        	d2.setChangeReason(RtsChangeReason.CR);
        	d2.setChangeType(RtsChangeType.I);
        	d2.setErrorCode(RtsErrorCode.Null);
            rpm.saveTuple(d2);
            
            
            /*
             * This function creates the person with his/her name and the time of assignment/
             *   assertion (t) and name of date of birth (tb_name), and returns a tetuple
             *   that references the interval from moment of birth to ~ta
             */
            TimeZone tzChicago = TimeZone.getTimeZone("America/Chicago");
            TemporalRegion t3 = createIndividualWithBirthdateAndReturnLifeIntervalTuple(
					tb_name, tzChicago, wHoganNameTxt,	wh, wh, rpm, ta, ten, null, null);
            
            rpm.addTemporalRegion(t3);
            
            /*
             * assign an IUI to the SNOMED-CT terminology
             */
            Iui snctCsIui = Iui.createFromString("59487A5D-C808-48A4-9DAE-894DCE866A96");        
            
            /*
             * PtoC that annotates W. Hogan with SNOMED-CT's "Father (person)" concept
             */
            PtoCTuple ptoc = new PtoCTuple();
            ptoc.setTupleIui(Iui.createRandomIui());
            ptoc.setReferentIui(wh);
            //ptoc.setAuthoringTimeIui(t.getReferentIui());
            ptoc.setAuthoringTimeReference(ta);
            ptoc.setAuthorIui(wh);
            ptoc.setConceptCui(new Cui("66839005"));
            ptoc.setTemporalReference(t3);  //the temporal region when a concept annotation "holds" is fairly meaningless
            ptoc.setConceptSystemIui(snctCsIui);  
            rpm.saveTuple(ptoc);
            
            /*
             * Metadata tuple for PtoC tuple
             */
        	MetadataTuple d3 = new MetadataTuple();
        	d3.setTupleIui(Iui.createRandomIui());
        	d3.setReferent(ptoc.getTupleIui());
        	d3.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
        	d3.setChangeReason(RtsChangeReason.CR);
        	d3.setChangeType(RtsChangeType.I);
        	d3.setErrorCode(RtsErrorCode.Null);
            rpm.saveTuple(d3);
            
            /*
             * W. Hogan instance of canis lupis familiaris (and hopefully a lucky one :-)
             * 
             * seriously, though, this is for testing purposes, I'm going to invalidate
             *   this tuple after the original commit
             */
            PtoUTuple ptouBad = new PtoUTuple();
            ptouBad.setTupleIui(Iui.createRandomIui());
            ptouBad.setReferentIui(wh);
            ptouBad.setRelationshipURI(instance_of);
            ptouBad.setRelationshipOntologyIui(roIui);
            //ptouBad.setAuthoringTimeIui(t.getReferentIui());
            ptouBad.setAuthoringTimeReference(ta);
            ptouBad.setAuthorIui(wh);
            ptouBad.setTemporalReference(t3);
            ptouBad.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/NCBITaxon_9615"));
            ptouBad.setUniversalOntologyIui(ncbiTaxonIui);
            rpm.saveTuple(ptouBad);
            
            /*
             * The original metadata tuple for the misinformed PtoU tuple
             */
        	MetadataTuple d4 = new MetadataTuple();
        	d4.setTupleIui(Iui.createRandomIui());
        	d4.setReferent(ptouBad.getTupleIui());
        	d4.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
        	d4.setChangeReason(RtsChangeReason.CR);
        	d4.setChangeType(RtsChangeType.I);
        	d4.setErrorCode(RtsErrorCode.Null);
            rpm.saveTuple(d4);
             
            
            //create another William Hogan
            tb_name = "1895-01-01";  //not his real birth date, either
            TimeZone tzNewYork = TimeZone.getTimeZone("America/New_York");
            String td_name = "1982-02";
            Iui wfh = Iui.createRandomIui();
            TemporalRegion t5 = createIndividualWithBirthdateAndReturnLifeIntervalTuple(
					tb_name, tzNewYork, wHoganNameTxt,	wfh, wh, rpm, ta, ten, td_name, tzNewYork);

            System.out.println("temporal reference t5: " + t5.toString());
            
            try {
				FileWriter fw = new FileWriter("/Users/hoganwr/rtstuples.txt");
				RtsTupleTextWriter rw = new RtsTupleTextWriter(fw);
				
				try {
					rpm.getTupleStream().forEach(i -> { try { rw.writeTuple(i); } catch (Exception e) { e.printStackTrace(); } } );
					rpm.getMetadataTupleStream().forEach(i -> { try { i.setAuthoringTimestamp(new Iso8601DateTime()); rw.writeTuple(i); } catch (Exception e) { e.printStackTrace(); } } );
					rpm.getTemporalRegionStream().forEach(i -> { try { rw.writeTemporalRegion(i); } catch (Exception e) { e.printStackTrace(); } } );
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				fw.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            /*
             * Save all the tuples accumulated thus far to Neo4J
             */
            rpm.commitTuples();
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple1 = rpm.getTuple(ptouBad.getTupleIui());
            System.out.println("\t" + tuple1.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple2 = rpm.getTuple(ownsPtoP.getTupleIui());
            System.out.println("\t" + tuple2.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple3 = rpm.getTuple(designatesPtoP.getTupleIui());
            System.out.println("\t" + tuple3.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple4 = rpm.getTuple(trefPtop.getTupleIui());
            System.out.println("\t" + tuple4.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple5 = rpm.getTuple(ten.getTupleIui());
            System.out.println("\t" + tuple5.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple6 = rpm.getTuple(ptoc.getTupleIui());
            System.out.println("\t" + tuple6.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");

            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple7 = rpm.getTuple(d4.getTupleIui());
            System.out.println("\t" + tuple7.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting all the PtoDE tuples per RtsStore Interface");
            TupleQuery tq = new TupleQuery();
            tq.addType(RtsTupleType.PTODETUPLE);
            Set<RtsTuple> result = rpm.runQuery(tq);
            for (RtsTuple rt : result) {
            	System.out.println("\t" + rt.toString());
            }
            
            System.out.println("Getting all the tuples that have referent of IUI for wh");
            TupleQuery tq2 = new TupleQuery();
            tq2.setReferentIui(wh);
            result = rpm.runQuery(tq2);
            for (RtsTuple rt : result) {
            	System.out.println("\t" + rt.toString());
            }

            System.out.println("Getting all the PtoU and PtoDE tuples that have referent of IUI for wh");
            TupleQuery tq3 = new TupleQuery();
            tq3.setReferentIui(wh);
            tq3.addType(RtsTupleType.PTOUTUPLE);
            tq3.addType(RtsTupleType.PTODETUPLE);
            result = rpm.runQuery(tq3);
            for (RtsTuple rt : result) {
            	System.out.println("\t" + rt.toString());
            }
           
            hello.graphDb = rpm.graphDb;
            
            //hello.shutDown();
            
            //hello.createDb();
            hello.queryRtsForEverythingAndDisplay();
            
            /*
             * Here's an interesting query:
             * match (n:data)-[:dr]-(n2:tuple)-[:iuip]-(n3:instance)-[:p]-(n4:tuple)-[:p]-(n5:instance)-[:iuip]-(n6:tuple)-[:uui]-(n7:universal) where n2.type = 'ptodr' and n.dr = 'William Hogan' and n4.type = 'ptop' and n6.type = 'ptou' and n7.uui = 'http://purl.obolibrary.org/obo/NCBITaxon_9606' return n, n2, n3, n4,n5,n6,n7;
             * 
             * Need to check that name is instance of personal name (n3 is iuip to ptou tuple with uui of URI for personal name from PNO)
             *   plus need to check that relation of n2 is "is concretized by"
             *   plus relation of n4 is URI for 'denotes'
             *   
             *   match (n:data)-[:dr]-(n2:tuple)-[:iuip]-(n3:instance)-[:p]-(n4:tuple)-[:p]-(n5:instance)-[:iuip]-(n6:tuple)-[:uui]-(n7:universal), n3-[:iuip]->(n8:tuple)-[:uui]->(n9:universal), n4-[:r]->(n10:relation) where n2.type = 'ptodr' and n.dr = 'William Hogan' and n4.type = 'ptop' and n6.type = 'ptou' and n7.uui = 'http://purl.obolibrary.org/obo/NCBITaxon_9606' and n8.type = 'ptou' and n9.uui = 'http://purl.obolibrary.org/obo/IAO_0020015' and n10.rui = 'http://purl.obolibrary.org/obo/IAO_0000219' return n,n2,n3,n4,n5,n6,n7,n8,n9,n10;
             */
            
            /*
             * after changing tuple types to labels, the query simplifies to:
             * 
             * match (n1:data)-[:dr]-(n2:ptode)-[:iuip]-(n3:instance)-[:p]-(n4:ptop)-[:p]-(n5:instance)-[:iuip]-(n6:ptou)-[:uui]-(n7:universal), n3-[:iuip]->(n8:ptou)-[:uui]->(n9:universal), n4-[:r]->(n10:relation) where n1.dr = 'William Hogan' and n7.uui = 'http://purl.obolibrary.org/obo/NCBITaxon_9606' and n9.uui = 'http://purl.obolibrary.org/obo/IAO_0020015' and n10.rui = 'http://purl.obolibrary.org/obo/IAO_0000219' return n1,n2,n3,n4,n5,n6,n7,n8,n9,n10;
             * 
             * match (n1:data)-[:dr]-(n2:ptode)-[:iuip]-(n3:instance)-[:p]-(n4:ptop)-[:p]-(n5:instance)-[:iuip]-(n6:ptou)-[:uui]-(n7:universal), 
             * 		  n3-[:iuip]->(n8:ptou)-[:uui]->(n9:universal), 
             *        n4-[:r]->(n10:relation)
             *  where n1.dr = 'William Hogan' and n7.uui = 'http://purl.obolibrary.org/obo/NCBITaxon_9606' and n9.uui = 'http://purl.obolibrary.org/obo/IAO_0020015' and n10.rui = 'http://purl.obolibrary.org/obo/IAO_0000219' return n1,n2,n3,n4,n5,n6,n7,n8,n9,n10;
             *  
             *  This query is equivalent to above:
               
               match (n1:data {dr:'William Hogan'})-[:dr]-(n2:ptode)-[:iuip]-(n3:instance)-[:p]-(n4:ptop)-[:p]-(n5:instance)-[:iuip]-(n6:ptou)-[:uui]-(n7:universal {uui:'http://purl.obolibrary.org/obo/NCBITaxon_9606'}), 
                      n3-[:iuip]->(n8:ptou)-[:uui]->(n9:universal {uui:'http://purl.obolibrary.org/obo/IAO_0020015'}), 
                      n4-[:r]->(n10:relation {rui:'http://purl.obolibrary.org/obo/IAO_0000219'}),
                      n2-[:about {valid_to:9223372036854775807}]-(),
                      n4-[:about {valid_to:9223372036854775807}]-(),
                      n6-[:about {valid_to:9223372036854775807}]-(),
                      n8-[:about {valid_to:9223372036854775807}]-()
              	return n1,n2,n3,n4,n5,n6,n7,n8,n9,n10;
              	
              	1407029450000
              	
              match (n1:data {dr:'William Hogan'})-[:dr]-(n2:ptode)-[:iuip]-(n3:instance)-[:p]-(n4:ptop)-[:p]-(n5:instance)-[:iuip]-(n6:ptou)-[:uui]-(n7:universal {uui:'http://purl.obolibrary.org/obo/NCBITaxon_9615'}), 
                      n3-[:iuip]->(n8:ptou)-[:uui]->(n9:universal {uui:'http://purl.obolibrary.org/obo/IAO_0020015'}), 
                      n4-[:r]->(n10:relation {rui:'http://purl.obolibrary.org/obo/IAO_0000219'}),
                      n2-[:about {valid_to:9223372036854775807}]-(),
                      n4-[:about {valid_to:9223372036854775807}]-(),
                      n6-[:about {valid_to:9223372036854775807}]-(),
                      n8-[:about {valid_to:9223372036854775807}]-()
              	return n1,n2,n3,n4,n5,n6,n7,n8,n9,n10;
             */
            
            Charset c = Charset.forName("UTF-8");
            System.out.println(c + "\t" + c.displayName() + "\t" + c.name() + "\tis registered: " + c.isRegistered() + "\tcan encode: " + c.canEncode());
            
            /*
             * For all the tuples we are inserting, we know all the parameters
             *   except possibly iuid and change reason (C)
             *   
             *   iui of metadata tuple: assign a new one
             *   iuit: iui of tuple this metadata tuple references
             *   td: generate timestamp right before end of transaction
             *   CT: I (inserting)
             *   C: could be any value (change in relevance or reality or belief, or
             *   		we recognized an error in an existing tuple and are
             *   		inserting this one with the correct information.)
             *   E: Null
             *   S: empty (if the inserted tuple is replacing an existing tuple,
             *      then the metadata tuple that invalidates the existing tuple
             *      must have an S parameter that points to the tuple we're inserting
             *      here). 
             */
           
            
            /*
             * Now invalidate the tuple that says W. Hogan is instance of dog,
             *   which per Ceusters' error coding is an error of type U1
             */
            MetadataTuple dCorrection = new MetadataTuple();
            dCorrection.setTupleIui(Iui.createRandomIui());
            dCorrection.setReferent(ptouBad.getTupleIui());
            dCorrection.setAuthoringTimestamp(new Iso8601DateTime());
            dCorrection.setAuthorIui(wh);
            dCorrection.setChangeReason(RtsChangeReason.XR);
            dCorrection.setErrorCode(RtsErrorCode.U1);
            dCorrection.setChangeType(RtsChangeType.X);
            //we could also point it at ptou if we wanted
            rpm.saveTuple(dCorrection);
            
            try {
                File f = new File("src/test/output");
                if (!f.exists()) f.mkdir();

  				FileWriter fw = new FileWriter("src/test/output/rtstuples.txt", true);
  				RtsTupleTextWriter rw = new RtsTupleTextWriter(fw);
  				
  				try {
  					rpm.getTupleStream().forEach(i -> { try { rw.writeTuple(i); } catch (Exception e) { e.printStackTrace(); } } );
  					rpm.getMetadataTupleStream().forEach(i -> { try { i.setAuthoringTimestamp(new Iso8601DateTime()); rw.writeTuple(i); } catch (Exception e) { e.printStackTrace(); } } );
  					rpm.getTemporalRegionStream().forEach(i -> { try { rw.writeTemporalRegion(i); } catch (Exception e) { e.printStackTrace(); } } );
  				} catch (Exception e) {
  					// TODO Auto-generated catch block
  					e.printStackTrace();
  				}
  				
  				FileReader fr_test = new FileReader("src/test/resources/rtstuples_old");
  				BufferedReader br_test = new BufferedReader(fr_test);
  				char[] read = new char[64];
  				
  				int i = 1;
  				char prior_read = '\0';
  				StringBuilder sb = new StringBuilder();
  				while (i > 0) {
  					i = br_test.read(read, 0, 64);
  					//System.out.println(read);
  					for (int j=0; j<i; j++) {
  						if (read[j] == '\n' && prior_read != '\\') {
  							System.out.println("tuple = \"" + sb.toString() + "\"");
  							sb = new StringBuilder();
  							prior_read = '\0';
  						} else {
  							sb.append(read[j]);
  							prior_read = read[j];
  						}
  					}
  				}
  				System.out.println(read);
  				
  				fr_test.close();
  				
  				/*LineNumberReader lnr = new LineNumberReader(fr_test);
  				String line = null;
  				while ((line=lnr.readLine())!=null) {
  					System.out.println("\""+ line + "\"");
  				}
  				lnr.close();
  				*/
  				
  				fw.close();
  				
  			} catch (IOException e) {
  				// TODO Auto-generated catch block
  				e.printStackTrace();
  			}
            
            rpm.commitTuples();
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple8 = rpm.getTuple(dCorrection.getTupleIui());
            System.out.println("\t" + tuple8.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            System.out.println("Getting Tuple by Iui per RtsStore Interface:");
            RtsTuple tuple9 = rpm.getTuple(d4.getTupleIui());
            System.out.println("\t" + tuple9.toString());
            System.out.println("End Getting Tuple by Iui per RtsStore Interface");
            
            //System.out.println(d1.toString());
            System.out.println(ptouBad.toString());
            System.out.println(dCorrection.toString());
            
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(Long.MAX_VALUE);
            System.out.println(cal);
            System.out.println("Long.MAX_VALUE=" + Long.MAX_VALUE);
            
            
            
            try {
				//RtsTupleTextParser ttr = new RtsTupleTextParser(new BufferedReader(new FileReader("/Users/hoganwr/rtstuples.txt")));
            	RtsTupleTextParser ttr = new RtsTupleTextParser(new BufferedReader(new FileReader("src/test/resources/test-tuple-generation.out")));
				ttr.parseTuples();
				
				Set<TemporalRegion> time = ttr.getTemporalRegions();
				Iterator<TemporalRegion> t = time.iterator();
				System.out.println(time.size() + " temporal regions.");
				while (t.hasNext()) {
					rpm.addTemporalRegion(t.next());
				}
				Iterator<RtsTuple> i = ttr.iterator();
				while (i.hasNext()) {
					rpm.saveTuple(i.next());
				}
				
				rpm.commitTuples();
				System.out.println(rpm.graphDb.toString());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
           
            hello.queryRtsForEverythingAndDisplay();
            
            System.out.println("*******************************************************************");
            System.out.println("*  Query IUI of entity denoted by PCORnet PATID ='321454' ");
            System.out.println("*******************************************************************");
            hello.runQueryAndDisplayResult("match (n:instance)-[r1:iuip]-(o:U)-[uui]->(q:universal), (n)-[p1:p]-(n2:P)-[r]->(n3:relation), (n2)-[p2:p]->(n4:instance), (n)-[r2:iuip]-(n5:E)-[r3:dr]->(n6:data) where q.uui = \"http://purl.obolibrary.org/obo/PCORnet/PCORnet_00000019\" and n3.rui=\"http://purl.obolibrary.org/obo/IAO_0000219\" and n6.dr = '321454'  return n4.iui;");
            
            String patidIriTxt = "http://purl.obolibrary.org/obo/PCORnet/PCORnet_00000019";
            String queryTemplateTxt = "match (n:instance)-[r1:iuip]-(o:U)-[uui]->(q:universal), (n)-[p1:p]-(n2:P)-[r]->(n3:relation), (n2)-[p2:p]->(n4:instance), (n)-[r2:iuip]-(n5:E)-[r3:dr]->(n6:data) where q.uui = $idTypeIri and n3.rui=\"http://purl.obolibrary.org/obo/IAO_0000219\" and n6.dr = $idValue  return n4.iui;";
    		HashMap<String, Object> parameters = new HashMap<String, Object>();
    		parameters.put("idTypeIri", patidIriTxt);
    		parameters.put("idValue", "321454");
    		hello.runQueryWithParametersAndDisplayResults(queryTemplateTxt, parameters);

            Set<ParticularReference> prs = rpm.getReferentsByTypeAndDesignatorType(null, new Uui("http://purl.obolibrary.org/obo/IAO_0020015"), "William Hogan");
            System.out.println(prs.size());
            Iterator<ParticularReference> pri = prs.iterator();
            while (pri.hasNext())
                System.out.println(pri.next());
            
            hello.shutDown();
	    }

		private static TemporalRegion createIndividualWithBirthdateAndReturnLifeIntervalTuple(
				String tb_name,	TimeZone tzBirth, String personsName, Iui wh, Iui authorIui, 
				RtsTuplePersistenceManager rpm, TemporalRegion ta, PtoDETuple ten,
				String td_name, TimeZone tzDeath) {
			
			Iui wh_chair = Iui.createRandomIui();
            Iui wh_name = Iui.createRandomIui();
            
            Set<RtsTuple> tset = new HashSet<RtsTuple>();
            
            /*
             * W. Hogan
             */
            ATuple a1 = new ATuple();
            a1.setAuthorIui(authorIui);
            a1.setReferentIui(wh);
            a1.setAuthoringTimestamp(new Iso8601DateTime());
            a1.setTupleIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's chair
             */
            ATuple a2 = new ATuple();
            a2.setAuthorIui(authorIui);
            a2.setReferentIui(wh_chair);
            a2.setAuthoringTimestamp(new Iso8601DateTime());
            a2.setTupleIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's full name
             */
            ATuple a3 = new ATuple();
            a3.setAuthorIui(authorIui);
            a3.setReferentIui(wh_name);
            a3.setAuthoringTimestamp(new Iso8601DateTime());
            a3.setTupleIui(Iui.createRandomIui());
            
            
            /*
             * Time during which W. Hogan has been owner of his chair
             */
            TemporalRegion t2 = new TemporalRegion(TemporalRegion.ONE_D_REGION_TYPE);
            rpm.addTemporalRegion(t2);
            
            
            /*
             * Time during which W. Hogan has been instance of human being
             */
            TemporalRegion t3 = new TemporalRegion(TemporalRegion.ONE_D_REGION_TYPE);
            rpm.addTemporalRegion(t3);
            
            /*
             * Day of W. Hogan's birth
             */
            Iso8601DateParser p = new Iso8601DateParser();
            Iso8601Date db = null;
			try {
				db = p.parse(tb_name);
			} catch (Iso8601DateParseException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
            TemporalRegion t4 = new TemporalRegion(db, tzBirth);
            rpm.addTemporalRegion(t4);
            
            /*
             * Name of day of W. Hogan's birth
             */
            PtoDETuple ten2 = new PtoDETuple();
            ten2.setTupleIui(Iui.createRandomIui());
            ten2.setReferent(t4);
            //ten2.setAuthoringTimeIui(t.getReferentIui());
            ten2.setAuthoringTimeReference(ta);
            ten2.setAuthorIui(authorIui);
            ten2.setData(tb_name.getBytes());
            ten2.setNamingSystem(gregorianIui);
            //ten2.setReferent(Iui.createRandomIui());
            try {
				ten2.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/BFO_0000058"));
				ten2.setRelationshipOntologyIui(bfoIui);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ten2.setDatatypeUui(new Uui("https://www.ietf.org/rfc/rfc3629.txt"));
            ten2.setDatatypeOntologyIui(characterEncodingsIui);
            //ten2.setNamingSystem(gregorianIui);
           
            
            /*
             * If we have a date of death, then create it and add it
             */
            TemporalRegion t7 = null;
            PtoDETuple ten3 = null;
            if (td_name != null) {
            	Iso8601Date deathDate = null;
				try {
					deathDate = p.parse(td_name);
				} catch (Iso8601DateParseException e1) {
					e1.printStackTrace();
				}
                /*
                 * Day of W. Hogan's death
                 */
            	t7 = new TemporalRegion(deathDate, tzDeath);
            	rpm.addTemporalRegion(t7);
            	                
                /*
                 * Name of day of W. Hogan's death
                 */
                ten3 = new PtoDETuple();
                ten3.setTupleIui(Iui.createRandomIui());
                //ten3.setTemporalEntityIui(t4.getReferentIui());
                ten3.setReferent(t7);
                //ten3.setAuthoringTimeIui(t.getReferentIui());
                ten3.setAuthoringTimeReference(ta);
                ten3.setAuthorIui(authorIui);
                ten3.setData(td_name.getBytes());
                ten3.setNamingSystem(gregorianIui);
                //ten3.setReferent(Iui.createRandomIui());
                try {
    				ten3.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/BFO_0000058"));
    				ten3.setRelationshipOntologyIui(bfoIui);
    			} catch (URISyntaxException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
                ten3.setDatatypeUui(new Uui("https://www.ietf.org/rfc/rfc3629.txt"));
                ten3.setDatatypeOntologyIui(characterEncodingsIui);
                //ten3.setNamingSystem(gregorianIui);
                
                /*
                 * PtoP for day of W. Hogan's death to time during which W. Hogan has been a human being
                 */
                PtoPTuple ptop4 = new PtoPTuple();
                ptop4.setTupleIui(Iui.createRandomIui());
                ptop4.setAuthorIui(authorIui);
                //ptop4.setAuthoringTimeIui(t.getReferentIui());
                ptop4.setAuthoringTimeReference(ta);
                ptop4.setReferent(t3);
                ptop4.addParticular(t7);
                try {
                	ptop4.setRelationshipURI(new URI("http://ctsi.ufl.edu/rts/overlaps"));
                	ptop4.setRelationshipOntologyIui(roIui);
    			} catch (URISyntaxException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
                ptop4.setTemporalReference(TemporalRegion.MAX_TEMPORAL_REGION);
                rpm.addTemporalRegion(TemporalRegion.MAX_TEMPORAL_REGION);
                trefPtop = ptop4;
                
                //tset.add(t7);
                tset.add(ten3);
                tset.add(ptop4);
                
                rpm.addTemporalReference(t7);
                rpm.saveTuple(ten3);
                rpm.saveTuple(ptop4);
            }
            
            /*
             * W. Hogan's chair owned by W. Hogan
             */
            PtoPTuple ptop = new PtoPTuple();
            ptop.setReferent(wh_chair);
            ptop.setAuthorIui(authorIui);
            try {
				ptop.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/OMIABIS_0000048"));
				ptop.setRelationshipOntologyIui(roIui);  //ok so it's not correct IUIo parameter.  Temporizing.
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //ptop.setAuthoringTimeIui(t.getReferentIui());
            ptop.setAuthoringTimeReference(ta);
            ptop.addParticular(wh);
            ptop.setTupleIui(Iui.createRandomIui());
            ptop.setTemporalReference(t2);
            ownsPtoP = ptop;
            //ptop.setTemporalEntityIui(t2.getReferent());
          
            /*
             * W. Hogan instance of human being
             */
            PtoUTuple ptou = new PtoUTuple();
            ptou.setTupleIui(Iui.createRandomIui());
            ptou.setReferentIui(wh);
            ptou.setRelationshipURI(instance_of);
            ptou.setRelationshipOntologyIui(roIui);
            //ptou.setAuthoringTimeIui(t.getReferentIui());
            ptou.setAuthoringTimeReference(ta);
            ptou.setAuthorIui(authorIui);
            //ptou.setTemporalEntityIui(t3.getReferent());
            ptou.setTemporalReference(t3);
            ptou.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/NCBITaxon_9606"));
            ptou.setUniversalOntologyIui(ncbiTaxonIui);
       
            /*
             * W. Hogan's name instance of personal name
             */
            PtoUTuple ptou3 = new PtoUTuple();
            ptou3.setTupleIui(Iui.createRandomIui());
            ptou3.setReferentIui(wh_name);
            ptou3.setRelationshipURI(instance_of);
            ptou3.setRelationshipOntologyIui(roIui);
            //ptou3.setAuthoringTimeIui(t.getReferentIui());
            ptou3.setAuthoringTimeReference(ta);
            ptou3.setAuthorIui(authorIui);
            //ptou3.setTemporalEntityIui(t3.getReferent());
            ptou3.setTemporalReference(t3);
            ptou3.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/IAO_0020015"));
            ptou3.setUniversalOntologyIui(pnoIui);
            
            /*
             * W. Hogan's name designates W. Hogan
             */
            PtoPTuple ptop2 = new PtoPTuple();
            ptop2.setReferent(wh_name);
            ptop2.setAuthorIui(authorIui);
            try {
				ptop2.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/IAO_0000219"));
				ptop2.setRelationshipOntologyIui(roIui); //NEED TO FIX
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //ptop2.setAuthoringTimeIui(t.getReferentIui());
            ptop2.setAuthoringTimeReference(ta);
            ptop2.addParticular(wh);
            ptop2.setTupleIui(Iui.createRandomIui());
            //ptop2.setTemporalEntityIui(t3.getReferent());  
            ptop2.setTemporalReference(t3);
            designatesPtoP = ptop2;
            
            /*
             * W. Hogan's name's digital representation
             */
            PtoDETuple ptodr = new PtoDETuple();
            ptodr.setTupleIui(Iui.createRandomIui());
            ptodr.setAuthorIui(authorIui);
            //ptodr.setAuthoringTimeIui(t.getReferentIui());
            ptodr.setAuthoringTimeReference(ta);
            ptodr.setReferent(wh_name);
            ptodr.setData(personsName.getBytes());
            try {
				ptodr.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/BFO_0000058"));
				ptodr.setRelationshipOntologyIui(bfoIui);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ptodr.setDatatypeUui(new Uui("https://www.ietf.org/rfc/rfc3629.txt"));
            //need to set datatype ontology IUI
            ptodr.setDatatypeOntologyIui(characterEncodingsIui);
            //need to set naming system IUI - not great but a hack for now to set it to
            // Proper Name Ontology
            ptodr.setNamingSystem(pnoIui);
            
            /*
             * PtoP for day of W. Hogan's birth to time during which W. Hogan has been a human being
             */
            PtoPTuple ptop3 = new PtoPTuple();
            ptop3.setTupleIui(Iui.createRandomIui());
            ptop3.setAuthorIui(authorIui);
            //ptop3.setAuthoringTimeIui(t.getReferentIui());
            ptop3.setAuthoringTimeReference(ta);
            ptop3.setReferent(t4);
            ptop3.addParticular(t3);
            try {
				ptop3.setRelationshipURI(new URI("http://ctsi.ufl.edu/rts/overlaps"));
				ptop3.setRelationshipOntologyIui(roIui);  //FIX
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //ptop3.setTemporalEntityIui(maxTimeIntervalIui);
            ptop3.setTemporalReference(TemporalRegion.MAX_TEMPORAL_REGION);
            
            tset.add(a1);
            tset.add(a2);
            tset.add(a3);
            tset.add(ptop);
            tset.add(ptop2);
            tset.add(ptop3);
            tset.add(ptou);
            tset.add(ptou3);
            tset.add(ten2);
            tset.add(ptodr);
                     
            rpm.saveTuple(a1);
            rpm.saveTuple(a2);
            rpm.saveTuple(a3);
            
            rpm.saveTuple(ptop);
            rpm.saveTuple(ptop2);
            rpm.saveTuple(ptop3);
            rpm.saveTuple(ptou);
            rpm.saveTuple(ptou3);
            rpm.saveTuple(ten2);
            rpm.saveTuple(ptodr);
                        
            Iterator<RtsTuple> it = tset.iterator();
            while (it.hasNext()) {
            	RtsTuple tnext = it.next();
            	MetadataTuple d = new MetadataTuple();
                d.setTupleIui(Iui.createRandomIui());
                d.setReferent(tnext.getTupleIui());
                d.setAuthorIui(authorIui);
                //d.setAuthoringTimestamp(new Iso8601DateTime());
                d.setChangeReason(RtsChangeReason.CR);
                d.setChangeType(RtsChangeType.I);
                d.setErrorCode(RtsErrorCode.Null);
                rpm.saveTuple(d);
            }
			return t3;
		}

	    void createDb()
	    {
            // tag::startDb[]
            mgmtSvc = new DatabaseManagementServiceBuilder( dbDirectory ).build();
            graphDb = mgmtSvc.database( DEFAULT_DATABASE_NAME );
            registerShutdownHook( mgmtSvc );
            // end::startDb[]
	    }
	    
	    void addData() {
	        
	             // tag::transaction[]
            try ( Transaction tx = graphDb.beginTx() )
            {
                // Database operations go here
                // end::transaction[]
                // tag::addData[]
                n1 = tx.createNode();
                n1.setProperty( "label", "electromagnetic force" );
                n2 = tx.createNode();
                n2.setProperty( "label", "fundamental physical force" );

                rel = n1.createRelationshipTo( n2, RelTypes.ISA );
                rel.setProperty( "label", "(shown by science)" );
                // end::addData[]

                // tag::readData[]
                System.out.print( n1.getProperty( "label" ) );
                System.out.print(" ");
                System.out.print(rel.getType().toString());
                System.out.print(" ");
                System.out.print( rel.getProperty( "label" ) );
                System.out.print(" ");
                System.out.print( n2.getProperty( "label" ) );
                // end::readData[]

                String stmt = ( (String) n1.getProperty( "label" ) )
                    + " " + rel.getType().toString() + " " 
                    + ( (String) rel.getProperty( "label" ) )
                    + " "+ ( (String) n2.getProperty( "label" ) );

                // tag::transaction[]
                tx.commit();
            }
	    }

    void removeData()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // tag::removingData[]
            // let's remove the data
            n1 = tx.getNodeById( n1.getId() );
            n2 = tx.getNodeById( n2.getId() );
            n1.getSingleRelationship( RelTypes.ISA, Direction.OUTGOING ).delete();
            n1.delete();
            n2.delete();
            // end::removingData[]

            tx.commit();
        }
    }

   void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // tag::shutdownServer[]
        mgmtSvc.shutdown();
        // end::shutdownServer[]
    }

    // tag::shutdownHook[]
    private static void registerShutdownHook( final DatabaseManagementService mgmtSvc )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                mgmtSvc.shutdown();
            }
        } );
    }
    // end::shutdownHook[]

	    
	    void queryRtsForEverythingAndDisplay() {
   
            String query = "MATCH (n) return n"; //"START n=node(*) RETURN n";
            
            try ( Transaction tx = graphDb.beginTx() ) {
            	
            	Result result = tx.execute(query);
            	//ResourceIterator<Map<String, Object>> i = result.iterator();
            	while (result.hasNext()) {
            		Map<String, Object> entry = result.next();
            		System.out.println(entry.size());
            		Set<String> keys = entry.keySet();
            		for (String key : keys) {
            			System.out.print("\t" + key);
            			Object o = entry.get(key);
            			Node n = (Node)o;
            			
            			System.out.print("\t" + n.getId());
            			
            			Iterator<Label> labels = n.getLabels().iterator();
            			String label =labels.next().toString();
            			System.out.print("\t" + label);
            			if (label.equals("instance")) {
            				System.out.print("\tiui = "+ n.getProperty("iui"));
            			} else if (label.equals("temporal_region")) { 
            				System.out.print("\ttref = " + n.getProperty("tref"));
            			} else if (label.equals("universal")) {
            				n.addLabel(Label.label("universal"));  //what happens if we try to add a label that is already there?
            				System.out.print("\tuui = " + n.getProperty("uui"));
            			} else if (label.equals("relation")) {
            				System.out.print("\trui = " + n.getProperty("rui"));
            			} else if (label.equals("tuple") || label.equals("D") || label.equals("P") 
            					|| label.equals("P_") || label.equals("U") || label.equals("U_") 
            					|| label.equals("A")) {        				
            				System.out.print("\tiui = "+ n.getProperty("iui"));
            				if (labels.hasNext()) {
            					String type = labels.next().toString();
            					System.out.print("\t" + type);
            				} else {
            					System.out.print("\tno other type!!");
            				}
            				
            				if (label.equals("a") || label.equals("te")) {
            					String tap = (String) n.getProperty("tap");
            					System.out.print("\ttap =" + tap);
            				} else if (label.equals("ten")) {
            					String name = (String)n.getProperty("name");
            					System.out.print("\tname = " + name);
            				} else if (label.equals("ptode")) {
            					
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
            				String direction = (n.getId() == r.getStartNode().getId()) ? "outgoing" : "incoming";
            				Node en = r.getEndNode();
            				Node sn = r.getStartNode();
            				RelationshipType rt = r.getType();
            				System.out.println("\t\tr" + r.getId() + "\t" + sn.getId() + "\t" + rt.name() + "\t" + en.getId() + "\t" + direction);// + "\t" + en.getProperty("type"));
            			}//*/
            			
            		}
            	}
            	
            	tx.close();
            
            }
	    }
	    
	    void runQueryAndDisplayResult(String query) {
	    	
	    	   try ( Transaction tx = graphDb.beginTx() ) {
	            	
	            	Result result = tx.execute(query);
	            	//ResourceIterator<Map<String, Object>> i = result.iterator();
	            	while (result.hasNext()) {
	            		Map<String, Object> entry = result.next();
	            		System.out.println(entry.size());
	            		Set<String> keys = entry.keySet();
	            		for (String key : keys) {
	            			System.out.print("\t" + key);
	            			Object o = entry.get(key);
	            			if (o instanceof Node) {
	            				Node n = (Node)o;
	            			
	            				System.out.print("\t" + n.getId());
	            			
	            				Iterator<Label> labels = n.getLabels().iterator();
	            				String label =labels.next().toString();
	            				System.out.print("\t" + label);
	            				if (label.equals("instance")) {
	            					System.out.print("\tiui = "+ n.getProperty("iui"));
	            				} else if (label.equals("temporal_region")) { 
	            					System.out.print("\ttref = " + n.getProperty("tref"));
	            				} else if (label.equals("universal")) {
	            					n.addLabel(Label.label("universal"));  //what happens if we try to add a label that is already there?
	            					System.out.print("\tuui = " + n.getProperty("uui"));
	            				} else if (label.equals("relation")) {
	            					System.out.print("\trui = " + n.getProperty("rui"));
	            				} else if (label.equals("tuple") || label.equals("D") || label.equals("P") 
	            						|| label.equals("P_") || label.equals("U") || label.equals("U_") 
	            						|| label.equals("A")) {        				
	            					System.out.print("\tiui = "+ n.getProperty("iui"));
	            					if (labels.hasNext()) {
	            						String type = labels.next().toString();
	            						System.out.print("\t" + type);
	            					} else {
	            						System.out.print("\tno other type!!");
	            					}
	            				
	            					if (label.equals("a") || label.equals("te")) {
	            						String tap = (String) n.getProperty("tap");
	            						System.out.print("\ttap =" + tap);
	            					} else if (label.equals("ten")) {
	            						String name = (String)n.getProperty("name");
	            						System.out.print("\tname = " + name);
	            					} else if (label.equals("ptode")) {
	            					
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
	            					String direction = (n.getId() == r.getStartNode().getId()) ? "outgoing" : "incoming";
	            					Node en = r.getEndNode();
	            					Node sn = r.getStartNode();
	            					RelationshipType rt = r.getType();
	            					System.out.println("\t\tr" + r.getId() + "\t" + sn.getId() + "\t" + rt.name() + "\t" + en.getId() + "\t" + direction);// + "\t" + en.getProperty("type"));
	            				}//*/
	            			
	            			} else if (o instanceof String) {
	            				String resultTxt =(String)o;
	            				System.out.println("\tString result is: " + resultTxt);
	            			
	            			}
	            		}
	            	}
	    	
	            	
	            	tx.commit();
	            }
	    }
	    
	    void runQueryWithParametersAndDisplayResults(String queryTemplateTxt, HashMap<String, Object> parameters) {
  	
	    	   try ( Transaction tx = graphDb.beginTx() ) {
	            	
	            	Result result = tx.execute(queryTemplateTxt, parameters);
	            	//ResourceIterator<Map<String, Object>> i = result.iterator();
	            	while (result.hasNext()) {
	            		Map<String, Object> entry = result.next();
	            		System.out.println(entry.size());
	            		Set<String> keys = entry.keySet();
	            		for (String key : keys) {
	            			System.out.print("\t" + key);
	            			Object o = entry.get(key);
	            			if (o instanceof Node) {
	            				Node n = (Node)o;
	            			
	            				System.out.print("\t" + n.getId());
	            			
	            				Iterator<Label> labels = n.getLabels().iterator();
	            				String label =labels.next().toString();
	            				System.out.print("\t" + label);
	            				if (label.equals("instance")) {
	            					System.out.print("\tiui = "+ n.getProperty("iui"));
	            				} else if (label.equals("temporal_region")) { 
	            					System.out.print("\ttref = " + n.getProperty("tref"));
	            				} else if (label.equals("universal")) {
	            					n.addLabel(Label.label("universal"));  //what happens if we try to add a label that is already there?
	            					System.out.print("\tuui = " + n.getProperty("uui"));
	            				} else if (label.equals("relation")) {
	            					System.out.print("\trui = " + n.getProperty("rui"));
	            				} else if (label.equals("tuple") || label.equals("D") || label.equals("P") 
	            						|| label.equals("P_") || label.equals("U") || label.equals("U_") 
	            						|| label.equals("A")) {        				
	            					System.out.print("\tiui = "+ n.getProperty("iui"));
	            					if (labels.hasNext()) {
	            						String type = labels.next().toString();
	            						System.out.print("\t" + type);
	            					} else {
	            						System.out.print("\tno other type!!");
	            					}
	            				
	            					if (label.equals("a") || label.equals("te")) {
	            						String tap = (String) n.getProperty("tap");
	            						System.out.print("\ttap =" + tap);
	            					} else if (label.equals("ten")) {
	            						String name = (String)n.getProperty("name");
	            						System.out.print("\tname = " + name);
	            					} else if (label.equals("ptode")) {
	            					
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
	            					String direction = (n.getId() == r.getStartNode().getId()) ? "outgoing" : "incoming";
	            					Node en = r.getEndNode();
	            					Node sn = r.getStartNode();
	            					RelationshipType rt = r.getType();
	            					System.out.println("\t\tr" + r.getId() + "\t" + sn.getId() + "\t" + rt.name() + "\t" + en.getId() + "\t" + direction);// + "\t" + en.getProperty("type"));
	            				}//*/
	            			
	            			} else if (o instanceof String) {
	            				String resultTxt =(String)o;
	            				System.out.println("\tString result is: " + resultTxt);
	            			
	            			}
	            		}
	            	}  	
	            	tx.commit();
	            }
	    }
}
 