package neo4jtest.test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import edu.uams.dbmi.rts.cui.Cui;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.metadata.RtsChangeReason;
import edu.uams.dbmi.rts.metadata.RtsChangeType;
import edu.uams.dbmi.rts.metadata.RtsErrorCode;
import edu.uams.dbmi.rts.template.ATemplate;
import edu.uams.dbmi.rts.template.MetadataTemplate;
import edu.uams.dbmi.rts.template.PtoCTemplate;
import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.uams.dbmi.rts.template.TeTemplate;
import edu.uams.dbmi.rts.template.TenTemplate;
import edu.uams.dbmi.rts.uui.Uui;
import edu.uams.dbmi.util.iso8601.Iso8601DateTime;
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
	    
	    static URI instance_of;
	    static Iui gregorianIui = Iui.createFromString("D4AF5C9A-47BA-4BF4-9BAE-F13A8ED6455E"),
	    		maxTimeIntervalIui = Iui.createFromString("26F1052B-311D-43B1-9ABC-B4E2EDD1B283");

	    public static void main( final String[] args )
	    {
	        App hello = new App();
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
            String ta_name = "2014-07-03T15:49:37.543";
            String tb_name = "1969-04-20";
            String wHoganNameTxt = "William Hogan";
            Iui wh = Iui.createRandomIui();
	       
            RtsTemplatePersistenceManager rpm = new RtsTemplatePersistenceManager();
            /*
             * Time of assertion of this set of templates
             */
            TeTemplate t = new TeTemplate();
            t.setAuthoringTimestamp(new Iso8601DateTime());
            t.setReferentIui(Iui.createRandomIui());
            t.setAuthorIui(wh);
            t.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInstant"));
            t.setTemplateIui(Iui.createRandomIui());
            
            /*
             * Name of time of assertion of this set of templates
             */
            TenTemplate ten = new TenTemplate();
            ten.setTemplateIui(Iui.createRandomIui());
            ten.setAuthorIui(wh);
            ten.setName(ta_name);
            ten.setNamingSystemIui(gregorianIui);
            ten.setReferentIui(Iui.createRandomIui());
            ten.setTemporalEntityIui(t.getReferentIui());
            ten.setAuthoringTimeIui(t.getReferentIui());   
            
            rpm.addTemplate(t);
            rpm.addTemplate(ten);
            
            //Metadata templates for t and ten
        	MetadataTemplate d = new MetadataTemplate();
            d.setTemplateIui(Iui.createRandomIui());
            d.setReferentIui(t.getTemplateIui());
            d.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
            d.setChangeReason(RtsChangeReason.CR);
            d.setChangeType(RtsChangeType.I);
            d.setErrorCode(RtsErrorCode.Null);
            rpm.addTemplate(d);
            
        	MetadataTemplate d2 = new MetadataTemplate();
        	d2.setTemplateIui(Iui.createRandomIui());
        	d2.setReferentIui(ten.getTemplateIui());
        	d2.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
        	d2.setChangeReason(RtsChangeReason.CR);
        	d2.setChangeType(RtsChangeType.I);
        	d2.setErrorCode(RtsErrorCode.Null);
            rpm.addTemplate(d2);
            
            
            TeTemplate t3 = createIndividualWithBirthdateAndReturnLifeIntervalTemplate(
					tb_name, wHoganNameTxt,	wh, wh, rpm, t, ten, null);
            
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
            ptoc.setTemporalEntityIui(t.getReferentIui());  //the temporal region when a concept annotation "holds" is fairly meaningless
            ptoc.setConceptSystemIui(snctCsIui);  
            rpm.addTemplate(ptoc);
            
        	MetadataTemplate d3 = new MetadataTemplate();
        	d3.setTemplateIui(Iui.createRandomIui());
        	d3.setReferentIui(ptoc.getTemplateIui());
        	d3.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
        	d3.setChangeReason(RtsChangeReason.CR);
        	d3.setChangeType(RtsChangeType.I);
        	d3.setErrorCode(RtsErrorCode.Null);
            rpm.addTemplate(d3);
            
            /*
             * W. Hogan instance of canis lupis familiaris (and hopefully a lucky one :-)
             * 
             * seriously, though, this is for testing purposes, I'm going to invalidate
             *   this template after the original commit
             */
            PtoUTemplate ptouBad = new PtoUTemplate();
            ptouBad.setTemplateIui(Iui.createRandomIui());
            ptouBad.setReferentIui(wh);
            ptouBad.setRelationshipURI(instance_of);
            ptouBad.setAuthoringTimeIui(t.getReferentIui());
            ptouBad.setAuthorIui(wh);
            ptouBad.setTemporalEntityIui(t3.getReferentIui());
            ptouBad.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/NCBITaxon_9615"));
            rpm.addTemplate(ptouBad);
            
        	MetadataTemplate d4 = new MetadataTemplate();
        	d4.setTemplateIui(Iui.createRandomIui());
        	d4.setReferentIui(ptouBad.getTemplateIui());
        	d4.setAuthorIui(wh);
            //d.setAuthoringTimestamp(new Iso8601DateTime());
        	d4.setChangeReason(RtsChangeReason.CR);
        	d4.setChangeType(RtsChangeType.I);
        	d4.setErrorCode(RtsErrorCode.Null);
            rpm.addTemplate(d4);
             
            
            //create another William Hogan
            tb_name = "1896-08-17";
            String td_name = "1982-02";
            Iui wfh = Iui.createRandomIui();
            TeTemplate t5 = createIndividualWithBirthdateAndReturnLifeIntervalTemplate(
					tb_name, wHoganNameTxt,	wfh, wh, rpm, t, ten, td_name);
            
            rpm.commitTemplates();
            
            hello.graphDb = rpm.graphDb;
            
            //hello.shutDown();
            
            //hello.createDb();
            hello.queryRts();
            
            /*
             * Here's an interesting query:
             * match (n:data)-[:dr]-(n2:template)-[:iuip]-(n3:instance)-[:p]-(n4:template)-[:p]-(n5:instance)-[:iuip]-(n6:template)-[:uui]-(n7:universal) where n2.type = 'ptodr' and n.dr = 'William Hogan' and n4.type = 'ptop' and n6.type = 'ptou' and n7.uui = 'http://purl.obolibrary.org/obo/NCBITaxon_9606' return n, n2, n3, n4,n5,n6,n7;
             * 
             * Need to check that name is instance of personal name (n3 is iuip to ptou template with uui of URI for personal name from PNO)
             *   plus need to check that relation of n2 is "is concretized by"
             *   plus relation of n4 is URI for 'denotes'
             *   
             *   match (n:data)-[:dr]-(n2:template)-[:iuip]-(n3:instance)-[:p]-(n4:template)-[:p]-(n5:instance)-[:iuip]-(n6:template)-[:uui]-(n7:universal), n3-[:iuip]->(n8:template)-[:uui]->(n9:universal), n4-[:r]->(n10:relation) where n2.type = 'ptodr' and n.dr = 'William Hogan' and n4.type = 'ptop' and n6.type = 'ptou' and n7.uui = 'http://purl.obolibrary.org/obo/NCBITaxon_9606' and n8.type = 'ptou' and n9.uui = 'http://purl.obolibrary.org/obo/IAO_0020015' and n10.rui = 'http://purl.obolibrary.org/obo/IAO_0000219' return n,n2,n3,n4,n5,n6,n7,n8,n9,n10;
             */
            
            /*
             * after changing template types to labels, the query simplifies to:
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
             * For all the templates we are inserting, we know all the parameters
             *   except possibly iuid and change reason (C)
             *   
             *   iui of metadata template: assign a new one
             *   iuit: iui of template this metadata template references
             *   td: generate timestamp right before end of transaction
             *   CT: I (inserting)
             *   C: could be any value (change in relevance or reality or belief, or
             *   		we recognized an error in an existing template and are
             *   		inserting this one with the correct information.)
             *   E: Null
             *   S: empty (if the inserted template is replacing an existing template,
             *      then the metadata template that invalidates the existing template
             *      must have an S parameter that points to the template we're inserting
             *      here). 
             */
           
            
            MetadataTemplate dCorrection = new MetadataTemplate();
            dCorrection.setTemplateIui(Iui.createRandomIui());
            dCorrection.setReferentIui(ptouBad.getTemplateIui());
            dCorrection.setAuthoringTimestamp(new Iso8601DateTime());
            dCorrection.setAuthorIui(wh);
            dCorrection.setChangeReason(RtsChangeReason.XR);
            dCorrection.setErrorCode(RtsErrorCode.U1);
            dCorrection.setChangeType(RtsChangeType.X);
            //we could also point it at ptou if we wanted
            rpm.addTemplate(dCorrection);
            rpm.commitTemplates();
            
            //System.out.println(d1.toString());
            System.out.println(ptouBad.toString());
            System.out.println(dCorrection.toString());
            
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(Long.MAX_VALUE);
            System.out.println(cal);
            System.out.println("Long.MAX_VALUE=" + Long.MAX_VALUE);
            
            hello.shutDown();
	    }

		private static TeTemplate createIndividualWithBirthdateAndReturnLifeIntervalTemplate(
				String tb_name,	String personsName, Iui wh, Iui authorIui, 
				RtsTemplatePersistenceManager rpm, TeTemplate t, TenTemplate ten,
				String td_name) {
			
			Iui wh_chair = Iui.createRandomIui();
            Iui wh_name = Iui.createRandomIui();
            
            Set<RtsTemplate> tset = new HashSet<RtsTemplate>();
            
            /*
             * W. Hogan
             */
            ATemplate a1 = new ATemplate();
            a1.setAuthorIui(authorIui);
            a1.setReferentIui(wh);
            a1.setAuthoringTimestamp(new Iso8601DateTime());
            a1.setTemplateIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's chair
             */
            ATemplate a2 = new ATemplate();
            a2.setAuthorIui(authorIui);
            a2.setReferentIui(wh_chair);
            a2.setAuthoringTimestamp(new Iso8601DateTime());
            a2.setTemplateIui(Iui.createRandomIui());
            
            /*
             * W. Hogan's full name
             */
            ATemplate a3 = new ATemplate();
            a3.setAuthorIui(authorIui);
            a3.setReferentIui(wh_name);
            a3.setAuthoringTimestamp(new Iso8601DateTime());
            a3.setTemplateIui(Iui.createRandomIui());
            
            
            /*
             * Time during which W. Hogan has been owner of his chair
             */
            TeTemplate t2 = new TeTemplate();
            t2.setAuthoringTimestamp(new Iso8601DateTime());
            t2.setReferentIui(Iui.createRandomIui());
            t2.setAuthorIui(authorIui);
            t2.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInterval"));
            t2.setTemplateIui(Iui.createRandomIui());
            
            /*
             * Time during which W. Hogan has been instance of human being
             */
            TeTemplate t3 = new TeTemplate();
            t3.setAuthoringTimestamp(new Iso8601DateTime());
            t3.setReferentIui(Iui.createRandomIui());
            t3.setAuthorIui(authorIui);
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
            t4.setAuthorIui(authorIui);
            
            /*
             * Name of day of W. Hogan's birth
             */
            TenTemplate ten2 = new TenTemplate();
            ten2.setTemplateIui(Iui.createRandomIui());
            ten2.setTemporalEntityIui(t4.getReferentIui());
            ten2.setAuthoringTimeIui(t.getReferentIui());
            ten2.setAuthorIui(authorIui);
            ten2.setName(tb_name);
            ten2.setNamingSystemIui(gregorianIui);
            ten2.setReferentIui(Iui.createRandomIui());
            
            /*
             * If we have a date of death, then create it and add it
             */
            TeTemplate t7 = null;
            TenTemplate ten3 = null;
            if (td_name != null) {
                /*
                 * Day of W. Hogan's death
                 */
            	t7 = new TeTemplate();
            	t7.setAuthoringTimestamp(new Iso8601DateTime());
            	t7.setReferentIui(Iui.createRandomIui());
            	t7.setTemplateIui(Iui.createRandomIui());
            	t7.setUniversalUui(new Uui("http://www.ifomis.org/bfo/1.1/span#TemporalInterval"));
            	t7.setAuthorIui(authorIui);
                
                /*
                 * Name of day of W. Hogan's death
                 */
                ten3 = new TenTemplate();
                ten3.setTemplateIui(Iui.createRandomIui());
                ten3.setTemporalEntityIui(t4.getReferentIui());
                ten3.setAuthoringTimeIui(t.getReferentIui());
                ten3.setAuthorIui(authorIui);
                ten3.setName(tb_name);
                ten3.setNamingSystemIui(gregorianIui);
                ten3.setReferentIui(Iui.createRandomIui());
                
                /*
                 * PtoP for day of W. Hogan's death to time during which W. Hogan has been a human being
                 */
                PtoPTemplate ptop4 = new PtoPTemplate();
                ptop4.setTemplateIui(Iui.createRandomIui());
                ptop4.setAuthorIui(authorIui);
                ptop4.setAuthoringTimeIui(t.getReferentIui());
                ptop4.setReferentIui(t3.getReferentIui());
                ptop4.addParticular(t7.getReferentIui());
                try {
                	ptop4.setRelationshipURI(new URI("http://ctsi.ufl.edu/rts/overlaps"));
    			} catch (URISyntaxException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
                ptop4.setTemporalEntityIui(maxTimeIntervalIui);
                
                tset.add(t7);
                tset.add(ten3);
                tset.add(ptop4);
                
                rpm.addTemplate(t7);
                rpm.addTemplate(ten3);
                rpm.addTemplate(ptop4);
            }
            
            /*
             * W. Hogan's chair owned by W. Hogan
             */
            PtoPTemplate ptop = new PtoPTemplate();
            ptop.setReferentIui(wh_chair);
            ptop.setAuthorIui(authorIui);
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
          
            /*
             * W. Hogan instance of human being
             */
            PtoUTemplate ptou = new PtoUTemplate();
            ptou.setTemplateIui(Iui.createRandomIui());
            ptou.setReferentIui(wh);
            ptou.setRelationshipURI(instance_of);
            ptou.setAuthoringTimeIui(t.getReferentIui());
            ptou.setAuthorIui(authorIui);
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
            ptou3.setAuthorIui(authorIui);
            ptou3.setTemporalEntityIui(t3.getReferentIui());
            ptou3.setUniversalUui(new Uui("http://purl.obolibrary.org/obo/IAO_0020015"));
            
            /*
             * W. Hogan's name designates W. Hogan
             */
            PtoPTemplate ptop2 = new PtoPTemplate();
            ptop2.setReferentIui(wh_name);
            ptop2.setAuthorIui(authorIui);
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
            ptodr.setAuthorIui(authorIui);
            ptodr.setAuthoringTimeIui(t.getReferentIui());
            ptodr.setReferentIui(wh_name);
            ptodr.setData(personsName.getBytes());
            try {
				ptodr.setRelationshipURI(new URI("http://purl.obolibrary.org/obo/BFO_0000058"));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ptodr.setDatatypeUui(new Uui("http://ctsi.ufl.edu/rts/UTF-8"));
            
            /*
             * PtoP for day of W. Hogan's birth to time during which W. Hogan has been a human being
             */
            PtoPTemplate ptop3 = new PtoPTemplate();
            ptop3.setTemplateIui(Iui.createRandomIui());
            ptop3.setAuthorIui(authorIui);
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
            
            tset.add(a1);
            tset.add(a2);
            tset.add(a3);
            tset.add(t2);
            tset.add(t3);
            tset.add(t4);
            tset.add(ptop);
            tset.add(ptop2);
            tset.add(ptop3);
            tset.add(ptou);
            tset.add(ptou3);
            tset.add(ten2);
            tset.add(ptodr);
                     
            rpm.addTemplate(a1);
            rpm.addTemplate(a2);
            rpm.addTemplate(a3);
            rpm.addTemplate(t2);
            rpm.addTemplate(t3);
            rpm.addTemplate(t4);
            rpm.addTemplate(ptop);
            rpm.addTemplate(ptop2);
            rpm.addTemplate(ptop3);
            rpm.addTemplate(ptou);
            rpm.addTemplate(ptou3);
            rpm.addTemplate(ten2);
            rpm.addTemplate(ptodr);
                        
            Iterator<RtsTemplate> it = tset.iterator();
            while (it.hasNext()) {
            	RtsTemplate tnext = it.next();
            	MetadataTemplate d = new MetadataTemplate();
                d.setTemplateIui(Iui.createRandomIui());
                d.setReferentIui(tnext.getTemplateIui());
                d.setAuthorIui(authorIui);
                //d.setAuthoringTimestamp(new Iso8601DateTime());
                d.setChangeReason(RtsChangeReason.CR);
                d.setChangeType(RtsChangeType.I);
                d.setErrorCode(RtsErrorCode.Null);
                rpm.addTemplate(d);
            }
			return t3;
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
            			
            			Iterator<Label> labels = n.getLabels().iterator();
            			String label =labels.next().toString();
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
            				String type = labels.next().toString();
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
            				String direction = (n.getId() == r.getStartNode().getId()) ? "outgoing" : "incoming";
            				Node en = r.getEndNode();
            				Node sn = r.getStartNode();
            				RelationshipType rt = r.getType();
            				System.out.println("\t\t" + r.getId() + "\t" + sn.getId() + "\t" + rt.name() + "\t" + en.getId() + "\t" + direction);// + "\t" + en.getProperty("type"));
            			}//*/
            			
            		}
            	}
            	
            	tx.success();
            
            }
	    }
	
}
