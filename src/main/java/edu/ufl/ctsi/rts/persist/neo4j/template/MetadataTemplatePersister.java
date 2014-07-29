package edu.ufl.ctsi.rts.persist.neo4j.template;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.metadata.RtsChangeType;
import edu.uams.dbmi.rts.template.MetadataTemplate;
import edu.uams.dbmi.rts.template.RtsTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public class MetadataTemplatePersister extends RtsTemplatePersister {

	MetadataTemplate d;
	
	InstanceNodeCreator inc;
	
	public MetadataTemplatePersister(GraphDatabaseService db, ExecutionEngine ee) {
		super(db, ee);
		inc = new InstanceNodeCreator(ee);
	}

	@Override
	protected void completeTemplate(RtsTemplate t) {
		d = (MetadataTemplate)t;
		// TODO Auto-generated method stub
		connectToTemplate();
		
		connectToActor();
		
		//change
		n.setProperty("c", d.getChangeType().toString());
		
		//change reason
		n.setProperty("ct", d.getChangeReason().toString());
		
		//error code
		n.setProperty("e", d.getErrorCode().toString());
		
		//replacements (s parameter)
		Set<Iui> r = d.getReplacementTemplateIuis();
		if (r.size() > 0) connectToReplacements(r.iterator());
	}

	/* connect the metadata template to the template it is about
	 * 
	 */
	private void connectToTemplate() {
		/*
		 * The target node is the template node
		 */
		Node target = tnc.persistEntity(d.getReferentIui().toString());
		
		long validMillis = d.getAuthoringTimestamp().getCalendar().getTimeInMillis();
		
		/*  
		 * If we're inserting the template, then we just create the relationship
		 *   and we're done.
		 *   
		 * Otherwise, we need to update the valid_to property on the most 
		 *  recent about relationship.
		 *  
		 *  Hmmmm.  Right now, this violates the RTS paradigm of never changing 
		 *    any data.  Dang.  However, not setting the valid_to property when
		 *    it is indefinite complicates querying because of the null problem,
		 *    and really we could look at Long.MAX_VALUE as not being real data.
		 */
		if (!d.getChangeType().equals(RtsChangeType.I)) {
			Iterator<Relationship> i = target.getRelationships(RtsRelationshipType.about).iterator();
			while (i.hasNext()) {
				Relationship ir = i.next();
				if ((Long)ir.getProperty("valid_to") == Long.MAX_VALUE) {
					ir.setProperty("valid_to", validMillis);
					break;  //only one will have this problem.
				}
			}
		}
		
		Relationship r = n.createRelationshipTo(target, RtsRelationshipType.about);
		r.setProperty("valid_to", Long.MAX_VALUE);
		r.setProperty("valid_from", validMillis);
		
		
		
		/*
		 * As an engineering solution for query purposes, checking all the metadata
		 *   templates connected to a template to figure out if the template is
		 *   valid seems like lots of overhead.  Therefore, we include a flag
		 *   on the template node.
		 *   
		 *   But it isn't a general solution.  It's fine for querying current state
		 *     of RTS, but querying RTS as it was at some arbitrary time t is not
		 *     supported.
		 
		RtsChangeType ct = d.getChangeType();
		if (ct.equals(RtsChangeType.X)) {
			target.setProperty("valid", false);
		} else {
			target.setProperty("valid", true);
		}
		*/
	}

	/*
	 * Could use a better name, but basically connect metadata template to the
	 * 	entity that acted on it.  Actions = insert, invalidate, revalidate.
	 */
	private void connectToActor() {
		Node target = inc.persistEntity(d.getAuthorIui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.iuid);		
	}
	
	private void connectToReplacements(Iterator<Iui> i) {
		while (i.hasNext()) {
			String iuiTxt = i.next().toString();
			Node target = tnc.persistEntity(iuiTxt);
			n.createRelationshipTo(target, RtsRelationshipType.s);
		}
	}

	@Override
	protected void setTemplateTypeProperty() {
		n.addLabel(RtsTemplateNodeLabel.d);
	}

}
