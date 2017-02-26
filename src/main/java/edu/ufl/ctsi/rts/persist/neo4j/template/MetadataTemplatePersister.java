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
import edu.uams.dbmi.util.iso8601.Iso8601DateTimeFormatter;
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

		connectToTemplate();
		
		connectToActor();

		//td
		Iso8601DateTimeFormatter dtf = new Iso8601DateTimeFormatter();
		String td = dtf.format(d.getAuthoringTimestamp());
		n.setProperty("td", td);
		
		//change type
		n.setProperty("ct", d.getChangeType().toString());
		
		//change reason
		n.setProperty("c", d.getChangeReason().toString());
		
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
		 * The target node is the template node, which for metadata templates
		 *   is the referent.
		 */
		Node target = tnc.persistEntity(d.getReferent().toString());
		
		/*
		 * This is the td parameter.  This time either starts or ends 
		 *   an interval during which the referent template is valid.
		 */
		long validMillis = d.getAuthoringTimestamp().getCalendar().getTimeInMillis();
		
		/*
		 * Get the change type
		 */
		RtsChangeType change = d.getChangeType();
		
		/*
		 * Get all previous about (aka iuit) relationships.
		 */
		Iterator<Relationship> i = target.getRelationships(RtsRelationshipType.about).iterator();
		
		
		/*
		 * If there are no previous about relationships, then the change type 
		 *   must be I(nsert).
		 */
		if (!i.hasNext()) {
			if (change.equals(RtsChangeType.I)) {
				Relationship r = n.createRelationshipTo(target, RtsRelationshipType.about);
				r.setProperty("valid_to", Long.MAX_VALUE);
				r.setProperty("valid_from", validMillis);
				r.setProperty("seq", 1);
			} else {
				throw new IllegalArgumentException("bad metadata template! no previous metadata, so change type should be insert but instead is " + change.toString());
			}
		} else {
			/*
			 * compute the next sequence number to add for the new about (iuit) 
			 *   relationship and what was the last action
			 */
			int seqMax = -1;
			Relationship lastAbout = null;
			while (i.hasNext()) {
				Relationship ir = i.next();
				int irSeq = (int) ir.getProperty("seq");
				if (irSeq > seqMax) {
					seqMax = irSeq;
					lastAbout = ir;
				}
			}
			Node irStart = lastAbout.getStartNode();
			String lastChange = (String) irStart.getProperty("ct");
			
			/*
			 * If the current action is invalidate and last action was insert or 
			 *   revalidate, then invalidate, else if the current action is 
			 *   revalidate and last action was invalidate, then add relationship
			 *   and update parameters accordingly
			 */
			if (change.equals(RtsChangeType.X) && !lastChange.equals(RtsChangeType.X.toString())) {
				lastAbout.setProperty("valid_to", validMillis);
				Relationship r = n.createRelationshipTo(target, RtsRelationshipType.about);
				r.setProperty("seq", ++seqMax);
			} else if (change.equals(RtsChangeType.R) && lastChange.equals(RtsChangeType.X.toString())) {
				Relationship r = n.createRelationshipTo(target, RtsRelationshipType.about);
				r.setProperty("valid_to", Long.MAX_VALUE);
				r.setProperty("valid_from", validMillis);
				r.setProperty("seq", ++seqMax);
			} else {
				throw new IllegalArgumentException("bad metadata template!  last change was " + lastChange + " and current change is " + change.toString());
			}
			
		}
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
