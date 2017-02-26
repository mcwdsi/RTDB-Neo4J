package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.PtoCTemplate;
import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.uams.dbmi.rts.template.PtoLackUTemplate;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.RelationNodeCreator;

public abstract class AssertionalTemplatePersister extends
		RepresentationalTemplatePersister {

	RelationNodeCreator rnc;
	
	TemporalReference taRef;
	TemporalReference trRef;
	
	TemporalReferencePersister trp;
	
	String rui;
	
	public AssertionalTemplatePersister(GraphDatabaseService db,
			ExecutionEngine ee) {
		super(db, ee);
		rnc = new RelationNodeCreator(this.ee);
		rui = null;
		trp = new TemporalReferencePersister(this.graphDb, this.ee);
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		getAssertionalParametersFromTemplate();
		connectToTimeOfAssertion();
		connectToTimeInReality();
		connectToRelation();
	}

	private void getAssertionalParametersFromTemplate() {
		if (templateToPersist instanceof PtoUTemplate) {
			PtoUTemplate ptou = (PtoUTemplate)templateToPersist;
			taRef = ptou.getAuthoringTimeReference();
			trRef = ptou.getTemporalReference();
			//taIui = ptou.getAuthoringTimeIui().toString();
			//trIui = ptou.getTemporalEntityIui().toString();
			rui = ptou.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoPTemplate) {
			PtoPTemplate ptop = (PtoPTemplate)templateToPersist;
			taRef = ptop.getAuthoringTimeReference();
			trRef = ptop.getTemporalReference();
			//taIui = ptop.getAuthoringTimeIui().toString();
			//trIui = ptop.getTemporalEntityIui().toString();
			rui = ptop.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoLackUTemplate) {
			PtoLackUTemplate ptolacku = (PtoLackUTemplate)templateToPersist;
			taRef = ptolacku.getAuthoringTimeReference();
			trRef = ptolacku.getTemporalReference();
			//taIui = ptolacku.getAuthoringTimeIui().toString();
			//trIui = ptolacku.getTemporalEntityIui().toString();
			rui = ptolacku.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoDETemplate) {
			PtoDETemplate ptodr = (PtoDETemplate)templateToPersist;
			//taIui = ptodr.getAuthoringTimeIui().toString();
			taRef = ptodr.getAuthoringTimeReference();
			rui = ptodr.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoCTemplate) {
			PtoCTemplate ptoc = (PtoCTemplate)templateToPersist;
			taRef = ptoc.getAuthoringTimeReference();
			trRef = ptoc.getTemporalReference();
			//taIui = ptoc.getAuthoringTimeIui().toString();
			//trIui = ptoc.getTemporalEntityIui().toString();
		}
		
	}

	private void connectToTimeOfAssertion() {
		/*
		 * First, persist (or get already persisted) temporal reference node
		 */
		trp.persistTemporalReference(taRef);
		Node target = trp.getNode();
		//Then, create relationship from this template to that node
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}

	private void connectToTimeInReality() {
		/* 
		 * If it is null, it is because we are persisting a PtoDETemplate, which
		 *   doesn't have a tr parameter.		
		 */
		if (trRef != null) {
			/*
			 * First, persist (or get already persisted) temporal reference node
			 */
			trp.persistTemporalReference(trRef);
			Node target = trp.getNode();
			//Then, create relationship from this template to that node
			n.createRelationshipTo(target, RtsRelationshipType.tr);
		}
	}

	private void connectToRelation() {
		/*
		 * If it is null, it is because we are persisting a PtoCoTemplate, which
		 *   doesn't have an r parameter.
		 */
		if (rui != null) {
			Node target = rnc.persistEntity(rui);
			n.createRelationshipTo(target, RtsRelationshipType.r);
		}
	}

}
