package edu.ufl.ctsi.rts.persist.neo4j.template;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.template.PtoDRTemplate;
import edu.uams.dbmi.rts.template.PtoLackUTemplate;
import edu.uams.dbmi.rts.template.PtoPTemplate;
import edu.uams.dbmi.rts.template.PtoUTemplate;
import edu.ufl.ctsi.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.RelationNodeCreator;

public abstract class AssertionalTemplatePersister extends
		RepresentationalTemplatePersister {

	RelationNodeCreator rnc;
	
	String taIui;
	String trIui;
	String rui;
	
	public AssertionalTemplatePersister(GraphDatabaseService db,
			ExecutionEngine ee) {
		super(db, ee);
		rnc = new RelationNodeCreator(this.ee);
		taIui = trIui = rui = null;
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
			taIui = ptou.getAuthoringTimeIui().toString();
			trIui = ptou.getTemporalEntityIui().toString();
			rui = ptou.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoPTemplate) {
			PtoPTemplate ptop = (PtoPTemplate)templateToPersist;
			taIui = ptop.getAuthoringTimeIui().toString();
			trIui = ptop.getTemporalEntityIui().toString();
			rui = ptop.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoLackUTemplate) {
			PtoLackUTemplate ptolacku = (PtoLackUTemplate)templateToPersist;
			taIui = ptolacku.getAuthoringTimeIui().toString();
			trIui = ptolacku.getTemporalEntityIui().toString();
			rui = ptolacku.getRelationshipURI().toString();
		} else if (templateToPersist instanceof PtoDRTemplate) {
			PtoDRTemplate ptodr = (PtoDRTemplate)templateToPersist;
			taIui = ptodr.getAuthoringTimeIui().toString();
			rui = ptodr.getRelationshipURI().toString();
		} //add PtoCo once it exists in RTDB
		
	}

	private void connectToTimeOfAssertion() {
		Node target = inc.persistEntity(taIui);
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}

	private void connectToTimeInReality() {
		/* 
		 * If it is null, it is because we are persisting a PtoDETemplate, which
		 *   doesn't have a tr parameter.		
		 */
		if (trIui != null) {
			Node target = inc.persistEntity(trIui);
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
