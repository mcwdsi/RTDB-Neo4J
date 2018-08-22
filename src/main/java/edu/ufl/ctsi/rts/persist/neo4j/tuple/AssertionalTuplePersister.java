package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.rts.tuple.PtoCTuple;
import edu.uams.dbmi.rts.tuple.PtoDETuple;
import edu.uams.dbmi.rts.tuple.PtoLackUTuple;
import edu.uams.dbmi.rts.tuple.PtoPTuple;
import edu.uams.dbmi.rts.tuple.PtoUTuple;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.persist.neo4j.entity.RelationNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalNodeCreator;

public abstract class AssertionalTuplePersister extends
		RepresentationalTuplePersister {

	RelationNodeCreator rnc;
	
	TemporalReference taRef;
	TemporalReference trRef;

	TemporalNodeCreator tnc;
	
	String rui;
	
	public AssertionalTuplePersister(GraphDatabaseService db) {
		super(db);
		rnc = new RelationNodeCreator(this.graphDb);
		rui = null;
		tnc = new TemporalNodeCreator(this.graphDb);
	}
	
	@Override
	public void handleTupleSpecificParameters() {
		getAssertionalParametersFromTuple();
		connectToTimeOfAssertion();
		connectToTimeInReality();
		connectToRelation();
	}

	private void getAssertionalParametersFromTuple() {
		if (tupleToPersist instanceof PtoUTuple) {
			PtoUTuple ptou = (PtoUTuple)tupleToPersist;
			taRef = ptou.getAuthoringTimeReference();
			trRef = ptou.getTemporalReference();
			//taIui = ptou.getAuthoringTimeIui().toString();
			//trIui = ptou.getTemporalEntityIui().toString();
			rui = ptou.getRelationshipURI().toString();
		} else if (tupleToPersist instanceof PtoPTuple) {
			PtoPTuple ptop = (PtoPTuple)tupleToPersist;
			taRef = ptop.getAuthoringTimeReference();
			trRef = ptop.getTemporalReference();
			//taIui = ptop.getAuthoringTimeIui().toString();
			//trIui = ptop.getTemporalEntityIui().toString();
			rui = ptop.getRelationshipURI().toString();
		} else if (tupleToPersist instanceof PtoLackUTuple) {
			PtoLackUTuple ptolacku = (PtoLackUTuple)tupleToPersist;
			taRef = ptolacku.getAuthoringTimeReference();
			trRef = ptolacku.getTemporalReference();
			//taIui = ptolacku.getAuthoringTimeIui().toString();
			//trIui = ptolacku.getTemporalEntityIui().toString();
			rui = ptolacku.getRelationshipURI().toString();
		} else if (tupleToPersist instanceof PtoDETuple) {
			PtoDETuple ptodr = (PtoDETuple)tupleToPersist;
			//taIui = ptodr.getAuthoringTimeIui().toString();
			taRef = ptodr.getAuthoringTimeReference();
			rui = ptodr.getRelationshipURI().toString();
		} else if (tupleToPersist instanceof PtoCTuple) {
			PtoCTuple ptoc = (PtoCTuple)tupleToPersist;
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
		//trp.persistTemporalRegion(taRef);
		
		Node target = tnc.persistEntity(taRef.toString());
		//Then, create relationship from this template to that node
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}

	private void connectToTimeInReality() {
		/* 
		 * If it is null, it is because we are persisting a PtoDETuple, which
		 *   doesn't have a tr parameter.		
		 */
		if (trRef != null) {
			/*
			 * First, persist (or get already persisted) temporal reference node
			 */
			//trp.persistTemporalRegion(trRef);
			Node target = tnc.persistEntity(trRef.toString());  // trp.getNode();
			//Then, create relationship from this tuple to that node
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
