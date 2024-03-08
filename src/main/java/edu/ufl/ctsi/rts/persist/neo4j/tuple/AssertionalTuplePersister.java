package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import edu.uams.dbmi.rts.iui.Iui;
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

	TemporalReferencePersister trp;
	
	String rui;
	Iui ontologyForRui;
	
	String uui;
	Iui ontologyForUui;
	
	public AssertionalTuplePersister(GraphDatabaseService db) {
		super(db);
		rnc = new RelationNodeCreator(this.graphDb);
		rui = null;
		uui = null;
		trp = new TemporalReferencePersister(this.graphDb);
	}
	
	@Override
	public void handleTupleSpecificParameters(Transaction tx) {
		getAssertionalParametersFromTuple();
		connectToTimeOfAssertion(tx);
		connectToTimeInReality(tx);
		connectToRelation(tx);
	}

	private void getAssertionalParametersFromTuple() {
		if (tupleToPersist instanceof PtoUTuple) {
			PtoUTuple ptou = (PtoUTuple)tupleToPersist;
			taRef = ptou.getAuthoringTimeReference();
			trRef = ptou.getTemporalReference();
			//taIui = ptou.getAuthoringTimeIui().toString();
			//trIui = ptou.getTemporalEntityIui().toString();
			rui = ptou.getRelationshipURI().toString();
			ontologyForRui = ptou.getRelationshipOntologyIui();
			uui = ptou.getUniversalUui().toString();
			ontologyForUui = ptou.getUniversalOntologyIui();
		} else if (tupleToPersist instanceof PtoPTuple) {
			PtoPTuple ptop = (PtoPTuple)tupleToPersist;
			taRef = ptop.getAuthoringTimeReference();
			trRef = ptop.getTemporalReference();
			//taIui = ptop.getAuthoringTimeIui().toString();
			//trIui = ptop.getTemporalEntityIui().toString();
			rui = ptop.getRelationshipURI().toString();
			ontologyForRui = ptop.getRelationshipOntologyIui();
		} else if (tupleToPersist instanceof PtoLackUTuple) {
			PtoLackUTuple ptolacku = (PtoLackUTuple)tupleToPersist;
			taRef = ptolacku.getAuthoringTimeReference();
			trRef = ptolacku.getTemporalReference();
			//taIui = ptolacku.getAuthoringTimeIui().toString();
			//trIui = ptolacku.getTemporalEntityIui().toString();
			rui = ptolacku.getRelationshipURI().toString();
			ontologyForRui = ptolacku.getRelationshipOntologyIui();
		} else if (tupleToPersist instanceof PtoDETuple) {
			PtoDETuple ptodr = (PtoDETuple)tupleToPersist;
			//taIui = ptodr.getAuthoringTimeIui().toString();
			taRef = ptodr.getAuthoringTimeReference();
			rui = ptodr.getRelationshipURI().toString();
			ontologyForRui = ptodr.getRelationshipOntologyIui();
			ontologyForUui = ptodr.getDatatypeOntologyIui();
		} else if (tupleToPersist instanceof PtoCTuple) {
			PtoCTuple ptoc = (PtoCTuple)tupleToPersist;
			taRef = ptoc.getAuthoringTimeReference();
			trRef = ptoc.getTemporalReference();
			//taIui = ptoc.getAuthoringTimeIui().toString();
			//trIui = ptoc.getTemporalEntityIui().toString();
		}
		
	}

	private void connectToTimeOfAssertion(Transaction tx) {
		/*
		 * First, persist (or get already persisted) temporal reference node
		 */
		
		Node target = trp.persistTemporalReference(taRef, tx);
		//Then, create relationship from this tuple to that node
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}

	private void connectToTimeInReality(Transaction tx) {
		/* 
		 * If it is null, it is because we are persisting a PtoDETuple, which
		 *   doesn't have a tr parameter.		
		 */
		if (trRef != null) {
			/*
			 * First, persist (or get already persisted) temporal reference node
			 */
			//trp.persistTemporalRegion(trRef);
			Node target = trp.persistTemporalReference(trRef, tx);
			//Then, create relationship from this tuple to that node
			n.createRelationshipTo(target, RtsRelationshipType.tr);
		}
	}

	private void connectToRelation(Transaction tx) {
		/*
		 * If it is null, it is because we are persisting a PtoCoTuple, which
		 *   doesn't have an r parameter.
		 */
		if (rui != null) {
			Node target = rnc.persistEntity(rui, tx);
			n.createRelationshipTo(target, RtsRelationshipType.r);
			Node ontology = inc.persistEntity(ontologyForRui.toString(), tx);
			Iterable<Relationship> ontologyRels = target.getRelationships(RtsRelationshipType.iuio);
			boolean hasOntologyTarget = false;
			for (Relationship rel : ontologyRels) {
				Node relTarget = rel.getEndNode();
				hasOntologyTarget = relTarget.equals(ontology);
			}
			if (!hasOntologyTarget)
				target.createRelationshipTo(ontology, RtsRelationshipType.iuio);			
		}
	}

}
