package edu.ufl.ctsi.rts.persist.neo4j.tuple;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.rts.tuple.PtoPTuple;
import edu.uams.dbmi.rts.tuple.PtoUTuple;
import edu.uams.dbmi.rts.tuple.component.RelationshipPolarity;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalNodeCreator;

public class PtoPTuplePersister extends AssertionalTuplePersister {
	
	//TemporalRegionPersister trp;
	TemporalNodeCreator tnc;
	
	public PtoPTuplePersister(GraphDatabaseService db) {
		super(db);
		//trp = new TemporalRegionPersister(this.graphDb);
		tnc = new TemporalNodeCreator(this.graphDb);
	}

	@Override
	protected void setTupleTypeProperty() {
		PtoPTuple ptop = (PtoPTuple)tupleToPersist;
		if (ptop.getRelationshipPolarity().equals(RelationshipPolarity.AFFIRMATIVE))
			n.addLabel(RtsTupleNodeLabel.ptop);
		else 
			n.addLabel(RtsTupleNodeLabel.ptop_negated);
	}
	
	@Override
	public void handleTupleSpecificParameters() {
		// superclass handles iuit, iuip, iuia, ta, tr, and r
		super.handleTupleSpecificParameters();
		
		/*
		 * Connect node to each particular node in p parameter
		 */
		connectToParticulars();
	}

	private void connectToParticulars() {
		int order = 1;
		PtoPTuple ptop = (PtoPTuple)tupleToPersist;
		Iterable<ParticularReference> p = ptop.getAllParticulars();
		for (ParticularReference i : p) {
			Node target = null;
			if (i instanceof Iui) {
				target = inc.persistEntity(i.toString());
			} else if (i instanceof TemporalReference) {
				//target = trp.persistTemporalRegion((TemporalRegion)i);
				target = tnc.persistEntity(((TemporalReference)i).toString());
			}
			Relationship r = n.createRelationshipTo(target, RtsRelationshipType.p);
			r.setProperty("relation order", Integer.toString(order));
			order++;
		}
	}

	@Override
	protected void connectToReferent() {
		/* gets connected to referent above in connectToParticulars, so this is a no op */
		
	}

}
