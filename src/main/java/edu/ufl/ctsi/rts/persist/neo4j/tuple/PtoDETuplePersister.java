package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import java.nio.charset.Charset;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.uams.dbmi.rts.tuple.PtoDETuple;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.DataNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.TemporalNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoDETuplePersister extends AssertionalTuplePersister {

	UniversalNodeCreator unc;
	DataNodeCreator dnc;
	
	public PtoDETuplePersister(GraphDatabaseService db) {
		super(db);
		unc = new UniversalNodeCreator(this.graphDb);
		dnc = new DataNodeCreator(this.graphDb);
	}

	@Override
	protected void setTupleTypeProperty() {
		n.addLabel(RtsTupleNodeLabel.E);
	}
	
	@Override
	public void handleTupleSpecificParameters() {
		super.handleTupleSpecificParameters();
		/*
		 * At this point, we've already handled iuit, iuip, iuia, ta, tr, and r.
		 */
		
		//connect to universal for data
		connectToUniversalNode();
		
		//connect to data
		connectToDataNode();
	}

	private void connectToUniversalNode() {
		PtoDETuple ptodr = (PtoDETuple)tupleToPersist;
		Node target = unc.persistEntity(ptodr.getDatatypeUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}

	private void connectToDataNode() {
		PtoDETuple ptodr = (PtoDETuple)tupleToPersist;
		String dataAsString = new String(ptodr.getData(), Charset.forName("UTF-8"));
		Node target = dnc.persistEntity(dataAsString);
		n.createRelationshipTo(target, RtsRelationshipType.dr);
	}
	
	@Override
	protected void connectToReferent() {
		ParticularReference p = ((PtoDETuple)tupleToPersist).getReferent();
		Node referentNode = null; 
		if (p instanceof Iui) {
			InstanceNodeCreator inc = new InstanceNodeCreator(graphDb);
			referentNode = inc.persistEntity(((PtoDETuple)tupleToPersist).getReferent().toString());
		} else if (p instanceof TemporalReference) {
			//TemporalRegionPersister trp = new TemporalRegionPersister(this.graphDb);
			//referentNode = trp.persistTemporalRegion((TemporalRegion)p);
			
			TemporalNodeCreator tnc = new TemporalNodeCreator(this.graphDb);
			referentNode = tnc.persistEntity(((TemporalReference)p).toString());
		}
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
