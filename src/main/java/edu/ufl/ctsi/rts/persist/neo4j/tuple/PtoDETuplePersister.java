package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import java.nio.charset.Charset;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

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
	public void handleTupleSpecificParameters(Transaction tx) {
		super.handleTupleSpecificParameters(tx);
		/*
		 * At this point, we've already handled iuit, iuip, iuia, ta, tr, and r.
		 */
		
		//connect to universal for data
		connectToUniversalNode(tx);
		
		//connect to data
		connectToDataNode(tx);
	}

	private void connectToUniversalNode(Transaction tx) {
		PtoDETuple ptodr = (PtoDETuple)tupleToPersist;
		Node target = unc.persistEntity(ptodr.getDatatypeUui().toString(), tx);
		n.createRelationshipTo(target, RtsRelationshipType.uui);
		
		if (ontologyForUui == null) System.out.println(new String(ptodr.getData()));
		Node ontology = inc.persistEntity(ontologyForUui.toString(), tx);
		Iterable<Relationship> ontologyRels = target.getRelationships(RtsRelationshipType.iuio);
		boolean hasOntologyTarget = false;
		for (Relationship rel : ontologyRels) {
			Node relTarget = rel.getEndNode();
			hasOntologyTarget = relTarget.equals(ontology);
		}
		if (!hasOntologyTarget)
			target.createRelationshipTo(ontology, RtsRelationshipType.iuio);	
	}

	private void connectToDataNode(Transaction tx) {
		PtoDETuple ptodr = (PtoDETuple)tupleToPersist;
		String dataAsString = new String(ptodr.getData(), Charset.forName("UTF-8"));
		Node target = dnc.persistEntity(dataAsString, tx);
		n.createRelationshipTo(target, RtsRelationshipType.dr);
	}
	
	@Override
	protected void connectToReferent(Transaction tx) {
		ParticularReference p = ((PtoDETuple)tupleToPersist).getReferent();
		Node referentNode = null; 
		if (p instanceof Iui) {
			InstanceNodeCreator inc = new InstanceNodeCreator(graphDb);
			referentNode = inc.persistEntity(
				((PtoDETuple)tupleToPersist).getReferent().toString(), tx);
		} else if (p instanceof TemporalReference) {
			//TemporalRegionPersister trp = new TemporalRegionPersister(this.graphDb);
			//referentNode = trp.persistTemporalRegion((TemporalRegion)p);
			
			TemporalNodeCreator tnc = new TemporalNodeCreator(this.graphDb);
			referentNode = tnc.persistEntity(((TemporalReference)p).toString(), tx);
		}
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
