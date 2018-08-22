package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.cui.Cui;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.tuple.PtoCTuple;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.ConceptNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;

public class PtoCTuplePersister extends AssertionalTuplePersister {
	
	ConceptNodeCreator cnc;
	
	Cui cui;
	Iui conceptSystemIui;
	
	public PtoCTuplePersister(GraphDatabaseService db) {
		super(db);
		cnc = new ConceptNodeCreator(this.graphDb);
	}
	
	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptoc");
		n.addLabel(RtsTupleNodeLabel.ptoc);
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		/* 
		 * superclass handles iuit, iuip, iuia, ta, tr
		 */
		super.handleTemplateSpecificParameters();
		
		getParametersFromTemplate();
		
		connectToConceptNode();
		
		connectToConceptSystemNode();
	}

	private void getParametersFromTemplate() {
		PtoCTuple ptoc = (PtoCTuple)templateToPersist;
		cui = ptoc.getConceptCui();
		conceptSystemIui = ptoc.getConceptSystemIui();
	}

	private void connectToConceptNode() {
		Node target = cnc.persistEntity(cui.toString());
		n.createRelationshipTo(target, RtsRelationshipType.co);
	}

	private void connectToConceptSystemNode() {
		Node target = inc.persistEntity(conceptSystemIui.toString());
		n.createRelationshipTo(target, RtsRelationshipType.cs);
	}
	
	@Override
	protected void connectToReferent() {
		InstanceNodeCreator inc = new InstanceNodeCreator(this.graphDb);
		Node referentNode = inc.persistEntity(((PtoCTuple)templateToPersist).getReferentIui().toString());
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
