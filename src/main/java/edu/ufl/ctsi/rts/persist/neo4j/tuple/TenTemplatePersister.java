package edu.ufl.ctsi.rts.persist.neo4j.tuple;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import edu.uams.dbmi.rts.tuple.TenTemplate;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTupleNodeLabel;

@Deprecated
public class TenTemplatePersister extends RepresentationalTuplePersister {
	
	String taRef;
	String nsIui;
	String name;
	String teRef;
	
	public TenTemplatePersister(GraphDatabaseService db) {
		super(db);
	}

	@Override
	public void handleTupleSpecificParameters(Transaction tx) {
		// TODO Auto-generated method stub
		/*
		 * iuit, iuip, iuia already handled, which leaves iuite, iuins, name,
		 *   and ta  
		 */
		getParametersFromTemplate();
		
		connectToTimeOfAssertion(tx);
		
		connectToTemporalEntity(tx);
		
		connectToNamespace(tx);
		
		addNameAsProperty();
	}

	private void getParametersFromTemplate() {
		TenTemplate ten = (TenTemplate)tupleToPersist;
		//taIui = ten.getAuthoringTimeIui().toString();
		taRef = ten.getAuthoringTimeReference().toString();
		nsIui = ten.getNamingSystemIui().toString();
		//teIui = ten.getTemporalEntityIui().toString();
		teRef = ten.getTemporalEntityReference().toString();
		name = ten.getName();
	}

	private void connectToTimeOfAssertion(Transaction tx) {
		Node target = inc.persistEntity(taRef, tx);
		n.createRelationshipTo(target, RtsRelationshipType.ta);
	}
	
	private void connectToTemporalEntity(Transaction tx) {
		Node target = inc.persistEntity(teRef, tx);
		n.createRelationshipTo(target, RtsRelationshipType.iuite);
	}
	
	private void connectToNamespace(Transaction tx) {
		Node target = inc.persistEntity(nsIui, tx);
		n.createRelationshipTo(target, RtsRelationshipType.iuins);
	}

	private void addNameAsProperty() {
		n.setProperty("name", name);
	}

	@Override
	protected void setTupleTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ten");
		n.addLabel(RtsTupleNodeLabel.TEN);
	}

	@Override
	protected void connectToReferent(Transaction tx) {
		// TODO Auto-generated method stub
		throw new IllegalStateException("TenTemplatePersister is deprecated.");
	}

}
