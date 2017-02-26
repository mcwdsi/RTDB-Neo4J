package edu.ufl.ctsi.rts.persist.neo4j.template;

import java.nio.charset.Charset;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import edu.uams.dbmi.rts.ParticularReference;
import edu.uams.dbmi.rts.iui.Iui;
import edu.uams.dbmi.rts.template.PtoDETemplate;
import edu.uams.dbmi.rts.time.TemporalReference;
import edu.ufl.ctsi.rts.neo4j.RtsRelationshipType;
import edu.ufl.ctsi.rts.neo4j.RtsTemplateNodeLabel;
import edu.ufl.ctsi.rts.persist.neo4j.entity.DataNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.InstanceNodeCreator;
import edu.ufl.ctsi.rts.persist.neo4j.entity.UniversalNodeCreator;

public class PtoDETemplatePersister extends AssertionalTemplatePersister {

	UniversalNodeCreator unc;
	DataNodeCreator dnc;
	
	public PtoDETemplatePersister(GraphDatabaseService db) {
		super(db);
		unc = new UniversalNodeCreator(this.graphDb);
		dnc = new DataNodeCreator(this.graphDb);
	}

	@Override
	protected void setTemplateTypeProperty() {
		//n.setProperty(TEMPLATE_TYPE_PROPERTY_NAME, "ptodr");
		n.addLabel(RtsTemplateNodeLabel.ptode);
	}
	
	@Override
	public void handleTemplateSpecificParameters() {
		super.handleTemplateSpecificParameters();
		/*
		 * At this point, we've already handled iuit, iuip, iuia, ta, tr, and r.
		 */
		
		//connect to universal for data
		connectToUniversalNode();
		
		//connect to data
		connectToDataNode();
	}

	private void connectToUniversalNode() {
		PtoDETemplate ptodr = (PtoDETemplate)templateToPersist;
		Node target = unc.persistEntity(ptodr.getDatatypeUui().toString());
		n.createRelationshipTo(target, RtsRelationshipType.uui);
	}

	private void connectToDataNode() {
		PtoDETemplate ptodr = (PtoDETemplate)templateToPersist;
		String dataAsString = new String(ptodr.getData(), Charset.forName("UTF-8"));
		Node target = dnc.persistEntity(dataAsString);
		n.createRelationshipTo(target, RtsRelationshipType.dr);
	}
	
	@Override
	protected void connectToReferent() {
		ParticularReference p = ((PtoDETemplate)templateToPersist).getReferent();
		Node referentNode = null; 
		if (p instanceof Iui) {
			InstanceNodeCreator inc = new InstanceNodeCreator(graphDb);
			referentNode = inc.persistEntity(((PtoDETemplate)templateToPersist).getReferent().toString());
		} else if (p instanceof TemporalReference) {
			TemporalReferencePersister trp = new TemporalReferencePersister(this.graphDb);
			referentNode = trp.persistTemporalReference((TemporalReference)p);
		}
		//This directionality is what I did on the Confluence page and it seems to make sense.
		referentNode.createRelationshipTo(n, RtsRelationshipType.iuip);
	}
}
