package edu.ufl.ctsi.neo4j;

import org.neo4j.graphdb.RelationshipType;

public enum RtsRelationshipType implements RelationshipType {
	/** 
	 * These relationship types are for A, Te, PtoP, PtoU, PtoLackU, PtoDR, and
	 * 		PtoCo templates (basically all templates except Metadata)
	 */
	iuip,		//HAS_REFERENT
	iuia,		//HAS_AUTHOR
	
	/** 
	 * These relationship types are for PtoP, PtoU, PtoLackU, PtoDR, and PtoCo
	 * 		templates
	 */
	ta,			//HAS_TIME_OF_ASSERTION
	tr,			//HOLDS_TRUE_AT
	
	/** 
	 * This relationship type is for PtoP, PtoU, PtoLackU, and PtoDR templates
	 */
	r,			//
	
	/**
	 * This relationship type is for PtoU, PtoLackU, and Te templates
	 */
	uui,
	
	
	/**
	 * This relationship type is for PtoP templates
	 */
	p,	
	
	/**
	 * This relationship type is for PtoDR templates
	 */
	dr,
	
	/**
	 *  These relationship types are for Ten templates 
	 */
	iuite,
	ns,
	
	/**
	 * These relationship types are for PtoCo templates
	 */
	co,
	cs,		//probably don't want this one, since we got rid of IUIo elsewhere.
	
	/** 
	 * These relationship types are for D templates (metadata).
	 */
	iuid,		//HAS_AGENT
	about;
	
	/**
	 * Note that the tap and td parameters (A and D templates, respectively), are
	 *   properties on nodes, and not relationship types.
	 */
}
