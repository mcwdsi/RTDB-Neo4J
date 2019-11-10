package edu.ufl.ctsi.rts.neo4j;

import org.neo4j.graphdb.RelationshipType;

public enum RtsRelationshipType implements RelationshipType {
	/** 
	 * These relationship types are for A, Te, PtoP, PtoU, PtoLackU, PtoDR, and
	 * 		PtoCo tuples (basically all tuples except Metadata)
	 */
	iuip,		//HAS_REFERENT
	iuia,		//HAS_AUTHOR
	
	/** 
	 * These relationship types are for PtoP, PtoU, PtoLackU, PtoDR, and PtoCo
	 * 		tuples
	 */
	ta,			//HAS_TIME_OF_ASSERTION
	tr,			//HOLDS_TRUE_AT
	
	/** 
	 * This relationship type is for PtoP, PtoU, PtoLackU, and PtoDE tuples
	 */
	r,			//
	
	/**
	 * This relationship type is for PtoU, PtoLackU, and Te tuples
	 */
	uui,
	
	
	/**
	 * This relationship type is for PtoP tuples
	 */
	p,	
	
	/**
	 * This relationship type is for PtoDE tuples
	 */
	dr,
	
	/**
	 *  These relationship types are for Ten tuples 
	 */
	iuite,
	iuins,
	
	/**
	 * These relationship types are for PtoCo tuples
	 */
	co,
	cs,		//probably don't want this one, since we got rid of IUIo elsewhere.
	
	/** 
	 * These relationship types are for D tuples (metadata).
	 */
	iuid,		//HAS_AGENT
	about,
	s;
	
	/**
	 * Note that the tap and td parameters (A and D tuples, respectively), are
	 *   properties on nodes, and not relationship types.
	 */
}
