package edu.ufl.ctsi.rts.neo4j;

import org.neo4j.graphdb.Label;

public enum RtsTupleNodeLabel implements Label {
	A("A"),
	U("U"),
	U_("U-"),
	P("P"),
	P_("P-"),
	L("LU"),
	E("E"),
	C("C"),
	TE("TE"),
	TEN("TEN"),
	D("D");
	
	String nodeLabelText;
	
	RtsTupleNodeLabel(String nodeLabelText) {
		this.nodeLabelText = nodeLabelText;
	}
	
	@Override
	public String toString() {
		return this.nodeLabelText;
	}
}
