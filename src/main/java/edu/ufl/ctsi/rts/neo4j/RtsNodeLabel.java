package edu.ufl.ctsi.rts.neo4j;

public enum RtsNodeLabel {
	INSTANCE("instance"),
	TYPE("universal"),
	RELATION("relation"),
	DATA("data"),
	TEMPLATE("template"), 
	CONCEPT("concept"),
	TEMPORAL_REGION("temporal_region");
	
	String labelText;
	
	RtsNodeLabel(String labelText) {
		this.labelText = labelText;
	}
	
	public String getLabelText() {
		return labelText;
	}
	
	@Override
	public String toString() {
		return labelText;
	}
}
