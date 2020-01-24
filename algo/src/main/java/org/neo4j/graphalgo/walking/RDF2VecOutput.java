package org.neo4j.graphalgo.walking;

public class RDF2VecOutput{
	public String result = "";    	
	public RDF2VecOutput (String value) {
		this.result = value; 
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	} 
	public String toString() {
		return this.result; 
	}
}