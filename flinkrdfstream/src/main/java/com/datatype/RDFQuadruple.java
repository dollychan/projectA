package com.datatype;


public class RDFQuadruple {
	
	public String subject;
	public String predicate;
	public String object;
	public long eventTime;
	
	public RDFQuadruple() {
		
	}
	
	public RDFQuadruple(String subject,String predicate,String object,long timestamp) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.eventTime = timestamp;  //获取当前的时间戳
	}
	
	public String getSubject() {
		return this.subject;
	}
	
	public String getPredicate() {
		return this.predicate;
	}
	
	public String getObject() {
		return this.object;
	}
	
	public long getEventTime() {
		return this.eventTime;
	}
	
	//用于从文件读取的每一行后封装成RDF
	public static RDFQuadruple fromString(String line,long EventTime) {
		
		String[] tokens = line.split("\t");
		if (tokens.length != 4) {
			throw new RuntimeException("Invalid record: " + line);
		}				
	    
		RDFQuadruple rdfQua = new RDFQuadruple(tokens[0],tokens[1],tokens[2],EventTime);
		return rdfQua;
	}
	
	public int hashCode() {
		return this.getSubject().hashCode();
	}
	
	public String toString() {
		return getSubject()+","+getPredicate()+","+getObject()+","+getEventTime();
	}
	
}
