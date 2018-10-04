package com.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;

public class IdMapping {
	
	public static TreeMap<String,String> str_index = new TreeMap<String,String>();
	public static TreeMap<String,String> str_normal = new TreeMap<String,String>();
	
	public IdMapping() {
		
	}
	
	public static TreeMap<String,String> load_str_index() throws IOException{
		System.out.println("loading ID-mapping file: str_index");	
		
		BufferedReader br = new BufferedReader(new FileReader("G:\\id_lubm_50\\str_index"));
		
		String line = null ;
		
		while((line=br.readLine())!=null) {
			String[] tokens = line.split("\t");
			str_index.put(tokens[0], tokens[1]);
		}
		
		
		br.close();
		br = null;		
		return str_index;
		
	}
	
	
	public static TreeMap<String,String> load_str_normal() throws IOException{
		System.out.println("loading ID-mapping file: str_normal");	
		
		BufferedReader br = new BufferedReader(new FileReader("G:\\id_lubm_50\\str_normal"));
		
		String line = null ;
		
		while((line=br.readLine())!=null) {
			String[] tokens = line.split("\t");
			str_normal.put(tokens[0], tokens[1]);
		}
		
		br.close();
		br = null;	
		return str_normal;
		
	}

}
