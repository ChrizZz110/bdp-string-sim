package org.bdp.string_sim.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DataCleaner implements MapFunction<Tuple2<Integer,String>,Tuple2<Integer,String>>{
	
	private boolean cleanMode = false;
	
	public DataCleaner() {
	}
	
	/**
	 * @param cleanMode Mode how to clean the given string in mapFunction, 
	 * false - leave brackets and content after comma.
	 * true - clean brackets&content and comma&following content. 
	 */
	public DataCleaner(boolean cleanMode) {
		this.cleanMode = cleanMode;
	}
	
	
	 /**
     * This Function maps a Tuple2 to a Tuple2 while performing various cleaning operations on the second element (property value of type label).
     * Cleaning operations are: 
     * - transform to lower Case
     * - eliminate whitespaces
     * - eliminate all brackets with its content
     * - eliminate special characters
     * @param conceptAttrTuple the input tuple from the concept_attribute dataset of type Tuple2<Integer, String>
     * @return Tuple2<> of type Integer and String => the id and cleaned property value
     * @throws Exception
     */
	@Override
	public Tuple2<Integer,String> map(Tuple2<Integer,String> conceptAttrTuple) throws Exception {
		String propertyValue = conceptAttrTuple.getField(1);
		
		if (cleanMode) {
			//remove all brackets with content
			propertyValue = propertyValue.replaceAll("\\(.*\\)", "");
			
			//remove comma with following content
			int commaIndex = propertyValue.indexOf(",");
			if (commaIndex != -1) {
				propertyValue =  propertyValue.substring(0, commaIndex);
			}
		}
		
		propertyValue = propertyValue
						//transform to lower Case
						.toLowerCase()
						//remove whitespaces
						.replaceAll("\\s+","")
						//remove "Rough breathing", "Smooth breathing", dot, comma, slash, hyphen
						.replaceAll("[‘’.,/-]","");
		
		return new Tuple2<Integer,String>(conceptAttrTuple.getField(0),propertyValue);
	}
}