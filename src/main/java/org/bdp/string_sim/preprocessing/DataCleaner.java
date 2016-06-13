package org.bdp.string_sim.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DataCleaner implements MapFunction<Tuple2<Integer,String>,Tuple2<Integer,String>>{
	
	 /**
     * This Function maps a Tuple2 to a Tuple2 while performing various cleaning operations on the second element (property value of type label).
     * Cleaning operations are: 
     * -transform to lower Case
     * -eliminate whitespaces
     * -eliminate all brackets with its content
     * 
     * @param conceptAttrTuple the input tuple from the concept_attribute dataset of type Tuple2<Integer, String>
     * @return Tuple2<> of type Integer and String => the id and the cleaned property value
     * @throws Exception
     */
	@Override
	public Tuple2<Integer,String> map(Tuple2<Integer,String> conceptAttrTuple) throws Exception {
		String propertyValue = ((String) conceptAttrTuple.getField(1))
								//transform to lower Case
								.toLowerCase()
								//remove whitespaces
								.replaceAll("\\s+","")
								//remove all brackets with content
								.replaceAll("\\(.*\\)", "")
								//remove dot, comma, slash, hyphen
								.replaceAll("[.,/-]","");
		
		//additional possible for more precision: remove all content after Comma
		
		return new Tuple2<Integer,String>(conceptAttrTuple.getField(0),propertyValue);
	}
}