package com.polarisft.tool.impl.trigger;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TreeSet;

import com.polarisft.tool.resources.StaticContainers;
import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.bean.BatchScriptBean;
import com.polarisft.tool.trigger.bean.TableInformationBean;
/**
 * @author arunkumar.roopchand
 * This class create the script for batch files.
 * KeySet hold the all key defined in BatchScriptResource.properties file.
 */
public class BatchFileGenerator extends AbstractScriptTemplate {
	String[] fileNameContainer;
	static LinkedList<BatchScriptBean> batchFileList;
	List<String> fileScript;
	String backSlash = "\\"  ;
	BatchScriptBean batchScripts;
	Set<String> keySet ;
	static Map<String,String> serevrMap;
	private StaticContainers staticContainers;
	/**
	 * compare the object of BatchScriptBean on property fileName.
	 * And sort the order on ascending. 
     */
	public static Comparator<BatchScriptBean> batchFileComparator = new Comparator<BatchScriptBean>() {
		public int compare(BatchScriptBean fruit1, BatchScriptBean fruit2) {
			String firstBatchFile = fruit1.getTableName().toUpperCase();
			String secondBatchFile = fruit2.getTableName().toUpperCase();
			return firstBatchFile.compareTo(secondBatchFile);
		}
	};
	public void generateScript(List<TableInformationBean> tableInformationBeanList,String outputDir) {
		String lineSeparator = System.getProperty("line.separator");
		StringBuffer batchFileContentsBuffer = null;
		ResourceBundle resourceBundle2 = ResourceBundle.getBundle("com.polarisft.tool.resources.BatchScriptResource", Locale.getDefault());
		/**
		 * KeySet hold the all key defined in BatchScriptResource.properties file.
		 * sortedKeys sort the key that is object of tree set.
		 * Iterate the sortedKeys and save into LinkedList object to maintain insertion order.
	     */
		 keySet = resourceBundle2.keySet();
		 @SuppressWarnings("unchecked")
		 Set<String> sortedKeys = new TreeSet(keySet);
		 Iterator<String> keysIterator = sortedKeys.iterator();
		 batchFileList = new LinkedList<BatchScriptBean>();
		 serevrMap = new HashMap<String,String>();
		 serevrMap = staticContainers.getMyMap();
		 String serverName = serevrMap.get("serverName");
		 String SQLServerUserName=serevrMap.get("SQLServerUserName");
		 String SQLServerPassword=serevrMap.get("SQLServerPassword");
		boolean isCompleted = true;
		while(keysIterator.hasNext()){
			String keys = keysIterator.next();
		
			if(isCompleted){
				batchScripts = new BatchScriptBean();
				isCompleted = false;
			}
			
			if(keys.contains("Name")){
				batchScripts.setTableName(resourceBundle2.getString(keys));
			}else if(keys.contains("Statement")){
				String statment = resourceBundle2.getString(keys);
				//int index = statment.indexOf("-S");
				//String sub1 = statment.substring(0, index+2);
				//String sub2 = statment.substring(index+2, statment.length());
				//String finalString = sub1+" "+serverName+" "+sub2;
				String finalString=statment.replace("*SQLS*", serverName);
				finalString=finalString.replace("*SQLU*", SQLServerUserName);
				finalString=finalString.replace("*SQLP*", SQLServerPassword);
				batchScripts.setScriptStatements(finalString);
			}else if(keys.contains("isMultipleStat")){
				batchScripts.setIsMultiStatements(resourceBundle2.getString(keys));
			}else if(keys.contains("isSourceOrTarget")){
				batchScripts.setIsSourceOrTarget(resourceBundle2.getString(keys));
				isCompleted = true;
			}
			if(isCompleted){
				batchFileList.add(batchScripts);
				isCompleted = false;
				batchScripts = new BatchScriptBean();
			}	
		}
		Collections.sort(batchFileList,BatchFileGenerator.batchFileComparator);
		for (BatchScriptBean batchfilelist : batchFileList) {
			batchFileContentsBuffer = new StringBuffer();
			if( batchfilelist.getIsMultiStatements().equalsIgnoreCase("y")){
				if(batchfilelist.getIsSourceOrTarget().equalsIgnoreCase("source")){
					for (TableInformationBean tableInformationBean : tableInformationBeanList) {
						batchFileContentsBuffer.append(batchfilelist.getScriptStatements()).append(tableInformationBean.getSourceTableName()).append(".sql").append(lineSeparator);
					}
				}else{
					for (TableInformationBean tableInformationBean : tableInformationBeanList) {
						batchFileContentsBuffer.append(batchfilelist.getScriptStatements()).append(tableInformationBean.getTargetTableName()).append(".sql").append(lineSeparator);
					}
				}
			}else{
				batchFileContentsBuffer.append(batchfilelist.getScriptStatements());
			}
			StringBuffer batchFileName = new StringBuffer(outputDir).append(batchfilelist.getTableName());
		    writeToBatchFile(batchFileName.toString(), batchFileContentsBuffer);
		    batchFileContentsBuffer = null;
		}
		batchFileContentsBuffer = new StringBuffer();
		for (BatchScriptBean batchfilelist : batchFileList) {
			batchFileContentsBuffer.append("Call "+batchfilelist.getTableName()).append(".bat").append(lineSeparator);
		}
		StringBuffer masterBatchFileName = new StringBuffer(outputDir).append("MasterBatchFile");
	    writeToBatchFile(masterBatchFileName.toString(), batchFileContentsBuffer);
	    batchFileContentsBuffer = null;

		
	}
	
}
	


