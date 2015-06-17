package com.polarisft.tool.impl.trigger;

import java.util.List;

import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.bean.ColumnInformationBean;

import com.polarisft.tool.trigger.bean.TableInformationBean;

public class DateColumnValidationScript extends AbstractScriptTemplate{
	
	
	StringBuffer DateColFileContentsBuffer;
	@Override
	public void generateScript(List<TableInformationBean> tableInformationBeanList,String outputDir) {
		String lineSeparator = System.getProperty("line.separator");
		//ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
		DateColFileContentsBuffer = new StringBuffer();
		
		for (TableInformationBean tableInformationBean : tableInformationBeanList) {
			List<ColumnInformationBean> columnInformationBeanList = tableInformationBean.getColumnInformationBeanList();
            int numberOfColumns = columnInformationBeanList.size();
            int numOfCol= 0;
        
            DateColFileContentsBuffer.append("SELECT b.* FROM "+tableInformationBean.getTargetDatabaseName()+"."+tableInformationBean.getTargetSchemaName()+"."+tableInformationBean.getTargetTableName()+" B").append(lineSeparator);
            DateColFileContentsBuffer.append(" where exists ( SELECT 1 FROM "+tableInformationBean.getSourceDatabaseName()+"."+tableInformationBean.getSourceSchemaName()+"."+tableInformationBean.getSourceTableName()+" A").append(lineSeparator);
            DateColFileContentsBuffer.append("Where ");
            boolean isFirst = true;
            boolean isDateFirst = true;
            for (ColumnInformationBean columnInformationbeanList : columnInformationBeanList) {
            	numOfCol++;
            	if (columnInformationbeanList.isPrimaryKey() && !columnInformationbeanList.isDateColumn()) {
            		if (isFirst == true) {
            			 
            			 DateColFileContentsBuffer.append("A.["+columnInformationbeanList.getSourceColumnName()+"] = B.["+columnInformationbeanList.getTargetColumnName()+"]").append(lineSeparator);
            			 isFirst = false ;
            		}else{
            			DateColFileContentsBuffer.append(" AND A.["+columnInformationbeanList.getSourceColumnName()+"] = B.["+columnInformationbeanList.getTargetColumnName()+"]").append(lineSeparator);
            		}
            			
            		}
            }
            numOfCol = 0;
            boolean isDateConditionPresent=false;
           for (ColumnInformationBean columnInformationbeanList : columnInformationBeanList) {
        	   numOfCol++;
            	 if(columnInformationbeanList.isDateColumn() ){
            			
            		if (isDateFirst == true) {
            		DateColFileContentsBuffer.append("AND (");
            		DateColFileContentsBuffer.append("( A.["+columnInformationbeanList.getSourceColumnName()+"] IS NOT NULL AND B.["+ columnInformationbeanList.getTargetColumnName()+"] IS NULL )").append(lineSeparator);
            		isDateFirst = false ;
            		}else {
            			DateColFileContentsBuffer.append("OR ( A.["+columnInformationbeanList.getSourceColumnName()+"] IS NOT NULL AND B.["+ columnInformationbeanList.getTargetColumnName()+"] IS NULL )").append(lineSeparator);
            		}
            		isDateConditionPresent=true;	
            	}	
            		
            }
           if(isDateConditionPresent){
        	   
        	   DateColFileContentsBuffer.append(") ");
           }
            DateColFileContentsBuffer.append(" AND AS_Transaction_DateTime IS NULL )" ).append(lineSeparator).append(lineSeparator);
            
		}
		StringBuffer dateColumnValidationFile = new StringBuffer(outputDir).append("DateColumnValidationScript");
		writeToFile(dateColumnValidationFile.toString(), DateColFileContentsBuffer);
		DateColFileContentsBuffer = null;
	}

}
