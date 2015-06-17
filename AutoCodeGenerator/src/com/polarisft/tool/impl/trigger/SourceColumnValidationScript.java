	package com.polarisft.tool.impl.trigger;

	import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.bean.ColumnInformationBean;
import com.polarisft.tool.trigger.bean.TableInformationBean;
	
	public class SourceColumnValidationScript extends AbstractScriptTemplate{
		
		
		StringBuffer SourceColFileContentsBuffer;
		@Override
		public void generateScript(List<TableInformationBean> tableInformationBeanList,String outputDir) {
			String lineSeparator = System.getProperty("line.separator");
			//ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
			SourceColFileContentsBuffer = new StringBuffer();
			 ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
			 String targetDatabaseName = resourceBundle.getString("autoscriptGenerator.targetDatabaseName");
			for (TableInformationBean tableInformationBean : tableInformationBeanList) {
				List<ColumnInformationBean> columnInformationBeanList = tableInformationBean.getColumnInformationBeanList();
	            int numberOfColumns = columnInformationBeanList.size();
	           
	        
	            SourceColFileContentsBuffer.append("SELECT b.* FROM "+targetDatabaseName+"."+tableInformationBean.getSourceSchemaName()+"."+tableInformationBean.getSourceTableName()+" B").append(lineSeparator);
	            SourceColFileContentsBuffer.append(" WHERE NOT EXISTS ( SELECT 1 FROM "+tableInformationBean.getSourceDatabaseName()+"."+tableInformationBean.getSourceSchemaName()+"."+tableInformationBean.getSourceTableName()+" A").append(lineSeparator);
	            SourceColFileContentsBuffer.append("WHERE ");
	            boolean isFirst = true;
	            
	            for (ColumnInformationBean columnInformationbeanList : columnInformationBeanList) {
	            	
	            	if (isFirst == true) {
	            		SourceColFileContentsBuffer.append("A.["+columnInformationbeanList.getSourceColumnName()+"] = B.["+columnInformationbeanList.getSourceColumnName()+"]").append(lineSeparator);
	            		isFirst = false ;
	            	}else{
	            		SourceColFileContentsBuffer.append("AND A.["+columnInformationbeanList.getSourceColumnName()+"] = B.["+columnInformationbeanList.getSourceColumnName()+"]").append(lineSeparator);
	            	}
	            
	            }
	            
	            SourceColFileContentsBuffer.append("AND DATEDIFF(DAY,WM_TRANSACTION_DATETIME,GETDATE())=0 )" ).append(lineSeparator).append(lineSeparator);
	            //SourceColFileContentsBuffer.append(lineSeparator).append(lineSeparator);
			}
			StringBuffer SourceColumnValidationFile = new StringBuffer(outputDir).append("SourceColumnValidationScript");
			writeToFile(SourceColumnValidationFile.toString(), SourceColFileContentsBuffer);
			SourceColFileContentsBuffer = null;
		}

	}

