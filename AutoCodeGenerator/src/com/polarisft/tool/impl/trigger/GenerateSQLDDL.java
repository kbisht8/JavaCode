package com.polarisft.tool.impl.trigger;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import com.polarisft.tool.resources.StaticContainers;
import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.bean.ColumnInformationBean;
import com.polarisft.tool.trigger.bean.StagingColumnInformationBean;
import com.polarisft.tool.trigger.bean.TableInformationBean;
public class GenerateSQLDDL extends AbstractScriptTemplate {
	private static Map<String,String> staticMap;
	private StaticContainers staticContainer;
	private String serverName ;
	private String RunSQLAgentJobs;
	public void generateScript(List<TableInformationBean> tableInformationBeanList, String outputPath) {
        String lineSeparator = System.getProperty("line.separator");
        StringBuffer stagingFileContentsBuffer = null;
        stagingFileContentsBuffer =  new StringBuffer();
        staticMap = staticContainer.getMyMap();
        serverName = staticMap.get("SQLServerHostName");
        RunSQLAgentJobs=staticMap.get("RunSQLAgentJobs");
        stagingFileContentsBuffer.append("WAITFOR DELAY '00:00:07'").append(lineSeparator);
        stagingFileContentsBuffer.append("GO").append(lineSeparator);
        ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
        for (TableInformationBean tableInformationBean : tableInformationBeanList) {
        	//Staging Tables
        	
        	stagingFileContentsBuffer.append("USE ["+tableInformationBean.getSourceDatabaseName()+"]").append(lineSeparator);
        	stagingFileContentsBuffer.append("GO").append(lineSeparator);
        	stagingFileContentsBuffer.append("CREATE TABLE ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".")
                    .append(tableInformationBean.getSourceTableName()).append("(").append(lineSeparator);
            List<StagingColumnInformationBean> stagingColumnInformationBeanListSTGTBL = tableInformationBean.getStagingColumnInformationBeanList();
            if(null != stagingColumnInformationBeanListSTGTBL){
	            int numberOfColumnsSTG = stagingColumnInformationBeanListSTGTBL.size();
	            for (int columnCount = 1; columnCount <= numberOfColumnsSTG; columnCount++) {
	                StagingColumnInformationBean stagingColumnInformationBeanSTGTBL = stagingColumnInformationBeanListSTGTBL.get(columnCount - 1);
	                stagingFileContentsBuffer.append(stagingColumnInformationBeanSTGTBL.getSourceColumnName());
	                stagingFileContentsBuffer.append(" ").append(stagingColumnInformationBeanSTGTBL.getSourceDataType());
	                String sourceColumnLength = stagingColumnInformationBeanSTGTBL.getSourceColumnLength();
	                int newLength = Integer.parseInt(sourceColumnLength) + 1;
	                if (sourceColumnLength != null && !sourceColumnLength.equals("")) {
	                	stagingFileContentsBuffer.append("(").append(newLength).append(")");
	                }
	                stagingFileContentsBuffer.append(" NULL,");
	                stagingFileContentsBuffer.append(lineSeparator);
	            }
	            stagingFileContentsBuffer.append("TRANSACTION_TYPE char(1) NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("WM_TRANSACTION_DATETIME datetime NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("AS_TRANSACTION_DATETIME datetime NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("PRIORITY decimal(3,0) NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("STG_TRANSACTION_DATETIME datetime NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("TR_TRANSACTION_DATETIME datetime NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("STG_ID bigint IDENTITY(1,1) NOT NULL,").append(lineSeparator);
	            stagingFileContentsBuffer.append("CONSTRAINT PK_"+tableInformationBean.getSourceTableName()+" PRIMARY KEY CLUSTERED").append(lineSeparator);
	            stagingFileContentsBuffer.append("(").append(lineSeparator);
	            stagingFileContentsBuffer.append("STG_ID ASC").append(lineSeparator);
	            stagingFileContentsBuffer.append(") WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]").append(lineSeparator);
	            stagingFileContentsBuffer.append(") ON [PRIMARY]").append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET ANSI_PADDING OFF").append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append("ALTER TABLE ").append(tableInformationBean.getSourceDatabaseName()).append(".["+tableInformationBean.getSourceSchemaName()+"].["+tableInformationBean.getSourceTableName()+"] ADD  DEFAULT (getdate()) FOR [STG_TRANSACTION_DATETIME]").append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append(lineSeparator);
	            

	            //Staging Tables Triggers

	            int tabCount = 0;

	            stagingFileContentsBuffer.append("IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID('").append(tableInformationBean.getSourceSchemaName()).append(".TRG_DFC_Insert")
	                    .append(tableInformationBean.getSourceTableName()).append("'))");
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
	            stagingFileContentsBuffer.append("DROP TRIGGER ").append(tableInformationBean.getSourceSchemaName()).append(".").append("TRG_DFC_Insert").append(tableInformationBean.getSourceTableName())
	                    .append(lineSeparator);
	            stagingFileContentsBuffer.append("END").append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append("CREATE TRIGGER ").append(tableInformationBean.getSourceSchemaName()).append(".TRG_DFC_Insert");
	            stagingFileContentsBuffer.append(tableInformationBean.getSourceTableName());
	            stagingFileContentsBuffer.append(" ON  ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".");
	            stagingFileContentsBuffer.append(tableInformationBean.getSourceTableName());
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append("   AFTER INSERT,UPDATE").append(lineSeparator);
	            stagingFileContentsBuffer.append("AS").append(lineSeparator);
	            
	            //Insert the global variable declarations and initialize it by reading the content from declarations input file 
	            stagingFileContentsBuffer.append(readDeclarationsFile(TRIGGER_DECLARATIONS, tabCount, IDENTIFIER_GLOBAL));
	            
	            stagingFileContentsBuffer.append("SET @StagingTableName='").append(tableInformationBean.getSourceTableName()).append("'").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @StoredProcedureName = 'SP_DFC_Load").append(tableInformationBean.getTargetTableName()).append("'").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("SET NOCOUNT ON;").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("BEGIN TRY").append(lineSeparator).append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("/*Get the NewRecordPresentFlag value from ETLControl Table for the specific Staging Table Name*/").append(lineSeparator).append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("SELECT @NewRecordPresentFlag = NewRecordPresentFlag").append(lineSeparator);
	            tabCount++;
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("FROM ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName());
	            stagingFileContentsBuffer.append(".ETLControlTable").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("WHERE StagingTableName = @StagingTableName").append(lineSeparator);
	            tabCount--;
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("/*Check whether the record Exists in ETL Control Table and NewRecordPresentFlag is 'N' */").append(lineSeparator).append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("SET @RowCount = @@ROWCOUNT").append(lineSeparator).append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("IF @RowCount = 1").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("IF @NewRecordPresentFlag = 'N'").append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("/*Updating ETL Control Table with NewRecordPresentFlag as 'Y'*/").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("UPDATE ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName());
	            stagingFileContentsBuffer.append(".ETLControlTable");
	            stagingFileContentsBuffer.append(" SET NewRecordPresentFlag = 'Y'").append(lineSeparator);
	            tabCount++;
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append(" WHERE StagingTableName = @StagingTableName").append(lineSeparator);
	            tabCount--;
	            tabCount--;
	            tabCount--;
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("END").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("ELSE IF @RowCount = 0").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("/*If Record not Exists in ETL Control Table Inserting New Record for this Staging Table Name*/").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName());
	            stagingFileContentsBuffer.append(".ETLControlTable (StagingTableName,");
	            stagingFileContentsBuffer.append("ProcedureName,");
	            stagingFileContentsBuffer.append("NewRecordPresentFlag,");
	            stagingFileContentsBuffer.append("IsProcedureRunningFlag,").append(lineSeparator);
	            tabCount++;
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("ExecutionBatchSize)").append(lineSeparator);
	            tabCount--;
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("   VALUES").append(lineSeparator);
	            tabCount++;
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("(");
	            stagingFileContentsBuffer.append("@StagingTableName,");
	            stagingFileContentsBuffer.append("@StoredProcedureName,");
	            stagingFileContentsBuffer.append("'Y',");
	            stagingFileContentsBuffer.append("'N',");
	            stagingFileContentsBuffer.append("@ExecutionBatchSize)").append(lineSeparator);
	            tabCount--;
	            tabCount--;
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("END").append(lineSeparator);
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("END TRY").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("BEGIN CATCH").append(lineSeparator).append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("PRINT CONVERT(NVARCHAR,Getdate(),121) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("  + @StagingTableName + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("  + CONVERT (NVARCHAR,ERROR_NUMBER()) + @ShortSpacing  + @ErrorDescription + ERROR_MESSAGE()").append(lineSeparator).append(lineSeparator).append(lineSeparator);
	            tabCount--;
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("END CATCH").append(lineSeparator);
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("SET NOCOUNT OFF").append(lineSeparator);
	            stagingFileContentsBuffer.append("END");
	            tabCount = 0;
            	}
            	stagingFileContentsBuffer.append(lineSeparator);
            	stagingFileContentsBuffer.append("GO");
            	stagingFileContentsBuffer.append(lineSeparator);
            	stagingFileContentsBuffer.append(lineSeparator);
            	stagingFileContentsBuffer.append(lineSeparator);
	            //Create SPInsert For Staging Database	
	            stagingFileContentsBuffer.append("CREATE PROCEDURE [").append(tableInformationBean.getSourceSchemaName()).append("].[SPInsert")
                .append(tableInformationBean.getSourceTableName()).append("] (").append(lineSeparator);
	            List<StagingColumnInformationBean> columnColumnInformationBeanListSPINSERT = tableInformationBean.getStagingColumnInformationBeanList();
	            List<ColumnInformationBean> columnInformationBeanListSPINSERT = tableInformationBean.getColumnInformationBeanList();
	            if(null != columnColumnInformationBeanListSPINSERT)
	            {
	            	int numberOfColumnsSPINSERT = columnInformationBeanListSPINSERT.size();
		            for (int columnCount = 1; columnCount <= numberOfColumnsSPINSERT; columnCount++) {
		                ColumnInformationBean columnInformationBean = columnInformationBeanListSPINSERT.get(columnCount- 1);
		                stagingFileContentsBuffer.append("@i_");
		                stagingFileContentsBuffer.append(columnInformationBean.getSourceColumnName());
		                stagingFileContentsBuffer.append("   ").append(columnInformationBean.getSourceDataType());
		                
		                String sourceColumnLength = columnInformationBean.getSourceColumnLength();
		                int newLength ;
		                if(!columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")){
		                	 newLength = Integer.parseInt(sourceColumnLength) + 1;
		                }else {
		                	 newLength = Integer.parseInt(sourceColumnLength);
		                }
		                if (sourceColumnLength != null && !sourceColumnLength.equals("")) {
		                	stagingFileContentsBuffer.append("(").append(newLength).append(")");
		                }
		         
		                stagingFileContentsBuffer.append(lineSeparator);
		                stagingFileContentsBuffer.append(",");
		            }
		            stagingFileContentsBuffer.append("@i_TRANSACTION_TYPE  CHAR(1)").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@i_WM_TRANSACTION_DATETIME  DATETIME").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@i_AS_TRANSACTION_DATETIME  DATETIME").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@i_PRIORITY NVARCHAR(8)").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@i_BufferSequenceNumber NUMERIC(18,0)").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@i_BufferTableName NVARCHAR(10)").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@o_FLAG NVARCHAR(10) = NULL OUT").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@o_ERRORCODE NVARCHAR(25) = NULL OUT").append(lineSeparator);
		            stagingFileContentsBuffer.append(",@o_ERRORDESCRIPTION NVARCHAR(150) = NULL OUT  )").append(lineSeparator);
		            stagingFileContentsBuffer.append("AS BEGIN").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET NOCOUNT ON").append(lineSeparator);
		            stagingFileContentsBuffer.append("BEGIN TRY").append(lineSeparator);
		            stagingFileContentsBuffer.append("DECLARE @l_STG_ID AS DECIMAL(18,2)").append(lineSeparator);
		            stagingFileContentsBuffer.append("IF ISNULL(@i_TRANSACTION_TYPE,'')=''").append(lineSeparator);
		            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_FLAG             = 'NG'").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORCODE        = NULL").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORDESCRIPTION = 'Transaction type can not be null or blank.'").append(lineSeparator);
		            stagingFileContentsBuffer.append("RETURN").append(lineSeparator);
		            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
		            stagingFileContentsBuffer.append("IF @i_WM_TRANSACTION_DATETIME  IS NULL").append(lineSeparator);
		            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_FLAG             = 'NG'").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORCODE        = NULL").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORDESCRIPTION = 'WM transaction datetime can not be null.'").append(lineSeparator);
		            stagingFileContentsBuffer.append("RETURN").append(lineSeparator);
		            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
		            stagingFileContentsBuffer.append("IF @i_AS_TRANSACTION_DATETIME   IS NULL").append(lineSeparator);
		            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_FLAG             = 'NG'").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORCODE        = NULL").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORDESCRIPTION = 'AS transaction datetime can not be null.'").append(lineSeparator);
		            stagingFileContentsBuffer.append("RETURN").append(lineSeparator);
		            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
		            stagingFileContentsBuffer.append("IF ISNULL(@i_PRIORITY,0) = 0").append(lineSeparator);
		            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_FLAG             = 'NG'").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORCODE        = NULL").append(lineSeparator);
		            stagingFileContentsBuffer.append("SET @o_ERRORDESCRIPTION = 'Priority can not be null or 0.'").append(lineSeparator);
		            stagingFileContentsBuffer.append("RETURN").append(lineSeparator);
		            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
	            
		            stagingFileContentsBuffer.append("INSERT INTO [").append(tableInformationBean.getSourceSchemaName()).append("].")
	                                             .append(tableInformationBean.getSourceTableName()).append("(").append(lineSeparator);
	            
		            for (int columnCount = 1; columnCount <= numberOfColumnsSPINSERT; columnCount++) {
	                ColumnInformationBean columnInformationBean = columnInformationBeanListSPINSERT.get(columnCount - 1);
	                stagingFileContentsBuffer.append(columnInformationBean.getSourceColumnName()).append(lineSeparator);
	                stagingFileContentsBuffer.append(",");
	        
	            	}
	            	stagingFileContentsBuffer.append("TRANSACTION_TYPE").append(lineSeparator);
	            	stagingFileContentsBuffer.append(",WM_TRANSACTION_DATETIME").append(lineSeparator);
	            	stagingFileContentsBuffer.append(",AS_TRANSACTION_DATETIME").append(lineSeparator);
	            	stagingFileContentsBuffer.append(",PRIORITY").append(lineSeparator);
	            	stagingFileContentsBuffer.append(")").append(lineSeparator);
	            	stagingFileContentsBuffer.append("VALUES (").append(lineSeparator);
	            
	            for (int columnCount = 1; columnCount <= numberOfColumnsSPINSERT; columnCount++) {
	                ColumnInformationBean columnInformationBean = columnInformationBeanListSPINSERT.get(columnCount - 1);
	                stagingFileContentsBuffer.append("@i_");
	                stagingFileContentsBuffer.append(columnInformationBean.getSourceColumnName()).append(lineSeparator);
	                stagingFileContentsBuffer.append(",");
	            	}
	            
	            stagingFileContentsBuffer.append("@i_TRANSACTION_TYPE").append(lineSeparator);
	            stagingFileContentsBuffer.append(",@i_WM_TRANSACTION_DATETIME").append(lineSeparator);
	            stagingFileContentsBuffer.append(",@i_AS_TRANSACTION_DATETIME").append(lineSeparator);
	            stagingFileContentsBuffer.append(",@i_PRIORITY )").append(lineSeparator).append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @l_STG_ID = SCOPE_IDENTITY()").append(lineSeparator).append(lineSeparator);
	            
	            
	            stagingFileContentsBuffer.append("IF (ISNULL(@l_STG_ID,0) > 0)").append(lineSeparator);
	            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_FLAG = 'OK'").append(lineSeparator);
	            stagingFileContentsBuffer.append("END").append(lineSeparator);
	            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
	            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_FLAG             = 'NG'").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_ERRORCODE        = NULL").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_ERRORDESCRIPTION = 'Problem with data.'").append(lineSeparator);
	            stagingFileContentsBuffer.append("RETURN").append(lineSeparator);
	            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
	            stagingFileContentsBuffer.append("END TRY").append(lineSeparator);
	            stagingFileContentsBuffer.append("BEGIN CATCH").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_FLAG             = 'NG'").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_ERRORCODE        = ERROR_NUMBER()").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET @o_ERRORDESCRIPTION = ERROR_MESSAGE()").append(lineSeparator);
	            stagingFileContentsBuffer.append("Insert Into ").append(tableInformationBean.getSourceSchemaName()).append(".DFCError Values(@l_STG_ID,getdate(),'").append(tableInformationBean.getSourceTableName()).append("',@o_ERRORCODE,@o_ERRORDESCRIPTION)").append(lineSeparator);
	            stagingFileContentsBuffer.append("END CATCH").append(lineSeparator);
	            stagingFileContentsBuffer.append("INSERT INTO Staging.DFCReconciliation Values (@i_BufferSequenceNumber, ISNULL(@l_STG_ID,0), '");
	            stagingFileContentsBuffer.append(tableInformationBean.getSourceTableName());
	            stagingFileContentsBuffer.append("',@i_BufferTableName, CASE WHEN ISNULL(@l_STG_ID,0) > 0 THEN '4' ELSE '5' END,NULL,GETDATE())").append(lineSeparator);
	            stagingFileContentsBuffer.append("SET NOCOUNT OFF;").append(lineSeparator);
	            stagingFileContentsBuffer.append("END").append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append(lineSeparator);
	            }
	            
	            stagingFileContentsBuffer.append(lineSeparator);
            	stagingFileContentsBuffer.append(lineSeparator);
            	stagingFileContentsBuffer.append(lineSeparator);
	            //SP_DFC STORED PROCEDURE FOR GDC
	            int tabCount=0;
	         	stagingFileContentsBuffer.append("USE ").append(tableInformationBean.getSourceDatabaseName()).append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append("IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('[").append(tableInformationBean.getSourceSchemaName());
	            stagingFileContentsBuffer.append("].[SP_DFC_Load").append(tableInformationBean.getTargetTableName()).append("]') AND type in ('P'))");
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append("BEGIN");
	            stagingFileContentsBuffer.append(lineSeparator);
	            tabCount++;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("DROP PROCEDURE ").append(tableInformationBean.getSourceSchemaName()).append(".").append("SP_DFC_Load").append(tableInformationBean.getTargetTableName());
	            stagingFileContentsBuffer.append(lineSeparator);
	            tabCount--;
	            stagingFileContentsBuffer.append(insertTabs(tabCount));
	            stagingFileContentsBuffer.append("END").append(lineSeparator);
	            stagingFileContentsBuffer.append("GO").append(lineSeparator);
	            stagingFileContentsBuffer.append("/*The Parameter for this procedure will be sent through the Scheduler which is executing").append(lineSeparator);
	            stagingFileContentsBuffer.append("the procedure. The Parameter will be maintained in the Work table for each table separately*/").append(lineSeparator);
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append("CREATE PROCEDURE ").append(tableInformationBean.getSourceSchemaName()).append(".").append("SP_DFC_Load");
	            stagingFileContentsBuffer.append(tableInformationBean.getTargetTableName());
	            stagingFileContentsBuffer.append(" (@BatchSize AS INT)");
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append("AS");
	            stagingFileContentsBuffer.append(lineSeparator);
	            stagingFileContentsBuffer.append("/*Variables for Maintaining Staging Values*/").append(lineSeparator);
	            List<ColumnInformationBean> columnInformationBeanListDFCLOAD = tableInformationBean.getColumnInformationBeanList();
	            int numberOfColumnsDFCLOAD = columnInformationBeanListDFCLOAD.size();
	             //This for loop is used to read all the source column names from
	             // excel.
	            for (ColumnInformationBean columnInformationBeanDFCLOAD : columnInformationBeanListDFCLOAD) {
	            	
	               if(columnInformationBeanDFCLOAD.getTargetColumnName()!= null){
	            	   stagingFileContentsBuffer.append("DECLARE @Source").append(columnInformationBeanDFCLOAD.getTargetColumnName().trim());
	            	   int newLength = Integer.parseInt(columnInformationBeanDFCLOAD.getSourceColumnLength()) + 1 ;
	            	   stagingFileContentsBuffer.append(" ").append(columnInformationBeanDFCLOAD.getSourceDataType()).append("(").append(newLength).append(")").append(lineSeparator);
	               }
	
	                   String[] nums = columnInformationBeanDFCLOAD.getSourceColumnLength().split(",");
	                   int decimal = Integer.parseInt(nums[0]);
	
	               if(columnInformationBeanDFCLOAD.getTargetDataType()!= null && !columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Nvarchar")){
	                    stagingFileContentsBuffer.append("DECLARE @Converted");
	                 
	                    if(columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Time")){
	                    	stagingFileContentsBuffer.append(columnInformationBeanDFCLOAD.getTargetColumnName()).append(" TIME");
	                    }
	                    
	                    if(columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Date")){
	                    	 stagingFileContentsBuffer.append(columnInformationBeanDFCLOAD.getTargetColumnName()).append(" DATE");
	                    	 
	                    }                     
	                 if(columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Decimal") ||
	                 columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Numeric")) {
	                    stagingFileContentsBuffer.append(columnInformationBeanDFCLOAD.getTargetColumnName()).append(" ").append(columnInformationBeanDFCLOAD.getTargetDataType());
	                }
	                String targetColumnLength = columnInformationBeanDFCLOAD.getTargetColumnLength();
	                if (targetColumnLength != null && !targetColumnLength.equals("")) {
	                    stagingFileContentsBuffer.append("(").append(targetColumnLength).append(")");
	                }
	                stagingFileContentsBuffer.append(lineSeparator);
	               }
	               if(columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Time") ||
	            		   columnInformationBeanDFCLOAD.getTargetDataType().equalsIgnoreCase("Date")){
	            	   stagingFileContentsBuffer.append("DECLARE @Converted");
	            	   stagingFileContentsBuffer.append(columnInformationBeanDFCLOAD.getTargetColumnName().trim()).append("Number");
	            	   stagingFileContentsBuffer.append(" NUMERIC(").append(decimal).append(")").append(lineSeparator);
	               }

	            }  //This for loop is used for declaring the DateConversion variables
             // and they are displayed only for those tables which have date
	           stagingFileContentsBuffer.append("/*Variables for Date Convertion */").append(lineSeparator);
               stagingFileContentsBuffer.append("DECLARE @ErrorNumberDateConverted INT").append(lineSeparator);
               stagingFileContentsBuffer.append("DECLARE @ErrorNumberDateConvertedTemp INT").append(lineSeparator);
               stagingFileContentsBuffer.append("DECLARE @ErrorDescriptionDateConverted VARCHAR(200)").append(lineSeparator);
                 
               stagingFileContentsBuffer.append("DECLARE @ErrorNumberTimeConverted INT").append(lineSeparator);
               stagingFileContentsBuffer.append("DECLARE @ErrorNumberTimeConvertedTemp INT").append(lineSeparator);
               stagingFileContentsBuffer.append("DECLARE @ErrorDescriptionTimeConverted VARCHAR(200)").append(lineSeparator);
                    
               stagingFileContentsBuffer.append("DECLARE @ErrorText VARCHAR(1000)").append(lineSeparator);

            //Insert the global variable declarations and initialize the variables by reading it from declarations input file.
            stagingFileContentsBuffer.append(readDeclarationsFile(PROCEDURE_DECLARATIONS, tabCount, IDENTIFIER_GLOBAL));
            
            stagingFileContentsBuffer.append("SET @SourceTableName = '").append(tableInformationBean.getSourceTableName()).append("'").append(lineSeparator);
            
            stagingFileContentsBuffer.append("BEGIN   /* Procedure (B1) Starts Here */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET NOCOUNT ON").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN TRY").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tIF OBJECT_ID('tempdb.DBO.#TempStagingTable') IS NOT NULL").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tBEGIN").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\tDROP TABLE #TempStagingTable").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tEND").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("CREATE TABLE #TempStagingTable (").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTempSequenceKey INT PRIMARY KEY,").append(lineSeparator);
            // This for loop is used for displaying the target column names.
	            for (ColumnInformationBean columnInformationBean : columnInformationBeanListDFCLOAD) {
	                stagingFileContentsBuffer.append(insertTabs(tabCount));
	                stagingFileContentsBuffer.append("\tTemp").append(columnInformationBean.getTargetColumnName());
	                
	                String[] nums = columnInformationBean.getSourceColumnLength().split(",");
	                int newLength = Integer.parseInt(nums[0]) + 1;
	                stagingFileContentsBuffer.append(" ").append(columnInformationBean.getSourceDataType());
	                stagingFileContentsBuffer.append("(" + newLength).append("),").append(lineSeparator);
	               
	            	}
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTempTransactionType CHAR(1),").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTempAS400TransactionDatetime DATETIME,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTempStagingID BIGINT  )").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Marking IsProcedureRunningFlag = 'Y' by upadting ETLControlTable table in order to prevent Schedular from initiating another instance of").append(
                    lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("execution of the same procedure while current procedure is executing*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("UPDATE ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".ETLControlTable")
                    .append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   SET IsProcedureRunningFlag = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" WHERE StagingTableName = @SourceTableName").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @GetInitialValueFlag = 'Y'").append(lineSeparator).append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END TRY").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN CATCH").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @StagingID = -1").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = CONVERT(NVARCHAR,GETDATE(),21) + @ShortSpacing + @TextFileBeginingText + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingIDText + CONVERT(NVARCHAR,@StagingID) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileProcessedDatetimeText + CONVERT(NVARCHAR,@ODSProcessedDateTime,121) + @TextFileSeparator + @ShortSpacing").append(
                    lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingTableText + @SourceTableName + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrorcodeText + CONVERT(NVARCHAR,ERROR_NUMBER()) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrordescriptionText + ERROR_MESSAGE()").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("PRINT @TextMessage").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Inserting into Error details to Error table in case of Error while inserting into # temp table */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".DFCError")
                    .append(lineSeparator);
            tabCount=tabCount+3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("( STG_ID,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" TR_TRANSACTION_DATETIME,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" StagingTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ErrorCode,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ErrorDescription").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" )").append(lineSeparator);
            tabCount=tabCount-3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("VALUES      (@StagingID,").append(lineSeparator);
            tabCount = tabCount+3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" @ODSProcessedDateTime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" @SourceTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ERROR_NUMBER(),").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ERROR_MESSAGE()").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" )").append(lineSeparator);
            tabCount = tabCount- 4;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END CATCH").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @GetInitialValueFlag = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /*BEGIN (B38)*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("WHILE(@IsRecordPresent =1)");
            stagingFileContentsBuffer.append(" /* Start the process of selecting the records */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN    /* Outer While BEGIN of Batch processing & Transfer of records from Staging to ODS(B2) Starts Here */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN TRY").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* TRUNCATE TempTable to delete all records and to keep the schema */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("TRUNCATE TABLE #TempStagingTable").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TempTableCount  = 0").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Inserting (Batchsize) records into #Temp table from Staging table where Transfer date is null*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET ROWCOUNT @BatchSize").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO #TempStagingTable ( TempSequenceKey,\n \t \t \t \t \t \t \t \t \t \t \t \t");
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                stagingFileContentsBuffer.append("Temp").append(columnInformationBean.getTargetColumnName().trim()).append(",\n \t \t \t \t \t \t \t \t \t \t \t \t");
            }
            stagingFileContentsBuffer.append("TempTransactionType,").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t \t \t \t \t    TempAS400TransactionDatetime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t \t \t \t \t    TempStagingID").append(lineSeparator);
            stagingFileContentsBuffer.append("\t \t \t \t \t \t \t \t \t          )").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("  (SELECT  ROW_NUMBER() OVER (ORDER BY STG_ID),");
            // This for loop is used for displaying the source column names.
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);

                stagingFileContentsBuffer.append(columnInformationBean.getSourceColumnName());
                stagingFileContentsBuffer.append(",").append(lineSeparator);
                stagingFileContentsBuffer.append("\t \t \t \t \t \t \t \t \t \t \t \t");
            }
            stagingFileContentsBuffer.append("TRANSACTION_TYPE,").append(lineSeparator);
            tabCount = tabCount + 7;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("AS_TRANSACTION_DATETIME,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("STG_ID").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("FROM ");
            stagingFileContentsBuffer.append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".");
            stagingFileContentsBuffer.append(tableInformationBean.getSourceTableName()).append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   WHERE TR_TRANSACTION_DATETIME IS NULL");
            stagingFileContentsBuffer.append(")").append(lineSeparator).append(lineSeparator);
            tabCount = tabCount-5;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TempTableCount = @@ROWCOUNT");
            stagingFileContentsBuffer.append("  /* @@ROWCOUNT Returns Number records for the above select statement*/").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @TempTableCount  = 0").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN ");
            stagingFileContentsBuffer.append(" /* If there are no records to process as per selection criteria set @IsRecordPresent = 0 to EXIT the process*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tSET @IsRecordPresent = 0").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END TRY ");
            stagingFileContentsBuffer.append("/* End of Inserting records in Temp Table from Staging table for the Batchsize*/").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN CATCH ");
            stagingFileContentsBuffer.append("/* Capture database related error for the above select statement */").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @StagingID = -1").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @IsRecordPresent = 0").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = CONVERT(NVARCHAR,GETDATE(),21) + @ShortSpacing + @TextFileBeginingText + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingIDText + CONVERT(NVARCHAR,@StagingID) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileProcessedDatetimeText + CONVERT(NVARCHAR,@ODSProcessedDateTime,121) + @TextFileSeparator + @ShortSpacing").append(
                    lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingTableText + @SourceTableName + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrorcodeText + CONVERT(NVARCHAR,ERROR_NUMBER()) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrordescriptionText + ERROR_MESSAGE()").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("PRINT @TextMessage").append(lineSeparator).append(lineSeparator);

            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Inserting into Error details to Error table in case of Error while inserting into # temp table */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".DFCError")
                    .append(lineSeparator);
            tabCount = tabCount+3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("( STG_ID,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" TR_TRANSACTION_DATETIME,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" StagingTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ErrorCode,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ErrorDescription").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" )").append(lineSeparator);
            tabCount =tabCount-3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("VALUES      (@StagingID,").append(lineSeparator);
            tabCount= tabCount+3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" @ODSProcessedDateTime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" @SourceTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ERROR_NUMBER(),").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" ERROR_MESSAGE()").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" )").append(lineSeparator).append(lineSeparator);
            tabCount = tabCount- 4;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END CATCH ");
            stagingFileContentsBuffer.append("/* End Capture of Database related error and inserting in Error table & writing to Error log*/").append(lineSeparator).append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tIF @TempTableCount > 0").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* Processing of Batch records from Temp Table (B3) Starts Here */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @LoopCount = 1    /* Initializing loop counter to 1 to start the loop */").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN TRAN BatchTransaction").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("WHILE (@TempTableCount >= @LoopCount)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* BEGIN (B4) for BatchTransaction Loop While TemptableCount>= LoopCount Starts */").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN TRY");
            stagingFileContentsBuffer.append(" /*BEGIN TRY -  Selecting of records from TempTable and Capturing while Selecting */").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSProcessedDateTime = GETDATE()").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/*Initializing Working Variables*/").append(lineSeparator);
           // This for loop is used for evaluating the date column and if there
           //  is a date column this text is displayed.
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isDateColumn()) {
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @TextMessage = NULL").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ErrorText = NULL").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ErrorDescriptionDateConverted = NULL").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ErrorNumberDateConverted = 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ErrorNumberDateConvertedTemp = 0").append(lineSeparator);
                    break;
                }
            }
            for (ColumnInformationBean columnInformationBean : columnInformationBeanListDFCLOAD) {
                if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar") || columnInformationBean.getTargetDataType().equalsIgnoreCase("Char")
                        || columnInformationBean.getTargetDataType().equalsIgnoreCase("Varchar")) {
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @Source").append(columnInformationBean.getTargetColumnName()).append(" = NULL").append(lineSeparator);
                }
                
                
                	if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Date") || 
                    		columnInformationBean.getSourceDataType().equalsIgnoreCase("DateTime") ||
                    		columnInformationBean.getTargetDataType().equalsIgnoreCase("Time") ){
                	
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @Source").append(columnInformationBean.getTargetColumnName()).append(" = NULL").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = NULL").append(lineSeparator);
                	}
                	
                	if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Decimal") ||
                			columnInformationBean.getTargetDataType().equalsIgnoreCase("Numeric")) {
                		stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @Source").append(columnInformationBean.getTargetColumnName()).append(" = NULL").append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @Converted");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = 0").append(lineSeparator);
                } 
            }
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            /* Initializing Working Variables by reading it from declarations input file */
            stagingFileContentsBuffer.append(readDeclarationsFile(PROCEDURE_DECLARATIONS, tabCount, IDENTIFIER_SET1));
            stagingFileContentsBuffer.append("/*Selecting records from # temp table based on Rownumber, which was assigned").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("when the records where inserted into #temp table*/").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SELECT  ");
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                stagingFileContentsBuffer.append("@Source").append(columnInformationBean.getTargetColumnName()).append(" = ");
                stagingFileContentsBuffer.append("Temp").append(columnInformationBean.getTargetColumnName());
                if (columnCount <= numberOfColumnsDFCLOAD) {
                    stagingFileContentsBuffer.append(",");
                }
                stagingFileContentsBuffer.append(lineSeparator);
                stagingFileContentsBuffer.append(insertTabs(tabCount));
                stagingFileContentsBuffer.append("\t\t");
            }
            stagingFileContentsBuffer.append("@SourceTransactionType = TempTransactionType,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t@SourceAS400TransactionDateTime = TempAS400TransactionDatetime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t@StagingID = TempStagingID").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" FROM   #TempStagingTable WITH (NOLOCK)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("WHERE   TempSequenceKey = @LoopCount").append(lineSeparator);
            boolean isfirstvisit = true;
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                
                if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Decimal") || columnInformationBean.getTargetDataType().equalsIgnoreCase("Numeric")) {
                	
                    stagingFileContentsBuffer.append(lineSeparator);
                   // stagingFileContentsBuffer.append("/* Added for Numeric Function */").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SELECT @Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());
                    stagingFileContentsBuffer.append(" = ConvertedDecimal,@ErrorNumberDateConvertedTemp =ErrorNumber,").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t@ErrorDescriptionDateConverted = ErrorDescription").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("  FROM ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".");
                   
                    stagingFileContentsBuffer.append("FNC_DFC_ConvertStringToDecimal").append("(@Source");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(",") ;
                    String[] nums = columnInformationBean.getTargetColumnLength().split(",");
                    int firstValue = Integer.parseInt(nums[0]);
                    int secondValue = 0 ;
                    int numLength = nums.length;
                    if(numLength > 1){
                    	secondValue = Integer.parseInt(nums[1]);
                    	stagingFileContentsBuffer.append(firstValue).append(",").append(secondValue);
                    }else{
                    stagingFileContentsBuffer.append(firstValue).append(",0");
                    }
                    
                    
                    stagingFileContentsBuffer.append(")");
                    stagingFileContentsBuffer.append(" /* Get Converted date format,Error code & Error Decsription ").append(lineSeparator);
                    stagingFileContentsBuffer.append("																						");
                    stagingFileContentsBuffer.append("\t by passing decimal value to ConvertDateCYYMM Function*/").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF @ErrorNumberDateConvertedTemp <> 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
                   
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\tSET @ErrorNumberDateConverted = @ErrorNumberDateConvertedTemp").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t/* appened the Field Name & Value with Error Description */ ").append(lineSeparator);
                    if (isfirstvisit) {
                        tabCount++;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorText = '");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' +  @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorDescriptionDateConverted = ''").append(lineSeparator);
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    } else {
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("		SET @ErrorText = @ErrorText + ' | ");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' + @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append("\t\t\t\t\t\tEND").append(lineSeparator).append(lineSeparator);
                    }
                    isfirstvisit = false;
                }
                
                if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Date")) {

                    stagingFileContentsBuffer.append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SELECT @Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append("Number");
                    stagingFileContentsBuffer.append(" = ConvertedDecimal,@ErrorNumberDateConvertedTemp =ErrorNumber,").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t@ErrorDescriptionDateConverted = ErrorDescription").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("  FROM ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".");
                    stagingFileContentsBuffer.append("FNC_DFC_ConvertStringToDecimal").append("(@Source");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(",") ;
                    stagingFileContentsBuffer.append(columnInformationBean.getSourceColumnLength()).append(",0)");
                    stagingFileContentsBuffer.append(" /* Get Converted date format,Error code & Error Decsription ").append(lineSeparator);
                    stagingFileContentsBuffer.append("																						");
                    stagingFileContentsBuffer.append("\t by passing decimal value to ConvertDateCYYMM Function*/").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF @ErrorNumberDateConvertedTemp <> 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
                   
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\tSET @ErrorNumberDateConverted = @ErrorNumberDateConvertedTemp").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t/* appened the Field Name & Value with Error Description */ ").append(lineSeparator);
                    if (isfirstvisit) {
                        tabCount++;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorText = '");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' +  @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorDescriptionDateConverted = ''").append(lineSeparator);
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    } else {
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("		SET @ErrorText = @ErrorText + ' | ");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' + @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append("\t\t\t\t\t\tEND").append(lineSeparator).append(lineSeparator);
                    }
                    stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
                    stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
                    stagingFileContentsBuffer.append("SELECT @Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());
                    stagingFileContentsBuffer.append(" = ConvertedDate,@ErrorNumberDateConvertedTemp =ErrorNumber,").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t@ErrorDescriptionDateConverted = ErrorDescription").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("  FROM ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".");
                   
                    stagingFileContentsBuffer.append("FNC_DFC_ConvertDate").append(columnInformationBean.getDateFunction()).append("(@Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName().trim()).append("Number");
                    stagingFileContentsBuffer.append(")");
                    stagingFileContentsBuffer.append(" /* Get Converted date format,Error code & Error Decsription ").append(lineSeparator);
                    stagingFileContentsBuffer.append("																						");
                    stagingFileContentsBuffer.append("\t by passing decimal value to ConvertDateCYYMM Function*/").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF @ErrorNumberDateConvertedTemp <> 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN");
                    if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Date")) {
                        String[] nums = columnInformationBean.getSourceColumnLength().split(",");
                        int decimal = Integer.parseInt(nums[0]);
                        if (decimal == 5) {
                            stagingFileContentsBuffer.append("\t\t\t\t\t/* If Converted Error No <> 0,means passed value is not in CYYMM Format*/").append(lineSeparator);
                        } else if (decimal == 6) {
                            stagingFileContentsBuffer.append("\t\t\t\t\t/* If Converted Error No <> 0,means passed value is not in YYYYMM Format*/").append(lineSeparator);
                        } else if (decimal == 7) {
                            stagingFileContentsBuffer.append("\t\t\t\t\t/* If Converted Error No <> 0,means passed value is not in CYYMMDD Format*/").append(lineSeparator);
                        } else if (decimal == 8) {
                            stagingFileContentsBuffer.append("\t\t\t\t\t/* If Converted Error No <> 0,means passed value is not in YYYYMMDD Format*/").append(lineSeparator);
                        }
                    }
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\tSET @ErrorNumberDateConverted = @ErrorNumberDateConvertedTemp").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t/* appened the Field Name & Value with Error Description */ ").append(lineSeparator);
                    if (isfirstvisit) {
                        tabCount++;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorText = '");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' +  @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorDescriptionDateConverted = ''").append(lineSeparator);
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    } else {
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("		SET @ErrorText = @ErrorText + ' | ");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' + @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append("\t\t\t\t\t\tEND").append(lineSeparator).append(lineSeparator);
                    }
                    stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    isfirstvisit = false;
                }
             
                else if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Time")){
                    stagingFileContentsBuffer.append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SELECT @Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append("Number");
                    stagingFileContentsBuffer.append(" = ConvertedDecimal,@ErrorNumberDateConvertedTemp =ErrorNumber,").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t@ErrorDescriptionDateConverted = ErrorDescription").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("  FROM ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".");
                    stagingFileContentsBuffer.append("FNC_DFC_ConvertStringToDecimal").append("(@Source");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(",") ;
                    stagingFileContentsBuffer.append(columnInformationBean.getSourceColumnLength()).append(",0)");
                    stagingFileContentsBuffer.append(" /* Get Converted date format,Error code & Error Decsription ").append(lineSeparator);
                    stagingFileContentsBuffer.append("																						");
                    stagingFileContentsBuffer.append("\t by passing decimal value to ConvertDateCYYMM Function*/").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF @ErrorNumberDateConvertedTemp <> 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
                   
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\tSET @ErrorNumberDateConverted = @ErrorNumberDateConvertedTemp").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t/* appened the Field Name & Value with Error Description */ ").append(lineSeparator);
                    if (isfirstvisit) {
                        tabCount++;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorText = '");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' +  @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorDescriptionDateConverted = ''").append(lineSeparator);
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    } else {
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("		SET @ErrorText = @ErrorText + ' | ");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' + @ErrorDescriptionDateConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append("\t\t\t\t\t\tEND").append(lineSeparator).append(lineSeparator);
                    }
                    stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
                    stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
                    
                    stagingFileContentsBuffer.append("SELECT @Converted");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());

                    stagingFileContentsBuffer.append(" = ConvertedTime,@ErrorNumberTimeConvertedTemp =ErrorNumber,").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t@ErrorDescriptionTimeConverted = ErrorDescription").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("  FROM ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".");
                    stagingFileContentsBuffer.append("FNC_DFC_ConvertTime").append(columnInformationBean.getDateFunction()).append("(@Source");
                    stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());
                    stagingFileContentsBuffer.append(")");
                    stagingFileContentsBuffer.append(" /* Get Converted date format,Error code & Error Decsription ").append(lineSeparator);
                    stagingFileContentsBuffer.append("																						");
                    stagingFileContentsBuffer.append("\t by passing decimal value to ConvertTimeHHMMSS Function*/").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF @ErrorNumberDateConvertedTemp <> 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);

                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\tSET @ErrorNumberTimeConverted = @ErrorNumberTimeConvertedTemp").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t/* appened the Field Name & Value with Error Description */ ").append(lineSeparator);
                    if (isfirstvisit) {
                        tabCount++;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorText = '");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' +  @ErrorDescriptionTimeConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("SET @ErrorDescriptionTimeConverted = ''").append(lineSeparator);
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    } else {
                        tabCount--;
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("		SET @ErrorText = @ErrorText + ' | ");
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" - ' + @ErrorDescriptionTimeConverted").append(lineSeparator);
                        stagingFileContentsBuffer.append("\t\t\t\t\t\tEND").append(lineSeparator).append(lineSeparator);
                    }
                    stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
                    isfirstvisit = false;
                }
                
            }
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isDateColumn()) {
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF ((@ErrorNumberDateConverted <> 0) OR  (@ErrorNumberTimeConverted <> 0)) ").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN /* BEGIN (B34) If any Error in Date Conversion").append(lineSeparator);
                    tabCount++;
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append(" assigning Error Code and Error Message for writting into Error Table and Error File */").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ODSActionFlag = 'ERROR'").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("END /* END (B34) If any Error in Date Conversion").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("     assigning Error Code and Error Message for writting into Error Table and Error File */").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("ELSE").append(lineSeparator).append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN /* BEGIN (35) If No Error in Date Conversion Proceed to validate the record */").append(lineSeparator);
                    break;
                }
            }
            stagingFileContentsBuffer.append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/*Check whether the record is available in target table and get the respective").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tAS400Transaction Timestamp */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SELECT @TargetAS400TransationDateTime = ISNULL(AS_TRANSACTION_DATETIME,0)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("  FROM ");
            stagingFileContentsBuffer.append(tableInformationBean.getTargetDatabaseName()).append(".");
            stagingFileContentsBuffer.append(tableInformationBean.getTargetSchemaName()).append(".").append(tableInformationBean.getTargetTableName()).append(" WITH (NOLOCK)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" WHERE ");
            boolean isFirstKey = true;
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isPrimaryKey()) {
                    if (!isFirstKey) {
                        stagingFileContentsBuffer.append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("   AND ");
                    }
                    if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")) {
                    	 stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Source").append(columnInformationBean.getTargetColumnName());
                         isFirstKey = false;
                    }
                    
                    else {
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Converted").append(columnInformationBean.getTargetColumnName());
                        isFirstKey = false;
                    } 

               
                  
                }
            }
            
            stagingFileContentsBuffer.append(lineSeparator);
            stagingFileContentsBuffer.append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            // stagingFileContentsBuffer.append("IF @TargetAS400TransationDateTime <> 0").append(lineSeparator); // Commented by Arun on 14-Mar-2014
            //stagingFileContentsBuffer.append("IF @TargetAS400TransationDateTime <> 0").append(lineSeparator);                         // Added     by Arun on 14-Mar-2014
            stagingFileContentsBuffer.append("IF @@RowCount <> 0").append(lineSeparator);                         // Added     by Kuldeep on 17-Mar-2015
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSRecordExists = 'Y'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
            
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSRecordExists = 'N'");
            stagingFileContentsBuffer.append(" /* IF the respective record does not Exist in Target Table, Check Deleted Table");
            stagingFileContentsBuffer.append(lineSeparator);
            tabCount = tabCount + 7;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("for the latest record for the Business Key in case record was deleted earlier").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("and current record being processed is a late arriving record*/").append(lineSeparator);
            tabCount = tabCount - 7;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tSELECT @DeleteAS400TransactionDateTime = ISNULL(MAX(AS_TRANSACTION_DATETIME),0)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t  FROM ");
            stagingFileContentsBuffer.append(tableInformationBean.getTargetDatabaseName()).append(".");
            stagingFileContentsBuffer.append(tableInformationBean.getTargetSchemaName()).append(".");
            stagingFileContentsBuffer.append(tableInformationBean.getTargetTableName()).append("Deleted WITH (NOLOCK)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t WHERE ");
            isFirstKey = true;
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isPrimaryKey()) {
                    if (!isFirstKey) {
                        stagingFileContentsBuffer.append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("\t   AND ");
                    }
                    if (columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")) {
                   	 stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Source").append(columnInformationBean.getTargetColumnName());
                        isFirstKey = false;
                   }
                   
                   else {
                       stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Converted").append(columnInformationBean.getTargetColumnName());
                       isFirstKey = false;
                   } 
                }
            }
            stagingFileContentsBuffer.append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @DeleteAS400TransactionDatetime <> 0").append(lineSeparator);

            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* If record exists in Deleted Table setting the @ODSDeletedRecordExists = 'Y' */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSDeletedRecordExists = 'Y'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END");
            stagingFileContentsBuffer.append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @SourceTransactionType = 'I'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* Begin processing of records with Transaction Type Insert */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSRecordExists = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /*(B8) Starts Here -  Compare Staging AS400-Timestamp with ODS AS400-timestamp to check for Late Arriving Records */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF (@SourceAS400TransactionDateTime > @TargetAS400TransationDateTime)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /*(B9) Arriving record is latest hence update of ODS Target Table -*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'UPDATE'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /*(B9)-*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /*(B10) Arriving record is older hence write Error log and Update Error Table */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /*(B10)-*/").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END  /*(B8) End of IF @ODSRecordExists = 'Y' */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN    /* BEGIN(B11) - IF Record does not exist in ODS Target Table */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSDeletedRecordExists ='N' OR (@SourceAS400TransactionDatetime > @DeleteAS400TransactionDatetime)").append(lineSeparator).append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	BEGIN  /* BEGIN (B12) Insert Record in ODS Target Table */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("		SET @ODSActionFlag = 'INSERT'").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /*(B12) End of Insert Record*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	BEGIN /* BEGIN(B14) Processing Late Arriving Record - Error condition */").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END	/* END(B14) */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END  /* END (B11) - IF Record does not exist in ODS Target Table */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END  /* END (B5) Processing for Records with Transaction type Insert  */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE IF @SourceTransactionType = 'U'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* Begin processing of records with Transaction Type \"Update\" (B6) */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSRecordExists = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* BEGIN(B15) Compare Staging AS400-Timestamp with ODS AS400-timestamp to check for Late Arriving Records */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @SourceAS400TransactionDateTime > @TargetAS400TransationDateTime").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* BEGIN (B16) Arriving record is latest hence update of ODS Target Table -*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'UPDATE'").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	END /* END (B16)-*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	BEGIN /* BEGIN(B17) Arriving record is older hence write Error log and Update Error Table -*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	SET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END/* END (B17)-*/").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END (B15) IF ODS Record Exists for Record with Transaction Type Update */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* BEGIN(B18) ODS Record DOES NOT Exist for Record with Transaction Type Update  -*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSDeletedRecordExists ='N' OR (@SourceAS400TransactionDatetime > @DeleteAS400TransactionDatetime)").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /*BEGIN (B19) INSERT Record in ODS Target Table as Arriving Record is latest*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tSET @ODSActionFlag = 'INSERT'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tEND /* END (B19)-*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN/*(B21) Starts Here - If Staging AS400 Date less than Deleted Table and also not present in ODS*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tSET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\tEND/* END(B21)*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	END /* END(B18) ODS Record DOES NOT Exist for Record with Transaction Type Update -*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END  /* END  (B6) Processing for Records with Transaction type Update  */").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE IF @SourceTransactionType = 'D'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* BEGIN (B22) processing of records with Transaction Type \"Delete\" */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSRecordExists = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* BEGIN(B23) Compare Staging AS400-Timestamp with ODS AS400-timestamp to check for Late Arriving Records  */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tIF @SourceAS400TransactionDateTime > @TargetAS400TransationDateTime").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	BEGIN /* BEGIN(B24) Arriving record with Transaction Type \"DELETE\" is latest hence").append(lineSeparator);
            tabCount = tabCount+3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" INSERT record in respective Deleted Table and DELETE Record from ODS Target Table  -*/").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'DELETE'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSDeleteFlag = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /*END (B24) Arriving record with Transaction Type \"DELETE\" is latest  -*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* BEGIN(B25) Arriving record is older hence write Error log and Update Error Table  -*/").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tSET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END	/* END(B25) Arriving record is older hence write Error log and Update Error Table  -*/").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END(B23) IF ODS Record Exists for Record with Transaction Type \"DELETE\" -*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* BEGIN(B26) IF ODS Record DOES NOT Exist for Record with Transaction Type \"DELETE\" -*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF  @ODSDeletedRecordExists ='N' OR (@SourceAS400TransactionDatetime > @DeleteAS400TransactionDatetime)").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* move records to deleted table */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /*(B27) INSERT Record in ODS DELETED Table").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("as respective record is not available in Deleted Table */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'DELETE'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END (B27)-*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN/* BEGIN(B29) Arriving record is older (Late Arriving) hence write Error log and Update Error Table */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END(B29) -*/").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END  /* END(B26) IF ODS Record DOES NOT Exist for Record with Transaction Type \"DELETE\" -*/").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END(B22) processing of records with Transaction Type \"Delete\"  */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN /* BEGIN (B39)*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionFlag = 'ERROR'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @SourceOtherTransactionType = 1").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END (B39)*/").append(lineSeparator);

            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isDateColumn()) {
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("END  /* END (35) If No Error in Date Conversion Proceed to validate the record */").append(lineSeparator).append(lineSeparator);
                    break;
                }
            }

            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSActionFlag = 'INSERT'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* BEGIN (B31) Insert into Target Table based on ODSActionFlag */").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName());
            stagingFileContentsBuffer.append(".").append(tableInformationBean.getTargetTableName()).append("(");
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(",").append(lineSeparator);
                stagingFileContentsBuffer.append(insertTabs(tabCount));
                stagingFileContentsBuffer.append("\t\t\t");
            }
            stagingFileContentsBuffer.append("AS_TRANSACTION_DATETIME,TR_TRANSACTION_DATETIME)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tVALUES(").append(lineSeparator);
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                
                if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")){
                	 stagingFileContentsBuffer.append(insertTabs(tabCount));
                     stagingFileContentsBuffer.append("\t\t\t@Source").append(columnInformationBean.getTargetColumnName()).append(",").append(lineSeparator);
                }
                else{
                	stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t\t\t@Converted").append(columnInformationBean.getTargetColumnName());
                    stagingFileContentsBuffer.append(",").append(lineSeparator);
                }
               
            }
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t@SourceAS400TransactionDateTime,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSProcessedDateTime").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(")").append(lineSeparator);
            tabCount =  tabCount- 4;
           stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END (B31) Insert into Target Table based on ODSActionFlag */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE IF @ODSActionFlag = 'UPDATE'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /*BEGIN (B32) Update of Target Table based on ODSActionFlag */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("UPDATE ").append(tableInformationBean.getTargetDatabaseName()).append(".");
            stagingFileContentsBuffer.append(tableInformationBean.getTargetSchemaName()).append(".").append(tableInformationBean.getTargetTableName()).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   SET");
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                stagingFileContentsBuffer.append(" ").append(columnInformationBean.getTargetColumnName()).append(" = ");
                if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")){
                	stagingFileContentsBuffer.append("@Source").append(columnInformationBean.getTargetColumnName());
                }
                else  {
                    stagingFileContentsBuffer.append("@Converted").append(columnInformationBean.getTargetColumnName());
                }

                if (columnCount <= numberOfColumnsDFCLOAD) {
                    stagingFileContentsBuffer.append(",");
                }
                stagingFileContentsBuffer.append(lineSeparator);
                stagingFileContentsBuffer.append(insertTabs(tabCount));
                stagingFileContentsBuffer.append("\t\t");
            }
            stagingFileContentsBuffer.append(" AS_TRANSACTION_DATETIME = @SourceAS400TransactionDateTime,").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" TR_TRANSACTION_DATETIME = @ODSProcessedDateTime").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" WHERE ");
            isFirstKey = true;
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isPrimaryKey()) {
                    if (!isFirstKey) {
                        stagingFileContentsBuffer.append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("   AND ");
                    }
                    if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")){
                    	stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Source").append(columnInformationBean.getTargetColumnName());
                        isFirstKey = false;
                    }
                    else {
                    	//Edit by Arun Kumar on 27th January 2014.
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Converted").append(columnInformationBean.getTargetColumnName());
                        isFirstKey = false;
                    } 
                   
                }
            }
            stagingFileContentsBuffer.append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /*END (B32) Update of Target Table based on ODSActionFlag */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	ELSE IF @ODSActionFlag = 'DELETE'").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /*BEGIN (B33) Delete from Target Table based on ODSActionFlag */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tINSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".")
                    .append(tableInformationBean.getTargetTableName().trim()).append("Deleted").append("(");
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(",").append(lineSeparator);
                stagingFileContentsBuffer.append(insertTabs(tabCount));
                stagingFileContentsBuffer.append("\t\t\t\t");
            }
            stagingFileContentsBuffer.append("AS_TRANSACTION_DATETIME,TR_TRANSACTION_DATETIME)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t VALUES(").append(lineSeparator);
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")){
                	 stagingFileContentsBuffer.append(insertTabs(tabCount));
                     stagingFileContentsBuffer.append("\t\t\t\t@Source").append(columnInformationBean.getTargetColumnName()).append(",").append(lineSeparator);
                }
                else  {
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("\t\t\t\t@Converted").append(columnInformationBean.getTargetColumnName());
                    stagingFileContentsBuffer.append(",").append(lineSeparator);
                }
               
            }
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t@SourceAS400TransactionDateTime,").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSProcessedDateTime").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(")").append(lineSeparator);
            tabCount = tabCount-3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @ODSDeleteFlag = 'Y'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	BEGIN").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("		DELETE FROM ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".")
                    .append(tableInformationBean.getTargetTableName()).append(lineSeparator);
            tabCount = tabCount+ 3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   WHERE ");
            isFirstKey = true;
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isPrimaryKey()) {
                    if (!isFirstKey) {
                        stagingFileContentsBuffer.append(lineSeparator);
                        stagingFileContentsBuffer.append(insertTabs(tabCount));
                        stagingFileContentsBuffer.append("\t AND ");
                    }
                    if(columnInformationBean.getTargetDataType().equalsIgnoreCase("Nvarchar")){
                    	 stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Source").append(columnInformationBean.getTargetColumnName());
                         isFirstKey = false;
                    }
                    else  {
                        stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName()).append(" = ").append("@Converted").append(columnInformationBean.getTargetColumnName());
                        isFirstKey = false;
                    } 
                  
                }
            }
            stagingFileContentsBuffer.append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END    /*END (B33) Delete from Target Table based on ODSActionFlag */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE IF @ODSActionFlag = 'ERROR'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
            boolean isDateColumn = false;
            for (int columnCount = 1; columnCount <= numberOfColumnsDFCLOAD; columnCount++) {
                ColumnInformationBean columnInformationBean = columnInformationBeanListDFCLOAD.get(columnCount - 1);
                if (columnInformationBean.isDateColumn()) {
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("IF @ErrorNumberDateConverted <> 0").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("BEGIN /* BEGIN (B35) Assigning Date Conversion Error Number & Error Text*/").append(lineSeparator);
                    tabCount++;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ODSActionErrorCode = @ErrorNumberDateConverted").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("SET @ODSActionErrorMessage = @ErrorText").append(lineSeparator);
                    tabCount--;
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("END /* END (B35) Assigning Date Conversion Error Number & Error Text*/").append(lineSeparator);
                    stagingFileContentsBuffer.append(insertTabs(tabCount));
                    stagingFileContentsBuffer.append("ELSE ");
                    isDateColumn = true;
                    break;
                }
            }
            if (!isDateColumn) {
                stagingFileContentsBuffer.append(insertTabs(tabCount));
            }
            stagingFileContentsBuffer.append("IF @SourceOtherTransactionType = 1").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN  /* BEGIN (B37) Assigning Error Number & Error Text if Transaction Type is other than 'I'or'U' OR 'D'*/").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionErrorCode = @ErrorNumberOtherTransactionType").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionErrorMessage = @ErrorMessageOtherTransactionType + @SourceTransactionType").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END (B37) Assigning Error Number & Error Text if Transaction Type is other than 'I'or'U' OR 'D'*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	BEGIN /* BEGIN (B36) Assigning Late Arrving Error Code & Message*/").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionErrorCode = @LateArrivingErrorCode").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @ODSActionErrorMessage = @LateArrivingMessage").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("	END /* END (B36) Assigning Late Arrving Error Code & Message */").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = CONVERT(NVARCHAR,GETDATE(),21) + @ShortSpacing + @TextFileBeginingText + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingIDText + CONVERT(NVARCHAR,@StagingID) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileProcessedDatetimeText + CONVERT(NVARCHAR,@ODSProcessedDateTime,121) + @TextFileSeparator + @ShortSpacing").append(
                    lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingTableText + @SourceTableName + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrorcodeText + CONVERT(NVARCHAR,@ODSActionErrorCode) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrordescriptionText + @ODSActionErrorMessage").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("PRINT @TextMessage").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Inserting into Error details to Error table in case of Error */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".DFCError ")
                    .append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t(STG_ID,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTR_TRANSACTION_DATETIME,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("StagingTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ErrorCode,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ErrorDescription)").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("VALUES(").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@StagingID,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSProcessedDatetime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@SourceTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSActionErrorCode,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSActionErrorMessage").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   )").append(lineSeparator);
            tabCount = tabCount-3;
             stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* After Processing Each Record the Staging Table is updated with Transfer datetime */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("UPDATE ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".")
                    .append(tableInformationBean.getSourceTableName()).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append(" SET TR_TRANSACTION_DATETIME = @ODSProcessedDateTime").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   WHERE STG_ID = @StagingID").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SAVE TRANSACTION Interim").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @LoopCount = @LoopCount + 1").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END TRY  /*END TRY - Capture database related error during processing */").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN CATCH  /*BEGIN CATCH Capture database related error during processing").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("assigning Error Code and Error Message for writting into Error Table and Error log */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF XACT_STATE() <> -1").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tROLLBACK TRANSACTION Interim").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ELSE").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tSET @EntireRollback = 'Y'").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tROLLBACK TRANSACTION BatchTransaction").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @FinalErrorInsert = 'Y'").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextFileBeginingText + CONVERT(NVARCHAR,GETDATE(),21)").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingIDText + CONVERT(NVARCHAR,@StagingID) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileProcessedDatetimeText + CONVERT(NVARCHAR,@ODSProcessedDateTime,121) + @TextFileSeparator + @ShortSpacing").append(
                    lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingTableText + @SourceTableName").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrorcodeText + CONVERT(NVARCHAR,ERROR_NUMBER()) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrordescriptionText + ERROR_MESSAGE()").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("PRINT @TextMessage").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Inserting into Error details to Error table in case of Error */").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".DFCError ")
                    .append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t(STG_ID,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTR_TRANSACTION_DATETIME,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("StagingTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ErrorCode,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ErrorDescription)").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("VALUES(").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@StagingID,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSProcessedDatetime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@SourceTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ERROR_NUMBER(),").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ERROR_MESSAGE()").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   )").append(lineSeparator);
            tabCount = tabCount- 3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END CATCH  /* END CATCH- Capture database related error during processing */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t/* After Processing Each Record the Staging Table is updated with Transfer datetime */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tIF @FinalErrorInsert ='Y' BREAK").append(lineSeparator).append(lineSeparator);

            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END  /* END (B4) for BatchTransaction Loop While TemptableCount>= LoopCount  */").append(lineSeparator).append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @EntireRollback = 'N' COMMIT TRANSACTION BatchTransaction").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* TRANSACTION END */").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /*END for BEGIN BatchTransaction Checking (B3) While (TempTableCount >0)  */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("IF @FinalErrorInsert ='Y' BREAK").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END of Outer While (B2) */").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END /* END (B38)*/").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN TRY /*BEGIN TRY - Capture error while updating ETL Control Table */").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Updating the ETLControlTable - ExecuteProcedureFlag to Y in case of Error encounter while inserting into # temp table */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("UPDATE ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".ETLControlTable")
                    .append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET IsProcedureRunningFlag = 'N' , NewRecordPresentFlag = 'N'").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("  WHERE StagingTableName = @SourceTableName").append(lineSeparator).append(lineSeparator);

            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("DROP TABLE #TempStagingTable").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END TRY /*END TRY - Capture error while updating ETL Control Table */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("BEGIN CATCH /*BEGIN CATCH - Capture error while updating ETL Control Table */").append(lineSeparator).append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = CONVERT(NVARCHAR,GETDATE(),21) + @ShortSpacing + @TextFileBeginingText + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingIDText + CONVERT(NVARCHAR,@StagingID) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileProcessedDatetimeText + CONVERT(NVARCHAR,@ODSProcessedDateTime,121) + @TextFileSeparator + @ShortSpacing").append(
                    lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileStagingTableText + @SourceTableName + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrorcodeText + CONVERT(NVARCHAR,ERROR_NUMBER()) + @TextFileSeparator + @ShortSpacing").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("SET @TextMessage = @TextMessage + @TextFileErrordescriptionText + ERROR_MESSAGE()").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("PRINT @TextMessage").append(lineSeparator).append(lineSeparator);

            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("/* Inserting into Error details to Error table in case of Error */").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName());
            stagingFileContentsBuffer.append(".DFCError ").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\t\t(STG_ID,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("\tTR_TRANSACTION_DATETIME,").append(lineSeparator);
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("StagingTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ErrorCode,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ErrorDescription)").append(lineSeparator);
            tabCount--;
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("VALUES(").append(lineSeparator);
            tabCount++;
            tabCount++;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@StagingID,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@ODSProcessedDatetime,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("@SourceTableName,").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ERROR_NUMBER(),").append(lineSeparator);
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("ERROR_MESSAGE()").append(lineSeparator);
            tabCount--;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("   )").append(lineSeparator).append(lineSeparator);
            tabCount =  tabCount- 3;
            stagingFileContentsBuffer.append(insertTabs(tabCount));
            stagingFileContentsBuffer.append("END CATCH /*END CATCH - Capture error while updating ETL Control Table */").append(lineSeparator);
            stagingFileContentsBuffer.append("	SET NOCOUNT OFF").append(lineSeparator);
            stagingFileContentsBuffer.append("END   /* END of Procedure(B1) Ends Here */");
            tabCount = 0;

            stagingFileContentsBuffer.append(lineSeparator);
            stagingFileContentsBuffer.append("GO").append(lineSeparator);
            stagingFileContentsBuffer.append(lineSeparator);
        	stagingFileContentsBuffer.append(lineSeparator);
        	stagingFileContentsBuffer.append(lineSeparator);
        	
            stagingFileContentsBuffer.append("USE ["+tableInformationBean.getSourceDatabaseName()+"]").append(lineSeparator);
        	stagingFileContentsBuffer.append("GO").append(lineSeparator);
        	stagingFileContentsBuffer.append(lineSeparator);
        	
        	//Create Procedure One Time Load For Staging
/*
            stagingFileContentsBuffer.append("CREATE PROC "+tableInformationBean.getSourceSchemaName()+".SPOTL_").append(tableInformationBean.getTargetTableName()).append(lineSeparator);
            stagingFileContentsBuffer.append("AS").append(lineSeparator);
        
        	List<ColumnInformationBean> ColumnInformationBeanList = tableInformationBean.getColumnInformationBeanList();
        	stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".").append("DFCError").append(lineSeparator);
        	stagingFileContentsBuffer.append("SELECT Stg_Id, GetDate(), '").append(tableInformationBean.getSourceTableName()).append("', 99997, 'Blank Data Or Wrong Date Format'").append(lineSeparator);
        	stagingFileContentsBuffer.append(" FROM ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append(tableInformationBean.getSourceTableName()).append(lineSeparator);
        	stagingFileContentsBuffer.append("WHERE  AS_TRANSACTION_DATETIME IS NULL").append(lineSeparator);
        	stagingFileContentsBuffer.append("AND TR_TRANSACTION_DATETIME IS NULL AND (").append(lineSeparator);
        	int numberOfColumns = ColumnInformationBeanList.size();
        	boolean isFirst = false;
        	for (int columnCount = 1; columnCount <= numberOfColumns; columnCount++) {
                 ColumnInformationBean ColumnInformationBean = ColumnInformationBeanList.get(columnCount - 1);
                if(ColumnInformationBean.isPrimaryKey()== true && ColumnInformationBean.getSourceDataType().equalsIgnoreCase("Nvarchar")){
             	   if (isFirst)
             		  stagingFileContentsBuffer.append(" OR ");
             	   isFirst = true;
             	  stagingFileContentsBuffer.append(" ISNULL("+ColumnInformationBean.getSourceColumnName()+",\'') = '' ").append(lineSeparator);
                }
               if(ColumnInformationBean.isPrimaryKey()== true && ColumnInformationBean.getTargetDataType().equalsIgnoreCase("Date")){
             	 if (isFirst)
             		stagingFileContentsBuffer.append(" OR ");
             	 isFirst = true;
             	stagingFileContentsBuffer.append("  "+tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append("[FNCDFCConvertDate"+ColumnInformationBean.getDateFunction()+"](["+ColumnInformationBean.getSourceColumnName()+"]) IS NULL").append(lineSeparator);
                }
              if(ColumnInformationBean.isPrimaryKey()== true && ColumnInformationBean.getTargetDataType().equalsIgnoreCase("Time")){
             	 if (isFirst)
             		stagingFileContentsBuffer.append(" OR ");
             	 isFirst = true;
             	stagingFileContentsBuffer.append("  "+tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append("[FNCDFCConvertTime"+ColumnInformationBean.getDateFunction()+"](["+ColumnInformationBean.getSourceColumnName()+"]) IS NULL").append(lineSeparator);
                }
            }
        	if (isFirst == false)
        	{
        		stagingFileContentsBuffer.append(" 1 = 2");
        	}
        	stagingFileContentsBuffer.append(")");
        	
        	stagingFileContentsBuffer.append(lineSeparator).append(lineSeparator).append("INSERT INTO ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".").append(tableInformationBean.getTargetTableName()+" WITH (TABLOCK)").append(lineSeparator);
        	stagingFileContentsBuffer.append("SELECT a.*").append(lineSeparator);
        	stagingFileContentsBuffer.append("FROM OPENROWSET('SQLNCLI', 'Server="+serverName+";Trusted_Connection=yes;\',").append(lineSeparator);
        	stagingFileContentsBuffer.append("'SELECT").append(lineSeparator);
        	
        	 
             if(null != ColumnInformationBeanList){
    	            numberOfColumns = ColumnInformationBeanList.size();
    	            for (int columnCount = 1; columnCount <= numberOfColumns; columnCount++) {
    	                ColumnInformationBean ColumnInformationBean = ColumnInformationBeanList.get(columnCount - 1);
    	                if(ColumnInformationBean.getDateFunction() != null && ColumnInformationBean.getTargetDataType().equalsIgnoreCase("Date")){
    	                	stagingFileContentsBuffer.append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append(" [FNCDFCConvertDate"+ColumnInformationBean.getDateFunction()+"](["+ColumnInformationBean.getSourceColumnName()+"])  AS "+ColumnInformationBean.getSourceColumnName()).append(",").append(lineSeparator);
    	                }else if(ColumnInformationBean.getDateFunction() != null && ColumnInformationBean.getTargetDataType().equalsIgnoreCase("Time")){
    	                	stagingFileContentsBuffer.append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append(" [FNCDFCConvertTime"+ColumnInformationBean.getDateFunction()+"](["+ColumnInformationBean.getSourceColumnName()+"])  AS "+ColumnInformationBean.getSourceColumnName()).append(",").append(lineSeparator);
    	                }
    	                else{
    	                	stagingFileContentsBuffer.append(ColumnInformationBean.getSourceColumnName()).append(",").append(lineSeparator);
    	                }
    	               
    	            }
    	            stagingFileContentsBuffer.append("[AS_TRANSACTION_DATETIME],").append(lineSeparator);
                stagingFileContentsBuffer.append("GETDATE()").append(lineSeparator);
                stagingFileContentsBuffer.append("FROM ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append(tableInformationBean.getSourceTableName()).append(lineSeparator);
                stagingFileContentsBuffer.append("WHERE  AS_TRANSACTION_DATETIME IS NULL").append(lineSeparator);
                stagingFileContentsBuffer.append("AND TR_TRANSACTION_DATETIME IS NULL").append(lineSeparator);
                for (int columnCount = 1; columnCount <= numberOfColumns; columnCount++) {
    	                ColumnInformationBean ColumnInformationBean = ColumnInformationBeanList.get(columnCount - 1);
    	               if(ColumnInformationBean.isPrimaryKey()== true && ColumnInformationBean.getSourceDataType().equalsIgnoreCase("Nvarchar")){
    	            	   stagingFileContentsBuffer.append("AND ISNULL("+ColumnInformationBean.getSourceColumnName()+",\'''') != '''' ").append(lineSeparator);
    	               }
    	              if(ColumnInformationBean.isPrimaryKey()== true && ColumnInformationBean.getTargetDataType().equalsIgnoreCase("Date")){
    	            	  stagingFileContentsBuffer.append("AND "+tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append("[FNCDFCConvertDate"+ColumnInformationBean.getDateFunction()+"](["+ColumnInformationBean.getSourceColumnName()+"]) IS NOT NULL").append(lineSeparator);
    	               }
    	             if(ColumnInformationBean.isPrimaryKey()== true && ColumnInformationBean.getTargetDataType().equalsIgnoreCase("Time")){
    	            	 stagingFileContentsBuffer.append("AND "+tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append("[FNCDFCConvertTime"+ColumnInformationBean.getDateFunction()+"](["+ColumnInformationBean.getSourceColumnName()+"]) IS NOT NULL").append(lineSeparator);
    	               }
                }
                
                stagingFileContentsBuffer.append("') AS a;").append(lineSeparator).append(lineSeparator);
                stagingFileContentsBuffer.append("UPDATE ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName()).append(".").append(tableInformationBean.getSourceTableName()).append(lineSeparator);
                stagingFileContentsBuffer.append("SET TR_TRANSACTION_DATETIME = GETDATE() ").append(lineSeparator);
                stagingFileContentsBuffer.append("WHERE AS_TRANSACTION_DATETIME IS NULL ").append(lineSeparator);
                stagingFileContentsBuffer.append("AND TR_TRANSACTION_DATETIME IS NULL").append(lineSeparator);

    	            }

            stagingFileContentsBuffer.append("GO");
            stagingFileContentsBuffer.append(lineSeparator);
         	stagingFileContentsBuffer.append(lineSeparator);
         	stagingFileContentsBuffer.append(lineSeparator);
             */
        	
        	//DFC TARGET TABLE 
        	
         	stagingFileContentsBuffer.append("USE ["+tableInformationBean.getTargetDatabaseName()+"]").append(lineSeparator);
        	stagingFileContentsBuffer.append("GO").append(lineSeparator);
        	
        	stagingFileContentsBuffer.append("CREATE TABLE ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".")
            .append(tableInformationBean.getTargetTableName()).append("(").append(lineSeparator);
        	List<ColumnInformationBean> columnInformationBeanListDFCTBL = tableInformationBean.getColumnInformationBeanList();

        	if(null != columnInformationBeanListDFCTBL)
		        	{
		        		int numberOfColumnsDFCTBL = columnInformationBeanListDFCTBL.size();
			        	for (int columnCount = 1; columnCount <= numberOfColumnsDFCTBL; columnCount++) 
			        	{
			        		ColumnInformationBean columnInformationBean = columnInformationBeanListDFCTBL.get(columnCount - 1);
			        		stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());
			        		stagingFileContentsBuffer.append(" ").append(columnInformationBean.getTargetDataType());
			        		String targetColumnLength = columnInformationBean.getTargetColumnLength();
				            if (targetColumnLength != null && !targetColumnLength.equals("")) {
				            	stagingFileContentsBuffer.append("(").append(targetColumnLength).append(")");
				            }
				            if (columnInformationBean.isPrimaryKey()) {
				            	stagingFileContentsBuffer.append(" NOT NULL");
				            }
				            stagingFileContentsBuffer.append(",");
				            stagingFileContentsBuffer.append(lineSeparator);
			        	}
				        stagingFileContentsBuffer.append("AS_TRANSACTION_DATETIME datetime NULL,").append(lineSeparator);
				        stagingFileContentsBuffer.append("TR_TRANSACTION_DATETIME datetime NULL").append(lineSeparator);
				        stagingFileContentsBuffer.append(");").append(lineSeparator);
				        stagingFileContentsBuffer.append("GO").append(lineSeparator);
				        stagingFileContentsBuffer.append("ALTER TABLE ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".")
				                .append(tableInformationBean.getTargetTableName()).append(" ADD CONSTRAINT PK_").append(tableInformationBean.getTargetTableName()).append(" PRIMARY KEY (");
				        boolean isFirstKeyDFCTBL = true;
				        for (int columnCount = 1; columnCount <= numberOfColumnsDFCTBL; columnCount++) {
				        	ColumnInformationBean columnInformationBean = columnInformationBeanListDFCTBL.get(columnCount - 1);
				        		if (columnInformationBean.isPrimaryKey()) {
				        			if (!isFirstKeyDFCTBL) {
				        				stagingFileContentsBuffer.append(",");
				        			}
				        			stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());
				        			isFirstKeyDFCTBL = false;
				        		}
				        }
			        stagingFileContentsBuffer.append(");").append(lineSeparator);
			        stagingFileContentsBuffer.append("GO").append(lineSeparator).append(lineSeparator);
		        }
        	
            stagingFileContentsBuffer.append(lineSeparator);
         	stagingFileContentsBuffer.append(lineSeparator);
         	stagingFileContentsBuffer.append(lineSeparator);
        
      //DFC TARGET DELETE TABLE 
        
        stagingFileContentsBuffer.append("CREATE TABLE ").append(tableInformationBean.getTargetDatabaseName()).append(".").append(tableInformationBean.getTargetSchemaName()).append(".").append("[")
        .append(tableInformationBean.getTargetTableName()).append("Deleted").append("]").append("(").append(lineSeparator);
        List<ColumnInformationBean> columnInformationBeanListDELTBL = tableInformationBean.getColumnInformationBeanList();
        if(null != columnInformationBeanListDELTBL){
        	int numberOfColumnsDELTBL = columnInformationBeanListDELTBL.size();
        	for (int columnCount = 1; columnCount <= numberOfColumnsDELTBL; columnCount++) {
        		ColumnInformationBean columnInformationBean = columnInformationBeanListDELTBL.get(columnCount - 1);
        		stagingFileContentsBuffer.append(columnInformationBean.getTargetColumnName());
        		stagingFileContentsBuffer.append(" ").append(columnInformationBean.getTargetDataType());
        		String targetColumnLength = columnInformationBean.getTargetColumnLength();
        		if (targetColumnLength != null && !targetColumnLength.equals("")) {
        			stagingFileContentsBuffer.append("(").append(targetColumnLength).append(")");
        		}
        		stagingFileContentsBuffer.append(",");
        		stagingFileContentsBuffer.append(lineSeparator);
        	}
        	stagingFileContentsBuffer.append("AS_TRANSACTION_DATETIME datetime NULL,").append(lineSeparator);
        	stagingFileContentsBuffer.append("TR_TRANSACTION_DATETIME datetime NULL").append(lineSeparator);
        	stagingFileContentsBuffer.append(");").append(lineSeparator);
        	stagingFileContentsBuffer.append("GO").append(lineSeparator);
        }
        
        
        stagingFileContentsBuffer.append(lineSeparator);
     	stagingFileContentsBuffer.append(lineSeparator);
     	stagingFileContentsBuffer.append(lineSeparator);
     	
        stagingFileContentsBuffer.append("INSERT INTO ").append(tableInformationBean.getSourceDatabaseName()).append(".").append(tableInformationBean.getSourceSchemaName());
        stagingFileContentsBuffer.append(".ETLControlTable (").append(lineSeparator);
        stagingFileContentsBuffer.append("\t\t  StagingTableName,");
        stagingFileContentsBuffer.append("ProcedureName,");
        stagingFileContentsBuffer.append("NewRecordPresentFlag,");
        stagingFileContentsBuffer.append("IsProcedureRunningFlag,");
        stagingFileContentsBuffer.append("ExecutionBatchSize)").append(lineSeparator);
        stagingFileContentsBuffer.append("   VALUES ");
        stagingFileContentsBuffer.append("(");
        stagingFileContentsBuffer.append("'").append(tableInformationBean.getSourceTableName()).append("'").append(",");
        stagingFileContentsBuffer.append("'").append("SP_DFC_Load").append(tableInformationBean.getTargetTableName()).append("'").append(",");
        stagingFileContentsBuffer.append("'Y',");
        stagingFileContentsBuffer.append("'N',");
        stagingFileContentsBuffer.append("100)").append(lineSeparator).append(lineSeparator).append(lineSeparator);
        stagingFileContentsBuffer.append("GO").append(lineSeparator);
        
        stagingFileContentsBuffer.append(lineSeparator);
     	stagingFileContentsBuffer.append(lineSeparator);
     	stagingFileContentsBuffer.append(lineSeparator);
     	
        stagingFileContentsBuffer.append("USE [msdb]").append(lineSeparator);
        stagingFileContentsBuffer.append("GO").append(lineSeparator);
        stagingFileContentsBuffer.append("BEGIN TRANSACTION").append(lineSeparator);
        stagingFileContentsBuffer.append("DECLARE @ReturnCode INT").append(lineSeparator);
        stagingFileContentsBuffer.append("SELECT @ReturnCode = 0").append(lineSeparator);
        stagingFileContentsBuffer.append("IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)").append(lineSeparator);
        stagingFileContentsBuffer.append("BEGIN").append(lineSeparator);
        stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
        stagingFileContentsBuffer.append("END").append(lineSeparator);
        stagingFileContentsBuffer.append("DECLARE @jobId BINARY(16)").append(lineSeparator);
        stagingFileContentsBuffer.append("EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'").append(resourceBundle.getString("autoscriptGenerator.postFixFileName")).append(tableInformationBean.getSourceDatabaseName().substring(0,3)).append(tableInformationBean.getSourceTableName()+"',").append(lineSeparator);
        stagingFileContentsBuffer.append("@enabled=1,").append(lineSeparator);
        stagingFileContentsBuffer.append("@notify_level_eventlog=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@notify_level_email=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@notify_level_netsend=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@notify_level_page=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@delete_level=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@description=N'No description available.',").append(lineSeparator);
		stagingFileContentsBuffer.append("@category_name=N'[Uncategorized (Local)]',").append(lineSeparator);
		stagingFileContentsBuffer.append("@owner_login_name=N'dfcuser', @job_id = @jobId OUTPUT").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
		
		stagingFileContentsBuffer.append("EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ExecuteProcedure', ").append(lineSeparator);
		stagingFileContentsBuffer.append("@step_id=1,").append(lineSeparator);
		stagingFileContentsBuffer.append("@cmdexec_success_code=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_success_action=4,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_success_step_id=2,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_fail_action=4,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_fail_step_id=2,").append(lineSeparator);
		stagingFileContentsBuffer.append("@retry_attempts=0, ").append(lineSeparator);
		stagingFileContentsBuffer.append("@retry_interval=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@os_run_priority=0, @subsystem=N'TSQL',").append(lineSeparator);
		stagingFileContentsBuffer.append("@command=N'USE "+tableInformationBean.getSourceDatabaseName()).append(lineSeparator);
		stagingFileContentsBuffer.append("DECLARE @VarNewRecordPresentFlag VARCHAR(1),").append(lineSeparator);
		stagingFileContentsBuffer.append("@VarIsProcedureRunningFlag VARCHAR(1),").append(lineSeparator);
		stagingFileContentsBuffer.append("@VarProcedureName VARCHAR(100),").append(lineSeparator);
		stagingFileContentsBuffer.append("@VarBatchSize VARCHAR(100),").append(lineSeparator);
		stagingFileContentsBuffer.append("@VarProcName VARCHAR(100)").append(lineSeparator);
		stagingFileContentsBuffer.append("SELECT @VarNewRecordPresentFlag = ISNULL(NewRecordPresentFlag,''Z''),@VarIsProcedureRunningFlag = ISNULL(IsProcedureRunningFlag,''Z''),").append(lineSeparator);
		stagingFileContentsBuffer.append("@VarProcedureName =  ISNULL(ProcedureName,''Z''), @VarBatchSize = ISNULL(ExecutionBatchSize,''0'')").append(lineSeparator);
		stagingFileContentsBuffer.append(" FROM Staging.ETLControlTable WITH (NOLOCK)").append(lineSeparator);
		stagingFileContentsBuffer.append("WHERE StagingTablename = ''"+tableInformationBean.getSourceTableName()+"''").append(lineSeparator);
		stagingFileContentsBuffer.append("IF   @VarNewRecordPresentFlag = ''Y'' AND @VarIsProcedureRunningFlag = ''N''").append(lineSeparator);
		stagingFileContentsBuffer.append("BEGIN ").append(lineSeparator);
		stagingFileContentsBuffer.append("SET @VarProcName =  ''Staging.'' + LTrim(@VarProcedureName) + ''  '' + convert(Varchar,@VarBatchSize)").append(lineSeparator);
		stagingFileContentsBuffer.append("EXEC ( @VarProcName ) ").append(lineSeparator);
		stagingFileContentsBuffer.append("END',").append(lineSeparator);
		stagingFileContentsBuffer.append("@database_name=N'"+tableInformationBean.getSourceDatabaseName()+"',").append(lineSeparator);
		//stagingFileContentsBuffer.append("@output_file_name=N'").append(resourceBundle.getString("autoscriptGenerator.jobsOutPutFilePath")).append(tableInformationBean.getSourceDatabaseName().substring(0,3)).append(tableInformationBean.getSourceTableName()).append(".txt',").append(lineSeparator);
		stagingFileContentsBuffer.append("@flags=2").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
		
		stagingFileContentsBuffer.append("EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CallProcedureStep',").append(lineSeparator);
		stagingFileContentsBuffer.append("@step_id=2,").append(lineSeparator);
		stagingFileContentsBuffer.append("@cmdexec_success_code=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_success_action=4,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_success_step_id=1,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_fail_action=4,").append(lineSeparator);
		stagingFileContentsBuffer.append("@on_fail_step_id=1,").append(lineSeparator);
		stagingFileContentsBuffer.append("@retry_attempts=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@retry_interval=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@os_run_priority=0, @subsystem=N'TSQL',").append(lineSeparator);
		stagingFileContentsBuffer.append("@command=N'WAITFOR DELAY ''00:00:10''").append(lineSeparator);
		stagingFileContentsBuffer.append("SELECT 0',").append(lineSeparator);
		stagingFileContentsBuffer.append("@database_name=N'"+tableInformationBean.getSourceDatabaseName()+"', ").append(lineSeparator);
		stagingFileContentsBuffer.append("@flags=0").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
		stagingFileContentsBuffer.append("EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
		stagingFileContentsBuffer.append("EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'"+tableInformationBean.getSourceDatabaseName().substring(0,3)+tableInformationBean.getSourceTableName()+"', ").append(lineSeparator);
		stagingFileContentsBuffer.append("@enabled=1,").append(lineSeparator);
		stagingFileContentsBuffer.append("@freq_type=64,").append(lineSeparator);
		stagingFileContentsBuffer.append("@freq_interval=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@freq_subday_type=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@freq_subday_interval=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@freq_relative_interval=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@freq_recurrence_factor=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@active_start_date=20130514, ").append(lineSeparator);
		stagingFileContentsBuffer.append("@active_end_date=99991231,").append(lineSeparator);
		stagingFileContentsBuffer.append("@active_start_time=0,").append(lineSeparator);
		stagingFileContentsBuffer.append("@active_end_time=235959,").append(lineSeparator);
		stagingFileContentsBuffer.append("@schedule_uid=N'eb4e12ee-1976-41f7-b129-21a42f590a2f'").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
		stagingFileContentsBuffer.append("EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback").append(lineSeparator);
		stagingFileContentsBuffer.append("COMMIT TRANSACTION").append(lineSeparator);
		stagingFileContentsBuffer.append("GOTO EndSave").append(lineSeparator);
		stagingFileContentsBuffer.append("QuitWithRollback:").append(lineSeparator);
		stagingFileContentsBuffer.append("IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION").append(lineSeparator);
		stagingFileContentsBuffer.append("EndSave:").append(lineSeparator);
		stagingFileContentsBuffer.append("GO").append(lineSeparator);
		stagingFileContentsBuffer.append(lineSeparator);
		stagingFileContentsBuffer.append(lineSeparator);
		
		if(RunSQLAgentJobs.equalsIgnoreCase("y")){
			stagingFileContentsBuffer.append("Exec msdb.dbo.sp_start_job '"+ tableInformationBean.getSourceDatabaseName().substring(0,3)).append(tableInformationBean.getSourceTableName()+"'");
			stagingFileContentsBuffer.append(lineSeparator);
			stagingFileContentsBuffer.append("GO").append(lineSeparator);
			stagingFileContentsBuffer.append(lineSeparator);
			stagingFileContentsBuffer.append(lineSeparator);

		}
       
		
           }
        StringBuffer triggerStagFileName = new StringBuffer(outputPath).append("SQLServerDDLScript");
        writeToFile(triggerStagFileName.toString(), stagingFileContentsBuffer);
        stagingFileContentsBuffer = null;
        }
	}
