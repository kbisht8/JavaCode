package com.polarisft.tool.impl.trigger;
import com.polarisft.tool.trigger.ScriptLogger;

import org.apache.poi.ss.formula.functions.TextFunction;
import java.awt.Font;
import java.io.*;
import java.sql.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import javax.print.attribute.standard.Severity;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFPalette;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.CellRangeAddress;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import com.ibm.as400.resource.Resource;
import com.ibm.db2.jcc.DB2Types;
import com.polarisft.tool.resources.StaticContainers;
import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.FileReader;
import com.polarisft.tool.trigger.ScriptLogger;
import com.polarisft.tool.trigger.bean.BatchScriptBean;
import com.polarisft.tool.trigger.bean.TableInformationBean;
import com.polarisft.tool.trigger.factory.FileReaderFactory;
import com.polarisft.tool.trigger.factory.ScriptTemplateFactory;
import com.sun.media.sound.InvalidFormatException;
import com.sun.xml.internal.fastinfoset.sax.Properties;
import com.sun.xml.internal.messaging.saaj.soap.GifDataContentHandler;

//import com.ibm.db2.jcc.am.CallableStatement;
public class GenerateMappingSheet {
	static Logger scriptLoger;
	private static	Map<String,String> staticMap;
	private static StaticContainers staticContainers;
	private static String SQLServerHostName="";
	private static String SQLServerUserName="";
	private static String SQLServerPassword="";
	private static String StagingDBName="";
	private static String StagingDBSchemaName="";
	private static String GDCDBName="";
	private static String GDCDBSchemaName="";
	private static String DB2UserName="";
	private static String DB2Password="";
	private static String DB2HostName="";
	public static String IsMappingSheet;
	public static String IsOneTimeLoad;
	private static String DeployOneTimeSetupScript="";
	private static String DeploySQLServerObjects="";
	private static String DDLScriptOutputPath="";
	private static String DFCEnvironment="";
	static Set<String> keySet ;
	static ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
	public static void main(String  XLSFileName) throws Exception 
    { 
		scriptLoger=new ScriptLogger().getScriptLogger();
		staticMap = new HashMap<String,String>();
		staticMap = staticContainers.getMyMap();
		SetGlobalVariables();
		ReadExcel(XLSFileName);
    } 
	private static void SetGlobalVariables(){
		SQLServerHostName=staticMap.get("serverName");
		SQLServerUserName=staticMap.get("SQLServerUserName");
		SQLServerPassword=staticMap.get("SQLServerPassword");
		DB2UserName=staticMap.get("DB2UserName");
		DB2Password=staticMap.get("DB2Password");
		StagingDBName=staticMap.get("MSSQLStagingDBName");
		StagingDBSchemaName=staticMap.get("StagingSchemaName");
		GDCDBName=staticMap.get("MSSQLGDCDBName");
		GDCDBSchemaName=staticMap.get("GDCSchemaName");
		DB2HostName=staticMap.get("DB2HostName");
		DeployOneTimeSetupScript=staticMap.get("DeployOneTimeSetupScript");
		DeploySQLServerObjects=staticMap.get("DeploySQLServerObjects");
		DDLScriptOutputPath=staticMap.get("DDLScriptOutputPath");
		DFCEnvironment=staticMap.get("DFCEnvironment");
	}
	private static boolean DeploySQLDDL(){
		String lineSeparator = System.getProperty("line.separator");
		StringBuffer batchFileContentsBuffer = null;
		String finalString;
		ResourceBundle resourceBundle2 = ResourceBundle.getBundle("com.polarisft.tool.resources.BatchScriptResource", Locale.getDefault());
		 keySet = resourceBundle2.keySet();
		 @SuppressWarnings("unchecked")
		 Set<String> sortedKeys = new TreeSet(keySet);
		 Iterator<String> keysIterator = sortedKeys.iterator();

		boolean isCompleted = true;
			while(keysIterator.hasNext()){
				String keys = keysIterator.next();
				if(isCompleted){
					isCompleted = false;
				}
				if(keys.contains("Statement")){
					
					String statment = resourceBundle2.getString(keys);
					finalString=statment.replace("*SQLS*", staticMap.get("TargetSQLServerHostName"));
					finalString=finalString.replace("*SQLU*", staticMap.get("TargetSQLServerUserName"));
					finalString=finalString.replace("*SQLP*", staticMap.get("TargetSQLServerPassword"));
					finalString=finalString.replace("OneTimeSetupScript",DDLScriptOutputPath +"OneTimeSetupScript");
					finalString=finalString.replace("SQLServerDDLScript",DDLScriptOutputPath +"SQLServerDDLScript");
					Runtime rt=Runtime.getRuntime();
					try {
						if(finalString.contains("OneTimeSetupScript") && DeployOneTimeSetupScript.equalsIgnoreCase("y")){
							rt.exec(new String[]{"cmd.exe","/c",finalString});	
						}
						if(finalString.contains("SQLServerDDLScript") && DeploySQLServerObjects.equalsIgnoreCase("y")){
							rt.exec(new String[]{"cmd.exe","/c",finalString});	
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else if(keys.contains("isSourceOrTarget")){
					isCompleted = true;
				}
				if(isCompleted){
					isCompleted = false;
				}	
			}
		return true;
		}
	
     
	private static ResultSet GetResultSet(String SQLQuery) throws Exception
	{
		 //String ConString=resourceBundle.getString("autoscriptGenerator.SQLConnectionString");
		  String ConString=staticMap.get("SQLServerConnectionString");
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);  
	      ResultSet rs = stmt.executeQuery(SQLQuery);
	      return rs;
	}
	
	private static ResultSet GetAS400KeyColumns(String AS400FileName) throws Exception
	{
		  //String ConString=resourceBundle.getString("autoscriptGenerator.SQLConnectionString");
		  String ConString=staticMap.get("SQLServerConnectionString");
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);  
	      String QueryText;
	      QueryText="Select Distinct AS400FileColumnName From AS400FileKeyInformation Where AS400FileName='"+AS400FileName+"'";
	      ResultSet rs = stmt.executeQuery(QueryText);
	      return rs;
	}
	
	
	private static String ProperCase(String inputString) throws java.io.IOException{
		 
		java.io.StringReader in = new java.io.StringReader(inputString.toLowerCase());
		 boolean precededBySpace = true;
		 StringBuffer properCase = new StringBuffer();    
		     while(true) {      
		 	int i = in.read();
		 	  if (i == -1)  break;      
		 	    char c = (char)i;
		 	    if (c == ' ' || c == '"' || c == '(' || c == '.' || c == '/' || c == '\\' || c == ',') {
		 	      properCase.append(c);
		 	      precededBySpace = true;
		 	   } else {
		 	      if (precededBySpace) { 
		 		 properCase.append(Character.toUpperCase(c));
		 	   } else { 
		 	         properCase.append(c); 
		 	   }
		 	   precededBySpace = false;
		 	}
		    }
		     
		return properCase.toString().replace(" ", "");    
		 
	}
	
	private static ResultSet GetDFCMetaData(String AS400FileName,String AS400Library) throws Exception
	{
		  String ConString=staticMap.get("SQLServerConnectionString");
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);  
	      String QueryText;
	      QueryText="Select Distinct AS400FileColumnName,GDCColumnName from DataDictionary Where AS400Library='"+AS400Library+"' And AS400FileName='"+AS400FileName+"'";
	      ResultSet rs = stmt.executeQuery(QueryText);
	      return rs;
	}
	
	private static void ReadExcel(String XLSFileName) throws Exception
	{
		try
		{
			String AS400LibraryName="";
			String AS400FileName ="";
			String GDCTableName="";
			String OneTimeLoadRequired="";
			String SetCTLDTL="";
			String EnableDataFileReplication="";
			String BufferPriority="";
			String AS400KeyFileName="";
			
			scriptLoger.info("Start Reading Index Sheet");			
			Workbook MasterSheet = WorkbookFactory.create(new FileInputStream(XLSFileName));
			Sheet indexSheet = MasterSheet.getSheetAt(0);
				//Create Mapping Sheet
				CellReference cellReference = new CellReference("B3");
				Row rowIndex = indexSheet.getRow(cellReference.getRow());
				Cell cellIndex = rowIndex.getCell(cellReference.getCol());
				if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
				{
					Iterator<Row> indesxSheetItr = indexSheet.iterator();
					while(indesxSheetItr.hasNext()){
						Row indexSheetRow = indesxSheetItr.next();
						AS400LibraryName=getCellValue(indexSheetRow.getCell(6));
						AS400FileName=getCellValue(indexSheetRow.getCell(2));
						GDCTableName=getCellValue(indexSheetRow.getCell(3));
						SetCTLDTL=getCellValue(indexSheetRow.getCell(8));
						EnableDataFileReplication=getCellValue(indexSheetRow.getCell(9));
						BufferPriority=getCellValue(indexSheetRow.getCell(10));
						AS400KeyFileName=getCellValue(indexSheetRow.getCell(11));
						if(getCellValue(indexSheetRow.getCell(8))!=null && !getCellValue(indexSheetRow.getCell(8)).isEmpty() && getCellValue(indexSheetRow.getCell(8)).equalsIgnoreCase("Y")){
							String JobName="";
							JobName=AS400LibraryName.substring(0, 2) + DFCEnvironment.trim()+ StagingDBName.substring(0, 3).trim()+"J"+BufferPriority.trim()+"1";
							SetupDataFileCTLDTL(JobName, AS400LibraryName ,AS400FileName,AS400KeyFileName);
						}

						if(getCellValue(indexSheetRow.getCell(5)) != null && !getCellValue(indexSheetRow.getCell(5)).isEmpty() && getCellValue(indexSheetRow.getCell(5)).equalsIgnoreCase("y")){
							scriptLoger.info("Generate Mapping Sheet For " + AS400LibraryName + "." +AS400FileName + ":STARTED" );
							GetSheetDetails(XLSFileName, AS400LibraryName, AS400FileName,GDCTableName);
							scriptLoger.info("Generate Mapping Sheet For " + AS400LibraryName + "." +AS400FileName + ":COMPLETED" );
							}
						
					}
				}
				
				
				//Create DDL Script
				scriptLoger.info("Start Creating DDL Objects:STARTED");
				List<TableInformationBean> tableInformationBeanList = null;
				FileReader fileReader = FileReaderFactory.create(XLSFileName);
				tableInformationBeanList = fileReader.getTableDetails();
				String outputDir = staticMap.get("DDLScriptOutputPath");
				int[] executionOptions = null;
				executionOptions = new int[] { 
                		ScriptTemplateFactory.GENERATE_ONE_TIME_SETUP_SCRIPT,
                		ScriptTemplateFactory.GENERATE_SQL_DDL,
                        //ScriptTemplateFactory.BATCH_FILE_GENERATOR
                  };
				if(tableInformationBeanList.size()>0){
					for (int optionCount = 0; optionCount < executionOptions.length; optionCount++) {
	                    AbstractScriptTemplate triggerTemplate = ScriptTemplateFactory.create(executionOptions[optionCount]);
	                    
	                    try {
	                        triggerTemplate.generateScript(tableInformationBeanList, outputDir);
	                        scriptLoger.info("files will be copied to :" + outputDir);
	                    } catch (Exception exp) {
	                    	exp.printStackTrace();
	                    	scriptLoger.warning(exp.getMessage());
	                        System.out.println(resourceBundle.getString("autoscriptGenerator.fileWriteError"));
	                        continue;
	                    }
	                }
				}
				scriptLoger.info("Start Creating DDL Objects:COMPLETED");
				
				//Deploy DDL & One Time Setup Tables
				if(DeployOneTimeSetupScript.equalsIgnoreCase("y") || DeploySQLServerObjects.equalsIgnoreCase("y")){
					scriptLoger.info("DDL Object Deployment Stated:STARTED");
					DeploySQLDDL();
					scriptLoger.info("DDL Object Deployment Stated:COMPLETED");
				}	
				
				//Run One Time Load Setup
				scriptLoger.info("One time load :STARTED");
					cellReference = new CellReference("B3");
					rowIndex = indexSheet.getRow(cellReference.getRow());
					cellIndex = rowIndex.getCell(cellReference.getCol());
					if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
					{
						Iterator<Row> indesxSheetItr = indexSheet.iterator();
						while(indesxSheetItr.hasNext()){
							Row indexSheetRow = indesxSheetItr.next();
							AS400LibraryName=getCellValue(indexSheetRow.getCell(6));
							AS400FileName=getCellValue(indexSheetRow.getCell(2));
							OneTimeLoadRequired=getCellValue(indexSheetRow.getCell(7));
							EnableDataFileReplication=getCellValue(indexSheetRow.getCell(9));
							BufferPriority=getCellValue(indexSheetRow.getCell(10));
							AS400KeyFileName=getCellValue(indexSheetRow.getCell(11));
							if(getCellValue(indexSheetRow.getCell(9))!=null && !getCellValue(indexSheetRow.getCell(9)).isEmpty() && getCellValue(indexSheetRow.getCell(9)).equalsIgnoreCase("Y")){
								String JobName=AS400LibraryName.substring(0, 2) + DFCEnvironment.trim()+ StagingDBName.substring(0, 3).trim()+"J"+BufferPriority.trim()+"1";
								EnableReplicatonDataFile(JobName, AS400LibraryName,AS400FileName);	
							}
							
							
							if(OneTimeLoadRequired!=null && OneTimeLoadRequired.equalsIgnoreCase("y")){
								scriptLoger.info(AS400LibraryName+"."+AS400FileName +" file will be processed for one time load");
								GetOneTimeLoadFile(AS400LibraryName,AS400FileName);
								scriptLoger.info(AS400LibraryName+"."+AS400FileName +" file processed successfully.");
							}
						}
					}
					scriptLoger.info("One time load :COMPLETED");
		}	
			
		catch(IOException e)
		{
			e.printStackTrace();
		}
		
	}
	
	private static void GetSheetDetails(String XLSFileName, String AS400LibraryName, String AS400FileName, String GDCTableName) throws Exception 
    { 
		
	  String ConString="jdbc:as400://"+	DB2HostName.trim()+"/"+DB2HostName.trim();
      Class.forName("com.ibm.as400.access.AS400JDBCDriver");
      Connection  conn = DriverManager.getConnection(ConString,DB2UserName,DB2Password); 
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);  
      String QueryText;
      QueryText="SELECT Trim(COLUMN_TEXT)  Description, 'NVARCHAR'  "
      		+ " StagingAttribute,CASE WHEN DATA_TYPE IN('DECIMAL','NUMERIC') THEN CAST(LENGTH AS NUMERIC(4))+1 ELSE CAST(LENGTH AS NUMERIC(4)) END  AS LENGTH, COLUMN_NAME"  
      		+ " StagingColumnName,"
      		+ " Case When COLUMN_TEXT='User' Then 'UserId' When COLUMN_TEXT='System' Then 'SystemId' When COLUMN_TEXT='Transaction' Then 'TransactionId' When COLUMN_TEXT='Order' Then 'OrderNumber' "
      		+ " When COLUMN_TEXT='Drop' Then 'DropOrder' When COLUMN_TEXT='Return' Then 'ReturnOrderNumber' When COLUMN_TEXT='Batch' Then 'BatchNumber' When COLUMN_TEXT='Date' Then 'DateValue' Else "
      		+ " replace(replace(replace(replace(replace(replace(Replace(Replace(Replace(Trim(COLUMN_TEXT),'',''),'(',''),')',''),'-',''),'''',''),'%',''),'/',''),'\',''),'#','S') End AS DFCColumnName,"
      		+ " CASE WHEN DATA_TYPE IN('CHAR','VARCHAR','NCHAR','GRAPHIC') THEN 'NVARCHAR'" 
      		+ " WHEN COLUMN_TEXT LIKE '%DATE%' THEN 'DATE' WHEN COLUMN_TEXT LIKE '%TIME%' THEN 'TIME'"
      		+ " WHEN DATA_TYPE ='DECIMAL' THEN 'NUMERIC' ELSE DATA_TYPE END AS DFCAttribute,"
      		+ " CASE WHEN COLUMN_TEXT like '%DATE%' then '' when COLUMN_TEXT like '%TIME%' then ''"
      		+ " WHEN DATA_TYPE IN('DECIMAL','NUMERIC')"   
      		+ " AND NUMERIC_SCALE>0 THEN CAST(LENGTH AS VARCHAR(5)) ||','||CAST(NUMERIC_SCALE AS VARCHAR(5))"
      		+ " ELSE CAST(LENGTH AS VARCHAR(10)) END AS DFCLENGTH,"
      		+ " CASE WHEN DATA_TYPE IN('DECIMAL','NUMERIC') OR   COLUMN_TEXT LIKE '%DATE' OR   COLUMN_TEXT LIKE '%TIME' THEN 'Yes'"
      		+ " ELSE 'None' END AS Conversion, 'Y' Required,"
      		+ " CASE WHEN  COLUMN_TEXT LIKE '%DATE%' OR  COLUMN_TEXT LIKE '%TIME' THEN 'Y' ELSE 'N' END AS IsDate,"
      		+ " CASE WHEN COLUMN_TEXT like '%DATE%' THEN CASE WHEN LENGTH=6 THEN 'CYYYYMM'" 
      		+ " WHEN LENGTH=7 THEN 'CYYMMDD'  ELSE '' END 	WHEN COLUMN_TEXT like '%TIME%' THEN"
      		+ " CASE WHEN LENGTH=6 THEN 'HHMMSS' ELSE '' END 	ELSE '' END AS DATECONVERSION"
      		+ " FROM QSYS2.SYSCOLUMNS WHERE TABLE_NAME = '"+AS400FileName.trim()+"' AND SYSTEM_TABLE_SCHEMA='"+AS400LibraryName.trim()+"'"
      		+ " ORDER BY ORDINAL_POSITION";
      
      	ResultSet rs = stmt.executeQuery(QueryText);
      	short i=0;
		while (rs.next()){
			i++;
			break;
		}
		if(i>0){
		scriptLoger.info("Sheet "+AS400FileName+" Is Being Processed.");
      	GenerateSheet(rs, XLSFileName, AS400FileName, GDCTableName, AS400LibraryName );
      	
      	
		}
		if(rs!=null){
      	rs.close();
      	}
      	if(stmt!=null)	{
      		stmt.close();
      	}
      	if(conn!=null){
      		conn.close();
      	}
      	
    } 

	//NewCode
	private static String GetCountryCode(String CountryCodePrefix ) throws Exception{
		String CountryCode="";
		String Query="Select CountryCode From CountryCodeMaster Where CountryPrefix='"+CountryCodePrefix.trim()+"'";
		ResultSet rs=GetResultSet(Query);
		while(rs.next()){
			CountryCode=rs.getString(1);
			break;
		}
		rs.close();
		return CountryCode;
	}
	private static boolean GenerateOneTimeLoadFile(String AS400LibraryName, String AS400FileName) throws Exception{
		  String Query="";
		  boolean RetValue=true;
		  String ConString="jdbc:as400://"+	DB2HostName+"/"+DB2HostName.trim();
		  
	      Class.forName("com.ibm.as400.access.AS400JDBCDriver");
	      Connection  conn = DriverManager.getConnection(ConString,DB2UserName,DB2Password);
	      
	      if (DB2HostName.toString().equalsIgnoreCase("AMWMAL")) {
	    	  Query="{Call RP"+DFCEnvironment.trim()+"DFCMAL.SPDOWNLOADFILE (";

	      } else {    
	    	  Query="{Call RP"+DFCEnvironment+"DFC"+StagingDBName.substring(0, 3).trim() +".SPDOWNLOADFILE (";
	      }
	      
	      Query=Query +"?,?,?,?,?,?,?)}";
	      try{
	    	    CallableStatement stmt = conn.prepareCall(Query);
	    	  	stmt.setString(1, StagingDBName.substring(0, 3).trim());
	    	  	stmt.setString(2, AS400FileName.trim());
	    	  	stmt.setString(3, AS400LibraryName.trim());
	    	  	stmt.setString(4,GetCountryCode(StagingDBName.substring(0, 3).trim()));
	    	    stmt.registerOutParameter("FLAG", Types.VARCHAR);
	    	    stmt.registerOutParameter("ERRORCODE", Types.VARCHAR);
	    	    stmt.registerOutParameter("ERRORDESCRIPTION", Types.VARCHAR);
	    	    stmt.execute();
	      }
	      catch (Exception ex){
	    	  RetValue=false;
	      }
	      return RetValue;
	}
	
	private static boolean SetupDataFileCTLDTL(String JobName, String AS400LibraryName, String AS400FileName,String KeyFileName) throws Exception{
		  String Query="";
		  boolean RetValue=true;
		  String ConString="jdbc:as400://"+	DB2HostName+"/"+DB2HostName.trim();
	      Class.forName("com.ibm.as400.access.AS400JDBCDriver");
	      Connection  conn = DriverManager.getConnection(ConString,DB2UserName,DB2Password); 
	      Query="{Call RPQADFC"+StagingDBName.substring(0, 3).trim() +".SPSETUPDATAFILE (";
	      Query=Query +"?,?,?,?)}";
	      try{
	    	    CallableStatement stmt = conn.prepareCall(Query);
	    	  	stmt.setString(1, JobName.trim());
	    	  	stmt.setString(2, AS400FileName.trim());
	    	  	stmt.setString(3, AS400LibraryName.trim());
	    	  	stmt.setString(4,KeyFileName.trim());
	    	  	stmt.registerOutParameter("FLAG", Types.VARCHAR);
	    	    stmt.registerOutParameter("ERRORCODE", Types.VARCHAR);
	    	    stmt.registerOutParameter("ERRORDESCRIPTION", Types.VARCHAR);
	    	    stmt.execute();
	      }
	      catch (Exception ex){
	    	  RetValue=false;
	      }
	      return RetValue;
	}
	
	private static boolean EnableReplicatonDataFile(String JobName, String AS400LibraryName, String AS400FileName) throws Exception{
		  String Query="";
		  boolean RetValue=true;
		  String ConString="jdbc:as400://"+	DB2HostName+"/"+DB2HostName.trim();
	      Class.forName("com.ibm.as400.access.AS400JDBCDriver");
	      Connection  conn = DriverManager.getConnection(ConString,DB2UserName,DB2Password); 
	      Query="{Call RPQADFC"+StagingDBName.substring(0, 3).trim() +".SPENABLEREPLICATIONDATAFILE (";
	      Query=Query +"?,?,?)}";
	      try{
	    	    CallableStatement stmt = conn.prepareCall(Query);
	    	  	stmt.setString(1, JobName.trim());
	    	  	stmt.setString(2, AS400FileName.trim());
	    	  	stmt.setString(3, AS400LibraryName.trim());
	    	    stmt.execute();
	      }
	      catch (Exception ex){
	    	  RetValue=false;
	      }
	      return RetValue;
	}
	
	private static boolean GetCSVFileFTP(String AS400FileName, String CountryPrefix) throws Exception
	{
		  boolean retValue=true;
		  try
		  {
		  String ConString=staticMap.get("SQLServerConnectionString");
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement();  
	      String QueryText;
	      String ColHeader=GetColumnHeader(AS400FileName);
	      QueryText="Exec DFCMetaData.Dbo.Usp_GetFTPFile '"+DB2HostName+"','"+AS400FileName+"','"+staticMap.get("DDLScriptOutputPath")+"','"+DB2UserName+"','"+DB2Password+"','"+CountryPrefix+"','"+ColHeader+"'";
	      stmt.executeQuery(QueryText);
	      stmt.close();
	      conn.close();
		  }
		  catch(Exception ex)
		  {
			  retValue=false;
		  }
		  return retValue;
	}
	
	private static String GetColumnHeader(String AS400FileName)
	{
		String ColumnHeader="";
		 try
		  {
		  String ConString=staticMap.get("TargetSQLServerConnectionString");
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);  
	      String QueryText;
	      QueryText="USE "+ StagingDBName +";WAITFOR DELAY '00:00:10'; SELECT NAME+'^|' FROM SYS.COLUMNS WHERE OBJECT_ID=OBJECT_ID('STAGING.STG_"+AS400FileName.trim()+"') AND NAME NOT IN ('TRANSACTION_TYPE','WM_TRANSACTION_DATETIME','AS_TRANSACTION_DATETIME','PRIORITY','STG_TRANSACTION_DATETIME','TR_TRANSACTION_DATETIME','STG_ID') ORDER BY COLUMN_ID FOR XML PATH ('')";
	      ResultSet rs = stmt.executeQuery(QueryText);
	      rs.beforeFirst();
	      while (rs.next()){
	    	  ColumnHeader=	rs.getString(1);
	    	  break;
	      }
	      stmt.close();
	      conn.close();
		  }
		  catch(Exception ex)
		  {
			  ColumnHeader="";
		  }
		return ColumnHeader;
	}

	
	private static boolean ExecuteSQLQuery(String Query) throws Exception
	{
		  boolean retValue=true;
		  try
		  {
		  String ConString=staticMap.get("SQLServerConnectionString");
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement();  
	      stmt.executeQuery(Query);
		  }
		  catch(Exception ex)
		  {
			  retValue=false;
		  }
		  return retValue;
	}
	
	
	private static void GenerateSheet(ResultSet rs, String XLSFileName, String AS400FileName, String GDCTableName, String AS400LibraryName) throws Exception 
	{
		
		try {
			HSSFWorkbook workbook = null;
		    File file = new File(XLSFileName);
		    FileInputStream fis=null;
		    if (file.exists())
		    {
		    	fis = new FileInputStream(file);
		    }
		    else
		    {
		    	file.createNewFile();
		    	fis = new FileInputStream(file);
		    }
			workbook= new HSSFWorkbook(fis);
		    HSSFSheet worksheet=null;
		    
		    int controlSheetIndex = workbook.getSheetIndex(AS400FileName);
            if (controlSheetIndex != -1)
            {
            	scriptLoger.info("Sheet "+AS400FileName+" Already Exists. Will Be Removed.");
                workbook.removeSheetAt(controlSheetIndex);
            }

	        worksheet = workbook.createSheet(AS400FileName);
		    
			SetColorPalette(workbook);
			
			HSSFRow rowDetail = worksheet.createRow((short) 7);
			HSSFRow row0 = worksheet.createRow((short) 0);
			HSSFRow row1 = worksheet.createRow((short) 1);
			HSSFRow row2 = worksheet.createRow((short) 2);
			HSSFRow row3 = worksheet.createRow((short) 3);
			HSSFRow row4 = worksheet.createRow((short) 4);
			HSSFRow row5 = worksheet.createRow((short) 5); //HeaderRow
			
			HSSFCell cellA0 = row0.createCell((short) 0);
			HSSFCell cellA1 = row1.createCell((short) 0);
			HSSFCell cellA2 = row2.createCell((short) 0);
			HSSFCell cellA3 = row3.createCell((short) 0);
			HSSFCell cellA4 = row4.createCell((short) 0);
			
			
			HSSFCell HCell0 = row5.createCell((short) 0);
			HCell0.setCellValue("Buffer Sequence");
			SetCellColor(workbook,HCell0);
			HSSFCell HCell1 = row5.createCell((short) 1);
			HCell1.setCellValue("Key");
			SetCellColor(workbook,HCell1);
			HSSFCell HCell2 = row5.createCell((short) 2);
			HCell2.setCellValue("Description");
			SetCellColor(workbook,HCell2);
			HSSFCell HCell3 = row5.createCell((short) 3);
			HCell3.setCellValue("Attribute");
			SetCellColor(workbook,HCell3);
			HSSFCell HCell4 = row5.createCell((short) 4);
			HCell4.setCellValue("Length");
			SetCellColor(workbook,HCell4);
			HSSFCell HCell5 = row5.createCell((short) 5);
			HCell5.setCellValue("Length");
			SetCellColor(workbook,HCell5);
			HSSFCell HCell6 = row5.createCell((short) 6);
			HCell6.setCellValue("Column Name");
			SetCellColor(workbook,HCell6);
			HSSFCell HCell7 = row5.createCell((short) 7);
			HCell7.setCellValue("");
			SetCellColor(workbook,HCell7);
			HSSFCell HCell8 = row5.createCell((short) 8);
			HCell8.setCellValue("Required Column Name");
			SetCellColor(workbook,HCell8);
			HSSFCell HCell9 = row5.createCell((short) 9);
			HCell9.setCellValue("Key");
			SetCellColor(workbook,HCell9);
			HSSFCell HCell10 = row5.createCell((short) 10);
			HCell10.setCellValue("Attribute");
			SetCellColor(workbook,HCell10);
			HSSFCell HCell11 = row5.createCell((short) 11);
			HCell11.setCellValue("Length");
			SetCellColor(workbook,HCell11);
			HSSFCell HCell12 = row5.createCell((short) 12);
			HCell12.setCellValue("Conversion");
			SetCellColor(workbook,HCell12);
			HSSFCell HCell13 = row5.createCell((short) 13);
			HCell13.setCellValue("Required");
			SetCellColor(workbook,HCell13);
			HSSFCell HCell14 = row5.createCell((short) 14);
			HCell14.setCellValue("IsDate");
			SetCellColor(workbook,HCell14);
			HSSFCell HCell15 = row5.createCell((short) 15);
			HCell15.setCellValue("Date Conversion");
			SetCellColor(workbook,HCell15);
					
			
			row0.setHeightInPoints((float) 17.75);
			cellA0.setCellValue("DFC Mapping Sheet");
			
			cellA1.setCellValue("Source Database Name");
			SetHeaderCell(workbook,cellA1);
			cellA2.setCellValue("Library");
			SetHeaderCell(workbook,cellA2);
			cellA3.setCellValue("File");
			SetHeaderCell(workbook,cellA3);
			cellA4.setCellValue("Schema Name");
			SetHeaderCell(workbook,cellA4);
			

			HSSFCell cellB1 = row1.createCell((short) 1);
			HSSFCell cellB2 = row2.createCell((short) 1);
			HSSFCell cellB3 = row3.createCell((short) 1);
			HSSFCell cellB4 = row4.createCell((short) 1);
			
			cellB1.setCellValue(StagingDBName.trim());
			
			cellB2.setCellValue(AS400LibraryName);
			
			cellB3.setCellValue("STG_"+AS400FileName.trim());
			
			cellB4.setCellValue(StagingDBSchemaName);
			
			cellA1 = row1.createCell((short) 11);
			cellA2 = row2.createCell((short) 11);
			cellA3 = row3.createCell((short) 11);
			
			
			cellA1.setCellValue("Target Database Name");
			SetHeaderCell(workbook,cellA1);
			cellA2.setCellValue("Schema Name");
			SetHeaderCell(workbook,cellA2);
			cellA3.setCellValue("Table Name");
			SetHeaderCell(workbook,cellA3);
			
			
			cellA1 = row1.createCell((short) 12);
			cellA2 = row2.createCell((short) 12);
			cellA3 = row3.createCell((short) 12);


			cellA1.setCellValue(GDCDBName);
			cellA2.setCellValue(GDCDBSchemaName);
			cellA3.setCellValue(GDCTableName);
			ResultSet RSColumnKey;
			ResultSet RSMetaData;
			RSMetaData=GetDFCMetaData(AS400FileName,AS400LibraryName);
			RSColumnKey= GetAS400KeyColumns(AS400FileName);
			short i=0;
			rs.beforeFirst();
			while (rs.next()) {
				rowDetail=worksheet.createRow((short) 6+i);
				HCell0 = rowDetail.createCell((short) 0);
				HCell0.setCellValue(i+1);
				HCell1 = rowDetail.createCell((short) 1);
				HCell1.setCellValue(GetKeyValues(RSColumnKey,rs.getString(4)));
				HCell2 = rowDetail.createCell((short) 2);
				HCell2.setCellValue(rs.getString(1));
				HCell3 = rowDetail.createCell((short) 3);
				HCell3.setCellValue(rs.getString(2));
				HCell4 = rowDetail.createCell((short) 4);
				HCell4.setCellValue(rs.getString(3));
				HCell5 = rowDetail.createCell((short) 5);
				HCell5.setCellValue("");
				HCell6 = rowDetail.createCell((short) 6);
				HCell6.setCellValue(rs.getString(4));
				HCell7 = rowDetail.createCell((short) 7);
				HCell7.setCellValue("");
				HCell8 = rowDetail.createCell((short) 8);
				HCell8.setCellValue(GetGDCColumnName(RSMetaData,rs.getString(4),rs.getString(5)));
				HCell9 = rowDetail.createCell((short) 9);
				HCell9.setCellValue(HCell1.getStringCellValue());
				HCell10 = rowDetail.createCell((short) 10);
				HCell10.setCellValue(rs.getString(6));
				HCell11 = rowDetail.createCell((short) 11);
				HCell11.setCellValue(rs.getString(7));
				HCell12= rowDetail.createCell((short) 12);
				HCell12.setCellValue(rs.getString(8));
				HCell13 = rowDetail.createCell((short) 13);
				HCell13.setCellValue(rs.getString(9));
				HCell14 = rowDetail.createCell((short) 14);
				HCell14.setCellValue(rs.getString(10));
				HCell15 = rowDetail.createCell((short) 15);
				HCell15.setCellValue(rs.getString(11));
				i++;
	        }

			RSMetaData.close();
			RSColumnKey.close();
			worksheet.addMergedRegion(new CellRangeAddress(1,4,2,10));
			worksheet.addMergedRegion(new CellRangeAddress(0,0,0,15));
			
			
			i=0;
			while(i<=15)
			{
				worksheet.autoSizeColumn(i);
				i++;
			}
			
			SetTopRow(workbook,cellA0);
			//workbook.write(fileOut);
			//fileOut.flush();
			//fileOut.close();
			fis.close();
			FileOutputStream fos =new FileOutputStream(new File(XLSFileName));
		    workbook.write(fos);
		    fos.close();

		    
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static String GetKeyValues(ResultSet RSColumnKey,String ColumnName) throws SQLException
	{
		String KeyValue="";
		while (RSColumnKey.next()) {
			if(ColumnName.equalsIgnoreCase(RSColumnKey.getString(1).toString())){
				KeyValue="Key";
				RSColumnKey.beforeFirst();
				break;
			}
		}
		return KeyValue;
	}
	
	private static String GetGDCColumnName(ResultSet RSMetaData,String AS400FileColumnName,String DefaultName) throws SQLException, IOException
	{
		while (RSMetaData.next()) {
			if(AS400FileColumnName.equalsIgnoreCase(RSMetaData.getString(1).toString())){
				DefaultName=RSMetaData.getString(2);
				RSMetaData.beforeFirst();
				break;
			}
		}
		return ProperCase(DefaultName);
	}
	private static void SetCellColor(HSSFWorkbook workbook,HSSFCell Cell)
	{
		HSSFCellStyle cellStyle = workbook.createCellStyle();
		cellStyle.setFillForegroundColor(HSSFColor.BLUE_GREY.index);
		cellStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		Cell.setCellStyle(cellStyle);
				
	}
	
	private static void SetHeaderCell(HSSFWorkbook workbook,HSSFCell Cell)
	{
		
		HSSFCellStyle cellStyle = workbook.createCellStyle();
		cellStyle.setFillForegroundColor(HSSFColor.BLUE.index);
		cellStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		Cell.setCellStyle(cellStyle);
	}
	
	private static void SetTopRow(HSSFWorkbook workbook,HSSFCell Cell)
	{
		
		HSSFCellStyle cellStyle = workbook.createCellStyle();
		cellStyle.setFillForegroundColor(HSSFColor.DARK_BLUE.index);
		HSSFFont font = workbook.createFont();
		font.setColor(HSSFColor.WHITE.index);
		font.setBoldweight((short) 100);
		cellStyle.setFont(font);
		cellStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
        cellStyle.setAlignment(cellStyle.ALIGN_CENTER);
        cellStyle.setVerticalAlignment(cellStyle.VERTICAL_CENTER);
		Cell.setCellStyle(cellStyle);
	}
	
	private static void SetColorPalette(HSSFWorkbook wb)
	{
		HSSFPalette palette = wb.getCustomPalette();
	    palette.setColorAtIndex(HSSFColor.BLUE_GREY.index,(byte) 127, (byte) 163, (byte) 207);
	    palette.setColorAtIndex(HSSFColor.BLUE.index, (byte) 59, (byte) 102, (byte) 155);
	    palette.setColorAtIndex(HSSFColor.DARK_BLUE.index, (byte) 0, (byte) 32, (byte) 96);

	}
	
	private static void SetBorder(HSSFSheet ws,HSSFWorkbook wb)
	{
		int col=0;
		int row =1;
		boolean hasdata=true;
				while (hasdata==true)
				{
					HSSFRow r = ws.getRow(row);
				    for (col=0;col<=15;col++)
				    {
				    	HSSFCell cell = r.getCell(col);
				    	HSSFCellStyle style = wb.createCellStyle();
				        style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
				        style.setBottomBorderColor(HSSFColor.BLACK.index);
				        style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
				        style.setLeftBorderColor(HSSFColor.GREEN.index);
				        style.setBorderRight(HSSFCellStyle.BORDER_THIN);
				        style.setRightBorderColor(HSSFColor.BLUE.index);
				        style.setBorderTop(HSSFCellStyle.BORDER_MEDIUM_DASHED);
				        style.setTopBorderColor(HSSFColor.BLACK.index);
				        cell.setCellStyle(style);

				    }
				    row++;
				    HSSFRow rCheck = ws.getRow(row);
				    HSSFCell cellCheck = rCheck.getCell(0);
				    if(cellCheck.getStringCellValue()=="")
				    {
				    	hasdata=false;
				    }

				}
		
		       


	}
	private static String getCellValue(Cell cell) {
		String cellValue = null;
		if (cell != null) {
			switch (cell.getCellType()) {
			case Cell.CELL_TYPE_BOOLEAN:
				cellValue = String.valueOf(cell.getBooleanCellValue());
				break;
			case Cell.CELL_TYPE_NUMERIC:
				cellValue = String.valueOf((int) cell.getNumericCellValue());
				break;
			case Cell.CELL_TYPE_STRING:
				cellValue = cell.getStringCellValue();
				break;
			default:
				break;
			}
		}
		
		return cellValue;
	}

	private static boolean GetOneTimeLoadFile( String AS400LibraryName, String AS400FileName)
	{
		boolean retValue=true;
		String Query="";
		try{
			if (GenerateOneTimeLoadFile(AS400LibraryName, AS400FileName)){
				if(GetCSVFileFTP( AS400FileName,StagingDBName.substring(0,3))){
					Query="Exec Usp_RunSSISPackage '"+StagingDBName+"'";
					if(ExecuteSQLQuery(Query)){
						 System.out.println("One Time Load Completed For " + AS400LibraryName + "." + AS400FileName);
					}
				}
			}
		}
		catch(Exception ex){
			retValue=false;
		}
		return retValue;
	}
}
