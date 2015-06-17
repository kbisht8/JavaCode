package com.polarisft.tool.impl.trigger;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import com.polarisft.tool.resources.StaticContainers;

/**
 * This class is used to read the information from the excel file.
 */
public class PopulateGlobalVariables  {
	static ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
	private Workbook workbook;
	Logger scriptLoger;
	private static  Map<String, String> myMap;
	private StaticContainers scont;
	private String CountryPrefix;

	public void AssignGlobalVariables(String fileName) throws Exception {
		workbook = WorkbookFactory.create(new FileInputStream(fileName));
		myMap = new HashMap<String, String>();
		myMap.put("ExcelFilePath", fileName);
		scont.setMyMap(myMap);
		Sheet indexSheet = workbook.getSheetAt(0);
	
		CellReference cellReference = new CellReference("P2");
		Row rowIndex = indexSheet.getRow(cellReference.getRow());
		Cell cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("CreateStagingDB", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("CreateStagingDB", "N");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P3");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("CreateGDCDB", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("CreateGDCDB", "N");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P4");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("GrantBPMUserAccess", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("GrantBPMUserAccess", "N");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P5");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("DeployOneTimeSetupScript", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("DeployOneTimeSetupScript", "N");
			scont.setMyMap(myMap);
		}
		
		
		cellReference = new CellReference("P6");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("DeploySQLServerObjects", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("DeploySQLServerObjects", "N");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P7");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("MSSQLStagingDBName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
			CountryPrefix=getCellValue(cellIndex).toString().trim().substring(0, 3);
		}
		else
		{
			System. out .println( "Microsoft SQL Server Staging DB Name Not Entered" );
			System.exit(0);
		}
		
		
		cellReference = new CellReference("P15");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("StagingSchemaName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "Microsoft SQL Server Staging Schema Name Not Entered" );
			System.exit(0);
		}
		
		
		cellReference = new CellReference("P8");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("MSSQLGDCDBName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "Microsoft SQL Server GDC DB Name Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P16");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("GDCSchemaName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "Microsoft SQL Server GDC Schema Name Not Entered" );
			System.exit(0);
		}
		
		
		
		
		cellReference = new CellReference("P9");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
			myMap.put("DB2UserName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "DB2 User Name Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P10");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
	         myMap.put("DB2Password", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "DB2 Password Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P11");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("SQLServerHostName", getCellValue(cellIndex));
            myMap.put("serverName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "SQL Server Host Name Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P12");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("SQLServerUserName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "SQL Server User Name Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P13");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("SQLServerPassword", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "SQL Server Password Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P14");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("DDLScriptOutputPath", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "Script OutPut Path Not Entered" );
			System.exit(0);
		}
		
		String ConString=resourceBundle.getString("autoscriptGenerator.SQLConnectionString");
		ConString=ConString.replace("*SQLS*", myMap.get("SQLServerHostName")).replace("*SQLU*", myMap.get("SQLServerUserName")).replace("*SQLP*", myMap.get("SQLServerPassword"));
		myMap.put("SQLServerConnectionString", ConString);
		scont.setMyMap(myMap);
		
		SetTargetServerParameters(ConString);
		
		
		cellReference = new CellReference("P17");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("DB2HostName", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			System. out .println( "DB2 HostName Not Entered" );
			System.exit(0);
		}
		
		cellReference = new CellReference("P18");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("RunSQLAgentJobs", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("RunSQLAgentJobs", "N");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P19");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("DFCEnvironment", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("DFCEnvironment", "QA");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P20");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("SQLOPUser", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("SQLOPUser", "N");
			scont.setMyMap(myMap);
		}
		
		cellReference = new CellReference("P21");
		rowIndex = indexSheet.getRow(cellReference.getRow());
		cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty())
		{
            myMap.put("SQLDevUser", getCellValue(cellIndex));
			scont.setMyMap(myMap);
		}
		else
		{
			myMap.put("SQLDevUser", "N");
			scont.setMyMap(myMap);
		}
		
	}
	
	private void SetTargetServerParameters(String ConString) throws Exception
	{
		  String Query="Select AWS_SQLInstanceName, AWS_SQLUserName, AWS_SQLPassword From DFCParameterMaster Where TargetRegionPrefix='"+CountryPrefix+"'";
		  //Query="Select 'jdbc:sqlserver://'+AWS_SQLInstanceName+';user='+AWS_SQLUserName+';password='+AWS_SQLPassword+';DatabaseName=*DBName*' From DFCParameterMaster";
	 
		  String TargetServerConString="";
	      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	      Connection  conn = DriverManager.getConnection(ConString); 
	      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);  
	      ResultSet rs = stmt.executeQuery(Query);
	      rs.first();
	    	  myMap.put("TargetSQLServerHostName", rs.getString(1));
	    	  scont.setMyMap(myMap);
	    	  
	    	  myMap.put("TargetSQLServerUserName", rs.getString(2));
	    	  scont.setMyMap(myMap);
	    	  
	    	  myMap.put("TargetSQLServerPassword", rs.getString(3));
	    	  scont.setMyMap(myMap);
	    	  
	    	  TargetServerConString="jdbc:sqlserver://"+rs.getString(1)+";user="+rs.getString(2)+";password="+rs.getString(3)+";DatabaseName=master";
	    	  myMap.put("TargetSQLServerConnectionString", TargetServerConString);
			  scont.setMyMap(myMap);
	
	      rs.close();
	      stmt.close();
	      conn.close();
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
	
}
