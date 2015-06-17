package com.polarisft.tool.impl.trigger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import com.polarisft.tool.resources.StaticContainers;
import com.polarisft.tool.trigger.FileReader;
import com.polarisft.tool.trigger.ScriptLogger;
import com.polarisft.tool.trigger.bean.ColumnInformationBean;
import com.polarisft.tool.trigger.bean.StagingColumnInformationBean;
import com.polarisft.tool.trigger.bean.TableInformationBean;
/**
 * This class is used to read the information from the excel file.
 */
public class ExcelFileReader implements FileReader {

	private String fileName;
	private Workbook workbook;
	Logger scriptLoger;
	private int sheetIndex1;
	private static  Map<String, String> myMap;
	private StaticContainers scont;
	public ExcelFileReader(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * This method is used to evaluate the excel file and store the sheet names
	 * in a list.
	 */
	public List<TableInformationBean> getTableDetails()throws InvalidFormatException, FileNotFoundException, IOException {
		
		scriptLoger = new ScriptLogger().getScriptLogger();
		
		List<TableInformationBean> tableDetailsList = new ArrayList<TableInformationBean>();
		workbook = WorkbookFactory.create(new FileInputStream(fileName));
		
		myMap = new HashMap<String, String>();
		myMap = scont.getMyMap();
		myMap.put("ExcelFilePath", fileName);
		scont.setMyMap(myMap);
		int numberOfSheets = workbook.getNumberOfSheets();
		String SheetName="";
		
		Sheet indexSheet = workbook.getSheetAt(0);
		CellReference cellReference = new CellReference("E2");
		Row rowIndex = indexSheet.getRow(cellReference.getRow());
		Cell cellIndex = rowIndex.getCell(cellReference.getCol());
		if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty() && getCellValue(cellIndex).equalsIgnoreCase("GenerateSQLDDL")){
			Iterator<Row> indesxSheetItr = indexSheet.iterator();
			while(indesxSheetItr.hasNext()){
				Row indexSheetRow = indesxSheetItr.next();
				if(getCellValue(indexSheetRow.getCell(4)) != null && !getCellValue(indexSheetRow.getCell(4)).isEmpty() && getCellValue(indexSheetRow.getCell(4)).equalsIgnoreCase("y")){
					sheetIndex1 = Integer.parseInt(getCellValue(indexSheetRow.getCell(1)));
					SheetName = getCellValue(indexSheetRow.getCell(2));
					//Sheet sheet = workbook.getSheetAt(sheetIndex1);
					Sheet sheet=workbook.getSheet(SheetName);
					TableInformationBean tableInformationBean;
					tableInformationBean = getTableInformationBean(sheet);
					tableDetailsList.add(tableInformationBean);
					
				}else{
					continue;
				}
			}
			return tableDetailsList;
		}else{
			for (int sheetIndex = sheetIndex1 ; sheetIndex < numberOfSheets; sheetIndex++) {
				String sheetName = workbook.getSheetName(sheetIndex);
				 System.out.println("Current sheet name is " + sheetName);
				if (sheetName.equalsIgnoreCase("Index")) {
					continue;
				}
				Sheet sheet = workbook.getSheetAt(sheetIndex);
				if (sheet.getLastRowNum() > 0) {
					Row row = sheet.getRow(0);
					String firstRowValue = getCellValue(row.getCell(0));
					if (firstRowValue != null && !firstRowValue.equalsIgnoreCase("DFC Mapping Sheet")) {
						continue;
					}
					TableInformationBean tableInformationBean;
					tableInformationBean = getTableInformationBean(sheet);
					tableDetailsList.add(tableInformationBean);
				}
			}
			return tableDetailsList;
		
		}
		

	}
	/**
	 * This method is used to store the information regarding the table.
	 * 
	 * @param sheet
	 * @return
	 */
	private TableInformationBean getTableInformationBean(Sheet sheet) {
		TableInformationBean tableInformationBean = new TableInformationBean();
		Row row = null;
		Cell cell = null;
		
		//try {
			CellReference cellReference = new CellReference("B4");
			row = sheet.getRow(cellReference.getRow());
			cell = row.getCell(cellReference.getCol());
			
			/**
			 * Added by ArunKumar on 18th march 2014.
			 * It will check the 'null' and empty  value of source filename field.
			 * And print the warning in log file 
			 */
			if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
				tableInformationBean.setSourceTableName(getCellValue(cell));
			}else{
				String logContainer = "Source FileName is missing in Sheet "+row.getSheet().getSheetName() ;
				scriptLoger.warning(logContainer);
			}
			
			/**
			 * Get the value of B5 column from sheet and set it to sourceSchemaName. 
			 */
			
			cellReference = new CellReference("B5");
			row = sheet.getRow(cellReference.getRow());
			cell = row.getCell(cellReference.getCol());
			
			/**
			 * Added by ArunKumar on 18th march 2014.
			 * It will check the 'null' and empty  value of source SchemaName field.
			 * And print the warning in log file 
			 */
			
			if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
				tableInformationBean.setSourceSchemaName(getCellValue(cell));
			}else{
				String logContainer = "Source SchemaName name is missing in Sheet "+row.getSheet().getSheetName() ;
				scriptLoger.warning(logContainer);
			}
			
			
			
			
			/**
			 * Get the value of L3 column from sheet . 
			 */
			cellReference = new CellReference("L3");
			row = sheet.getRow(cellReference.getRow());
			cell = row.getCell(cellReference.getCol());
			/**
			 * Check if value of L3 column is "Schema Name" 
			 * than set value of M3 column from sheet to targetSchemaName been component. 
			 */
			
			if (getCellValue(cell).equalsIgnoreCase("Schema Name")) {
				cellReference = new CellReference("M3");
				row = sheet.getRow(cellReference.getRow());
				cell = row.getCell(cellReference.getCol());
				/**
				 * Added by ArunKumar on 18th march 2014.
				 * It will check the 'null' and empty  value of Target SchemaName field.
				 * And print the warning in log file 
				 */
				if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
					tableInformationBean.setTargetSchemaName(getCellValue(cell));
				}else{
					String logContainer = "Target SchemaName value is missing in Sheet "+row.getSheet().getSheetName() ;
					scriptLoger.warning(logContainer);
				}
				
			} else {
					String logContainer = "Target SchemaName name is missing in Sheet "+row.getSheet().getSheetName() ;
					scriptLoger.warning(logContainer);
			}
			/**
			 * Check the value of L2 column in sheet. 
			 */
			cellReference = new CellReference("L2");
			row = sheet.getRow(cellReference.getRow());
			cell = row.getCell(cellReference.getCol());
			/**
			 * Check if value of L2 column is "Target Database Name"
			 *  than set value of M2 column from sheet to targetDatabaseName. 
			 */
			if (getCellValue(cell).equalsIgnoreCase("Target Database Name")) {
				cellReference = new CellReference("M2");
				row = sheet.getRow(cellReference.getRow());
				cell = row.getCell(cellReference.getCol());
				/**
				 * Added by ArunKumar on 18th march 2014.
				 * It will check the 'null' and empty  value of Target Database field.
				 * And print the warning in log file 
				 */
				
				if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
					tableInformationBean.setTargetDatabaseName(getCellValue(cell));
				}else{
					String logContainer = "Target Database Value is missing in Sheet "+row.getSheet().getSheetName() ;
					scriptLoger.warning(logContainer);
				}
			}else {
				String logContainer = "Target Database Name is missing in Sheet "+row.getSheet().getSheetName() ;
				scriptLoger.warning(logContainer);
			}
			/**
			 * Get the value of A2 column from sheet. 
			 */
			cellReference = new CellReference("A2");
			row = sheet.getRow(cellReference.getRow());
			cell = row.getCell(cellReference.getCol());
			/**
			 * Check if value of A2 column is "Source Database Name" 
			 * than set value of B2 column from sheet to sourceDatabaseName been component. 
			 */
			if (getCellValue(cell).equalsIgnoreCase("Source Database Name")) {
				cellReference = new CellReference("B2");
				row = sheet.getRow(cellReference.getRow());
				cell = row.getCell(cellReference.getCol());
				/**
				 * Added by ArunKumar on 18th march 2014.
				 * It will check the 'null' and empty  value of Source Database field.
				 * And print the warning in log file 
				 */
				
				if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
					tableInformationBean.setSourceDatabaseName(getCellValue(cell));
				}else{
					String logContainer = "Source Database Value is missing in Sheet "+row.getSheet().getSheetName() ;
					scriptLoger.warning(logContainer);
				}
			} else {
				String logContainer = "Source Database Name is missing in Sheet "+row.getSheet().getSheetName() ;
				scriptLoger.warning(logContainer);
			}
			/**
			 * Check the value of L4 column in sheet . 
			 */
			cellReference = new CellReference("L4");
			row = sheet.getRow(cellReference.getRow());
			cell = row.getCell(cellReference.getCol());
			/**
			 * Check if value of L4 column is "Table Name" 
			 * than set value of M4 column from sheet to targetTableName been component. 
			 */
			if (getCellValue(cell).equalsIgnoreCase("Table Name")) {
				cellReference = new CellReference("M4");
				row = sheet.getRow(cellReference.getRow());
				cell = row.getCell(cellReference.getCol());
				
				/**
				 * Added by ArunKumar on 18th march 2014.
				 * It will check the 'null' and empty  value of Target Table field.
				 * And print the warning in log file 
				 */
				if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
					tableInformationBean.setTargetTableName(getCellValue(cell));
				}else{
					String logContainer = "Target Table value is missing in Sheet "+row.getSheet().getSheetName() ;
					scriptLoger.warning(logContainer);
				}
			} else {
				String logContainer = "Target Table name is missing in Sheet "+row.getSheet().getSheetName() ;
				scriptLoger.warning(logContainer);
			}
			/*
			 * Written by arun kumar 14th March 2014 replacement of for loop.
			 *
			 */
			String loggerContainer;
			Iterator<Row> itr = sheet.iterator();
			while(itr.hasNext()){
				Row currentRow = itr.next();
				
				if(currentRow.getRowNum()>=6){
					if (getCellValue(currentRow.getCell(0)) != null && !getCellValue(currentRow.getCell(0)).isEmpty()) {
						tableInformationBean.addStagingColumnInformationBean(getStagingColumnInformationBean(currentRow));
					}
					if (getCellValue(currentRow.getCell(0)) != null && !getCellValue(currentRow.getCell(0)).isEmpty()) {
						if ("Y".equalsIgnoreCase(getCellValue(currentRow.getCell(13)))) {
							tableInformationBean.addColumnInformationBean(getColumnInformationBean(currentRow));
						}
					}
				}
				loggerContainer = "Successfully read Sheet Name ="+currentRow.getSheet().getSheetName()+" Row Index = "+currentRow.getRowNum(); 
				scriptLoger.info(loggerContainer);
			}
			
			
			
			/*for (int rowIndex = 6; rowIndex <= numberOfRows; rowIndex++) {
				Row currentRow = sheet.getRow(rowIndex);
				if (currentRow == null || currentRow.getCell(0) == null) {
					break;
				}
				*//**
				 * Added By Arun Kumar .
				 * Call the method that separate the access 
				 * control of rows for source database. 
				 *//*
				if (getCellValue(currentRow.getCell(1)) == null || (!"Attach".equals(getCellValue(currentRow.getCell(1))))) {
					tableInformationBean.addStagingColumnInformationBean(getStagingColumnInformationBean(sheet.getRow(rowIndex)));
				}
				*//**
				 * if condition check the cell(1) value ,if it is null or not equal to "Attach" it will return true.
				 * Another if condition check cell(13) value,if it is "Y" then it will add the whole row data in list.
				 * If it value is "N" then it will avoid that row and next row will iterate. 
				 * control of rows for source database. 
				 *//*
				if (getCellValue(currentRow.getCell(1)) == null || (!"Attach".equals(getCellValue(currentRow.getCell(1))))) {
					if ("Y".equalsIgnoreCase(getCellValue(currentRow.getCell(13)))) {
						tableInformationBean.addColumnInformationBean(getColumnInformationBean(sheet.getRow(rowIndex)));
					}
				}
			}*/
		//} catch (Exception e) {
			/**
			 * Added By Arun Kumar It will throw the exception and print the.
			 * sheet name,cell index number,row index number. After reading the.
			 * cell value that cause of exception. 
			 * 
			 */
			//scriptLoger.log(Level.WARNING, "Exception", e.fillInStackTrace().getMessage());
			/*scriptLoger.info("Exception in sheet " + sheet.getSheetName()
					+ ",After reading Cell no = " + cell.getColumnIndex()
					+ ", Row index no =" + row.getRowNum() + ", cell value = "
					+ getCellValue(cell));*/
			//System.out.println( "cause = "+e.getCause().getMessage());
			//e.printStackTrace();
			/*System.out.println("Exception in sheet " + sheet.getSheetName()
					+ ",After reading Cell no = " + cell.getColumnIndex()
					+ ", Row index no =" + row.getRowNum() + ", cell value = "
					+ getCellValue(cell));*/
		//}
		return tableInformationBean;
	}
	/**
	 * This method is used to get the data type of the cell value
	 */
	private String getCellValue(Cell cell) {
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

	/**
	 * This method is used to store the information regarding the Columns.
	 * 
	 * @param row
	 * @return
	 */
	private ColumnInformationBean getColumnInformationBean(Row row) {
		String sourceDataType = null;
		String sourceColumnLength = null;
		
		ColumnInformationBean columnInformationBean = new ColumnInformationBean();
		/**
		 * This if condition is check column(1)  value is primary key or not
		 * If column value is primary key then set the been component as true 
		 * The default value is false
		 */
		String primaryKeyValue = getCellValue(row.getCell(1));
		if (primaryKeyValue != null && primaryKeyValue.equalsIgnoreCase("Key")) {
			columnInformationBean.setPrimaryKey(true);
		}
		/**
		 * This code is added by ArunKumar on 19th march 2014.
		 * First 'if' statement check cell value for null,empty and space,if find the space.
		 * Remove the space from column "Column name" value.
		 * 'else if' statement check for for null,empty .
		 * 'else' statement add warning to log file.
		 */
		String sourceColumnName = null;
		if(getCellValue(row.getCell(6))!= null && !getCellValue(row.getCell(6)).isEmpty() && getCellValue(row.getCell(6)).contains(" ")){
			 sourceColumnName = (getCellValue(row.getCell(6))).replaceAll(" ", "");
		}else if(getCellValue(row.getCell(6))!= null && !getCellValue(row.getCell(6)).isEmpty()){
			 sourceColumnName = getCellValue(row.getCell(6));
		}else{
			String logContainer = "Source Coulmn Name value is missing in Sheet "+row.getSheet().getSheetName() ;
			scriptLoger.warning(logContainer);
		}
		columnInformationBean.setSourceColumnName(sourceColumnName);
		/*This code is blocked by ArunKumar on 19th March 2014
		 * Code has meaning less functionality 
		 *  
		 * if (sourceColumnName.contains("(")) {
			sourceColumnName = sourceColumnName.substring(0,(sourceColumnName.indexOf("(")));
			columnInformationBean.setSourceColumnName(sourceColumnName);
		}*/
		/**
		 * This code is added by ArunKumar on 19th march 2014.
		 * First 'if' statement check cell value for null and empty.
		 * 'else' statement add warning to log file.
		 */
		if(getCellValue(row.getCell(3)) != null && !getCellValue(row.getCell(3)).isEmpty()){
			sourceDataType = (getCellValue(row.getCell(3))).toUpperCase();
			columnInformationBean.setSourceDataType(sourceDataType);
		}else{
			String logContainer = "Source Coulmn Attribute value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum() ;
			scriptLoger.warning(logContainer);
		}
		/**
		 * This code is added by ArunKumar on 19th march 2014.
		 * First 'if' statement check cell value for null and empty.
		 * 'else' statement add warning to log file.
		 */
		if(getCellValue(row.getCell(4)) != null && !getCellValue(row.getCell(4)).isEmpty()){
			sourceColumnLength = getCellValue(row.getCell(4));
		}else{
			String logContainer = "Source Coulmn Length value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum();
			scriptLoger.warning(logContainer);
		}
		
		
		/**
		 * Edit By Arun Kumar on 10th Jan 2014 
		 * This if condition check source data type is "Decimal" or not.
		 * If source data type is decimal than it concatenate the sourceColumnLength and decimalPrecision values.
		 * The concatenate format will be ex. Decimal(10,0)
		 **/
		if (sourceDataType.equalsIgnoreCase("DECIMAL")) {
			String decimalPrecision = getCellValue(row.getCell(5));
			if (decimalPrecision != null && !decimalPrecision.equals("")) {
				sourceColumnLength = sourceColumnLength + ","+ decimalPrecision;
			}
		}
		columnInformationBean.setSourceColumnLength(sourceColumnLength);
		if(getCellValue(row.getCell(8)) != null && !getCellValue(row.getCell(8)).isEmpty()){
			columnInformationBean.setTargetColumnName(getCellValue(row.getCell(8)));
		}else{
			String logContainer = "Target Required Coulmn Length value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum();
			scriptLoger.warning(logContainer);
		}
		if(getCellValue(row.getCell(10))!= null && !getCellValue(row.getCell(10)).isEmpty()){
			columnInformationBean.setTargetDataType((getCellValue(row.getCell(10))).toUpperCase());
		}else{
			String logContainer = "Target Attribute Coulmn  value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum();
			scriptLoger.warning(logContainer);
		}
		columnInformationBean.setTargetColumnLength(getCellValue(row.getCell(11)));
										
		/**
		 * This if condition check the column "IsDate" value is 'Y' or N
		 * if column value is 'Y' then it set the true value in been component.It also check the column(15) value
		 * is null or not .
		 * If not null then it set the value in been component.
		 * Column(15) value is date format ex. CYYMMDD 
		 * If date format is missing in excel sheet ,It will be error in excel sheet.
		 **/
		if ("Y".equalsIgnoreCase(getCellValue(row.getCell(14)))) {
			columnInformationBean.setDateColumn(true);
			if (row.getCell(15) != null) {
				columnInformationBean.setDateFunction(getCellValue(row.getCell(15)));
			}
		}
		return columnInformationBean;
	}

	/**
	 * Added By Arun Kumar on 10th Jan 2014 To Access rows for Staging table
	 * This function will read the source data base cell values independently that are required
	 * for staging table script. This function will remove the dependency from
	 * target database cell reading and creating row.
	 **/
	private StagingColumnInformationBean getStagingColumnInformationBean(Row row) {
		StagingColumnInformationBean stagingColumnInformationBean = new StagingColumnInformationBean();
		if (row != null) {
			String primaryKeyValue = getCellValue(row.getCell(1));
			/**
			 * This if condition is check column(1)  value is primary key or not
			 * If column value is primary key then set the been component as true 
			 * The default value is false
			 */
			if (primaryKeyValue != null && primaryKeyValue.equalsIgnoreCase("Key")) {
				stagingColumnInformationBean.setPrimaryKey(true);
			}
			/**
			 * This statement remove the space from column "Column name" value.
			 */
			
			//String sourceColumnName = (getCellValue(row.getCell(6))).replaceAll(" ", "");
			String sourceColumnName = null;
			if(getCellValue(row.getCell(6))== null){
				System.out.println("null value = "+getCellValue(row.getCell(1))+" row number = "+row.getCell(1).getRowIndex());
			}
			if(getCellValue(row.getCell(6))!= null && getCellValue(row.getCell(6)).contains(" ")){
				 sourceColumnName = (getCellValue(row.getCell(6))).replaceAll(" ", "");
			}else{
				sourceColumnName = getCellValue(row.getCell(6));
				//System.out.println("No space = "+getCellValue(row.getCell(6)));
			}
			
			/*if (sourceColumnName.contains("(")) {
				sourceColumnName = sourceColumnName.substring(0,(sourceColumnName.indexOf("(")));
			}*/
			
			stagingColumnInformationBean.setSourceColumnName(sourceColumnName);
			String sourceDataType = (getCellValue(row.getCell(3))).toUpperCase();
			stagingColumnInformationBean.setSourceDataType(sourceDataType);
			String sourceColumnLength = getCellValue(row.getCell(4));
			/**
			 * Edit By Arun Kumar on 10th Jan 2014 
			 * This if condition check source data type is "Decimal" or not.
			 * If source data type is decimal than it concatenate the sourceColumnLength and decimalPrecision values.
			 * The concatenate format will be ex. Decimal(10,0)
			 **/
			if (sourceDataType.equalsIgnoreCase("DECIMAL")) {
				String decimalPrecision = getCellValue(row.getCell(5));
				if (decimalPrecision != null && !decimalPrecision.equals("")) {
					sourceColumnLength = sourceColumnLength + ","+ decimalPrecision;
				}
			}
			stagingColumnInformationBean.setSourceColumnLength(sourceColumnLength);

		}
		return stagingColumnInformationBean;
	}

}
