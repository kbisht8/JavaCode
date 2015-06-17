package com.polarisft.tool.impl.trigger;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import com.polarisft.tool.resources.StaticContainers;
import com.polarisft.tool.resources.SupportedFunctions;
import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.bean.ColumnInformationBean;
import com.polarisft.tool.trigger.bean.TableInformationBean;


public class DebugExcelSheet extends AbstractScriptTemplate{
	private StaticContainers scont;
	private static  Map<String, String> myMap;
	private String fileName;
	private Workbook workbook;
	Logger scriptLoger;
	private int sheetIndex1;
	private StringBuffer debugExcelContentsBuffer = new StringBuffer();
	String lineSeparator = System.getProperty("line.separator");
	String outputPath;
	 public void generateScript(List<TableInformationBean> tableInformationBeanList, String outputPath) {
		 	
	        	this.outputPath = outputPath;
	        myMap = scont.getMyMap();
	        fileName = myMap.get("ExcelFilePath");
	     List<TableInformationBean> tableDetailsList = new ArrayList<TableInformationBean>();
			try {
				workbook = WorkbookFactory.create(new FileInputStream(fileName));
				int numberOfSheets = workbook.getNumberOfSheets();
				Sheet indexSheet = workbook.getSheetAt(0);
				CellReference cellReference = new CellReference("E2");
				Row rowIndex = indexSheet.getRow(cellReference.getRow());
				Cell cellIndex = rowIndex.getCell(cellReference.getCol());
				if(getCellValue(cellIndex)!= null && !getCellValue(cellIndex).isEmpty() && getCellValue(cellIndex).equalsIgnoreCase("Script Flag")){
					Iterator<Row> indesxSheetItr = indexSheet.iterator();
					while(indesxSheetItr.hasNext()){
						Row indexSheetRow = indesxSheetItr.next();
						if(getCellValue(indexSheetRow.getCell(4)) != null && !getCellValue(indexSheetRow.getCell(4)).isEmpty() && getCellValue(indexSheetRow.getCell(4)).equalsIgnoreCase("y")){
							sheetIndex1 = Integer.parseInt(getCellValue(indexSheetRow.getCell(1)));
							Sheet sheet = workbook.getSheetAt(sheetIndex1);
							//TableInformationBean tableInformationBean;
							//tableInformationBean = getTableInformationBean(sheet);
							validateMappingSheet(sheet);
							//tableDetailsList.add(tableInformationBean);
							
						}else{
							continue;
						}
					}
					//return tableDetailsList;
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
							if (firstRowValue != null && !firstRowValue.equalsIgnoreCase("Replication Mapping for webMethods")) {
								continue;
							}
							//TableInformationBean tableInformationBean;
							//tableInformationBean = getTableInformationBean(sheet);
							validateMappingSheet(sheet);
							//tableDetailsList.add(tableInformationBean);
						}
					}
					//return tableDetailsList;
				}
				StringBuffer debugExcelFileName = new StringBuffer(outputPath).append("DebugExcelSheet");
		        writeToFile(debugExcelFileName.toString(), debugExcelContentsBuffer);
		        //debugExcelContentsBuffer = null;
		        
			} catch (InvalidFormatException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	 }
	 
	
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
		
		//private void getTableInformationBean(Sheet sheet) {
			void validateMappingSheet(Sheet sheet){
			//TableInformationBean tableInformationBean = new TableInformationBean();
			Row row = null;
			Cell cell = null;
			
			//try {
			/*	CellReference cellReference = new CellReference("B4");
				row = sheet.getRow(cellReference.getRow());
				cell = row.getCell(cellReference.getCol());*/
			
				cell = SupportedFunctions.getCellByReference("A2", sheet);
				if (getCellValue(cell).equalsIgnoreCase("Source Database Name")) {
					cell = SupportedFunctions.getCellByReference("B2", sheet);
								if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
									if(SupportedFunctions.hasWhiteSpace(getCellValue(cell))){
										String logContainer = "Source Database name value contains space in Sheet "+row.getSheet().getSheetName() ;
										debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
									}
								}else{
						String logContainer = "Source Database name is missing in Sheet "+row.getSheet().getSheetName() ;
						debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
					}
				}else{
					debugExcelContentsBuffer.append("Check mapping sheet for Source Database Column.(Cell Reference - A2").append(lineSeparator);
				}
				
					
				cell = SupportedFunctions.getCellByReference("A4", sheet);
					if (getCellValue(cell).equalsIgnoreCase("File")) {
							cell = SupportedFunctions.getCellByReference("B4", sheet);
							if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
								if(SupportedFunctions.hasWhiteSpace(getCellValue(cell))){
									String logContainer = "Source Table name value contains space in Sheet "+row.getSheet().getSheetName() ;
									debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
								}
							}else{
								String logContainer = "Source Table name is missing in Sheet "+row.getSheet().getSheetName() ;
								debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
							}
					}else{
						debugExcelContentsBuffer.append("Check mapping sheet for Source Table Name Column.(Cell Reference - A4").append(lineSeparator);
					}
					
					cell = SupportedFunctions.getCellByReference("A5", sheet);
						if (getCellValue(cell).equalsIgnoreCase("Schema Name")) {				
								cell = SupportedFunctions.getCellByReference("B5", sheet);
								
								if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
									if(SupportedFunctions.hasWhiteSpace(getCellValue(cell))){
										String logContainer = "Source Schema name value contains space in Sheet "+row.getSheet().getSheetName() ;
										debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
									}
								}else{
									String logContainer = "Source Schema name is missing in Sheet "+row.getSheet().getSheetName() ;
									debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
								}
						}else{
							debugExcelContentsBuffer.append("Check mapping sheet for Source Scehma Name Column.(Cell Reference - A5").append(lineSeparator);
						}
					
				
				cell = SupportedFunctions.getCellByReference("L3", sheet);
					if (getCellValue(cell).equalsIgnoreCase("Schema Name")) {
						cell = SupportedFunctions.getCellByReference("M3", sheet);
						if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
							if(SupportedFunctions.hasWhiteSpace(getCellValue(cell))){
								String logContainer = "Target Schema name value contains space in Sheet "+row.getSheet().getSheetName() ;
								debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
							}
						}else{
							String logContainer = "Target Schema name is missing in Sheet "+row.getSheet().getSheetName() ;
							debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
						}
					}else{
						debugExcelContentsBuffer.append("Check mapping sheet for Target Scehma Name Column.(Cell Reference - L3").append(lineSeparator);
					}
				
				cell = SupportedFunctions.getCellByReference("L2", sheet);
					if (getCellValue(cell).equalsIgnoreCase("Target Database Name")) {
						cell = SupportedFunctions.getCellByReference("M2", sheet);
						if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
							if(SupportedFunctions.hasWhiteSpace(getCellValue(cell))){
								String logContainer = "Target Database name value contains space in Sheet "+row.getSheet().getSheetName() ;
								debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
							}
						}else{
							String logContainer = "Target Database name is missing in Sheet "+row.getSheet().getSheetName() ;
							debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
						}
					}else{
						debugExcelContentsBuffer.append("Check mapping sheet for Target Database Name Column.(Cell Reference - L2").append(lineSeparator);
					}
					
					cell = SupportedFunctions.getCellByReference("L4", sheet);
					if (getCellValue(cell).equalsIgnoreCase("Table Name")) {
						cell = SupportedFunctions.getCellByReference("M4", sheet);
						if(getCellValue(cell)!= null && !getCellValue(cell).isEmpty()){
							if(SupportedFunctions.hasWhiteSpace(getCellValue(cell))){
								String logContainer = "Target Table name value contains space in Sheet "+row.getSheet().getSheetName() ;
								debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
							}
						}else{
							String logContainer = "Target Table name is missing in Sheet "+row.getSheet().getSheetName() ;
							debugExcelContentsBuffer.append(logContainer).append(lineSeparator);
						}
					}else{
						debugExcelContentsBuffer.append("Check mapping sheet for Target Table Name Column.(Cell Reference - L4").append(lineSeparator);
					}
							
				//String loggerContainer;
				Iterator<Row> itr = sheet.iterator();
				while(itr.hasNext()){
					Row currentRow = itr.next();
					if(currentRow.getRowNum()>=6){				
						String cellVal = getCellValue(currentRow.getCell(0));
							/*if (cellVal != null && !cellVal.isEmpty()) {
								
								//Add condition to check the value of BUF SEQ column is in Int or not
								//String logContainer = "BUF SEQ column has non integer values in Sheet "+row.getSheet().getSheetName()+" row number:"+currentRow.getRowNum();
								debugExcelContentsBuffer.append("BUF SEQ column has non integer values in Sheet "+currentRow.getSheet().getSheetName()+" row number:"+currentRow.getRowNum()).append(lineSeparator);
								//}
							}else{
								debugExcelContentsBuffer.append("Check Buf SEQ column in sheet "+row.getSheet().getSheetName()+ " row number:"+currentRow.getRowNum()).append(lineSeparator);
							}*/
					
						cellVal = getCellValue(currentRow.getCell(1)); 
						if(cellVal != null && !cellVal.isEmpty()) {
							if(!cellVal.equalsIgnoreCase("key")){
								debugExcelContentsBuffer.append("Only value 'Key' can be added to column in second column of row "+currentRow.getRowNum()+" in "+ row.getSheet().getSheetName());
							}
							
						}
					
					
					//loggerContainer = "Successfully read Sheet Name ="+currentRow.getSheet().getSheetName()+" Row Index = "+currentRow.getRowNum(); 
				}
				//return tableInformationBean;
		}
		
		/*private ColumnInformationBean getColumnInformationBean(Row row) {
			String sourceDataType = null;
			String sourceColumnLength = null;
			ColumnInformationBean columnInformationBean = new ColumnInformationBean();
			String primaryKeyValue = getCellValue(row.getCell(1));
			if (primaryKeyValue != null && primaryKeyValue.equalsIgnoreCase("Key")) {
				columnInformationBean.setPrimaryKey(true);
			}
			String sourceColumnName = null;
			if(getCellValue(row.getCell(6))!= null && !getCellValue(row.getCell(6)).isEmpty() && getCellValue(row.getCell(6)).contains(" ")){
				 sourceColumnName = (getCellValue(row.getCell(6))).replaceAll(" ", "");
			}else if(getCellValue(row.getCell(6))!= null && !getCellValue(row.getCell(6)).isEmpty()){
				 sourceColumnName = getCellValue(row.getCell(6));
			}else{
				String logContainer = "Source Coulmn Name value is missing in Sheet "+row.getSheet().getSheetName() ;
				//scriptLoger.warning(logContainer);
			}
			columnInformationBean.setSourceColumnName(sourceColumnName);
			if(getCellValue(row.getCell(3)) != null && !getCellValue(row.getCell(3)).isEmpty()){
				sourceDataType = (getCellValue(row.getCell(3))).toUpperCase();
				columnInformationBean.setSourceDataType(sourceDataType);
			}else{
				String logContainer = "Source Coulmn Attribute value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum() ;
				//scriptLoger.warning(logContainer);
			}
			if(getCellValue(row.getCell(4)) != null && !getCellValue(row.getCell(4)).isEmpty()){
				sourceColumnLength = getCellValue(row.getCell(4));
			}else{
				String logContainer = "Source Coulmn Length value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum();
				//scriptLoger.warning(logContainer);
			}
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
				//scriptLoger.warning(logContainer);
			}
			if(getCellValue(row.getCell(10))!= null && !getCellValue(row.getCell(10)).isEmpty()){
				columnInformationBean.setTargetDataType((getCellValue(row.getCell(10))).toUpperCase());
			}else{
				String logContainer = "Target Attribute Coulmn  value is missing in Sheet "+row.getSheet().getSheetName()+" Row Number "+ row.getRowNum();
				//scriptLoger.warning(logContainer);
			}
			columnInformationBean.setTargetColumnLength(getCellValue(row.getCell(11)));
			if ("Y".equalsIgnoreCase(getCellValue(row.getCell(14)))) {
				columnInformationBean.setDateColumn(true);
				if (row.getCell(15) != null) {
					columnInformationBean.setDateFunction(getCellValue(row.getCell(15)));
				}
			}
			return columnInformationBean;
			
		}*/
			}
}
