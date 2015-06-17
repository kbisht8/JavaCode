package com.polarisft.tool.trigger.bean;

/**
 * This class file is used to store the information about the columns.
 * 
 */
public class StagingColumnInformationBean {
	/** Evaluates whether it is a primary key */
	private boolean isPrimaryKey;

	/** Holds the source column name */
	private String sourceColumnName;

	/** Holds the source data type */
	private String sourceDataType;

	/** Holds the source column length */
	private String sourceColumnLength;
	
	/** Holds the source column date  */
	private boolean isDateColumn;

	/** Store whether it is a date column or not */
	private String dateFunction;

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	/**
	 * This method is used to set the Primary key
	 * 
	 * @param isPrimaryKey
	 */
	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	/**
	 * This method is used to get the source column name
	 * 
	 * @return
	 */
	public String getSourceColumnName() {
		return sourceColumnName;
	}

	/**
	 * This method is used to set the source column name
	 * 
	 * @param sourceColumnName
	 */
	public void setSourceColumnName(String sourceColumnName) {
		this.sourceColumnName = sourceColumnName;
	}

	/**
	 * This method is used to get the source data type
	 * 
	 * @return
	 */
	public String getSourceDataType() {
		return sourceDataType;
	}

	/**
	 * This method is used to set the source data type
	 * 
	 * @param sourceDataType
	 */
	public void setSourceDataType(String sourceDataType) {
		this.sourceDataType = sourceDataType;
	}

	/**
	 * This method is used to get the source column length
	 * 
	 * @return
	 */
	public String getSourceColumnLength() {
		return sourceColumnLength;
	}

	/**
	 * This method is used to set the source column length
	 * 
	 * @param sourceColumnLength
	 */
	public void setSourceColumnLength(String sourceColumnLength) {
		this.sourceColumnLength = sourceColumnLength;
	}

	public boolean isDateColumn() {
		return isDateColumn;
	}

	/**
	 * This method is used to set the date column
	 * 
	 * @param isDateColumn
	 */
	public void setDateColumn(boolean isDateColumn) {
		this.isDateColumn = isDateColumn;
	}

	/**
	 * This method is used to get the date function
	 * 
	 * @return
	 */
	public String getDateFunction() {
		return dateFunction;
	}

	/**
	 * This method is used to set the date function
	 * 
	 * @param dateFunction
	 */
	public void setDateFunction(String dateFunction) {
		this.dateFunction = dateFunction;
	}

}
