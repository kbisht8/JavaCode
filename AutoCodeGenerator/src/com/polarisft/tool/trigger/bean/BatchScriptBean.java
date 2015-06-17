package com.polarisft.tool.trigger.bean;

/**
 * @author arunkumar.roopchand
 * BatchScriptBean holds the value of properties file.
 *
 */
public class BatchScriptBean {
	/**
	 * tableName holds the value of property file.
	 * That is the name of created  batch file .
	 */
    private String tableName;
    /**
	 * scriptStatements holds the value of scripts.
	 * script that is defined in the properties file.
	 */
	private String scriptStatements;
	 /**
	 * isMultiStatements holds the value of property file.
	 * that is define script has multiple line statement or single line statement.
	 */
	private String isMultiStatements;
	/**
	 * isSourceOrTarget holds the value of property file.
	 * that is define table will access source data base or from target database.
	 */
	private String isSourceOrTarget;
	
	/**
	 * This method is used to get the script file name.
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}
	/**
	 * This method is used to get the script statement.
	 * @return
	 */
	public String getScriptStatements() {
		return scriptStatements;
	}
	/**
	 * This method is used to get the value that is multiple statement or not.
	 * @return
	 */
	public String getIsMultiStatements() {
		return isMultiStatements;
	}
	/**
	 * This method is used to set the script file name.
	 * @param tableName
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	/**
	 * This method is used to set the script statement.
	 * @param scriptStatements
	 */
	public void setScriptStatements(String scriptStatements) {
		this.scriptStatements = scriptStatements;
	}
	/**
	 * This method is used to set the multiple statement value or not.
	 * @param scriptStatements
	 */
	public void setIsMultiStatements(String isMultiStatements) {
		this.isMultiStatements = isMultiStatements;
	}
	/**
	 * This method is used to get the table from source database or target database.
	 * @return 
	 */
	public String getIsSourceOrTarget() {
		return isSourceOrTarget;
	}
	/**
	 * This method is used to set the tables from source database or target database.
	 * @param  isSourceOrTarget
	 */
	public void setIsSourceOrTarget(String isSourceOrTarget) {
		this.isSourceOrTarget = isSourceOrTarget;
	}
	
	

}
