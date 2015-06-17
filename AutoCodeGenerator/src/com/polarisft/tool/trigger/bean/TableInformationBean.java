package com.polarisft.tool.trigger.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to store the details of each source and target table.
 * 
 */
public class TableInformationBean {

	/** Holds the source table name */
	private String sourceTableName;

	/** Holds the target table name */
	private String targetTableName;

	/** Holds the source schema name */
	private String sourceSchemaName;

	/** Holds the target schema name */
	private String targetSchemaName;

	/** Holds the details of all columns present in the source and target table */
	private List<ColumnInformationBean> columnInformationBeanList;

	/** Holds the details of all columns present in the source table */
	private List<StagingColumnInformationBean> stagingColumnInformationBeanList;

	/**
	 * This method is used to get the source table name
	 * 
	 * @return the source table name
	 */
	/** Holds the target Database name */
	private String targetDatabaseName;

	/** Holds the source Database name */
	private String sourceDatabaseName;

	/**
	 * This method is used to get Source Table Name
	 * 
	 * @return
	 */
	public String getSourceTableName() {
		return sourceTableName;
	}

	/**
	 * This method is used to set the source table name
	 * 
	 * @param sourceTableName
	 */
	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	/**
	 * This method is used to get the target table name
	 * 
	 * @return the target table name
	 */
	public String getTargetTableName() {
		return targetTableName;
	}

	/**
	 * This method is used to set the target table name
	 * 
	 * @param targetTableName
	 */
	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	/**
	 * This method is used to get the source schema name
	 * 
	 * @return the source schema name
	 */
	public String getSourceSchemaName() {
		return sourceSchemaName;
	}

	/**
	 * This method is used to set the source schema name
	 * 
	 * @param sourceSchemaName
	 */
	public void setSourceSchemaName(String sourceSchemaName) {
		this.sourceSchemaName = sourceSchemaName;
	}

	/**
	 * This method is used to get the target schema name
	 * 
	 * @return the target schema name
	 */
	public String getTargetSchemaName() {
		return targetSchemaName;
	}

	/**
	 * This method is used to set the target schema name
	 * 
	 * @param targetSchemaName
	 */
	public void setTargetSchemaName(String targetSchemaName) {
		this.targetSchemaName = targetSchemaName;
	}

	/**
	 * This method is used to get the details of all columns present in source
	 * and target table
	 * 
	 * @param the
	 *            ColumnInformationBean list
	 */
	public List<ColumnInformationBean> getColumnInformationBeanList() {
		return columnInformationBeanList;
	}

	/**
	 * Added By ArunKumar This method is used to get the details of all columns
	 * present in source table
	 * 
	 * 
	 * @param the
	 *            StagingColumnInformationBean list
	 */
	public List<StagingColumnInformationBean> getStagingColumnInformationBeanList() {
		return stagingColumnInformationBeanList;
	}

	/**
	 * This method is used to set the list of ColumnInformationBean for all
	 * columns present in the source and target table
	 * 
	 * @param columnInformationBeanList
	 */
	public void setColumnInformationBeanList(List<ColumnInformationBean> columnInformationBeanList) {
		this.columnInformationBeanList = columnInformationBeanList;
	}

	/**
	 * Added By ArunKumar This method is used to set the list of
	 * StagingColumnInformationBean for all Staging Table columns present in the
	 * source and target table
	 * 
	 * @param setStagingColumnInformationBeanList
	 */
	public void setStagingColumnInformationBeanList(List<StagingColumnInformationBean> stagingColumnInformationBeanList) {
		this.stagingColumnInformationBeanList = stagingColumnInformationBeanList;
	}

	/**
	 * This method is used to set the StagingColumnInformationBean for staging
	 * table column present in the source and target table
	 * 
	 * @param columnInformationBean
	 */
	public void addColumnInformationBean(ColumnInformationBean columnInformationBean) {
		if (this.columnInformationBeanList == null) {
			this.columnInformationBeanList = new ArrayList<ColumnInformationBean>();
		}
		this.columnInformationBeanList.add(columnInformationBean);
	}

	/**
	 * Added By ArunKumar This method is used to add the
	 * addStagingColumnInformationBean for each column present in the source
	 * table
	 * 
	 * @param columnInformationBean
	 */
	public void addStagingColumnInformationBean(StagingColumnInformationBean stagingColumnInformationBeanList) {
		if (this.stagingColumnInformationBeanList == null) {
			this.stagingColumnInformationBeanList = new ArrayList<StagingColumnInformationBean>();
		}
		this.stagingColumnInformationBeanList
				.add(stagingColumnInformationBeanList);
	}

	/**
	 * This method is used to get Target DatabaseName
	 * 
	 * @return
	 */
	public String getTargetDatabaseName() {
		return targetDatabaseName;
	}

	/**
	 * This method is used to set Target DatabaseName
	 * 
	 * @param targetDatabaseName
	 */
	public void setTargetDatabaseName(String targetDatabaseName) {
		this.targetDatabaseName = targetDatabaseName;
	}

	/**
	 * This method is used to get Source DatabaseName
	 * 
	 * @return
	 */
	public String getSourceDatabaseName() {
		return sourceDatabaseName;
	}

	/**
	 * This method is used to set Source DatabaseName
	 * 
	 * @param sourceDatabaseName
	 */
	public void setSourceDatabaseName(String sourceDatabaseName) {
		this.sourceDatabaseName = sourceDatabaseName;
	}

}
