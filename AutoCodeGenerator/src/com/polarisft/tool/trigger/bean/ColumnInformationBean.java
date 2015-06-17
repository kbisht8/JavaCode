package com.polarisft.tool.trigger.bean;

/**
 * This class file is used to store the information about the columns.
 * 
 */
public class ColumnInformationBean {
    /** Evaluates whether it is a primary key */
    private boolean isPrimaryKey;

    /** Holds the source column name */
    private String sourceColumnName;

    /** Holds the source data type */
    private String sourceDataType;

    /** Holds the source column length */
    private String sourceColumnLength;

    /** Holds the target column name */
    private String targetColumnName;

    /** Holds the target data type */
    private String targetDataType;

    /** Holds the targetColumnLength */
    private String targetColumnLength;

    /** Evaluates whether it is a date column */
    private boolean isDateColumn;

    /** Holds the source table name */
    private String dateFunction;

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
/**
 * This method is used to set the Primary key
 * @param isPrimaryKey
 */
    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }
/**
 * This method is used to get the source column name
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
 *  This method is used to get the source data type
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
/**
 * This method is used to get Target Column Name
 * @return
 */
    public String getTargetColumnName() {
        return targetColumnName;
    }

    /**
     * This method is used to set the target column length
     * 
     * @param targetColumnName
     */
    public void setTargetColumnName(String targetColumnName) {
        this.targetColumnName = targetColumnName;
    }
/**
 * This method is used to get the target data type
 * @return
 */
    public String getTargetDataType() {
        return targetDataType;
    }

    /**
     * This method is used to set the target data type
     * 
     * @param targetDataType
     */
    public void setTargetDataType(String targetDataType) {
        this.targetDataType = targetDataType;
    }
/**
 *  This method is used to get the target column length
 * @return
 */
    public String getTargetColumnLength() {
        return targetColumnLength;
    }

    /**
     * This method is used to set the target column length
     * 
     * @param targetColumnLength
     */
    public void setTargetColumnLength(String targetColumnLength) {
        this.targetColumnLength = targetColumnLength;
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
