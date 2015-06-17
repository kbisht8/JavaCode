package com.polarisft.tool.trigger.factory;

import com.polarisft.tool.impl.trigger.BatchFileGenerator;
import com.polarisft.tool.impl.trigger.DateColumnValidationScript;
import com.polarisft.tool.impl.trigger.GenerateOneTimeSetupScript;
import com.polarisft.tool.impl.trigger.GenerateSQLDDL;

import com.polarisft.tool.impl.trigger.SourceColumnValidationScript;
import com.polarisft.tool.impl.trigger.DebugExcelSheet;
import com.polarisft.tool.trigger.AbstractScriptTemplate;

/**
 * This class file is used to read the input option from the user.
 */
public class ScriptTemplateFactory {

    /** Holds Option to generate the ReconciliationWrapper */
    public static final int RECONCILITION_WRAPPER = 0;

    /** Holds Option to generate the TRIGGER */
    public static final int TRIGGER_OPTION = 1;

    /** Holds Option to generate the PROCEDURES */
    public static final int PROCEDURE_OPTION = 2;

    /** Holds Option to generate the TARGETDDLSCRIPT */
    public static final int TARGET_DDL_SCRIPT_OPTION = 3;

    /** Holds Option to generate the RECONCILIATIONAUDITLOG */
    public static final int RECONCILIATION_AUDIT_LOG = 4;

    /** Holds Option to generate the RECONCILITIONSUMMARY */
    public static final int RECONCILITION_SUMMARY = 5;

    /** Holds Option to generate the RECONCILITIONDETAIL */
    public static final int RECONCILITION_DETAIL = 6;
    
    /** Added By Arun Kumar on 10th January 2014 
     * Holds Option to generate all the Staging Table Scripts */
    public static final int STAGING_TABLE_SCRIPT = 7;
    
    /** Added By Arun Kumar on 20th January 2014 
     * Holds Option to generate all the jobs Scripts */
    public static final int JOB_SCRIPT = 8;
    
    /** Holds Option to generate all jobs Scripts */
    public static final int SCRIPT_ALL = 9;
    
    /*public static final int TEST_SCRIPT = 10;*/
    public static final int DEBUG_EXCEL_SHEET = 11;
    
    /** Written by arun kumar for batch file generation*/
    public static final int BATCH_FILE_GENERATOR = 12;
    
    /** Written by arun kumar for spotl file generation*/
    public static final int SPOTL_PROCEDURE_GENERATOR = 13;
   
    /** Written by Ashish jain for data test script file generation*/
    public static final int DATA_TEST_SCRIPT_GENERATOR = 14 ;
   
    /** Written by arun kumar for spotl file generation*/
    public static final int SSV_TO_STAGING_TEST_SCRIPT = 15;
    
    /** Written by Ashish jain for RECON TABLE file generation*/
    public static final int RECON_TABLE_CREATION_SCRIPT = 16 ;
    
    public static final int SCHEMA_CREATION_SCRIPT = 17;
    
    public static final int FUNCTION_CREATION_SCRIPT = 18 ; 
    
    public static final int DATE_COlUMN_VALIDATION_SCRIPT = 19 ;
    
    public static final int SOURCE_COLUMN_VALIDATION_SCRIPT = 20 ;
    
    public static final int TESTING_DATA_CREATION_SCRIPT = 21;
    
    public static final int SP_INSERT_STAGING_TABLE_PROCEDURE_SCRIPT = 22;
    
    public static final int GENERATE_ONE_TIME_SETUP_SCRIPT = 23;
    
    public static final int GENERATE_SQL_DDL = 24;
   
    public static AbstractScriptTemplate create(int option) {

        AbstractScriptTemplate triggerTemplate = null;
        switch (option) {
       case DEBUG_EXCEL_SHEET:
			triggerTemplate = new DebugExcelSheet();
			break;  
		case BATCH_FILE_GENERATOR:
			triggerTemplate = new BatchFileGenerator();
			break;
		 case  DATE_COlUMN_VALIDATION_SCRIPT:
			 triggerTemplate = new DateColumnValidationScript();
	            break;
		 case  SOURCE_COLUMN_VALIDATION_SCRIPT:
			 triggerTemplate = new SourceColumnValidationScript();
	            break;
		 case GENERATE_ONE_TIME_SETUP_SCRIPT:
		 	triggerTemplate = new GenerateOneTimeSetupScript();
		 	break;
		 case GENERATE_SQL_DDL:
				triggerTemplate = new GenerateSQLDDL();
			 	break;
        }
        
        return triggerTemplate;
    }
}
