package com.polarisft.tool.impl.trigger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Scanner;
import java.util.logging.Logger;


import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.polarisft.tool.resources.StaticContainers;
import com.polarisft.tool.trigger.FileReader;
import com.polarisft.tool.trigger.AbstractScriptTemplate;
import com.polarisft.tool.trigger.ScriptLogger;
import com.polarisft.tool.trigger.bean.TableInformationBean;
import com.polarisft.tool.trigger.factory.FileReaderFactory;
import com.polarisft.tool.trigger.factory.ScriptTemplateFactory;
/**
 * This class is used to read the database information from the file and
 * generate *.sql files.
 * 
 * @param args
 */
public class AutoScriptGenerator {

    /**
     * This method is used to read the database information from the file and
     * generate *.sql files.
     * 
     * @param args
     */
static	Map<String,String> serevrMap;
static StaticContainers staticContainers;
    public static void main(String[] args) throws Exception {
    	Logger scriptLoger;
    	scriptLoger = new ScriptLogger().getScriptLogger();
        List<TableInformationBean> tableInformationBeanList = null;
        Scanner inputReader = new Scanner(System.in);
        ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
        serevrMap = new HashMap<String,String>();
        scriptLoger.info("Start Executing Java Script");
        System.out.println("Please Enter Mapping Sheet Template File Name With Full Path And Extension");
        String MappingSheetTemplateFileName = inputReader.nextLine();
        scriptLoger.info(MappingSheetTemplateFileName + " is provided as input parameter file by user");
        FileReader fileReader = FileReaderFactory.create(MappingSheetTemplateFileName);
        if (fileReader == null) {
             	System.out.println(resourceBundle.getString("autoscriptGenerator.fileNotFoundException"));
             	scriptLoger.info(MappingSheetTemplateFileName + " is invalid file or file not found . Program will exit");
                System.exit(0);
               }

            try {
            	scriptLoger.info(MappingSheetTemplateFileName + " is valid file and read by program successfully.");
            	scriptLoger.info("Setting global parameters into static containder : Started");
            	PopulateGlobalVariables PopVar=new PopulateGlobalVariables();
            	PopVar.AssignGlobalVariables(MappingSheetTemplateFileName);
            	scriptLoger.info("Setting global parameters into static containder: Completed");
            	serevrMap = staticContainers.getMyMap();
            	
            	
            	GenerateMappingSheet.main(MappingSheetTemplateFileName);
            	scriptLoger.info("Execution completed. Program will exit");
            	System.out.println("Completed");
            	System.exit(0);

            	tableInformationBeanList = fileReader.getTableDetails();
            	
            	GenerateMappingSheet.IsMappingSheet="N";GenerateMappingSheet.IsOneTimeLoad="Y";
            	GenerateMappingSheet.main(MappingSheetTemplateFileName);
            	
            } catch (InvalidFormatException e) {
                System.out.println(resourceBundle.getString("autoscriptGenerator.invalidFileFormatException"));
                System.exit(0);
            } catch (FileNotFoundException e) {
                System.out.println(resourceBundle.getString("autoscriptGenerator.fileNotFoundException"));
                System.exit(0);
            } catch (IOException e) {
                System.out.println(resourceBundle.getString("autoscriptGenerator.ioException"));
                System.exit(0);
            }
       

            int selectedOption=9;

            if (selectedOption == 10) {
                System.exit(1);
            } else if (selectedOption >= 0 && selectedOption < 10 || selectedOption==11 || selectedOption==12 || selectedOption==13 || 
            		   selectedOption==14 || selectedOption==15 || selectedOption==16 ||selectedOption==17 || selectedOption==18 || 
            		   selectedOption==19|| selectedOption==20|| selectedOption==21 || selectedOption==22) {
            	
                System.out.println("Generate procedure");
                String outputDir = serevrMap.get("DDLScriptOutputPath");
                File file = new File(outputDir);
                if (!file.exists()) {
                	file.mkdirs();
                }
                int[] executionOptions = null;
                if (selectedOption == 9) {
                    executionOptions = new int[] { 
                    		ScriptTemplateFactory.GENERATE_ONE_TIME_SETUP_SCRIPT,
                    		ScriptTemplateFactory.GENERATE_SQL_DDL,
                            ScriptTemplateFactory.BATCH_FILE_GENERATOR
                      };
                } else {
                    executionOptions = new int[] { selectedOption };
                }

                for (int optionCount = 0; optionCount < executionOptions.length; optionCount++) {
                    AbstractScriptTemplate triggerTemplate = ScriptTemplateFactory.create(executionOptions[optionCount]);
                    
                    try {
                        triggerTemplate.generateScript(tableInformationBeanList, outputDir);
                        System.out.println(resourceBundle.getString("autoscriptGenerator.optionCompleted"));
                       
                    } catch (Exception exp) {
                    	exp.printStackTrace();
                    	
                        System.out.println(resourceBundle.getString("autoscriptGenerator.fileWriteError"));
                        continue;
                    }
                }
                System.out.println("Generate procedure....2");
            } else {
                System.out.println(resourceBundle.getString("autoscriptGenerator.invalidOption"));
            }


    }

}
