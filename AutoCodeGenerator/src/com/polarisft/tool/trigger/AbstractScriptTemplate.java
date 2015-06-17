package com.polarisft.tool.trigger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import com.polarisft.tool.trigger.bean.TableInformationBean;

/**
 * This class file is used to write the buffer contents into a file and name it
 * with the table name with .sql as extension.
 */
public abstract class AbstractScriptTemplate {
    
    
    protected static String PROCEDURE_DECLARATIONS = "/com/polarisft/tool/declarations/ProcedureDeclarations.txt";
    
    protected static String TRIGGER_DECLARATIONS = "/com/polarisft/tool/declarations/TriggerDeclarations.txt";
    
    protected static String RECONCILITION_DETAIL_DECLARATIONS = "/com/polarisft/tool/declarations/ReconciliationDetailDeclarations.txt";
    
    protected static String RECONCILITION_SUMMARY_DECLARATIONS = "/com/polarisft/tool/declarations/ReconciliationSummaryDeclarations.txt";
    
    protected static String RECON_SUMMARY_WRAPPER_DECLARATIONS = "/com/polarisft/tool/declarations/ReconWrapperSummaryDeclarations.txt";
    
    protected static String RECON_DETAIL_WRAPPER_DECLARATIONS = "/com/polarisft/tool/declarations/ReconWrapperDetailDeclarations.txt";
    
    private static String IDENTIFIER_STARTS_SUFFIX = " - STARTS#";
    
    private static String IDENTIFIER_ENDS_SUFFIX = " - ENDS#";
    
    protected static String IDENTIFIER_GLOBAL = "#GLOBAL";
    
    protected static String IDENTIFIER_SET1= "#SET1";
    
    protected static String IDENTIFIER_SET2 = "#SET2";
    
    protected static String IDENTIFIER_SET3 = "#SET3";
    
    protected static String IDENTIFIER_SET4 = "#SET4";
    
    protected static String IDENTIFIER_DECLARE1 = "#DECLARE1";
    
    
    /**
     * This method is used to generate the *.sql file in the specified 
     * outputpath
     */
    public abstract void generateScript(List<TableInformationBean> tableInformationBeanList, String outputDir);
    /**
     * This method is used to generate the *.sql file in the specified 
     * outputpath
     */
    protected void writeToFile(String fileName, StringBuffer contentBuffer) {
        BufferedWriter bufferedWriter = null;
        try {
            StringBuffer triggerFileName = new StringBuffer(fileName).append(".sql");
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(triggerFileName.toString()), "UTF-8"));
            bufferedWriter.write(contentBuffer.toString());

        } catch (UnsupportedEncodingException e) {
            System.out.println("Unable to Create sql file. Try again");
        } catch (FileNotFoundException e) {
            System.out.println("Unable to Create sql file. Try again");
        } catch (IOException e) {
            System.out.println("Unable to Create sql file. Try again");
        } finally {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                System.out.println("Unable to Create sql file. Try again");
            }
        }
    }
    
    protected void writeToBatchFile(String fileName, StringBuffer contentBuffer) {
        BufferedWriter bufferedWriter = null;
        try {
            StringBuffer triggerFileName = new StringBuffer(fileName).append(".bat");
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(triggerFileName.toString()), "UTF-8"));
            bufferedWriter.write(contentBuffer.toString());

        } catch (UnsupportedEncodingException e) {
            System.out.println("Unable to Create sql file. Try again");
        } catch (FileNotFoundException e) {
            System.out.println("Unable to Create sql file. Try again");
        } catch (IOException e) {
            System.out.println("Unable to Create sql file. Try again");
        } finally {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                System.out.println("Unable to Create sql file. Try again");
            }
        }
    }
    
    /**
     * This method is used to read the SQL variable declarations file and store the content as string buffer.
     * @param filePath - file path
     * @return StringBuffer containing the file contents
     */
    protected StringBuffer readDeclarationsFile(String filePath, int tabCount, String identifier) {
        StringBuffer fileContents = new StringBuffer();
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(
                    this.getClass().getResourceAsStream(filePath)));
            String line;
            boolean isIdentifierFound = false;
            while((line = bufferedReader.readLine()) != null) {
                if((identifier + IDENTIFIER_STARTS_SUFFIX).equals(line)) {
                    isIdentifierFound = true;
                    continue;
                } else if ((identifier + IDENTIFIER_ENDS_SUFFIX).equals(line)) {
                    break;
                }
                if(isIdentifierFound) {
                    fileContents.append(line).append("\n").append(insertTabs(tabCount));
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("Unable to read declarations file. Try again");
        } catch (IOException e) {
            System.out.println("Unable to read declarations file. Try again");
        } finally {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                System.out.println("Unable to read declarations file. Try again");
            }
        }
        return fileContents;

    }
    
    /**
     * This method is used to insert tab spaces where ever required
     * @param numberOfTabs
     * @return StringBuffer object with tabs
     */
    protected StringBuffer insertTabs(int numberOfTabs) {
        StringBuffer tabBuffer = new StringBuffer();
        for (int tabCount = 1; tabCount <= numberOfTabs; tabCount++) {
            tabBuffer.append("\t");
        }
        return tabBuffer;
    }
}
