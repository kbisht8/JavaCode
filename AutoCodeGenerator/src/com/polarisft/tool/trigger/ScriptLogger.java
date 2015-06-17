package com.polarisft.tool.trigger;
import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import com.polarisft.tool.impl.trigger.ExcelFileReader;

/**
 * @author arunkumar.roopchand
 * This class creates the log file that is written as per requirement.
 */
public class ScriptLogger {
	/**
	 * This class is used to read the information from the excel file.
	 */
	ExcelFileReader efr;
	/**
	 * logger holds the object of Logger class.
	 */
	private final static Logger logger = Logger.getLogger(ExcelFileReader.class.getName()); 
	/**
	 * get the resource bundle that has the constant properties.
	 */
	private final ResourceBundle resourceBundle = ResourceBundle.getBundle("com.polarisft.tool.resources.AutoScriptGeneratorResource", Locale.getDefault());
	 /**
	 * This method create a folder and log file.
	 * If folder is already exists then new folder will not be created.
	 * FileHandler will handle the log file.
	 * SimpleFormatter add the simple format of text in  FileHandler.
	 * @return logger
	 * @throws SecurityException
	 * @throws IOException
	 */
	public Logger getScriptLogger() throws SecurityException, IOException{
		 FileHandler fh;
		 File file = new File(resourceBundle.getString("autoscriptGenerator.scriptLogFolder"));
         if (!file.exists()) {
         	file.mkdirs();
         }
	     fh = new FileHandler(resourceBundle.getString("autoscriptGenerator.scriptLogFilePath"),true);
	     logger.addHandler(fh);
	     SimpleFormatter formatter = new SimpleFormatter();  
	     fh.setFormatter(formatter);		
		 return logger;
	 }
}
