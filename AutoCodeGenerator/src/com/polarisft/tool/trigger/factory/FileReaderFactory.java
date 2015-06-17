package com.polarisft.tool.trigger.factory;

import com.polarisft.tool.impl.trigger.ExcelFileReader;
import com.polarisft.tool.trigger.FileReader;

/**
 * This class file is used to Evaluate the excel file extension or type
 */
public class FileReaderFactory {
    /**
     * This method is used to create the *sql files for the object returned.
     */
    public static FileReader create(String fileName) {

        FileReader fileReader = null;

        if (fileName != null) {
            String fileExtension = fileName.substring(fileName.lastIndexOf('.') + 1);
            if (fileExtension.equalsIgnoreCase("xls") || fileExtension.equalsIgnoreCase("xlsx")) {
                fileReader = new ExcelFileReader(fileName);
            }
        }
        return fileReader;
    }
}
