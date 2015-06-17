package com.polarisft.tool.trigger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.polarisft.tool.trigger.bean.TableInformationBean;

/**
 * This interface is used to define the method tableDetails which contains the
 * information about the table.
 */
public interface FileReader {
/**
 * This method is used to read the input file and generate .sql files.
 * @return
 * @throws InvalidFormatException
 * @throws FileNotFoundException
 * @throws IOException
 */
    public List<TableInformationBean> getTableDetails() throws InvalidFormatException, FileNotFoundException, IOException;

}
