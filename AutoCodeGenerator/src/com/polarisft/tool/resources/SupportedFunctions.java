package com.polarisft.tool.resources;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellReference;


public class SupportedFunctions {
	
	public static boolean hasWhiteSpace(String str){
		str = str.trim();
		boolean iswhitespace= false;
		for (char c : str.toCharArray()) {
		    if (Character.isWhitespace(c)) {
		    	iswhitespace = true;
		    	break;
		    }else{
		    	iswhitespace= false;
		    }
		}
		return iswhitespace;
	}
	
	public static String namingConventionValidation(String str){
		str = str.trim();
		String errorMessage = null;
		boolean isFirstCharNumber = true;
		boolean isError = false;
		for (char c : str.toCharArray()) {
			if(isFirstCharNumber){
				if(Character.isDigit(c)){
					isError= true;
			    	errorMessage = "Error :"+c+" Number character exists in first place of value "+str+"'";
			    }
				isFirstCharNumber = false;
			}else if(c == '#' || c == '@' || c == '*'|| c == '$' || c == '&' || c == '%' || 
					c == '('|| c == ')' || c == '.' || c == '{' || c == '}' || c == '/' ||
					c == '\\' || c == ':' || c == '"'|| c == ','|| c == ';'|| c == '='|| c == '?' || c == '-'){
				isError = true;
		    	errorMessage = errorMessage +" \n"+ "Error :"+"'"+c+"'"+" special character exists in Value "+str;
		    }else if (Character.isWhitespace(c)) {
		    	isError = true;
		    	if(errorMessage != null){
		    		errorMessage = errorMessage+" \n" + "Error : White space exists in Value "+str;
		    	}else{
		    		errorMessage ="Error : White space exists in Value "+str;
		    	}
		    	
		    }
		}
		   if(isError){
			   return errorMessage;
			   
		   }else{
			   errorMessage = "NotError";
			   return errorMessage;
		   }
	}
	
	public static Cell getCellByReference(String cellRef,Sheet sheet){
		
		CellReference cellReference = new CellReference(cellRef);
		Row row = sheet.getRow(cellReference.getRow());
		Cell cell = row.getCell(cellReference.getCol());
		return cell;
		}
	
	
}

