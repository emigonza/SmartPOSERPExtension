package com.smj.callout;

import java.util.Properties;

import org.compiere.model.CalloutEngine;
import org.compiere.model.GridField;
import org.compiere.model.GridTab;

/**
 * Callout que se encarga de llenar el campo name cuando el campo name1 y/o name2 es modificado
 * @author Freddy Rodriguez
 *
 */
public class CalloutFillName extends CalloutEngine { 

	/**
	 * se encarga de llenar el campo name cuando el campo name1 y/o name2 es modificado
	 * @param ctx
	 * @param WindowNo
	 * @param mTab
	 * @param mField
	 * @param value
	 * @return
	 */
	 public String fillName (Properties ctx, int WindowNo, GridTab mTab, GridField mField, Object value){
		 String name2 = (String)mTab.getValue("name2");
		 String name1 = (String)mTab.getValue("name1");
		 if (name2 == null){
			 name2 = "";
		 }
		 if (name1==null){
			 name1 = "";
		 }
		 String name = name2 +" "+name1;
		 mTab.setValue("name", name.trim());
		 return "";
	 }//fillName
}//CalloutFillName
