package com.smj.callout;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;

import org.compiere.model.CalloutEngine;
import org.compiere.model.GridField;
import org.compiere.model.GridTab;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
/**
 * Callout que se encarga de validar que no esté activo para el mismo producto otra unidad por defecto
 * @author Freddy Rodriguez
 *
 */
public class CalloutdefaultSalesPOS extends CalloutEngine{

	/**
	 * se encarga de validar que no esté activo para el mismo producto otra unidad por defecto
	 * @param ctx
	 * @param WindowNo
	 * @param mTab
	 * @param mField
	 * @param value
	 * @return
	 */
	public String defaultSalesPOS (Properties ctx, int WindowNo, GridTab mTab, GridField mField, Object value){
		 Boolean defValue = (Boolean)mTab.getValue("defaultSalesPOS");
		 Integer productId = (Integer)mTab.getValue("m_product_id");
		 
		 String sql = "SELECT count(1) as total FROM M_ProductPrice " +
		 		" WHERE m_product_id = "+productId+"  AND defaultSalesPOS = 'Y' ";
		 PreparedStatement pstmt = null;   
		 ResultSet rs = null;
		 Integer total = 0;
		 try{
			 pstmt = DB.prepareStatement(sql, null); 
			 rs = pstmt.executeQuery();		
			 if (rs.next()){
			  total = rs.getInt("total");
			 }//while isTrx
		 } catch (SQLException e) {
			    log.log(Level.WARNING, "CalloutdefaultSalesPOS ", e); 
				  e.getStackTrace();
		 }//try/catch
		 finally{
				  DB.close(rs, pstmt);
				  pstmt = null;
				  rs = null;
		 }//finally
		 if (defValue && total >=1){
			 mTab.setValue("defaultSalesPOS", false);
			 return Msg.translate(Env.getCtx(), "msgDefaultSalesPOS").trim();
		 }
		 return "";
	 }//defaultSalesPOS
	
}//CalloutdefaultSalesPOS
