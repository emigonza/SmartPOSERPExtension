package com.smj.util;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Pattern;

import org.compiere.apps.form.Allocation;
import org.compiere.model.MUOMConversion;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.compiere.util.Trx;


public class DataQueries{
	/**	Logger			*/
	public static CLogger log = CLogger.getCLogger(Allocation.class);
	
	/**
	 * regresa el mensaje de una etiqueta
	 * @param value
	 * @return
	 */
	protected String getMessage(String value){
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT msgtext FROM ad_message WHERE value = '"+value+"' ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String msg = "";
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())	{
				msg = rs.getString("msgtext");
				
			}//if rs	
		}//try
		catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		return msg;
	}//getMessage
	
	/**
	 * regresa la lista de precios para el producto
	 * @param nameTrx
	 * @return
	 */
	protected HashMap<String, BigDecimal> getPriceList(Integer productoId){
		StringBuffer sql = new StringBuffer();
		sql.append("select  pricelist, pricestd, pricelimit from M_ProductPrice where m_product_id = "+productoId+" ");
		sql.append("and m_pricelist_version_id = ( select  max(m_pricelist_version_id) from M_ProductPrice ");
		sql.append("where m_product_id = "+productoId+") ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		HashMap<String, BigDecimal> priceList = new HashMap<String, BigDecimal>();
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())	{
				priceList.put("pricelist", rs.getBigDecimal("pricelist"));
				priceList.put("pricestd", rs.getBigDecimal("pricestd"));
				priceList.put("pricelimit", rs.getBigDecimal("pricelimit"));
			}//while rs	
		}//try
		catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		return priceList;
	}//getPriceList
	
	/**
	 * Regresa el valor del ID de la localicacion para un tercero 
	 * @param bpartnerValue
	 * @return
	 */
	protected Integer getLocationPartner(Integer bpartnerValue){
		Integer location = 0;
		StringBuffer sql = new StringBuffer();
		sql.append(" select c_bpartner_location_ID from c_bpartner_location where isactive = 'Y' ");
		sql.append(" and c_bpartner_ID =  "+bpartnerValue+" ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				location = rs.getInt("c_bpartner_location_ID");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		return location;
	}//getLotationPartner
	
	/**
	 * regresa el codigo de localizacion de la localizacion de un tercero
	 * @param bpartnerLocation
	 * @return
	 */
	protected Integer getLocation(Integer bpartnerLocation){
		Integer location = 0;
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT c_location_id FROM c_bpartner_location WHERE isactive = 'Y' ");
		sql.append(" AND c_bpartner_location_id = "+bpartnerLocation+" ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				location = rs.getInt("c_location_id");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		return location;
		
	}//getLotationPartner
	
	/**
	 * Regresa el valor del ID del representante de ventas, la organizacion, cliente y otros
	 * segun el loginName
	 * @param loginName
	 * @return
	 */
	protected HashMap<String, Integer> getSalesRep(String loginName){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT u.ad_user_id, u.ad_org_id, u.ad_client_id, u.c_bpartner_id, p.c_bp_group_id  FROM ad_user u, c_bpartner p ");
		sql.append(" WHERE u.isactive = 'Y' AND u.c_bpartner_id = p.c_bpartner_id AND u.name = '"+loginName+"' ");
		HashMap<String, Integer> data = new HashMap<String, Integer>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if(rs.next ()){
				data.put("ad_user_id", rs.getInt("ad_user_id"));
				data.put("ad_org_id", rs.getInt("ad_org_id"));
				data.put("ad_client_id", rs.getInt("ad_client_id"));
				data.put("c_bpartner_id", rs.getInt("c_bpartner_id"));
				data.put("c_bp_group_id", rs.getInt("c_bp_group_id"));
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return data;
	}//getSalesRep
	
	
	/**
	 * Regresa el valor del ID del representante de ventas, la organizacion, cliente y otros
	 * segun el Id del tercero
	 * @param partnerId
	 * @return
	 */
	protected HashMap<String, Integer> getSalesRepByPartner(String partnerId){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT u.ad_user_id, u.ad_org_id, u.ad_client_id, u.c_bpartner_id, p.c_bp_group_id  FROM ad_user u, c_bpartner p ");
		sql.append(" WHERE u.isactive = 'Y' AND u.c_bpartner_id = p.c_bpartner_id AND p.c_bpartner_id = "+partnerId+" ");
		HashMap<String, Integer> data = new HashMap<String, Integer>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			rs.next ();
				data.put("ad_user_id", rs.getInt("ad_user_id"));
				data.put("ad_org_id", rs.getInt("ad_org_id"));
				data.put("ad_client_id", rs.getInt("ad_client_id"));
				data.put("c_bpartner_id", rs.getInt("c_bpartner_id"));
				data.put("c_bp_group_id", rs.getInt("c_bp_group_id"));
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return data;
	}//getSalesRep
	
	/**
	 * regresa el codigo de tax (impiesto) segun su nombre
	 * @param taxName
	 * @return
	 */
	protected Integer getTaxId(String taxName){
		Integer code = -1;
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT C_Tax_id FROM C_Tax WHERE isactive = 'Y' ");
		sql.append(" AND name = '"+taxName+"' ");
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			rs.next ();
			code = rs.getInt("C_Tax_id");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return code;
	}//getTaxId
	
	/**
	 * regresa el codigo del tercero por su numero de identificacion
	 * @param taxId
	 * @return
	 */
	protected Integer getPartnerByTaxId(String taxId){
		Integer code = 0;
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT c_bpartner_id FROM c_bpartner WHERE taxid = '"+taxId.trim()+"' ");
//		System.out.println("SQL partner ....."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				code = rs.getInt("c_bpartner_id");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		System.out.println("partner code ...."+code);
		return code;
	}//getPartnerByTaxId
	
	/**
	 * Funcion que elimina acentos y caracteres especiales de
	 * una cadena de texto.
	 * @param input
	 * @return cadena de texto limpia de acentos y caracteres especiales.
	 */
	public String removeSpecialCharacter(String input) {
	    // Descomposicion canonica
	    String normalized = Normalizer.normalize(input, Normalizer.Form.NFD);
	    // Nos quedamos unicamente con los caracteres ASCII
	    Pattern pattern = Pattern.compile("\\P{ASCII}+");
	    return pattern.matcher(normalized).replaceAll("");
	}//remove
	
	/**
	 * regresa el codigo del pais por nombre
	 * @param name
	 * @return
	 */
	protected Integer getCountry(String name){
		Integer code = 0;
		name = removeSpecialCharacter(name).toUpperCase();
		if (name.equals("Panama")) return 274;
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT c_country_id FROM c_country WHERE UPPER(name) = '"+name.trim()+"' ");
//		System.out.println("country SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				code = rs.getInt("c_country_id");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return code;
	}//getCountry
	
	/**
	 * regresa el codigo de region por nombre y codigo de pais
	 * @param name
	 * @param country
	 * @return
	 */
	protected Integer getRegion(String name, Integer country){
		Integer code = 0;
		name = removeSpecialCharacter(name).toUpperCase();
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT c_region_id FROM c_region WHERE c_country_id = "+country+" ");
		sql.append(" AND UPPER(name) = '"+name.trim()+"' ");
//		System.out.println("region SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				code = rs.getInt("c_region_id");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return code;
	}//getRegion
	
	/**
	 * regresa el codigo de la ciudad por nombre y codigo de region
	 * @param name
	 * @param region
	 * @return
	 */
	protected Integer getCity(String name, Integer region){
		Integer code = 0;
		name = (name).toUpperCase();
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT c_city_id FROM c_city WHERE c_region_id = "+region+" ");
		sql.append(" AND UPPER(name) = '"+name.trim()+"' ");
//		System.out.println("city SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				code = rs.getInt("c_city_id");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return code;
	}//getCity
	
	/**
	 * regresa el nombre de la ciudad segun el codigo
	 * @param cityCod
	 * @return
	 */
	protected String getCityName(String cityCod){
		String name = "";
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT name FROM c_city WHERE c_city_id = "+cityCod.trim()+" ");
//		System.out.println("getCityName SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				name = rs.getString("name");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return name;
	}//getCityName
	
	/**
	 * regresa el listado de factura pendientes de pago
	 * @param code
	 * @return
	 */
	protected LinkedList<HashMap<String, Object>> getOpenItem(String code){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT documentno, c_invoice_id, c_order_id, grandtotal, openamt FROM RV_OpenItem ");
		sql.append(" WHERE issotrx = 'Y' AND c_bpartner_id = "+code+" ");
		sql.append(" ORDER BY dateinvoiced, c_invoice_id ASC" );
//		System.out.println("getOpenItem SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		LinkedList<HashMap<String, Object>> lista = new LinkedList<HashMap<String, Object>>();
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			while (rs.next ()){
				HashMap<String, Object> items = new HashMap<String, Object>();
				items.put("documentno", rs.getString("documentno"));
				items.put("c_invoice_id", rs.getInt("c_invoice_id"));
				items.put("c_order_id", rs.getInt("c_order_id"));
				items.put("grandtotal", rs.getBigDecimal("grandtotal"));
				items.put("openamt", rs.getBigDecimal("openamt"));
				lista.add(items);
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return lista;
	}//getOpenItem
	
	/**
	 * regresa los valores de entrega y orgen para hacer la devolucion a partir del codigo del ticket
	 * @param code
	 * @return
	 */
	protected HashMap<String, String> getInOutId(String code){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT i.ad_client_id, i.ad_org_id, i.c_bpartner_id, i.m_inout_id FROM c_order o, m_inout i ");
		sql.append(" WHERE i.c_order_id = o.c_order_id AND o.poreference = '"+code.trim()+"' ");
//		System.out.println("getInOutId SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		HashMap<String, String> items = new HashMap<String, String>();
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				
				items.put("ad_client_id", rs.getString("ad_client_id"));
				items.put("ad_org_id", rs.getString("ad_org_id"));
				items.put("c_bpartner_id", rs.getString("c_bpartner_id"));
				items.put("m_inout_id", rs.getString("m_inout_id"));
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return items;
	}//getInOutId
	
	/**
	 * regresa el codigo del cliente a partir de la organizacion
	 * @param code
	 * @return
	 */
	protected Integer getClient(String code){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT ad_client_id FROM ad_org WHERE ad_org_id = "+code.trim()+" ");
//		System.out.println("getClient SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Integer items = 0;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				items = rs.getInt("ad_client_id");
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return items;
	}//getClient
	
	/**
	 * regresa el codigo del almacen a partir de la organizacion
	 * @param code
	 * @return
	 */
	protected Integer getWarehouse(Integer code){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT M_Warehouse_ID FROM M_Warehouse WHERE ad_org_id = "+code+" ");
//		System.out.println("getWarehouse SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Integer items = 0;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				items = rs.getInt("M_Warehouse_ID");
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return items;
	}//getWarehouse
	
	/**
	 * regresa el codigo del almacen a partir de la organizacion
	 * @param code
	 * @return
	 */
	protected Integer getLocator(Integer code){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT m_locator_id FROM m_locator WHERE ad_org_id = "+code+" ");
//		System.out.println("getWarehouse SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Integer items = 0;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				items = rs.getInt("m_locator_id");
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return items;
	}//getWarehouse
	
	/**
	 * actualiza la organizacion cuando es Organizacion "*"
	 * @param trx
	 * @param orgID
	 * @param clientId
	 * @param locationID
	 * @return
	 */
	protected Boolean updateOrgZero(Trx trx, Integer orgID,Integer clientId, Integer locationID){
		  StringBuffer sql = new StringBuffer();
		  sql.append("update c_location set ad_org_id =" +orgID+", ad_client_id="+clientId+" ");
		  sql.append("where c_location_id ="+locationID+" ");
		  System.out.println("updateOrgZero SQL: "+sql.toString());
		  PreparedStatement pstmt = null;
		  Integer rs = -1;
		  Boolean update = false;
		  try {
		   pstmt = DB.prepareStatement (sql.toString(),trx.getTrxName());
		   rs = pstmt.executeUpdate();		   
		   update = rs>0?true:false;
		  }//try
		  catch (Exception e) {
		   log.log (Level.SEVERE, sql.toString(), e);
		   e.printStackTrace();
		  }
		  finally {
		   DB.close(pstmt); 
		   rs = null; pstmt = null;
		  }
//System.out.println(update);
		  return update;
	}//updateOrgZero

	/**
	 * verifica que no este creada una orden para el mismo numero de ticket POS
	 * @param value
	 * @return
	 */
	protected Integer getExistsTicket(String value){
		Integer code = 0;
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT count(1) as total FROM c_order WHERE poreference = '"+value+"' ");
//		System.out.println("city SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ())
				code = rs.getInt("total");
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return code;
	}//getExistsTicket
	
	/**
	 * Conversion factor of UOM for a product
	 * @param productId
	 * @param uomId
	 * @param trx
	 * @return
	 */
	public BigDecimal getConversionFactorUOM(int productId,int uomId, Trx trx )
    {
		PreparedStatement	pstmt = null;
		ResultSet 			rs = null;
		BigDecimal 			divideRate = new BigDecimal(1); 
				
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT  umo.c_uom_conversion_id as umoCid");
		sql.append("  FROM  c_uom_conversion  as umo ");
		sql.append(" WHERE  m_product_id = "+productId+" and c_uom_to_id = "+uomId+"");
		try{
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
		
			if(rs.next()){
				int umoConversionId = rs.getInt("umoCid");
				MUOMConversion uomC = new MUOMConversion(Env.getCtx(), umoConversionId, null);
				divideRate = uomC.getDivideRate();
			
			}
		}catch(SQLException e){
			log.log (Level.SEVERE, sql.toString(), e);
			e.printStackTrace();
		}finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
	return divideRate;
    }
	// 	
	
	
	protected String getListKeyFromName(String id, String name, Trx trx ) {
		PreparedStatement	pstmt = null;
		ResultSet 			rs = null;
		String  			key = null; 
//		String 				id = Msg.getMsg(Env.getCtx(), "currencyId").trim();
				
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT  value ");
		sql.append("  FROM  AD_Ref_List ");
		sql.append(" WHERE  AD_Reference_ID = "+id+" and name ILIKE '%"+name+"%' ");
		
		try{
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
		
			if(rs.next()){
				key = rs.getString("value");
			}
		}catch(SQLException e){
			log.log (Level.SEVERE, sql.toString(), e);
			e.printStackTrace();
		}finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		
		return key;
	}
	
	protected int getCashBook(int clientId, int orgId, Trx trx ) {
		PreparedStatement	pstmt = null;
		ResultSet 			rs = null;
		int     			key = 0; 
//		String 				id = Msg.getMsg(Env.getCtx(), "currencyId").trim();
				
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT  C_CashBook_id ");
		sql.append("  FROM  C_CashBook ");
		sql.append(" WHERE  AD_client_ID = "+clientId+" and AD_org_id = "+orgId+" ");
		
		try{
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
		
			if(rs.next()){
				key = rs.getInt("C_CashBook_id");
			}
		}catch(SQLException e){
			log.log (Level.SEVERE, sql.toString(), e);
			e.printStackTrace();
		}finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		
		return key;
	}
	
	public int getDocTypeId(String name,int orgId,int clientId,Trx trx)
    {
        String docTypeSQl = "SELECT C_DocType_ID FROM C_DocType "
            + "WHERE name like  = ? and ad_client_id = " +clientId + " and ad_org_id = " + orgId ;
        
        int docTypeId = DB.getSQLValue(trx.getTrxName(), docTypeSQl, "%"+name+"%");
        
        return docTypeId;
    }
	
	/**
	 * regresa la lista de precios para el producto
	 * @param nameTrx
	 * @return
	 */
	protected List<Integer> getPaymentIdList(Integer orderId){
		StringBuffer sql = new StringBuffer();
		sql.append("select  c_payment_id from c_payment where c_order_id = "+orderId+" ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Integer> paymentList = new ArrayList<Integer>();
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			while (rs.next ())	{
				paymentList.add(rs.getInt("c_payment_id"));
			}//while rs	
		}//try
		catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		return paymentList;
	}//getPriceList
	
	protected Integer getOrderId(String code){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT c_order_id FROM c_order WHERE poreference = '"+code.trim()+"' ");
//		System.out.println("getClient SQL.."+sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Integer items = 0;
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				items = rs.getInt("c_order_id");
			}
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return items;
	}//getClient
	
	
	protected String getAdminLogin(String clientId){
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT U.NAME AS NAME");
		sql.append(" FROM AD_ROLE AS R");
		sql.append(" ,AD_USER_ROLES AS UR");
		sql.append(" ,AD_USER AS U");
		sql.append(" WHERE R.NAME ILIKE '%Admin%' ");
		sql.append(" AND R.ISACTIVE = 'Y' ");
		sql.append(" AND R.AD_CLIENT_ID = "+clientId);
		sql.append(" AND R.AD_ORG_ID = 0");
		sql.append(" AND UR.AD_ROLE_ID = R.AD_ROLE_ID");
		sql.append(" AND U.AD_USER_ID = UR.AD_USER_ID");
		sql.append(" AND R.AD_CLIENT_ID = U.AD_CLIENT_ID");
		sql.append(" AND R.AD_ORG_ID = U.AD_ORG_ID");

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		String items = "";
		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				items = rs.getString("NAME");
			}else
				items =  Msg.getMsg(Env.getCtx(), "userAdmin").trim();
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
		
		return items;
	}//getClient
	
	
	
	
}//Query