package com.smj.util;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Level;

import org.compiere.model.MAttachment;
import org.compiere.model.MAttachmentEntry;
import org.compiere.model.MBPartner;
import org.compiere.model.MBPartnerLocation;
import org.compiere.model.MCity;
import org.compiere.model.MCountry;
import org.compiere.model.MLocation;
import org.compiere.model.MPaymentTerm;
import org.compiere.model.MProduct;
import org.compiere.model.MProductCategory;
import org.compiere.model.MProductPrice;
import org.compiere.model.MRegion;
import org.compiere.model.MStorage;
import org.compiere.model.MTax;
import org.compiere.model.MTaxCategory;
import org.compiere.model.MUOM;
import org.compiere.model.MUOMConversion;
import org.compiere.model.MUser;
import org.compiere.process.SvrProcess;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.compiere.util.Trx;

/**
 * This process retrieves all the info from the ERP, build XML messages and send them to the POS
 * using JMS to be read and process by the POS synchronization process 
 * @author pedrorozo
 *
 */
public class SincronizaPOS extends SvrProcess {

	protected CLogger log = CLogger.getCLogger(super.getClass());
	
	public void syncSpecificPOS(String client,String organization, String pcName){
		Properties ctx = Env.getCtx();
		ctx.setProperty("#AD_Client_ID", client);
		ctx.setProperty("#AD_Org_ID", organization);
		
		process(client,organization,ctx,pcName);
	}
	
	@Override
	protected void prepare() {
	}
	/**
	 * Complete synchronization process
	 * @param client
	 * @param organization
	 * @param ctx
	 * @param pcName
	 * @return
	 */
	private String process(String client,String organization,Properties ctx,String pcName){
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		ResultSet rs = null;
		
		Trx trx = Trx.get(Trx.createTrxName("AL"), true);
		int idItem;
		StringBuffer sql = new StringBuffer();
		try {
           //Envia a la cola JMS todas las Categorias de impuesto
			log.log(Level.INFO,"Tax Categories");
			sql = new StringBuffer();
			sql.append("select C_TaxCategory_id from C_TaxCategory  ");
			sql.append("where ad_client_id = "+client+"  and isactive = 'Y' ");
			
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs11 = pstmt.executeQuery ();
           			
			while (rs11.next ())	{
				idItem = rs11.getInt("C_TaxCategory_id");
				MTaxCategory taxCat = new MTaxCategory(ctx, idItem, null);
				MQClient.sendMessage(taxCat.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
 			}
			rs11.close();
			pstmt.close();
			
			// Envia a la cola JMS todas los impuestos
			log.log(Level.INFO,"Taxes");
			sql = new StringBuffer();
			sql.append("select C_Tax_id from C_Tax ");
			sql.append("where ad_client_id = "+client+"  and isactive = 'Y' ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs12 = pstmt.executeQuery ();
           			
			while (rs12.next())	{
				idItem = rs12.getInt("C_Tax_id");
				MTax tax = new MTax(ctx, idItem, null);
				MQClient.sendMessage(tax.getXmlRepresentation(),"POS-SYNC","",organization,pcName);
 			}
			rs12.close();
			pstmt.close();
			
			// Envia a la cola JMS todas los Terminos de Pago
			log.log(Level.INFO,"Payment terms");
			sql = new StringBuffer();
			sql.append("select C_PaymentTerm_id from C_PaymentTerm ");
			sql.append("where ad_client_id = "+client+"  and isactive = 'Y' ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs13 = pstmt.executeQuery ();
           			
			while (rs13.next ())	{
				idItem = rs13.getInt("C_PaymentTerm_id");
				MPaymentTerm pay = new MPaymentTerm(ctx, idItem, null);
				MQClient.sendMessage(pay.getXmlRepresentation(),"POS-SYNC","", organization,pcName);
 			}
			rs13.close();
			pstmt.close();
			
			// Envia a la cola JMS todos las Unidades
			log.log(Level.INFO,"Unit of Measure");
			sql = new StringBuffer();
			sql.append("select C_UOM_ID from C_UOM ");
			sql.append("where (ad_client_id = "+client+" and ad_org_id =  "+organization+") ");
			sql.append(" or (ad_client_id =0) and isactive = 'Y' ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs6 = pstmt.executeQuery ();
           			
			while (rs6.next ())	{
				idItem = rs6.getInt("c_uom_id");
				MUOM uom = MUOM.get(ctx, idItem);
				MQClient.sendMessage(uom.getXmlRepresentation(),"POS-SYNC","",organization,pcName);
 			}
			rs6.close();
			pstmt.close();

			// Envia a la cola JMS todas las categorias de producto 
		    log.log(Level.INFO,"Product Categories");
			sql = new StringBuffer();
			sql.append("select b.M_Product_Category_ID  from M_Product_Category b,  M_Product_Category b1 ");
			sql.append("where b.ad_client_id = "+client+" and b.ad_org_id =  "+organization );
			sql.append(" and b.m_product_category_parent_id = b1.m_product_category_id ");
			sql.append(" and b1.name = 'POS' ");
			sql.append(" and  b.isactive = 'Y' ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs2 = pstmt.executeQuery ();
           			
			while (rs2.next ())	{
			// envia la cola la informacion del producto basica
				idItem = rs2.getInt("M_Product_Category_ID");
				MProductCategory p2 = MProductCategory.get(ctx, idItem);
				MQClient.sendMessage(p2.getXmlRepresentation(),"POS-SYNC","",organization,pcName);
				
 			}
			rs2.close();
			pstmt.close();

			// Envia a la cola JMS todos los productos 
			log.log(Level.INFO,"Products");
			sql = new StringBuffer();
			sql.append("select b.M_Product_ID  from M_Product b, M_Product_Category b1,  M_Product_Category b2 ");
			sql.append("where b.ad_client_id = "+client+" and b.ad_org_id =  "+organization );
			sql.append(" and b.m_product_category_id = b1.m_product_category_id ");
			sql.append(" and b1.m_product_category_parent_id = b2.m_product_category_id ");
			sql.append(" and b2.name = 'POS' ");
			sql.append(" and  b.isactive = 'Y' ");
			sql.append(" and  b1.isactive = 'Y' ");

			log.log(Level.INFO, sql.toString());
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs3 = pstmt.executeQuery ();
			
			ArrayList<Integer> productList = new ArrayList<Integer>();
			while(rs3.next ()){
				productList.add(rs3.getInt("M_Product_ID"));
			}
			rs3.close();
			pstmt.close();
			Iterator<Integer> iteratorProduct = productList.iterator();  
			
			while (iteratorProduct.hasNext())	{
				idItem = iteratorProduct.next();
				MProduct p3 = MProduct.get(ctx, idItem);
				MQClient.sendMessage(p3.getXmlRepresentation(),"POS-SYNC","", organization,pcName);
				
				//Envia la informacion de existencias 
				
				log.log(Level.INFO,"SUMS current qty of products (existencias) from M_storage");			
				StringBuffer sql2 = new StringBuffer();
				sql2.append("select sum(qtyonhand) from m_storage ");
				sql2.append("where m_product_id = "+ idItem );
				log.log(Level.INFO,sql2.toString());
				
				pstmt = DB.prepareStatement (sql2.toString(), trx.getTrxName());
				ResultSet rs4 = pstmt.executeQuery ();
				int totalProducto = 0;
				if (rs4.next ())	{
					totalProducto = rs4.getInt(1);
				}
				rs4.close();
				pstmt.close();
				
				MStorage[] p4 = MStorage.getOfProduct(ctx,  idItem, trx.getTrxName());
				if (p4.length > 0)
				{
				MQClient.sendMessage(p4[0].getXmlRepresentation(),"POS-SYNC",Integer.toString(totalProducto), organization,pcName);
				}
				
 			}
			
			// Product prices 
			log.log(Level.INFO,"Product - List prices");
			sql = new StringBuffer();
			sql.append("select b.M_Product_ID,b.m_pricelist_version_id  from M_Productprice b ");
			sql.append("where b.ad_client_id = "+client+" and b.ad_org_id =  "+organization);
			sql.append("and b.isactive = 'Y' ");
			//log.log(Level.INFO,sql);
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs5 = pstmt.executeQuery ();
           			
			while (rs5.next ())	{
				idItem = rs5.getInt("M_Product_ID");
				int idVersion = rs5.getInt("m_pricelist_version_id");
				MProductPrice p4 = MProductPrice.get(ctx, idVersion, idItem, trx.getTrxName());
				MQClient.sendMessage(p4.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
 			}
			rs5.close();
			pstmt.close();
			
			// Factores de conversion de las Unidades
			log.log(Level.INFO,"Conversion factors for units");
			sql = new StringBuffer();
			sql.append("select C_UOM_Conversion_ID from C_UOM_Conversion  ");
			sql.append("where ad_client_id = "+client+" and ad_org_id =  "+organization+" ");
			sql.append("and isactive = 'Y' ");
			//log.log(Level.INFO,sql);
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs7 = pstmt.executeQuery ();
           			
			while (rs7.next ())	{
				idItem = rs7.getInt("c_uom_Conversion_ID");
				MUOMConversion uomC = new MUOMConversion(ctx, idItem, null);
				MQClient.sendMessage(uomC.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
 			}
			rs7.close();
			pstmt.close();
			
			//  Paises
			log.log(Level.INFO,"Countries");
			sql = new StringBuffer();
			sql.append(" SELECT c_country_id FROM c_country where isactive = 'Y' "); //WHERE c_country_id in (274, 156)
			//log.log(Level.INFO,sql);
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs8 = pstmt.executeQuery ();
           			
			while (rs8.next ())	{
				idItem = rs8.getInt("c_country_id");
				MCountry ctry = MCountry.get(ctx, idItem);
				MQClient.sendMessage(ctry.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
 			}
			rs8.close();
			pstmt.close();
			
			// Regiones
			log.log(Level.INFO,"Regions");
			sql = new StringBuffer();
			sql.append("SELECT c_region_id FROM c_region where isactive = 'Y'   "); //WHERE c_country_id in (274, 156) 
			//log.log(Level.INFO,sql);
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs9 = pstmt.executeQuery ();
           			
			while (rs9.next ())	{
				idItem = rs9.getInt("c_region_id");
				MRegion reg = MRegion.get(ctx, idItem);
				MQClient.sendMessage(reg.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
 			}
			rs9.close();
			pstmt.close();
			
			// Regiones
			log.log(Level.INFO,"Cities");
			sql = new StringBuffer();
			sql.append("SELECT c_city_id FROM c_city where isactive = 'Y'   "); //WHERE c_country_id in (274, 156)
			//log.log(Level.INFO,sql);
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			ResultSet rs10 = pstmt.executeQuery ();
           			
			while (rs10.next ())	{
				idItem = rs10.getInt("c_city_id");
				MCity city = MCity.get(ctx, idItem);
				MQClient.sendMessage(city.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
 			}
			rs10.close();
			pstmt.close();
			
			// Terceros 
			log.log(Level.INFO,"Business Partners - Customers");	
			sql = new StringBuffer();
			sql.append("select b.c_bpartner_id, b.name, b.name2, name1 from c_bpartner b ");
			sql.append("where b.ad_client_id = "+client+" and b.ad_org_id =  "+organization );
			sql.append(" and b.isactive = 'Y' ");
			sql.append(" and b.iscustomer = 'Y'  ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
			
			while (rs.next ())	{
				idItem = rs.getInt("c_bpartner_id");
				String name1= rs.getString("name1");
				MBPartner p1 = MBPartner.get(ctx, idItem);
				
				
				MQClient.sendMessage(p1.getXmlRepresentation(),"POS-SYNC",name1, organization, pcName);
				if(p1.getTaxID() == null || p1.getTaxID().trim().equals("")){
					StringBuffer auxXML = new StringBuffer();
					auxXML.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
					auxXML.append("<entityDetail><type>SYNC-ERROR</type><detail>");
					auxXML.append("<Value>");
					auxXML.append("Business partner");
					auxXML.append(p1.get_ValueAsString("name"));
					auxXML.append(" (BPartnerId = ");
					auxXML.append(p1.get_ID());
					auxXML.append(")"); 
					auxXML.append(" doesn´t have a TaxID ");
					auxXML.append("</Value>");
					auxXML.append("</detail></entityDetail>");
					MQClient.sendMessage(auxXML.toString(),"POS-SYNC","", organization, pcName);
				}
					
			//  BPARTNER_LOCATION 
			
			    // obtiene todas las localizaciones-partner de un tercero
				MBPartnerLocation p2[] = MBPartnerLocation.getForBPartner(ctx, p1.get_ID(),trx.getTrxName()  );
				
				if(p2.length == 0){
					StringBuffer auxXML = new StringBuffer();
					auxXML.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
					auxXML.append("<entityDetail><type>SYNC-ERROR</type><detail>");
					auxXML.append("<Value>");
					auxXML.append("Business Partner ");
					auxXML.append(p1.getName());
					auxXML.append(" (BPartnerId = ");
					auxXML.append(p1.get_ID());
					auxXML.append(")"); 
					auxXML.append(" doesn´t have location ");
					auxXML.append("</Value>");
					auxXML.append("</detail></entityDetail>");
					MQClient.sendMessage(auxXML.toString(),"POS-SYNC","", organization, pcName);
				}else{
					for (int i=0;i< p2.length;i++){
						// envia los registros de la tabla BPARTNER_LOCATION 
						MQClient.sendMessage(p2[i].getXmlRepresentation(),"POS-SYNC","", organization, pcName);
						// aqui botine el registro location de cada BPARTNER_LOCATION y lo envia tambien
						MLocation loc = MLocation.get(ctx, p2[i].get_ValueAsInt("c_location_id") , trx.getTrxName());
						MQClient.sendMessage(loc.getXmlRepresentation(),"POS-SYNC","", organization, pcName);
					}
				}

				// Users - usuarios
				MUser u[] = MUser.getOfBPartner(ctx, idItem);
				
				for (int i=0;i< u.length;i++){
				 //	envia los registros de la tabla USERS de este tercero a la cola JMS 
					MQClient.sendMessage(u[i].getXmlRepresentation(),"POS-SYNC","", organization, pcName);
				}
			}//while rs consulta partners
			rs.close();
			pstmt.close();
			
			// Sent Credit Card types 
			log.log(Level.INFO,"Credit Card types");	
			sql = new StringBuffer();
			sql.append("select value,name from AD_Ref_List " );
			sql.append("where AD_Reference_id = "+Msg.getMsg(Env.getCtx(), "creditCardTypeId").trim()+" ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
			while(rs.next()){
				StringBuffer auxXML = new StringBuffer();
				auxXML.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
				auxXML.append("<entityDetail><type>CreditCard</type><detail>");
				
				auxXML.append("<Value>");
				auxXML.append(rs.getString("value"));
				auxXML.append("</Value>");
				
				auxXML.append("<Name>");
				auxXML.append(rs.getString("name"));
				auxXML.append("</Name>");
				
				auxXML.append("</detail></entityDetail>");
				MQClient.sendMessage(auxXML.toString(),"POS-SYNC","", organization, pcName);
			}
			rs.close();
			pstmt.close();
			
			
			//Envia attachments
			
			log.log(Level.INFO,"Send product attachments ");	
			sql = new StringBuffer();
			sql.append("SELECT record_id FROM ad_attachment " );
			sql.append("where ad_table_id = 208 and ad_client_id = " + client + " and ad_org_id =  "+organization+" ");
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
			while(rs.next()){
				MAttachment attachment = MAttachment.get(ctx, 208, rs.getInt("record_id"));
				MProduct mProduct = MProduct.get(Env.getCtx(),  attachment.getRecord_ID());
				if(canSendProduct(client,organization, mProduct.get_ValueAsString("m_product_category_id"))){
					MAttachmentEntry entry =attachment.getEntry(0);
					MQClient.sendMessageBlob(entry.getData(), attachment.getRecord_ID()+"", organization);
				}
			}
			rs.close();
			pstmt.close();


			// Send price lists
			sendPriceListVersion(client,organization, pcName);
			MQClient.sendMessage("<?xml version=\"1.0\" encoding=\"UTF-8\"?><entityDetail><type>SYNC-END</type><detail><Value>SYNC-END</Value></detail></entityDetail>","POS-SYNC","", organization, pcName);
				
		} catch (Exception e) {
			log.log (Level.SEVERE, null, e);
            e.printStackTrace();
            MQClient.sendMessage("<?xml version=\"1.0\" encoding=\"UTF-8\"?><entityDetail><type>SYNC-END-WITH-ERRORS</type><detail><Value>SYNC-END-WITH-ERRORS "+e.getMessage()+"</Value></detail></entityDetail>","POS-SYNC","", organization, pcName);
            return "Synchronization with POS migth have errors please check logs !";
		}
		finally	{
			DB.close(rs, pstmt); trx.close();
			rs = null; pstmt = null;
		}
		
		return "Successful POS Synchronization - Sincronicación exitosa !";
	}

	@Override
	protected String doIt() {
		Properties ctx = Env.getCtx();
		String organization = ""+Env.getAD_Org_ID(ctx);
		String client = ""+Env.getAD_Client_ID(ctx);
		
		return process(client,organization,ctx,"");
	}

	/**
	 * trae la listas de precios por cada version del producto arma el xml y lo envia a la cola
	 * @throws SQLException
	 */
	private void sendPriceListVersion(String client, String organization, String pcName) throws SQLException{
		log.log(Level.INFO,"SendPriceListVersion");
		StringBuffer sql = new StringBuffer();
		StringBuffer xml = new StringBuffer();
		sql.append("SELECT p.m_product_id, l.M_PriceList_id, v.M_PriceList_Version_id, v.name as version, l.name as list, ");
		sql.append("p.pricelist, p.pricestd, p.pricelimit, p.defaultSalesPOS  ");
		sql.append("from M_PriceList_Version v, M_PriceList l, M_ProductPrice p ");
		sql.append("where v.M_PriceList_id = l.M_PriceList_id AND v.M_PriceList_Version_id = p.M_PriceList_Version_id ");
		sql.append("and v.ad_client_id = "+client+" and v.ad_org_id = "+organization+" ");
		
		PreparedStatement pstmt = DB.prepareStatement (sql.toString(), null);
		ResultSet rs = pstmt.executeQuery ();
		StringBuffer head = new StringBuffer();
       	head.append("<?xml version=\"1.0\" ?><entityDetail><type>PRICELISTVERSION</type><detail>");
       	head.append("<AD_CLIENT_ID>"+client+"</AD_CLIENT_ID>");
       	head.append("<AD_ORG_ID>"+organization+"</AD_ORG_ID>");	
		while (rs.next ())	{
			xml = new StringBuffer();
			xml.append(head);
			xml.append("<M_PRODUCT_ID>"+rs.getInt("m_product_id")+"</M_PRODUCT_ID>");
			xml.append("<M_PRICELIST_ID>"+rs.getInt("M_PriceList_id")+"</M_PRICELIST_ID>");
			xml.append("<M_PRICELIST_VERSION_ID>"+rs.getInt("M_PriceList_Version_id")+"</M_PRICELIST_VERSION_ID>");
			xml.append("<VERSION>"+rs.getString("version")+"</VERSION>");
			xml.append("<LIST>"+rs.getString("list")+"</LIST>");
			xml.append("<PRICELIST>"+rs.getString("pricelist")+"</PRICELIST>");
			xml.append("<PRICESTD>"+rs.getString("pricestd")+"</PRICESTD>");
			xml.append("<PRICELIMIT>"+rs.getString("pricelimit")+"</PRICELIMIT>");
			xml.append("<DEFAULTSALESPOS>"+rs.getString("defaultSalesPOS")+"</DEFAULTSALESPOS>");
			xml.append("</detail></entityDetail>");
			MQClient.sendMessage(xml.toString(),"POS-SYNC","", organization,pcName);
			}//while
	}//sendPriceListVersion
	
	
	/**
	 * Returns list of products
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
	}//getCurrency
	
	public boolean canSendProduct(String client, String organization,String productCategoryId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			StringBuffer sql = new StringBuffer();		
			sql.append("select b2.M_Product_Category_ID  from M_Product_Category b1,  M_Product_Category b2 ");
			sql.append("where b1.ad_client_id = "+client+" and b1.ad_org_id =  "+organization );
			sql.append(" and b1.m_product_category_parent_id = b2.m_product_category_id ");
			sql.append(" and b2.name = 'POS' ");
			sql.append(" and b1.m_product_category_id = " + productCategoryId);
			
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			
			if(rs.next()){
				return true;
			}
		}catch(Exception e){
			e.printStackTrace();
			log.log (Level.SEVERE, null, e);
		}
		finally	{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		return false;
	}
	
}//SincronizaPOS
