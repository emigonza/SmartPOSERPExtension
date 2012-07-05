package com.smj.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import org.compiere.model.MAttachment;
import org.compiere.model.MAttachmentEntry;
import org.compiere.model.MBPartner;
import org.compiere.model.MBPartnerLocation;
import org.compiere.model.MClient;
import org.compiere.model.MLocation;
import org.compiere.model.MPaymentTerm;
import org.compiere.model.MProduct;
import org.compiere.model.MProductCategory;
import org.compiere.model.MProductPrice;
import org.compiere.model.MStorage;
import org.compiere.model.MTax;
import org.compiere.model.MTaxCategory;
import org.compiere.model.MUOM;
import org.compiere.model.MUOMConversion;
import org.compiere.model.MUser;
import org.compiere.model.MWarehouse;
import org.compiere.model.ModelValidationEngine;
import org.compiere.model.ModelValidator;
import org.compiere.model.PO;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;

public class ERPPosInterceptor implements ModelValidator {

	private int m_AD_Client_ID;
	/**	Logger			*/
	public static CLogger log = CLogger.getCLogger(ERPPosInterceptor.class);
	
	
	@Override
	public void initialize(ModelValidationEngine engine, MClient client) {
		if (client != null) {
			m_AD_Client_ID = client.getAD_Client_ID();
			}
		//register for model change on C_Order
		engine.addModelChange(MProduct.Table_Name, this);
		engine.addModelChange(MProductCategory.Table_Name, this);
		engine.addModelChange(MBPartner.Table_Name, this);
		engine.addModelChange(MBPartnerLocation.Table_Name, this);
		engine.addModelChange(MLocation.Table_Name, this);
		engine.addModelChange(MProductPrice.Table_Name, this);
		engine.addModelChange(MStorage.Table_Name, this);
		engine.addModelChange(MWarehouse.Table_Name, this);
		engine.addModelChange(MUser.Table_Name, this);
		engine.addModelChange(MUOM.Table_Name, this);
		engine.addModelChange(MUOMConversion.Table_Name, this);
		engine.addModelChange(MTaxCategory.Table_Name, this);
		engine.addModelChange(MTax.Table_Name, this);
		engine.addModelChange(MPaymentTerm.Table_Name, this);
		engine.addModelChange(MAttachment.Table_Name, this);
		//register for document events on MOrder
		//engine.addDocValidate(MOrder.Table_Name, this);
		
	}

	@Override
	public int getAD_Client_ID() {
		
		return m_AD_Client_ID;
	}
    /**
     * Llamado cuando un usuario da login a adempeire (util para restringir acceso a ciertas cosas)
     */
	@Override
	public String login(int AD_Org_ID, int AD_Role_ID, int AD_User_ID) {
		
		return null;
	}

	@Override
	/**
	 * Para eventos del modelo (escritura a tablas)
	 */
	public String modelChange(PO po, int type) throws Exception {
		if (type == TYPE_AFTER_CHANGE || type == TYPE_AFTER_NEW || type == TYPE_CHANGE)
		{
			// seccion para manejo de existencias al dia
System.out.println("Tname:"+po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim());
System.out.println("org:"+Env.getAD_Org_ID(Env.getCtx()));
System.out.println("Client:"+Env.getAD_Client_ID(Env.getCtx()));
			if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("STORAGE")))			
			{			
				String idCampo = po.get_ValueAsString("M_Product_ID");
//				String pedido = po.get_ValueAsString("QtyOnHand");
				int totalProducto = 0;
				
//				Properties ctx = Env.getCtx();
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				StringBuffer sql = new StringBuffer();
				try {
//					System.out.println("calcula sumatoria de existencias en M_storage");			
					
					sql.append("select sum(qtyonhand) from m_storage  ");
					sql.append("where m_product_id = "+ idCampo );
					System.out.println(sql);
					//System.out.println(sql);
					pstmt = DB.prepareStatement (sql.toString(), po.get_TrxName());
					rs = pstmt.executeQuery ();
					
					if (rs.next ())	{
						totalProducto = rs.getInt(1);
					}	
				} catch (Exception e) {
					log.log (Level.SEVERE, sql.toString(), e);
		            e.printStackTrace();
		            return "Sincronizacion con POS tuvo errores consultar log !";
				}
				finally	{
					DB.close(rs, pstmt); 
					rs = null; pstmt = null;
				}			
			 
			String productXML = po.getXmlRepresentation();
			String organization = po.get_ValueAsString("ad_org_id");
			MQClient.sendMessage(productXML,"POS-SYNC",Integer.toString(totalProducto), organization,"");
			}
			else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("BPARTNER")))			
			{		
				String productXML = po.getXmlRepresentation();
				String organization = po.get_ValueAsString("ad_org_id");
				String name1= po.get_ValueAsString("name1");
				MQClient.sendMessage(productXML,"POS-SYNC",name1, organization,"");
			}
			else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("PRODUCTPRICE")))			
			{		
				String productXML = po.getXmlRepresentation();
				String organization = po.get_ValueAsString("ad_org_id");
				MQClient.sendMessage(productXML,"POS-SYNC","", organization,"");
				String productID= po.get_ValueAsString("m_product_id");
				sendPriceListVersion(productID, po.get_TrxName());
			}
			else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("PRODUCT_CATEGORY")))			
			{
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				try{
					String organization = po.get_ValueAsString("ad_org_id");
					String productCategoryParentId = po.get_ValueAsString("m_product_category_parent_id");
					if(productCategoryParentId != null && !productCategoryParentId.trim().equals("")){
						StringBuffer sql = new StringBuffer();
						sql.append("select b1.M_Product_Category_ID  from M_Product_Category b1 ");
						sql.append("where b1.ad_client_id = "+m_AD_Client_ID+" and b1.ad_org_id =  "+organization );
						sql.append(" and b1.m_product_category_id = " + productCategoryParentId);
						sql.append(" and b1.name = 'POS' ");
						pstmt = DB.prepareStatement (sql.toString(), null);
						rs = pstmt.executeQuery ();
						
						if(rs.next()){
							String productXML = po.getXmlRepresentation();
							System.out.println("XML:" + productXML);
							MQClient.sendMessage(productXML,"POS-SYNC","", organization,"");
						}else{
							String name = "PRODUCT-CATEGORY";
							sendDeleteMessage(name, po.get_ID()+"", po.get_ValueAsString("ad_org_id"));
						}
					}else{
						String name = "PRODUCT-CATEGORY";
						sendDeleteMessage(name, po.get_ID()+"", po.get_ValueAsString("ad_org_id"));
					}
				}catch(Exception e){
					log.log (Level.SEVERE, null, e);
					e.printStackTrace();
				}
				finally	{
					DB.close(rs, pstmt); 
					rs = null; pstmt = null;
				}
				
				
			}
			else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("PRODUCT")))			
			{
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				try{
				String organization = po.get_ValueAsString("ad_org_id");
				String productCategoryParentId = po.get_ValueAsString("m_product_category_id");
				
				StringBuffer sql = new StringBuffer();		
				sql.append("select b2.M_Product_Category_ID  from M_Product_Category b1,  M_Product_Category b2 ");
				sql.append("where b1.ad_client_id = "+m_AD_Client_ID+" and b1.ad_org_id =  "+organization );
				sql.append(" and b1.m_product_category_parent_id = b2.m_product_category_id ");
				sql.append(" and b2.name = 'POS' ");
				sql.append(" and b1.m_product_category_id = " + productCategoryParentId);
				
				pstmt = DB.prepareStatement (sql.toString(), null);
				rs = pstmt.executeQuery ();
				
				if(rs.next()){
					String productXML = po.getXmlRepresentation();
					System.out.println("XML:" + productXML);
					MQClient.sendMessage(productXML,"POS-SYNC","", organization,"");
				}
				}catch(Exception e){
					e.printStackTrace();
					log.log (Level.SEVERE, null, e);
				}
				finally	{
					DB.close(rs, pstmt); 
					rs = null; pstmt = null;
				}
				
			}
			else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("LOCATION")))			
			{		
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				String organization = po.get_ValueAsString("ad_org_id");
				String locationId = po.get_ValueAsString("c_location_id");
				StringBuffer sql = null;
				
				if(organization.equalsIgnoreCase("0")){
					
					try{
						sql = new StringBuffer();		
						sql.append("select b.ad_org_id as orgId  from c_bpartner_location b ");
						sql.append("where b.c_location_id = "+locationId );
						pstmt = DB.prepareStatement (sql.toString(), null);
						rs = pstmt.executeQuery ();
						if(rs.next()){
							organization = rs.getString("orgId");
						}else{
							organization = ""+Env.getAD_Org_ID(Env.getCtx());
						}
					}catch(Exception e){
						e.printStackTrace();
						log.log (Level.SEVERE, null, e);
					}
					finally	{
						DB.close(rs, pstmt); 
						rs = null; pstmt = null;
					}
				}

				String productXML = po.getXmlRepresentation();
				System.out.println("XML:" + productXML);
				if(!organization.trim().equals("0"))
					MQClient.sendMessage(productXML,"POS-SYNC","", organization,"");
				
			}else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("ATTACHMENT")))
			{
				if(po.get_ValueAsString("AD_TABLE_id").equalsIgnoreCase("208")){
					String organization = po.get_ValueAsString("ad_org_id");

					MAttachment attachment = (MAttachment)po;
					MProduct mProduct = MProduct.get(Env.getCtx(),  attachment.getRecord_ID());
					if(canSendProduct(organization, mProduct.get_ValueAsString("m_product_category_id"))){
						MAttachmentEntry entry =attachment.getEntry(0);
						MQClient.sendMessageBlob(entry.getData(), attachment.getRecord_ID()+"", organization);
					}

				}
			}
			else {
				String productXML = po.getXmlRepresentation();
				String organization = po.get_ValueAsString("ad_org_id");
				MQClient.sendMessage(productXML,"POS-SYNC","", organization,"");
			}
		}else if(type == TYPE_AFTER_DELETE){
			String organization;
			StringBuffer xml;
			String name="";
			String id = po.get_ID()+"";
			
			if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("PRODUCT"))){
				if(canSendProduct(po.get_ValueAsString("ad_org_id"),po.get_ValueAsString("m_product_category_id")))
					name = "PRODUCT";
				else
					return null;
			}else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("PRODUCT_CATEGORY"))){
				if(canSendProductCategory(po.get_ValueAsString("ad_org_id"),po.get_ValueAsString("m_product_category_parent_id")))
					name = "PRODUCT-CATEGORY";
				else 
					return null;
			}else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("BPARTNER"))){
				if(po.get_ValueAsBoolean("IsCustomer"))
					name = "BPARTNER";
				else
					return null;
			}else if ((po.get_TableName().substring(po.get_TableName().indexOf('_') + 1).toUpperCase().trim().equalsIgnoreCase("ATTACHMENT"))) {
				if(po.get_ValueAsString("AD_TABLE_id").equalsIgnoreCase("208")){
					MAttachment attachment = (MAttachment)po;
					MProduct mProduct = MProduct.get(Env.getCtx(),  attachment.getRecord_ID());
					if(canSendProduct(po.get_ValueAsString("ad_org_id"), mProduct.get_ValueAsString("m_product_category_id"))){
						name = "PHOTO";
						id= attachment.getRecord_ID()+"";
					}else
						return null;
				}else
					return null;
			}else{
				return null;
			}
			
			sendDeleteMessage(name,id,po.get_ValueAsString("ad_org_id"));
		}
		return null;
	}
	
	private boolean canSendProduct(String organization,String productCategoryId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			StringBuffer sql = new StringBuffer();		
			sql.append("select b2.M_Product_Category_ID  from M_Product_Category b1,  M_Product_Category b2 ");
			sql.append("where b1.ad_client_id = "+m_AD_Client_ID+" and b1.ad_org_id =  "+organization );
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
	
	private boolean canSendProductCategory(String organization, String productCategoryParentId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		if(productCategoryParentId == null || productCategoryParentId.equals("") )
			return false;
		try{
			StringBuffer sql = new StringBuffer();
			sql.append("select b1.M_Product_Category_ID  from M_Product_Category b1 ");
			sql.append("where b1.ad_client_id = "+m_AD_Client_ID+" and b1.ad_org_id =  "+organization );
			sql.append(" and b1.m_product_category_id = " + productCategoryParentId);
			sql.append(" and b1.name = 'POS' ");
	
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

	/**
	 * trae la listas de precios por cada version del producto arma el xml y lo envia a la cola
	 * @throws SQLException
	 */
	private void sendPriceListVersion(String productID, String trxName) throws SQLException{
		StringBuffer sql = new StringBuffer();
		StringBuffer xml = new StringBuffer();
		sql.append("SELECT p.m_product_id, l.M_PriceList_id, v.M_PriceList_Version_id, v.name as version, l.name as list, ");
		sql.append("p.pricelist, p.pricestd, p.pricelimit, v.ad_client_id, v.ad_org_id,  p.defaultSalesPOS  ");
		sql.append("from M_PriceList_Version v, M_PriceList l, M_ProductPrice p ");
		sql.append("where v.M_PriceList_id = l.M_PriceList_id AND v.M_PriceList_Version_id = p.M_PriceList_Version_id ");
		sql.append("and p.m_product_id = "+productID+" ");
		
		PreparedStatement pstmt = DB.prepareStatement (sql.toString(), trxName);
		ResultSet rs = pstmt.executeQuery ();
       
		while (rs.next ())	{
			String organization = ""+rs.getInt("ad_org_id");
			xml = new StringBuffer();
			xml.append("<?xml version=\"1.0\" ?><entityDetail><type>PRICELISTVERSION</type><detail>");
			xml.append("<AD_CLIENT_ID>"+rs.getInt("ad_client_id")+"</AD_CLIENT_ID>");
	       	xml.append("<AD_ORG_ID>"+rs.getInt("ad_org_id")+"</AD_ORG_ID>");	
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
			MQClient.sendMessage(xml.toString(),"POS-SYNC","", organization,"");
			}//while
	}//sendPriceListVersion
	
	/**
	 * Para eventos del modelo (escritura a documentos)
	 */
	@Override
	public String docValidate(PO po, int timing) {
		return null;
	}
	
	private void sendDeleteMessage(String name, String id, String  organization){
		StringBuffer xml = new StringBuffer();
		xml.append("<?xml version=\"1.0\" ?><entityDetail><type>DELETE-"+name+"</type><detail>");
		xml.append("<"+name+"-ID>" + id +  "</"+name+"-ID>");
		xml.append("</detail></entityDetail>");

		MQClient.sendMessage(xml.toString(),"POS-SYNC","", organization,"");
	}

}
