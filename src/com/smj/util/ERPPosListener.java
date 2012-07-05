package com.smj.util;


import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.compiere.model.MAllocationHdr;
import org.compiere.model.MAllocationLine;
import org.compiere.model.MBPartner;
import org.compiere.model.MBPartnerLocation;
import org.compiere.model.MInvoice;
import org.compiere.model.MLocation;
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
import org.compiere.model.MPayment;
import org.compiere.model.MUOMConversion;
import org.compiere.model.MUser;
import org.compiere.model.X_M_Production;
import org.compiere.model.X_M_ProductionPlan;
import org.compiere.process.DocAction;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.compiere.util.Trx;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.smj.model.InOutFromRma;
import com.smj.model.InvoiceFromRma;
import com.smj.model.ProcessingProduction;
import com.smj.model.RmaFromInOut;

public class ERPPosListener extends DataQueries implements MessageListener {
	protected CLogger log = CLogger.getCLogger(super.getClass());
    private final static SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    /**
     * Creates queue connection
     * @throws JMSException
     */
    public void run() 	throws JMSException {
    	//String subject = Msg.getMsg(Env.getCtx(), "inqueue").trim();
    	String subject = null;
    	PreparedStatement pstmt = null;
		ResultSet rs = null;
		StringBuffer sql = new StringBuffer();
		String url = Msg.getMsg(Env.getCtx(), "urlqueue").trim() ;
    	String mquser = Msg.getMsg(Env.getCtx(), "userqueue").trim();
    	String mqpasswd = Msg.getMsg(Env.getCtx(), "pwdqueue").trim();
    	ConnectionFactory connectionFactory = null;
    	Session session = null;
    	Connection connection = null;
    	
    	log.log(Level.INFO,"........................ Datos: "+subject+".."+url+".."+mquser+"..."+mqpasswd);
    	Trx trx = Trx.get(Trx.createTrxName("AL"), true);
    	
    	try{
    		sql.append("SELECT c.ad_client_id as id");
			sql.append("  FROM ad_client as c ");
			sql.append(" where c.ad_client_id >= 1000000");
			
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
			
			while(rs.next()){
				try{
					subject = "ERP"+rs.getString("id");
					connectionFactory = new ActiveMQConnectionFactory(mquser ,mqpasswd ,url); 
					connection = connectionFactory.createConnection();
					connection.start();
					session = connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
					
					Destination destination = session.createQueue(subject);
					MessageConsumer consumer = session.createConsumer(destination);
					consumer.setMessageListener(this);
					log.log(Level.INFO, "ERP Esperando Mensajes de los POS en cola:"+subject);
					System.out.println("ERP Esperando Mensajes de los POS en cola:"+subject);
				}catch(JMSException e ){
					log.log(Level.SEVERE, "Error creating order", e);
					e.printStackTrace();
				}
			}
    		
    	}catch (Exception e)	{
			log.log(Level.SEVERE, "Error creating order", e);
			e.printStackTrace();
		}finally{			
			DB.close(rs, pstmt);
			if(trx != null && trx.isActive())
				trx.close();
			rs = null; pstmt = null;
		}
    }

    /**
     * Triggered when a new message arrives to the ERP queue
     */
    public void onMessage(Message message) {
 		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
 	    try {
            TextMessage textMessage = (TextMessage) message;
        	Document document = null;
    		DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    		builderFactory.setNamespaceAware(true);
    		DocumentBuilder builder = builderFactory.newDocumentBuilder();
    		org.xml.sax.InputSource inStream = new org.xml.sax.InputSource();
    		String xml2 = textMessage.getText();
    		
    		// cleans garbage in the XML 
    		xml2 = xml2.trim();
    		int ini = xml2.indexOf("<entityDetail>");
    		xml2 = xml2.substring(ini);
    		xml = xml + xml2;
    		inStream.setCharacterStream(new java.io.StringReader(xml));
    		
    		document = builder.parse(inStream);
    		String tabla =  document.getElementsByTagName("type").item(0).getTextContent();
    		System.out.println("SMensaje Recibido:"+xml);
    		log.log(Level.INFO,"Mensaje Recibido:"+xml);
    		if (tabla.equalsIgnoreCase("ticket")) { //Procesa Ventas
    			processTicket(document);
    		}else if (tabla.equalsIgnoreCase("CUSTOMER_POS")) { //Procesa Terceros
    			processBPartner(document);
    		}else if (tabla.equalsIgnoreCase("customer-payment")) { //Procesa abono a facturas
    			processPayment(document);
    		} else if (tabla.equalsIgnoreCase("inventory-shipping")) { // process inventory shipping (before finishing the ticket) 
    		    processProductInventoryShipping(document);   // 
    			log.log(Level.WARNING, "**** Mensaje de Partial Refund *** "+tabla);
    		} else if (tabla.equalsIgnoreCase("product-refund-closed-ticket")) { //Procesa para devoluciones
    			processCompleteRefund(document);
    		} else if (tabla.equalsIgnoreCase("cash-in-out")) { //Procesa para devoluciones
    			processCashInOut(document);
    		} else if (tabla.equalsIgnoreCase("VOID-TICKET")) { //Procesa para devoluciones
    			processVoidTicket(document);
    		} else if(tabla.equalsIgnoreCase("synchronization-request")){
    			syncPOS(document);
    		} else  {
            	log.log(Level.WARNING, "**** Mensaje no reconocido aun por el listerner del ERP *** "+tabla);
    		}
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
    }
 
    /**
     * Procesa las anulaciones de las ordenes de compra
     * @param document
     */
    private void processVoidTicket(Document document) {
    	String ticketId = document.getElementsByTagName("id").item(0).getTextContent();
    	String pcName = document.getElementsByTagName("machine-hostname").item(0).getTextContent();
    	String org = document.getElementsByTagName("organization").item(0).getTextContent();
    	int client = this.getClient(org);
    	Trx trx = Trx.get(Trx.createTrxName("AL"), true);
    	boolean createNewTrx = false;
    	try{
    		int orderId = getOrderId(pcName.trim()+"_"+ticketId.trim());
    		Properties ctx = Env.getCtx();
    		
    		ctx.setProperty("#AD_Client_ID", client+"");
    		ctx.setProperty("#AD_Org_ID", org);
    		ctx.setProperty("#AD_Language", Msg.getMsg(Env.getCtx(), "defLanguage").trim());
    		MOrder order = new MOrder(ctx, orderId, trx.getTrxName());
    		
    		order =  new MOrder(ctx, orderId, trx.getTrxName());
    		for (MInvoice invoice: order.getInvoices()){
    			if(!MInvoice.DOCSTATUS_Completed.equals(invoice.getDocStatus())){
    				invoice.completeIt();
    				invoice.setDocStatus(MInvoice.STATUS_Completed);
    				invoice.save();
    				createNewTrx = true;
    			}
    		}
    		if(createNewTrx ){
    			trx.commit();
    			trx.close();
    			trx = Trx.get(Trx.createTrxName("AL"), true);
    			order =  new MOrder(ctx, orderId, trx.getTrxName());
    			createNewTrx = false;
    		}
    		
    		order.voidIt();
    		order.setDocAction("VO");
    		order.setDocStatus("VO");
    		order.save();
    		//order.voidIt();
    		trx.commit();
    		
    		List<Integer> paymentIdList = getPaymentIdList(orderId);

    		for(Integer i : paymentIdList){
    			MPayment payment = new MPayment(ctx, i, trx.getTrxName());
    			MAllocationHdr[] allocations =  MAllocationHdr.getOfPayment(ctx,payment.get_ID(), trx.getTrxName());
    			
    			for(MAllocationHdr a: allocations){
    				if(!MAllocationHdr.STATUS_Completed.equals(a.getDocStatus())){
    					a.set_TrxName(trx.getTrxName());
    					a.completeIt();
    					a.setDocStatus(MAllocationHdr.STATUS_Completed);
    					a.save();
    					createNewTrx = true;
    				}
    			}
    			
    			
    		}
    		
    		if(createNewTrx ){
    			trx.commit();
    			trx.close();
    			trx = Trx.get(Trx.createTrxName("AL"), true);
    			order =  new MOrder(ctx, orderId, trx.getTrxName());
    			paymentIdList = getPaymentIdList(orderId);
    		}
    		
    		for(Integer i : paymentIdList){
    			MPayment payment = new MPayment(ctx, i, trx.getTrxName());
    			
    			payment.reverseCorrectIt();
    			payment.setDocStatus("RE");
    			payment.setDocAction("--");
    			payment.save();
    		}
    		
    		
    		trx.commit();
    	}catch(Exception e){
    		e.printStackTrace();
    		trx.rollback();
    		String msg = " Error Procesando anulacion Ticket No " + ticketId +" - Message:: "+e.getMessage();
    		msg = msg +" - Cause:: "+ e.getCause();
    		sendError(msg, org,pcName);
    	}finally{
    		if(trx != null){
    			trx.close();
    		}
    	}
	}

    /**
     * Procesa el mensaje de sincronizacion enviado desde el pos
     * @param document
     */
	private void syncPOS(Document document) {
    	SincronizaPOS sincronizaPOS = new SincronizaPOS();
    	String client = document.getElementsByTagName("client").item(0).getTextContent();
    	String organization = document.getElementsByTagName("org").item(0).getTextContent();
    	String pcName = document.getElementsByTagName("pcTerminal").item(0).getTextContent();
    	sincronizaPOS.syncSpecificPOS(client, organization, pcName);
	}

	/**
     * Receives document and maps fields amongorder, invoice, shipment and payment
     * @param document
     * @return
     */
    private Boolean processTicket(Document document){
    	Boolean flag = false;
    	HashMap<String , Object> datos = new HashMap<String, Object>();
    	datos.put("organization", document.getElementsByTagName("organization").item(0).getTextContent());
    	datos.put("articlesCount", document.getElementsByTagName("articlesCount").item(0).getTextContent());
    	datos.put("m_dDate", document.getElementsByTagName("m_dDate").item(0).getTextContent());
    	datos.put("m_sId", document.getElementsByTagName("m_sId").item(0).getTextContent());
    	datos.put("name", document.getElementsByTagName("name").item(0).getTextContent());
    	datos.put("subTotal", document.getElementsByTagName("subTotal").item(0).getTextContent());
    	datos.put("tax", document.getElementsByTagName("tax").item(0).getTextContent());
    	datos.put("m_iTicketId", document.getElementsByTagName("m_iTicketId").item(0).getTextContent());
    	datos.put("tickettype", document.getElementsByTagName("tickettype").item(0).getTextContent());
    	datos.put("total", document.getElementsByTagName("total").item(0).getTextContent());
    	datos.put("totalPaid", document.getElementsByTagName("totalPaid").item(0).getTextContent());

    	datos.put("waiter-login", document.getElementsByTagName("waiter-login").item(0).getTextContent());
    	datos.put("transactionID", document.getElementsByTagName("transactionID").item(0).getTextContent());
    	String customerId = document.getElementsByTagName("customerId").item(0).getTextContent();
    	datos.put("customerId", customerId );
    	if (customerId.equals(null) || customerId.equals("null")){
    		datos.put("m_Customer", "");
    	}else{
    		NodeList nodeCus = document.getElementsByTagName("m_Customer");
        		HashMap<String , Object> data = new HashMap<String, Object>();
        		data.put("taxid", nodeCus.item(0).getAttributes().getNamedItem("taxid").getTextContent());
        		data.put("id", nodeCus.item(0).getAttributes().getNamedItem("id").getTextContent());
        		data.put("firstname", nodeCus.item(0).getAttributes().getNamedItem("firstname").getTextContent());
        		data.put("lastname", nodeCus.item(0).getAttributes().getNamedItem("lastname").getTextContent());
        		data.put("card", nodeCus.item(0).getAttributes().getNamedItem("card").getTextContent());
        		
        	datos.put("m_Customer", data );
    	}//if/ese
    	//list of products for order
    	NodeList nodes = document.getElementsByTagName("m_aLine");
    	NodeList nodeT = document.getElementsByTagName("tax-line");
    	LinkedList<HashMap<String, Object>> lista = new LinkedList<HashMap<String,Object>>();
    	for (int i = 0; i < nodes.getLength(); i++) {
    		String productId = nodes.item(i).getAttributes().getNamedItem("productID").getTextContent();
    		if (!(productId == null || productId.trim().equals("null"))){
	    		HashMap<String , Object> data = new HashMap<String, Object>();
				// aqui se extrae los atributos y valores del tag en un loop
	    		data.put("multiply", nodes.item(i).getAttributes().getNamedItem("multiply").getTextContent());
	    		data.put("price", nodes.item(i).getAttributes().getNamedItem("price").getTextContent());
	    		data.put("priceTax", nodes.item(i).getAttributes().getNamedItem("priceTax").getTextContent());
	    		data.put("productAttSetId", nodes.item(i).getAttributes().getNamedItem("productAttSetId").getTextContent());
	    		data.put("productAttSetInstDesc", nodes.item(i).getAttributes().getNamedItem("productAttSetInstDesc").getTextContent());
	    		data.put("productAttSetInstId", nodes.item(i).getAttributes().getNamedItem("productAttSetInstId").getTextContent());
	    		data.put("productCategoryID", nodes.item(i).getAttributes().getNamedItem("productCategoryID").getTextContent());
	    		data.put("productID", nodes.item(i).getAttributes().getNamedItem("productID").getTextContent());
	    		data.put("productName", nodes.item(i).getAttributes().getNamedItem("productName").getTextContent());
	    		data.put("productTaxCategoryID", nodes.item(i).getAttributes().getNamedItem("productTaxCategoryID").getTextContent());
	    		data.put("subValue", nodes.item(i).getAttributes().getNamedItem("subValue").getTextContent());
	    		data.put("tax", nodes.item(i).getAttributes().getNamedItem("tax").getTextContent());
	    		data.put("taxRate", nodes.item(i).getAttributes().getNamedItem("taxRate").getTextContent());
	    		data.put("m_iLine", nodes.item(i).getAttributes().getNamedItem("m_iLine").getTextContent());
	    		data.put("value", nodes.item(i).getAttributes().getNamedItem("value").getTextContent());
	    		data.put("unitID", nodes.item(i).getAttributes().getNamedItem("unitID").getTextContent());
	    		data.put("discountValue", nodes.item(i).getAttributes().getNamedItem("discountValue").getTextContent());
	    		data.put("discountRate", nodes.item(i).getAttributes().getNamedItem("discountRate").getTextContent());
		    		HashMap<String , Object> dataT = new HashMap<String, Object>();
		    		dataT.put("applicationOrder", nodeT.item(i).getAttributes().getNamedItem("applicationOrder").getTextContent());
		    		dataT.put("name", nodeT.item(i).getAttributes().getNamedItem("name").getTextContent());
		    	data.put("tax_name", nodeT.item(i).getAttributes().getNamedItem("name").getTextContent() );
		    		dataT.put("order", nodeT.item(i).getAttributes().getNamedItem("order").getTextContent());
		    		dataT.put("parentID", nodeT.item(i).getAttributes().getNamedItem("parentID").getTextContent());
		    		dataT.put("rate", nodeT.item(i).getAttributes().getNamedItem("rate").getTextContent());
		    		dataT.put("taxCategoryID", nodeT.item(i).getAttributes().getNamedItem("taxCategoryID").getTextContent());
		    		dataT.put("taxCustCategoryID", nodeT.item(i).getAttributes().getNamedItem("taxCustCategoryID").getTextContent());
		    		dataT.put("taxCustCategoryID", nodeT.item(i).getAttributes().getNamedItem("taxCustCategoryID").getTextContent());
	    		data.put("tax-line", dataT);
	    	datos.put("m_Customer", data );
	    		lista.add(data);
    		}//if productId null
		}//for nodos
    	//nodo Payment
    	NodeList nodePay = document.getElementsByTagName("payment");
    	LinkedList<HashMap<String, Object>> lista2 = new LinkedList<HashMap<String,Object>>();
    	for (int i = 0; i < nodePay.getLength(); i++) {//payment information
    		HashMap<String , Object> data = new HashMap<String, Object>();
    		data.put("paymentId", nodePay.item(i).getAttributes().getNamedItem("id").getTextContent());
    		data.put("paynotes", nodePay.item(i).getAttributes().getNamedItem("notes").getTextContent());
    		String paymentName = nodePay.item(i).getAttributes().getNamedItem("name").getTextContent(); 
    		data.put("name",paymentName );
    		data.put("total",nodePay.item(i).getAttributes().getNamedItem("total").getTextContent() );
    		if(paymentName.trim().equalsIgnoreCase("check")){
    			data.put("account-no",nodePay.item(i).getAttributes().getNamedItem("account-no").getTextContent());
    			data.put("check-no",nodePay.item(i).getAttributes().getNamedItem("check-no").getTextContent());
    			data.put("micr",nodePay.item(i).getAttributes().getNamedItem("micr").getTextContent());
    		}else if(paymentName.trim().equalsIgnoreCase("Credit Card")){
    			data.put("credit-card-type",nodePay.item(i).getAttributes().getNamedItem("credit-card-type").getTextContent());
    			data.put("number",nodePay.item(i).getAttributes().getNamedItem("number").getTextContent());
    			data.put("expiration-date",nodePay.item(i).getAttributes().getNamedItem("expiration-date").getTextContent());
    		}
    		lista2.add(data);
    	}//payment information end
    	datos.put("payments", lista2);
    	
//    	datos.put("paymentId", nodePay.item(0).getAttributes().getNamedItem("id").getTextContent());
//    	datos.put("paynotes", nodePay.item(0).getAttributes().getNamedItem("notes").getTextContent());
    	datos.put("m_aLine", lista);
    	datos.put("machine-hostname", document.getElementsByTagName("machine-hostname").item(0).getTextContent());
    	datos.put("PriceListID", document.getElementsByTagName("priceListID").item(0).getTextContent());
//    	imprime(datos);    // for debugging purpuses print content of data receive in hashmap
    	//revisa que no se haya creado previamente una orden con el mismo numero de ticket POS.
    	String poReference = ((String) datos.get("machine-hostname")).trim()+"_"+((String) datos.get("m_iTicketId")).trim();
    	Integer total = getExistsTicket(poReference);
    	if (total >=1 ){
    		throw new IllegalStateException("Ticket Exists : "+poReference);
    	}else{
    		createOrder(datos);
    	}
    	return flag;
    } //processTicket
    
    /**
     * Receives Document y maps field for business partner creation 
     * @param document
     * @return
     */
    private Boolean processBPartner(Document document){
    	Boolean flag = false;
		HashMap<String , Object> datos = new HashMap<String, Object>();
		datos.put("organization", document.getElementsByTagName("organization").item(0).getTextContent().trim());
		datos.put("C_BPartner_ID", document.getElementsByTagName("id").item(0).getTextContent());
		datos.put("name" , document.getElementsByTagName("name").item(0).getTextContent());
		datos.put("name1" , document.getElementsByTagName("firstName").item(0).getTextContent());
		datos.put("name2" , document.getElementsByTagName("lastName").item(0).getTextContent());
		datos.put("taxId" , document.getElementsByTagName("taxID").item(0).getTextContent());
		datos.put("value", document.getElementsByTagName("searchkey").item(0).getTextContent());
//		datos.put("DUNS", duns);
		datos.put("card" , document.getElementsByTagName("card").item(0).getTextContent());
		datos.put("maxDebt" , document.getElementsByTagName("maxDebt").item(0).getTextContent());
		datos.put("curdate" , document.getElementsByTagName("curdate").item(0).getTextContent());
		datos.put("curdebt" , document.getElementsByTagName("curdebt").item(0).getTextContent());
		datos.put("email" , document.getElementsByTagName("email").item(0).getTextContent());
		
		datos.put("region" , document.getElementsByTagName("region").item(0).getTextContent());
		datos.put("country" , document.getElementsByTagName("country").item(0).getTextContent());
		datos.put("city" , document.getElementsByTagName("city").item(0).getTextContent());
		datos.put("address" , document.getElementsByTagName("address").item(0).getTextContent());
		datos.put("address2" , document.getElementsByTagName("address2").item(0).getTextContent());
		datos.put("phoneNumber" , document.getElementsByTagName("phone").item(0).getTextContent());
		datos.put("phone2" , document.getElementsByTagName("phone2").item(0).getTextContent());
		datos.put("fax" , document.getElementsByTagName("fax").item(0).getTextContent());
		datos.put("postal" , document.getElementsByTagName("postal").item(0).getTextContent());
		datos.put("card" , document.getElementsByTagName("card").item(0).getTextContent());
		datos.put("fax" , document.getElementsByTagName("fax").item(0).getTextContent());
		datos.put("taxExempt" , document.getElementsByTagName("taxExempt").item(0).getTextContent());

		
//		imprime(datos);
		Trx trx = Trx.get(Trx.createTrxName("AL"), true);
        createBPartner(trx, datos); 		
        
        return flag;
    }//processBPartner
    
    /**
     * Receives Document y maps fields for payments 
     * @param document
     * @return
     */
    private Boolean processPayment(Document document){
    	Boolean flag = false;
    	String customerID = document.getElementsByTagName("customerID").item(0).getTextContent();
    	String payment = document.getElementsByTagName("payment-value").item(0).getTextContent();
    	String organization =  document.getElementsByTagName("organization").item(0).getTextContent();
		BigDecimal pay = new BigDecimal(payment);
		
		Date dateInvoice = new Date();
		HashMap<String, Integer> dataLogin = getSalesRepByPartner(customerID);
		Trx trx = Trx.get(Trx.createTrxName("AL"), true);
		dataLogin.put("currencyId", Integer.parseInt(Msg.getMsg(Env.getCtx(), "currencyId").trim()));
		String description = Msg.translate(Env.getCtx(), "description");
		LinkedList<HashMap<String, Object>> list = getOpenItem(customerID);
		try {
			Integer orgId = Integer.parseInt(organization);
			if (orgId >0){
				dataLogin.put("ad_client_id", getClient(organization));
				dataLogin.put("ad_org_id", orgId);
			}
			
			Boolean ok = createPaymentList(trx, dateInvoice, dataLogin, description, list, pay);
			if (!ok){
				trx.rollback();
			}else{
				trx.commit();
			}
			trx.close();
		} catch (InterruptedException e) {
			log.log (Level.SEVERE, null, e);
			e.printStackTrace();
			String msg = " Error procesando pago - Message:: "+e.getMessage();
    		msg = msg +" - Cause:: "+ e.getCause();
    		String pcName = document.getElementsByTagName("machine-hostname").item(0).getTextContent();
    		sendError(msg, organization,pcName);
		}
		
        return flag;
    }//processPayment
    
    
    /**
     * Receives Document and maping fields to create payments for Cash In out  
     * @param document
     * @return
     */
    private Boolean processCashInOut(Document document){
    	Boolean flag = false;
    	String customerID = document.getElementsByTagName("customerID").item(0).getTextContent();
    	String waiterLogin = document.getElementsByTagName("waiter-login").item(0).getTextContent();
    	String payment = document.getElementsByTagName("payment-value").item(0).getTextContent();
    	String organization =  document.getElementsByTagName("organization").item(0).getTextContent();
    	String inOut = document.getElementsByTagName("in-out").item(0).getTextContent();
		BigDecimal pay = new BigDecimal(payment);
		int clientId = getClient(organization);
		
		Date dateInvoice = new Date();
		//HashMap<String, Integer> dataLogin = getSalesRepByPartner(customerID);
		HashMap<String, Integer> dataLogin = getSalesRep(waiterLogin);
		if(dataLogin.size() == 0){
			dataLogin = getSalesRep(getAdminLogin(clientId+""));
		}
		Trx trx = Trx.get(Trx.createTrxName("AL"), true);
		dataLogin.put("currencyId", Integer.parseInt(Msg.getMsg(Env.getCtx(), "currencyId").trim()));
		
		
		dataLogin.put("c_bpartner_id", new Integer(customerID));
		Integer location = getLocationPartner(dataLogin.get("c_bpartner_id"));   // location ID 
		dataLogin.put("locationId", location);
		
		
		String description = Msg.translate(Env.getCtx(), "description");
		try {
			Integer orgId = Integer.parseInt(organization);
			if (orgId >0){
				dataLogin.put("ad_client_id", getClient(organization));
				dataLogin.put("ad_org_id", orgId);
			}
			
			Boolean ok = createPaymentForInOut(trx, dateInvoice, dataLogin, description,  pay, inOut);
			if (!ok){
				trx.rollback();
			}else{
				trx.commit();
			}
			trx.close();
		} catch (InterruptedException e) {
			log.log (Level.SEVERE, null, e);
			e.printStackTrace();
			String msg = " Error Procesando Entra y salida de caja - Message:: "+e.getMessage();
    		msg = msg +" - Cause:: "+ e.getCause();
    		String pcName = document.getElementsByTagName("machine-hostname").item(0).getTextContent();
    		sendError(msg, organization,pcName);
		}
		
        return flag;
    }//processPayment
    
    
    /**
     * Creates a complete refund (RMA, InOut and Invoice) 
     * @param document
     * @return
     */
    private Boolean processCompleteRefund(Document document){
    	Boolean flag = false;
    	HashMap<String , String> datos = new HashMap<String, String>();
    	datos.put("productId", document.getElementsByTagName("productId").item(0).getTextContent());
    	datos.put("productName", document.getElementsByTagName("productName").item(0).getTextContent());
    	datos.put("m_dDate", document.getElementsByTagName("m_dDate").item(0).getTextContent());
    	datos.put("unit", document.getElementsByTagName("unit").item(0).getTextContent());
    	datos.put("unit-amount", document.getElementsByTagName("unit-amount").item(0).getTextContent());
    	datos.put("price", document.getElementsByTagName("price").item(0).getTextContent());
    	datos.put("tax-name", document.getElementsByTagName("tax-name").item(0).getTextContent());
    	datos.put("subValue", document.getElementsByTagName("subValue").item(0).getTextContent());
    	datos.put("m_sId", document.getElementsByTagName("m_sId").item(0).getTextContent());
    	datos.put("m_iTicketId", document.getElementsByTagName("m_iTicketId").item(0).getTextContent());
    	datos.put("waiter-login", document.getElementsByTagName("waiter-login").item(0).getTextContent());
    	datos.put("value", document.getElementsByTagName("value").item(0).getTextContent());
    	datos.put("machine-hostname", document.getElementsByTagName("machine-hostname").item(0).getTextContent());
    	datos.put("PriceListID", document.getElementsByTagName("priceListID").item(0).getTextContent());
    	
    	String organization =  document.getElementsByTagName("organization").item(0).getTextContent();
		Integer orgId = 0;
		try {
			orgId = Integer.parseInt(organization);
		}catch (Exception e) {
		}
		Integer clientId = 0;
		if (orgId >0){
			clientId= getClient(organization);
			datos.put("ad_client_id", ""+clientId);
			datos.put("ad_org_id", organization);
		}
    	
    	HashMap<String, Integer> dataLogin = null;
    	String waiterLogin =document.getElementsByTagName("waiter-login").item(0).getTextContent();
    	if(waiterLogin.equalsIgnoreCase("AdminSistema")){
    		//waiterLogin = Msg.getMsg(Env.getCtx(), "userAdmin").trim();
    		waiterLogin = getAdminLogin(clientId+"");
    	}
    	dataLogin = getSalesRep(waiterLogin);
		if(dataLogin.size() == 0){
//			dataLogin = getSalesRep(Msg.getMsg(Env.getCtx(), "userAdmin").trim());
			dataLogin = getSalesRep(getAdminLogin(clientId+""));
		}
		datos.put("ad_user_id",""+dataLogin.get("ad_user_id"));

    	
    	String poReference = ((String) datos.get("machine-hostname")).trim()+"_"+((String) datos.get("m_iTicketId")).trim();
    	//datos para la devolucion
    	System.out.println("SProcesando devolucion");
    	HashMap<String, String> items = getInOutId(poReference);
    	
    	if (items.isEmpty()){
    		return false;
    	}
    	
		int inOutId = Integer.parseInt(items.get("m_inout_id")) ;
		
		
		
		datos.put("c_bpartner_id", items.get("c_bpartner_id"));
		datos.put("Name" , "Devolucion desde el POS "+document.getElementsByTagName("productName").item(0).getTextContent());
		datos.put("Amount" , document.getElementsByTagName("value").item(0).getTextContent());  // total amount of items to return 
		datos.put("RmaTypeID" , "1000000");    // type of RMA  - tipo de devolucion 
		datos.put("Qty" ,document.getElementsByTagName("productAmount").item(0).getTextContent());   // InOut Line ID  - liena entrega  asociada
		datos.put("DocTypeID" , "1000029");  // RMA  document type
		datos.put("bpartner_location", ""+getLocation(Integer.parseInt(items.get("c_bpartner_id"))));

		Trx trx = Trx.get(Trx.createTrxName("AL"), true);
		try{
			if(document.getElementsByTagName("affects-stock").item(0).getTextContent().trim().equalsIgnoreCase("true")){
				RmaFromInOut rmac = new RmaFromInOut();
		        Integer rma = rmac.createRMAfromInOut(trx, datos, inOutId);
		     // this section create a new Customer Return (INOUT document from a RMA)
				InOutFromRma r2i = new InOutFromRma();
				r2i.generateInOutFromRMA( rma,trx);
				InvoiceFromRma r2n = new InvoiceFromRma();
		    	r2n.generateInvoiceFromRMA(rma,trx);
			}else{
				datos.put("affects-stock-description",document.getElementsByTagName("affects-stock-description").item(0).getTextContent() );
				InvoiceFromRma r2n = new InvoiceFromRma();
				r2n.generateInvoice(trx, datos, inOutId);
			}
	
	    	trx.commit();
    	
		}catch(Exception e){
			log.log (Level.SEVERE, null, e);
			e.printStackTrace();
			trx.rollback();
			String msg = " Error procesando devolucion:: " + ((String) datos.get("m_iTicketId")).trim() +" - Message:: "+e.getMessage();
    		msg = msg +" - Cause:: "+ e.getCause();
    		String pcName = document.getElementsByTagName("machine-hostname").item(0).getTextContent();
    		sendError(msg, organization,pcName);
		}finally{
			if(trx != null && trx.isActive())
				trx.close();
		}
    	 
    	return flag;
    }//processRmaInOut
    
    /**
     * Creates a POS order (sales)  with lines and payment receipt
     * @param datos
     * @return
     */
    private String createOrder(HashMap<String , Object> datos) {
    	PreparedStatement pstmt = null;
		ResultSet rs = null;
    	
			Date dateInvoice = new Date();
			try {
				dateInvoice = sdf.parse((String) datos.get("m_dDate"));
			} catch (ParseException e1) {
				log.log(Level.WARNING, "Error al formatear la fecha ",e1);
				e1.printStackTrace();
			}
			Integer location = 0;   // location ID  PartnerLocation
			String description = Msg.translate(Env.getCtx(), "description"); //"orden de prueba desde java";  // descripcion de la order
			Integer productId = -1;   // product id  : frutos del pacifico ..
			Integer TaxID = Integer.parseInt(Msg.getMsg(Env.getCtx(), "TaxID").trim());  // id de tax   . ITBMS
			@SuppressWarnings("unchecked")
			LinkedList<HashMap<String, Object>> lista = (LinkedList<HashMap<String, Object>>) datos.get("m_aLine");
			LinkedList<HashMap<String, Object>> listaOrder = new LinkedList<HashMap<String,Object>>();
			//contiene la informacion de cliente, organizacion y otros
			HashMap<String, Integer> dataLogin = new HashMap<String, Integer>();
			String organization =  (String) datos.get("organization");
			Integer orgId = Integer.parseInt(organization);
			Integer clientId = getClient(organization);
			if (datos.get("waiter-login").equals("AdminSistema")){
//				datos.put("waiter-login", Msg.getMsg(Env.getCtx(), "userAdmin").trim());
				datos.put("waiter-login", getAdminLogin(clientId+""));
			}
			//busca la informacion de representante de ventas, cliente, organizacion y otros
			dataLogin = getSalesRep((String) datos.get("waiter-login"));
			if(dataLogin.size() == 0){
				//dataLogin = getSalesRep(Msg.getMsg(Env.getCtx(), "userAdmin").trim());
				dataLogin = getSalesRep(getAdminLogin(clientId+""));
				datos.put("waiter-login", getAdminLogin(clientId+""));
				
			}
			dataLogin.put("PaymentTerm", Integer.parseInt(Msg.getMsg(Env.getCtx(), "PaymentTerm").trim()));
			//dataLogin.put("PriceListID", Integer.parseInt(Msg.getMsg(Env.getCtx(), "PriceListID").trim()));
			dataLogin.put("PriceListID", Integer.parseInt(((String) datos.get("PriceListID")).trim()));
			dataLogin.put("DocTypeTargetID", Integer.parseInt(Msg.getMsg(Env.getCtx(), "DocTypeTargetID").trim()));
			dataLogin.put("currencyId", Integer.parseInt(Msg.getMsg(Env.getCtx(), "currencyId").trim()));
			if (!(datos.get("customerId").equals(null) || datos.get("customerId").equals("null"))){
				dataLogin.put("c_bpartner_id", Integer.parseInt((String) datos.get("customerId")));
				location= getLocationPartner(dataLogin.get("c_bpartner_id"));   // location ID 
				dataLogin.put("locationId", location);
			}else{
				dataLogin.put("c_bpartner_id", Integer.parseInt(Msg.getMsg(Env.getCtx(), "partnerDefault").trim()));
				dataLogin.put("locationId", Integer.parseInt(Msg.getMsg(Env.getCtx(), "PartnerLocation").trim()));
			}
			
			MOrder order = null;
			MOrderLine line = null;
			Trx trx = Trx.get(Trx.createTrxName("AL"), true);
		try{
			
			if (orgId >0){
				dataLogin.put("ad_client_id", getClient(organization));
				dataLogin.put("ad_org_id", orgId);
				datos.put("ad_client_id", getClient(organization));
				datos.put("ad_org_id", orgId);
			}
			
			dataLogin.put("M_Warehouse_ID", getWarehouse(dataLogin.get("ad_org_id")));
System.out.println("M_Warehouse_ID:"+getWarehouse(dataLogin.get("ad_org_id"))) ;
			
				order = new MOrder(Env.getCtx(), 0, trx.getTrxName());
				order.setIsActive(true);
				order.setIsSOTrx(true);
				order.setAD_Client_ID(dataLogin.get("ad_client_id"));
				order.setAD_Org_ID(dataLogin.get("ad_org_id"));
				String poReference = ((String) datos.get("machine-hostname")).trim()+"_"+((String) datos.get("m_iTicketId")).trim();
				order.setPOReference(poReference);
				order.setDocStatus(DocAction.STATUS_Drafted);   // draft  - borrador
				order.setDocAction(DocAction.ACTION_Complete);   
				order.setC_DocType_ID(dataLogin.get("DocTypeTargetID"));
				order.setC_DocTypeTarget_ID(dataLogin.get("DocTypeTargetID"));
				order.setM_Warehouse_ID(dataLogin.get("M_Warehouse_ID"));
				order.setProcessing(false);
				order.setProcessed(true);
				order.setIsApproved(true);
				order.setIsCreditApproved(false);
				order.setIsDelivered(false);
				order.setIsInvoiced(false);
				order.setIsPrinted(false);
				order.setIsTransferred(false);
				order.setIsSelected(false);
				order.setSalesRep_ID(dataLogin.get("ad_user_id"));
				order.setDateOrdered(new Timestamp(dateInvoice.getTime()));
				order.setDatePromised(new Timestamp(dateInvoice.getTime()));
				order.setDatePrinted(new Timestamp(dateInvoice.getTime()));
				order.setDateAcct(new Timestamp(dateInvoice.getTime()));
				order.setC_BPartner_ID(dataLogin.get("c_bpartner_id"));
				order.setC_BPartner_Location_ID(dataLogin.get("locationId"));
				order.setDescription(description);
				order.setIsDiscountPrinted(false);
				order.setPaymentRule("P"); ///B
				order.setC_PaymentTerm_ID(dataLogin.get("PaymentTerm"));
				order.setM_PriceList_ID(dataLogin.get("PriceListID"));
				order.setC_ConversionType_ID(114);
				order.setC_Currency_ID(dataLogin.get("currencyId"));
				order.setInvoiceRule("I"); //D
				order.setPriorityRule("5");
				order.setFreightAmt (Env.ZERO);
				order.setChargeAmt (Env.ZERO);
				order.setTotalLines (Env.ZERO);
				order.setGrandTotal (Env.ZERO);
				order.setPosted(false);
				order.setDeliveryRule("A");
				order.setFreightCostRule("I");
				order.setDeliveryViaRule("P");
				order.setIsTaxIncluded (false);
				order.setIsDropShip(false);
				order.setSendEMail (false);
				order.setIsSelfService(false);
				
				Boolean pok = order.save();
				Integer orderId = order.getC_Order_ID();
				
				
				if (!pok){
					trx.rollback();
					trx.close();
					throw new IllegalStateException("Could not Create order: "+order.toString());
				}else{
					
					Iterator<HashMap<String, Object>> itLista = lista.iterator();
					while (itLista.hasNext()){
						HashMap<String , Object> data = itLista.next();
						String product = (String) data.get("productID");						
						line = new MOrderLine(Env.getCtx(), 0, trx.getTrxName());
						line.setAD_Org_ID(dataLogin.get("ad_org_id"));
						line.setAD_Client_ID(dataLogin.get("ad_client_id"));
						line.setIsActive(true);
						line.setC_Order_ID(orderId);
						line.setC_BPartner_ID(dataLogin.get("c_bpartner_id"));
						line.setC_BPartner_Location_ID(dataLogin.get("locationId"));
						line.setDateOrdered(new Timestamp(dateInvoice.getTime()));
						line.setDatePromised(new Timestamp(dateInvoice.getTime()));
						
						line.setM_Warehouse_ID(dataLogin.get("M_Warehouse_ID"));
						
						String unid = (String) data.get("unitID");
						Integer unidId = -1;
						if (!(unid == null || unid.trim().equals("null"))){
							unidId = Integer.parseInt(unid);
							line.setC_UOM_ID(unidId);
						}
						BigDecimal divideRate =  new BigDecimal(0);
						BigDecimal qty2 =  new BigDecimal((String) data.get("multiply")).setScale(2, RoundingMode.HALF_UP);
						datos.put("locatorID", getLocator(dataLogin.get("ad_org_id")));
						
						if (product.trim().equals("propina")){
							Integer charge = Integer.parseInt(Msg.getMsg(Env.getCtx(), "c_charge_id").trim());
							line.setC_Charge_ID(charge);
						}else{
							productId = Integer.parseInt(product);
							
							ProcessingProduction pp = new ProcessingProduction(); 
							
							if(pp.isProductCreatedByProductionProcess(productId, trx, Env.getCtx())){
								X_M_Production production = pp.createProductionProcessFromProducto(trx, datos, Env.getCtx());
								List<X_M_ProductionPlan> productionPlanList =   pp.createProductionPlan(trx, datos, production, Env.getCtx(), productId, qty2);
								boolean create = pp.doProsses(trx, datos, production, productionPlanList, Env.getCtx());
								if(create)
									pp.doProsses(trx, datos, production, productionPlanList, Env.getCtx());
							}
							
							HashMap<String, BigDecimal> priceList = getPriceList(productId);  
							line.setM_Product_ID(productId);
							line.setPriceList(priceList.get("pricelist"));
							line.setPriceLimit(priceList.get("pricelimit"));
							data.put("priceList", priceList);
							
							if(productId > -1 && unidId > -1){//proceso de conversion de unidades
								StringBuffer sql = new StringBuffer();
								sql.append("SELECT  umo.c_uom_conversion_id as umoCid");
								sql.append("  FROM  c_uom_conversion  as umo ");
								sql.append(" WHERE  m_product_id = "+productId+" and c_uom_to_id = "+unidId+"");
								
								pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
								rs = pstmt.executeQuery ();
								
								if(rs.next()){
									int umoConversionId = rs.getInt("umoCid");
									MUOMConversion uomC = new MUOMConversion(Env.getCtx(), umoConversionId, null);
									divideRate = uomC.getDivideRate();
									qty2 = divideRate.multiply(qty2);
								}
							}
							
							
						}
						
						//quantity
						
						String qty = (String) data.get("multiply");     // original desde el pos. // cantidad
						line.setQtyEntered(new BigDecimal(qty));
						line.setQtyOrdered(qty2);
						line.setC_Currency_ID(dataLogin.get("currencyId"));
						
						TaxID = getTaxId((String) data.get("tax_name"));
						if (TaxID <= 0) TaxID = Integer.parseInt(Msg.getMsg(Env.getCtx(), "TaxID").trim());
						line.setC_Tax_ID(TaxID);
						
						//Price
						String price = (String) data.get("price");
						String discount = (String) data.get("discountValue");
						if (discount.trim()== null || discount.trim().equals("null")){
							discount = "0";
				 		}
						BigDecimal priceDecimal =new BigDecimal(price).setScale(2,RoundingMode.HALF_UP);
						line.setPriceEntered(priceDecimal);

						BigDecimal totalValue = priceDecimal.multiply(qty2);
System.out.println("totalValue"+totalValue);
						// actual price = price of current UOM diveided by factor to know how much would be in the minimum UOM of the product
						if(divideRate.doubleValue() == 0)
							divideRate = new BigDecimal(1).setScale(2,RoundingMode.HALF_UP);
						 
						BigDecimal priceActual = null;

						try{
							priceActual = priceDecimal.divide(divideRate);
						}catch(java.lang.ArithmeticException ae){
							priceActual = priceDecimal.divide(divideRate,2,RoundingMode.HALF_UP);
						}
						line.setPriceActual(priceActual);
						line.setLineNetAmt(totalValue);
						line.setIsDescription(false);
						line.setFreightAmt(Env.ZERO);
						line.setProcessed(true);
						line.setRRAmt(Env.ZERO);
						line.setQtyLostSales(Env.ZERO);
						line.setM_AttributeSetInstance_ID(0);
						Boolean lok = line.save();
						if (!lok){
							trx.rollback();
							trx.close();
							throw new IllegalStateException("Could not Create Order Line: "+line.toString());
	          			}else{
	          				data.put("lineOrderId", line.getC_OrderLine_ID());
	          			}
						listaOrder.add(data);
						}//while iterator
						
     					}//if/else
					BigDecimal grandTotal =new BigDecimal(((String)datos.get("total"))); 
					pok = order.save();
					System.out.println(order.getGrandTotal());
					
					order.completeIt();

                 /*
					// try to complete de invoice (previouly in not a complete status)
					
					int  invoiceId = order.getInvoices()[0].get_ID();
                    System.out.print("Factura generada por la orden POS"+invoiceId);					
					MInvoice invoice = new MInvoice (Env.getCtx(),invoiceId , trx.getTrxName());
					String status = invoice.completeIt();   			
					System.out.print("Estado de completada de la factura"+status);
					*/
					order.setDocStatus(DocAction.STATUS_Completed);
					order.setDocAction(DocAction.ACTION_Complete);
					pok = order.save();
	
					
					if (!pok ){
						trx.rollback();
						trx.close();
						throw new IllegalStateException("Could not Update order: "+order.toString());
					}
					
					/**
					// Creates shipment and invoice - not required anymore because completeIt() method perfomr that logic
					 * 
					LinkedList<HashMap<String, Object>> listaIO = createShipment(trx, getDateOrdered(), order.get_ID(),  
						 dataLogin, listaOrder, description);
					if(datos.get("not-create-invoice") == null){ //if para verificar si se crea factura
						//crea factua
						MInvoice invoice = createInvoice(trx, dateInvoice, orderId, listaIO, dataLogin, description);
					}//si no se crea factura
					**/
					List<HashMap<String, Object>> list = (List)datos.get("payments");
					for(HashMap<String, Object> data : list){
						String total =  (String) data.get("total");
						
						String paymentType = (String) data.get("paymentId");
						if (!(paymentType.trim().equals("debt"))){
							Integer payType = Integer.parseInt(paymentType.trim());
							// create payment Receipt
							System.out.println(order.getC_Invoice_ID());
							createPayment(trx, dateInvoice, orderId, new BigDecimal(total), order.getInvoices()[0].get_ID(), dataLogin, description, payType, Env.ZERO,data);
						}// if debit .....
					}
					
					MInvoice[] invoices = order.getInvoices();
					for(MInvoice invoice : invoices){
						if(!MInvoice.DOCSTATUS_Completed.equals(invoice.getDocStatus())){
		    				invoice.completeIt();
		    				invoice.setDocStatus(MInvoice.STATUS_Completed);
		    				invoice.setGrandTotal(grandTotal);
		    				invoice.save();
		    			}
					}
					
					
					trx.commit();
									
					//trx.close();
					
				}catch (Exception e)	{
					log.log(Level.SEVERE, "Error creating order", e);
					e.printStackTrace();
					try{
						String msg = " Error Creando Orden:: " + ((String) datos.get("m_iTicketId")).trim() +" - Message:: "+e.getMessage();
			    		msg = msg +" - Cause:: "+ e.getCause();
			    		sendError(msg, organization,"");
					}catch(Exception e1){
						e1.printStackTrace();
					}
				}finally{					
					DB.close(rs, pstmt);
					rs = null; pstmt = null;
					if(trx != null)
						trx.close();
				}
		
		return "Sincronizacion con POS exitosa !";
	}

    /**
     * Cretaes payment receipts
     * @param trx
     * @param dateInvoice
     * @param orderId
     * @param payment
     * @param invoiceId
     * @param dataLogin
     * @param description
     * @param payType
     * @param amount
     * @param to
     * @return
     * @throws InterruptedException
     */
    private Boolean createPayment(Trx trx,Date dateInvoice, Integer orderId ,BigDecimal payment, 
			 Integer invoiceId, HashMap<String, Integer> dataLogin, 
			 String description, Integer payType, BigDecimal amount,HashMap<String, Object> to )
	throws InterruptedException{
    	return createPayment(trx, dateInvoice, orderId, payment, invoiceId, dataLogin, description, payType, amount, to, true);
    }
		
	
	/**
	 * Cretaes payment receipts
	 * @param trx
	 * @param dateInvoice
	 * @param orderId
	 * @param payment
	 * @param invoiceId
	 * @param DocAction
	 * @param DocStatus
	 * @param dataLogin
	 * @param description
	 * @return
	 * @throws InterruptedException
	 */
	private Boolean createPayment(Trx trx,Date dateInvoice, Integer orderId ,BigDecimal payment, 
			 Integer invoiceId, HashMap<String, Integer> dataLogin, 
			 String description, Integer payType, BigDecimal amount,HashMap<String, Object> to,boolean isReceipt)
	throws InterruptedException{
		Properties ctx = Env.getCtx();
		ctx.setProperty("#AD_Client_ID", dataLogin.get("ad_client_id").toString());
		ctx.setProperty("#AD_Org_ID", dataLogin.get("ad_org_id").toString());

		Boolean lok = false;
		MPayment data = new MPayment(ctx, 0, trx.getTrxName());

		data.setAD_Client_ID(dataLogin.get("ad_client_id"));
System.out.println(dataLogin.get("ad_org_id"));
		data.setAD_Org_ID(dataLogin.get("ad_org_id"));
System.out.println(data.getAD_Org_ID());
		data.setIsActive(true);
		data.setDateAcct(new Timestamp(dateInvoice.getTime()));
		data.setC_BPartner_ID(dataLogin.get("c_bpartner_id"));
		data.setTrxType("S");
		if(orderId != null && orderId != 0)
			data.setC_Order_ID(orderId);
		data.setIsReceipt(true);
		data.setBankAccountDetails(payType);
		if(invoiceId != null && invoiceId != 0)
			data.setC_Invoice_ID(invoiceId);
//		data.setC_DocType_ID(1000008);   // AR Receipt  code
		data.setC_DocType_ID(isReceipt);
		
		//data.setC_DocType_ID(false);
		data.setDocumentNo("1000000");
		
		if(to == null){
			data.setTenderType("K");
			data.setCreditCardType("M");
		}else{
			String auxStr = ((String)to.get("name")).trim();
			String id = Msg.getMsg(Env.getCtx(), "tenderId").trim();
			data.setTenderType(getListKeyFromName(id,auxStr,trx));
			
			auxStr = ((String)to.get("credit-card-type")); 
			id = Msg.getMsg(Env.getCtx(), "creditCardTypeId").trim();
			if(auxStr != null)
				data.setCreditCardType(getListKeyFromName(id,auxStr.trim(),trx));
			else
				data.setCreditCardType("M");
			
			
			auxStr = ((String)to.get("expiration-date")); 
			if(auxStr != null){
				auxStr = auxStr.trim();
				if(auxStr.length() >= 2 )
					data.setCreditCardExpMM(new Integer(auxStr.substring(0,2)));
				else
					data.setCreditCardExpMM(0);
				
				if(auxStr.length() >= 4)
					data.setCreditCardExpYY(new Integer(auxStr.substring(2,4)));
				else
					data.setCreditCardExpMM(0);
			}else{
				data.setCreditCardExpMM(1);
				data.setCreditCardExpYY(3);
			}
			
			
			data.setC_CashBook_ID(this.getCashBook(dataLogin.get("ad_client_id"), dataLogin.get("ad_org_id"), trx));
			
			auxStr = (String)to.get("account-no"); 
			if(auxStr != null)
				data.setAccountNo(auxStr);
			
			auxStr = (String)to.get("check-no"); 
			if(auxStr != null)
				data.setCheckNo(auxStr);
			
			auxStr = (String)to.get("micr"); 
			if(auxStr != null)
				data.setMicr(auxStr);
			
			auxStr = (String)to.get("number"); 
			if(auxStr != null)
				data.setCreditCardNumber(auxStr);
		}

		data.setDocAction(DocAction.ACTION_Complete);
		data.setDocStatus(DocAction.STATUS_Drafted);
		data.setPayAmt(payment);
		data.setC_Currency_ID(dataLogin.get("currencyId"));
		data.setIsAllocated(true);
		data.setDescription(description);
		data.setC_ConversionType_ID(114); 
		data.setPosted(false);
		data.setProcessed(true);
		data.setIsApproved(true);
		data.setOProcessing("N");
		
		data.setRoutingNo("");
		data.setAccountNo("");
		data.setIsOverUnderPayment(false);
		data.setR_CVV2Match(false);
		data.setOverUnderAmt(amount);

		Boolean pok = data.save(); 

		if (!pok){
			//trx.rollback();
			//trx.close();
			throw new IllegalStateException("Could not Create Payment: "+data.toString());
		}
		else{
			lok = closePayment(trx, dataLogin, invoiceId, orderId, data.getC_Payment_ID(), payment, Env.ZERO, data.get_ID());
			if (!lok){
				return false;
			}
			data.setDocumentNo(Integer.toString(data.get_ID()));
			data.setDocAction(DocAction.ACTION_Close);
			data.setDocStatus(DocAction.STATUS_Completed);
			lok = data.save();
			
			//data.completeIt();
			
			if (!lok ){
				trx.rollback();
				trx.close();
				log.log(Level.WARNING, "Could not Update Payment: "+data.toString());
			}//if (!lok) 
			
		}//if/else 
		
		data.setAD_Org_ID(Env.getAD_Org_ID(Env.getCtx()));
		data.save();
		System.out.println(data.getAD_Org_ID() + " \nEnv:" +Env.getAD_Org_ID(Env.getCtx()));
		return lok;
	}//createPayment
	
	

	/**
	 * create payment receptis for the partial payments 
	 * @param trx
	 * @param dateInvoice
	 * @param dataLogin
	 * @param description
	 * @param list
	 * @param pay
	 * @return
	 * @throws InterruptedException
	 */
	private Boolean createPaymentList(Trx trx,Date dateInvoice, HashMap<String, Integer> dataLogin, String description,
			LinkedList<HashMap<String, Object>> list, BigDecimal total)
	throws InterruptedException{
		Boolean lok = false;
		Iterator<HashMap<String, Object>> itList = list.iterator();
		int compare = 1;
		while (itList.hasNext() && compare > 0){
			HashMap<String, Object> items = (HashMap<String, Object>) itList.next();
			
			//clculo del pago de la factura ......}
			BigDecimal totalInvoice = (BigDecimal) items.get("openamt");
			BigDecimal pay = Env.ZERO;
			BigDecimal amount = Env.ZERO;
			compare = total.compareTo(totalInvoice);
			
			if (compare >= 0){
				//-----------Factura menor o igual que pago
				pay = totalInvoice;
				amount = Env.ZERO;
			}else{
				//-----------Factura MAYOR que pago
				pay = total;
				amount = totalInvoice.subtract(pay);
			}

			lok = createPayment(trx, dateInvoice, (Integer)items.get("c_order_id"), pay, (Integer)items.get("c_invoice_id"), dataLogin,
					description, Integer.parseInt(Msg.getMsg(Env.getCtx(), "BankAccount").trim()), amount, null);
			if (!lok){
				return lok;
			}
			total = total.subtract(totalInvoice);
		}//while incluye facturas
		
		return lok;
	}//createPayment
	
	/**
	 * Creates Payment for cashIn or CashOut
	 * @param trx
	 * @param dateInvoice
	 * @param dataLogin
	 * @param description
	 * @param total
	 * @param inOut
	 * @return
	 * @throws InterruptedException
	 */
	private Boolean createPaymentForInOut(Trx trx,Date dateInvoice, HashMap<String, Integer> dataLogin, String description,
			BigDecimal total,String inOut) throws InterruptedException{
		Boolean lok = false;
			
		boolean isReceipt = true;
		String msj = "";
		if(inOut.trim().equalsIgnoreCase("cashout")){
			isReceipt = false;
			msj = Msg.translate(Env.getCtx(), "cashOut");
		}else{
			msj = Msg.translate(Env.getCtx(), "cashIn");
		}
		
		description = description +" "+ msj;
		
		lok = createPayment(trx, dateInvoice, null, total, null, dataLogin,
				description, Integer.parseInt(Msg.getMsg(Env.getCtx(), "BankAccount").trim()), total, null, isReceipt);
		if (!lok){
			return lok;
		}
		
		return lok;
	}//createPayment
	
	/**
	 * Complete payments relared with an invoice and order
	 * @param trx
	 * @param dataLogin
	 * @param invoiceId
	 * @param orderId
	 * @param paymentId
	 * @param pay
	 * @param amount
	 * @param id
	 * @return
	 * @throws InterruptedException
	 */
	private Boolean closePayment(Trx trx, HashMap<String, Integer> dataLogin, Integer invoiceId, Integer orderId, Integer paymentId,
			BigDecimal pay, BigDecimal amount, Integer id)
	throws InterruptedException{
		Boolean lok = false;
			//crea la autorizacion de factura pagada
			MAllocationHdr hdr = new MAllocationHdr(Env.getCtx(), 0, trx.getTrxName());
			hdr.setAD_Client_ID(dataLogin.get("ad_client_id"));
			hdr.setAD_Org_ID(dataLogin.get("ad_org_id"));
			hdr.setIsActive(true);
			hdr.setDescription("pago: "+id);
			hdr.setDateTrx(new Timestamp(new Date().getTime()));
			hdr.setDateAcct(new Timestamp(new Date().getTime()));
			hdr.setC_Currency_ID(dataLogin.get("currencyId"));
			hdr.setApprovalAmt(pay);
			hdr.setDocAction(DocAction.ACTION_Complete);
			hdr.setDocStatus(DocAction.STATUS_Drafted);
			hdr.setIsApproved(true);
			hdr.setProcessing(false);
			hdr.setProcessed(true);
			hdr.setPosted(false);
			lok =  hdr.save();
			if (!lok){
				return false;
			}
			//crea el registro de factura pagada
			MAllocationLine line = new MAllocationLine(Env.getCtx(), 0, trx.getTrxName());
			line.setAD_Client_ID(dataLogin.get("ad_client_id"));
			line.setAD_Org_ID(dataLogin.get("ad_org_id"));
			line.setIsActive(true);
			line.setIsManual(false);
			if(invoiceId != null && invoiceId != 0)
				line.setC_Invoice_ID(invoiceId);
			line.setC_BPartner_ID(dataLogin.get("c_bpartner_id"));
			if(orderId != null && orderId !=0)
				line.setC_Order_ID(orderId);
			line.setC_Payment_ID(paymentId);
			line.setAmount(pay);
			line.setDiscountAmt(Env.ZERO);
			line.setWriteOffAmt(Env.ZERO);
			line.setOverUnderAmt(amount);
			line.setC_AllocationHdr_ID(hdr.getC_AllocationHdr_ID());
			
			lok = line.save();
			if (!lok){
				return false;
			}
			//completa el estado de la autorizacion de pago
			hdr.setDocAction(DocAction.ACTION_Close);
			//hdr.setDocStatus(DocAction.STATUS_Completed);
			lok = hdr.save();
			hdr.completeIt();
			
			if (!lok){
				return false;
			}
			
			
		return lok;
	}//closePayment
	
	
	/**
	 * Creates business partner and its location
	 * @param trx
	 * @param to
	 * @return
	 */
	private Integer createBPartner(Trx trx, HashMap <String,Object> to){
		Integer code = 0;
		if (!(to.get("C_BPartner_ID")==null || to.get("C_BPartner_ID").equals("null")))
			code = new Integer((String) to.get("C_BPartner_ID"));
		//consulta los datos de organizacion, cliente y codigos para el usuario admin
		Integer organId = Integer.parseInt((String)to.get("organization"));
		Integer clientId = 0;
		if (organId >0){
			clientId= getClient(organId+"");
		}
		
		//HashMap<String, Integer> dataLogin = getSalesRep(Msg.getMsg(Env.getCtx(), "userAdmin").trim());
		HashMap<String, Integer> dataLogin = getSalesRep(getAdminLogin(clientId+""));
		try {
			String organization =  (String) to.get("organization");
			Integer orgId = Integer.parseInt(organization);
			if (orgId >0){
				dataLogin.put("ad_client_id", getClient(organization));
				dataLogin.put("ad_org_id", orgId);
			}
			MBPartner partner = new MBPartner(Env.getCtx(), code, trx.getTrxName());
			partner.setAD_Client_ID(dataLogin.get("ad_client_id"));
			partner.setAD_Org_ID(dataLogin.get("ad_org_id"));
			partner.setIsActive(true);
			partner.setC_BP_Group_ID(dataLogin.get("c_bp_group_id"));
			partner.setName2((String) to.get("name2"));
			partner.setName((String) to.get("name"));
			partner.set_ValueOfColumn("name1", to.get("name1"));
			partner.setTaxID((String) to.get("taxId"));
			partner.setIsTaxExempt(Boolean.parseBoolean((String)to.get("taxExempt")));
			if(!((String) to.get("value")).trim().equalsIgnoreCase("")){
				partner.setValue((String) to.get("value"));
			}else
				partner.setValue((new Date()).getTime()+"");
			partner.setIsCustomer(true);
			partner.setSO_CreditLimit(new BigDecimal(Double.parseDouble((String)to.get("maxDebt"))));
	
			Boolean ok = partner.save();
			if (ok){
				if(!partner.getValue().trim().equals(partner.get_ID()+"")){
					partner.setValue(partner.get_ID()+"");
					partner.save();
				}
				createUser(trx, dataLogin, to, partner);
				dataLogin.put("code", partner.getC_BPartner_ID());
				Integer loc = createBPartnerLocation(trx, dataLogin, to, partner);
				if (loc <= 0){
					code = -1;
					trx.commit();
				}
				else {
					trx.commit();
				}
				
			}else{
				code = -1;
			}//if loc
		}catch (Exception e) {
			log.log (Level.SEVERE, null, e);
            e.printStackTrace();
		} finally{
			if(trx != null && trx.isActive())
				trx.close();
		}
		
		return code;
	}//createBPartner
	
	/**
	 * Creates business partner location
	 *  
	 * @param trx
	 * @param codes
	 * @param to
	 * @param partner
	 * @return
	 */
	private Integer createBPartnerLocation(Trx trx, HashMap<String, Integer> dataLogin, HashMap<String,Object> to, MBPartner partner){
		Integer code = 0;
		String city = (String) to.get("city");
		String codeLocal = (String) to.get("address2");
		if (codeLocal.trim()==null || codeLocal.trim().equals("null")){
			if (city == null || city.equals("null")){
				return 0;
			}
		}else{
			code = new Integer(codeLocal.trim());
		}
		if (city == null || city.equals("null"))
			return 0;
		
		Boolean create = true;
		Integer localization = 0;
		if (code > 0){
			create = false;
			code = getLocationPartner(dataLogin.get("code"));
			localization = getLocation(code);
		}
		localization = createLocation(trx, dataLogin, to, create, localization);
		if (localization > 0){
			MBPartnerLocation bl = null;
			if (code > 0)
				bl = new MBPartnerLocation(Env.getCtx(), code, trx.getTrxName());
			else
				bl = new MBPartnerLocation(partner);
			if (!create)
				localization = bl.getC_Location_ID();
			else
				bl.setC_Location_ID(localization);
			
			bl.setAD_Client_ID(dataLogin.get("ad_client_id"));
			bl.setAD_Org_ID(dataLogin.get("ad_org_id"));
			bl.setPhone((String) to.get("phoneNumber"));
			bl.setPhone2((String) to.get("phone2"));
			bl.setFax((String) to.get("fax"));
			bl.setC_BPartner_ID(dataLogin.get("code"));
			Boolean lok = bl.save();
			if (lok){
				code = bl.getC_BPartner_Location_ID();
			}else{
				return -1;
			}
		}else{
			return -1;
		}
		return code;
	}//createBPartnerLocation
	
	
	
	/**
	 * Creates contact (user) information for a business partner 
	 * @param trx
	 * @param codes
	 * @param to
	 * @param partner
	 * @return
	 */
	private Integer createUser(Trx trx, HashMap<String, Integer> dataLogin, HashMap<String,Object> to, MBPartner partner){
		Integer code = 0;
		String email =(String)to.get("email") ;
		MUser   mUser = null;
		StringBuffer sql= null;
		int 	userId = 0;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try{
			if (email.trim()==null || email.trim().equals("null")){
				if (email == null || email.equals("null")){
					return 0;
				}
			}
			
			sql = new StringBuffer();
			sql.append("select u.ad_user_id as id from ad_user u ");
			sql.append("where u.ad_client_id = "+Env.getAD_Client_ID(Env.getCtx())+" and u.ad_org_id =  "+Env.getAD_Org_ID(Env.getCtx())+ " and u.c_bpartner_id = "+ partner.get_ID() );
			
			pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
			rs = pstmt.executeQuery ();
			
			if(rs.next()){
				userId = rs.getInt("id");
				mUser = new MUser(Env.getCtx(), userId, trx.getTrxName());
			}else{
				mUser = new MUser(partner);
			}
				
			mUser.setAD_Client_ID(dataLogin.get("ad_client_id"));
			mUser.setAD_Org_ID(dataLogin.get("ad_org_id"));
			mUser.setEMail(email);
			Boolean lok = mUser.save();
			if (lok){
				code = mUser.get_ID();
			}else{
				return -1;
			}
		}catch(SQLException e){
			log.log(Level.WARNING, "al crear o actualizar la informacion de contacto del tercero ",e);
			e.printStackTrace();
		}finally{
			DB.close(rs, pstmt); 
			rs = null; pstmt = null;
		}
		
		return code;
	}//createUser
	
	/**
	 * Creates a location
	 * @param trx
	 * @param codes
	 * @param to
	 * @param create
	 * @param localization
	 * @return
	 */
	private Integer createLocation(Trx trx, HashMap<String, Integer> dataLogin, HashMap<String,Object> to, Boolean create, Integer localization){
		MLocation lc = null;
		Integer code = 0;
		if (localization > 0 )
			code = localization;
		String country = (String) to.get("country");
		String region = (String) to.get("region");
		String cityCod = (String) to.get("city");
		
		if (country.trim()==null || country.trim().equals("null") ||
				region.trim()==null || region.trim().equals("null") ||
				cityCod.trim()==null || cityCod.trim().equals("null")){
			return 0;
		}

		Integer countryId = new Integer(country.trim());
		Integer regionId = new Integer(region.trim());
		Integer cityId = new Integer(cityCod.trim());
		String cityName = getCityName(cityCod);
		Properties ctx = Env.getCtx();
		ctx.setProperty("#AD_Client_ID", dataLogin.get("ad_client_id").toString());
		ctx.setProperty("#AD_Org_ID", dataLogin.get("ad_org_id").toString());
		if (create)
			lc = new MLocation(Env.getCtx(), countryId, regionId,cityName, trx.getTrxName());
		else{
			lc = new MLocation(Env.getCtx(), localization, trx.getTrxName());
		}
		lc.setC_City_ID(cityId);
		lc.setC_Region_ID(regionId);
		lc.setC_Country_ID(countryId);
		lc.setCity(cityName);
		lc.setPostal((String) to.get("postal"));
		lc.setAddress1(((String) to.get("address")).trim() + " ");

		lc.setAD_Client_ID(dataLogin.get("ad_client_id"));
		lc.setAD_Org_ID(dataLogin.get("ad_org_id"));
		
		//System.out.println("Client_ID:"+lc.getAD_Client_ID());
		//System.out.println("Org_ID:"+lc.getAD_Org_ID());
		
		Boolean ok = lc.save();
		if (ok){
			code = lc.getC_Location_ID();
			if(lc.getAD_Org_ID() == 0){
				updateOrgZero(trx, dataLogin.get("ad_org_id"), dataLogin.get("ad_client_id"), code);
				lc = null;
				lc = new MLocation(Env.getCtx(), code, trx.getTrxName());
				
				lc.setAD_Client_ID(dataLogin.get("ad_client_id"));
				lc.setAD_Org_ID(dataLogin.get("ad_org_id"));
				lc.setPostal((String) to.get("postal"));
				lc.setAddress1(((String) to.get("address")).trim());
				
				lc.save();
			}
			
		}
		return code;
	}//createLocation

    
//  /**
//   * Prints Hashmap content for debuggin purposes
//   */
//  private void imprime(HashMap<String , Object> datos){
//  	System.out.println("......................................................................................");
//  	Iterator iter = datos.entrySet().iterator();
//  	Map.Entry e;
//  	LinkedList<HashMap<String, Object>> lista = null;
//  	while (iter.hasNext()) {
//	    	e = (Map.Entry)iter.next();
//	    	if (e.getKey().equals("m_aLine"))
//	    		lista = (LinkedList<HashMap<String, Object>>) e.getValue();
//	    	System.out.println("Clave: " + e.getKey() + " | Valor: " + e.getValue());
//  	}
//  	
//  	if (lista != null){
//  		Iterator itLista = lista.iterator();
//  		while (itLista.hasNext()){
//  			HashMap<String , String> data = (HashMap<String, String>) itLista.next();
//  			Iterator itera = data.entrySet().iterator();
//  	    	Map.Entry ea;
//  	    	System.out.println("******************************************");
//  	    	while (itera.hasNext()) {
//  		    	ea = (Map.Entry)itera.next();
//  		    	System.out.println("........Clave: " + ea.getKey() + " | Valor: " + ea.getValue());
//  	    	}//while hashmap
//  		}//while listas
//  	}//if
//  }//imprime
  

    /**
     * recibe el Document y mapea los campos para la creacion de la devolucion
     * @param document
     * @return
     */
    private Boolean processProductInventoryShipping(Document document){
    	Boolean flag = false;
    	HashMap<String , Object> datos = new HashMap<String, Object>();
    	datos.put("organization", document.getElementsByTagName("organization").item(0).getTextContent());
    	datos.put("articlesCount", document.getElementsByTagName("productAmount").item(0).getTextContent());
    	datos.put("m_dDate", document.getElementsByTagName("m_dDate").item(0).getTextContent());
    	datos.put("description", document.getElementsByTagName("description").item(0).getTextContent());
    	datos.put("m_sId", "");
    	datos.put("name", "");
    	datos.put("subTotal", "");
    	datos.put("tax", "");
    	datos.put("m_iTicketId", "");//SE PUEDE TRAER DEL POS
    	datos.put("tickettype", "");
    	datos.put("total", "0");
    	datos.put("totalPaid", "0");
    	datos.put("waiter-login", document.getElementsByTagName("waiter-login").item(0).getTextContent());
    	datos.put("transactionID", "");
    	datos.put("customerId", "null" );
    	datos.put("m_Customer", "");
    	
    	//lista de los productos de la orden
     	LinkedList<HashMap<String, Object>> lista = new LinkedList<HashMap<String,Object>>();
    	
		String productId = document.getElementsByTagName("productId").item(0).getTextContent();
		HashMap<String , Object> data = new HashMap<String, Object>();
		// aqui se extrae los atributos y valores del tag en un loop
		data.put("multiply", document.getElementsByTagName("productAmount").item(0).getTextContent());
		data.put("price", document.getElementsByTagName("price").item(0).getTextContent());
		data.put("priceTax", "");
		data.put("productAttSetId", "");
		data.put("productAttSetInstDesc", "");
		data.put("productAttSetInstId", "");
		data.put("productCategoryID", "");
		data.put("productID", productId);
		data.put("productName", document.getElementsByTagName("productName").item(0).getTextContent());
		data.put("productTaxCategoryID",document.getElementsByTagName("productTaxCategoryID").item(0).getTextContent());
		data.put("subValue", document.getElementsByTagName("subValue").item(0).getTextContent());
		data.put("tax", "");
		data.put("taxRate", "");
		data.put("m_iLine","");
		data.put("value", document.getElementsByTagName("value").item(0).getTextContent());
		data.put("unitID", document.getElementsByTagName("unit").item(0).getTextContent());
		data.put("discountValue","null");
		data.put("discountRate", "null");
    		HashMap<String , Object> dataT = new HashMap<String, Object>();
    		dataT.put("applicationOrder", "");
    		dataT.put("name", "");
    	data.put("tax_name", document.getElementsByTagName("tax-name").item(0).getTextContent());
    		dataT.put("order", "");
    		dataT.put("parentID", "");
    		dataT.put("rate", "");
    		dataT.put("taxCategoryID", "");
    		dataT.put("taxCustCategoryID", "");
    		dataT.put("taxCustCategoryID", "");
		data.put("tax-line", dataT);
    	datos.put("m_Customer", data );
    	datos.put("not-create-invoice", "not-create-invoice");
		datos.put("machine-hostname", document.getElementsByTagName("machine-hostname").item(0).getTextContent());
		datos.put("PriceListID", document.getElementsByTagName("priceListID").item(0).getTextContent());
    	lista.add(data);
    	datos.put("m_aLine", lista);
    	createWarehouseOrder(datos);
		
    	return flag;
    }//processProductRefund
    

    /**
     * Creates warehouse order (order and shipping)
     * @param datos
     * @return
     */
    private String createWarehouseOrder(HashMap<String , Object> datos) {
    	PreparedStatement pstmt = null;
		ResultSet rs = null;
    	
			Date dateInvoice = new Date();
			try {
				dateInvoice = sdf.parse((String) datos.get("m_dDate"));
			} catch (ParseException e1) {
				log.log(Level.WARNING, "Error al formatear la fecha ",e1);
				e1.printStackTrace();
			}
			Integer location = 0;   // location ID  PartnerLocation
			String description = Msg.translate(Env.getCtx(), "description")+": Salida de Inventario - Razón:"+datos.get("description"); //"orden de prueba desde java";  // descripcion de la order
			Integer productId = -1;   // product id  : frutos del pacifico ..
			Integer TaxID = Integer.parseInt(Msg.getMsg(Env.getCtx(), "TaxID").trim());  // id de tax   . ITBMS
			@SuppressWarnings("unchecked")
			LinkedList<HashMap<String, Object>> lista = (LinkedList<HashMap<String, Object>>) datos.get("m_aLine");
			LinkedList<HashMap<String, Object>> listaOrder = new LinkedList<HashMap<String,Object>>();
			
			String organization =  (String) datos.get("organization");
			Integer orgId = Integer.parseInt(organization);
			Integer clientId =0;
			if (orgId >0){
				clientId = getClient(organization);
			}
			//contiene la informacion de cliente, organizacion y otros
			HashMap<String, Integer> dataLogin = new HashMap<String, Integer>();
			if (datos.get("waiter-login").equals("AdminSistema")){
//				datos.put("waiter-login", Msg.getMsg(Env.getCtx(), "userAdmin").trim());
				datos.put("waiter-login", getAdminLogin(clientId+""));
			}
			//busca la informacion de representante de ventas, cliente, organizacion y otros
			dataLogin = getSalesRep((String) datos.get("waiter-login"));
			if(dataLogin.size() == 0){
				//dataLogin = getSalesRep(Msg.getMsg(Env.getCtx(), "userAdmin").trim());
				dataLogin = getSalesRep(getAdminLogin(clientId+""));
			}
			
			dataLogin.put("PaymentTerm", Integer.parseInt(Msg.getMsg(Env.getCtx(), "PaymentTerm").trim()));
			//dataLogin.put("PriceListID", Integer.parseInt(Msg.getMsg(Env.getCtx(), "PriceListID").trim()));
			dataLogin.put("PriceListID", Integer.parseInt(((String) datos.get("PriceListID")).trim()));
			dataLogin.put("DocTypeTargetID", Integer.parseInt(Msg.getMsg(Env.getCtx(), "DocTypeTargetID").trim()));
			dataLogin.put("currencyId", Integer.parseInt(Msg.getMsg(Env.getCtx(), "currencyId").trim()));
			if (!(datos.get("customerId").equals(null) || datos.get("customerId").equals("null"))){
				dataLogin.put("c_bpartner_id", Integer.parseInt((String) datos.get("customerId")));
				location= getLocationPartner(dataLogin.get("c_bpartner_id"));   // location ID 
				dataLogin.put("locationId", location);
			}else{
				dataLogin.put("c_bpartner_id", Integer.parseInt(Msg.getMsg(Env.getCtx(), "partnerDefault").trim()));
				dataLogin.put("locationId", Integer.parseInt(Msg.getMsg(Env.getCtx(), "PartnerLocation").trim()));
			}
			
			MOrder order = null;
			MOrderLine line = null;
			Trx trx = Trx.get(Trx.createTrxName("AL"), true);
		try{
			
			if (orgId >0){
				dataLogin.put("ad_client_id", getClient(organization));
				dataLogin.put("ad_org_id", orgId);
			}
			dataLogin.put("M_Warehouse_ID", getWarehouse(dataLogin.get("ad_org_id")));
			
				order = new MOrder(Env.getCtx(), 0, trx.getTrxName());
				order.setIsActive(true);
				order.setIsSOTrx(true);
				order.setAD_Client_ID(dataLogin.get("ad_client_id"));
				order.setAD_Org_ID(dataLogin.get("ad_org_id"));
				String poReference = ((String) datos.get("machine-hostname")).trim()+"_"+((String) datos.get("m_iTicketId")).trim();
				order.setPOReference(poReference);
				order.setDocStatus(DocAction.STATUS_Drafted);   // draft  - borrador
				order.setDocAction(DocAction.ACTION_Complete);   
				int wareHouseOrderType = 1000032;
				
				order.setC_DocType_ID(wareHouseOrderType);
				order.setC_DocTypeTarget_ID(wareHouseOrderType);
				order.setM_Warehouse_ID(dataLogin.get("M_Warehouse_ID"));
				order.setProcessing(false);
				order.setProcessed(true);
				order.setIsApproved(true);
				order.setIsCreditApproved(false);
				order.setIsDelivered(false);
				order.setIsInvoiced(false);
				order.setIsPrinted(false);
				order.setIsTransferred(false);
				order.setIsSelected(false);
				order.setSalesRep_ID(dataLogin.get("ad_user_id"));
				order.setDateOrdered(new Timestamp(dateInvoice.getTime()));
				order.setDatePromised(new Timestamp(dateInvoice.getTime()));
				order.setDatePrinted(new Timestamp(dateInvoice.getTime()));
				order.setDateAcct(new Timestamp(dateInvoice.getTime()));
				order.setC_BPartner_ID(dataLogin.get("c_bpartner_id"));
				order.setC_BPartner_Location_ID(dataLogin.get("locationId"));
				order.setDescription(description);
				order.setIsDiscountPrinted(false);
				order.setPaymentRule("P"); ///B
				order.setC_PaymentTerm_ID(dataLogin.get("PaymentTerm"));
				order.setM_PriceList_ID(dataLogin.get("PriceListID"));
				order.setC_ConversionType_ID(114);
				order.setC_Currency_ID(dataLogin.get("currencyId"));
				order.setInvoiceRule("I"); //D
				order.setPriorityRule("5");
				order.setFreightAmt (Env.ZERO);
				order.setChargeAmt (Env.ZERO);
				order.setTotalLines (Env.ZERO);
				order.setGrandTotal (Env.ZERO);
				order.setPosted(false);
				order.setDeliveryRule("A");
				order.setFreightCostRule("I");
				order.setDeliveryViaRule("P");
				order.setIsTaxIncluded (false);
				order.setIsDropShip(false);
				order.setSendEMail (false);
				order.setIsSelfService(false);
				
				Boolean pok = order.save();
				Integer orderId = order.getC_Order_ID();
				
				
				if (!pok){
					trx.rollback();
					trx.close();
					throw new IllegalStateException("Could not Create order: "+order.toString());
				}else{
					Iterator<HashMap<String, Object>> itLista = lista.iterator();
					while (itLista.hasNext()){
						HashMap<String , Object> data = itLista.next();
						String product = (String) data.get("productID");
						line = new MOrderLine(Env.getCtx(), 0, trx.getTrxName());
						line.setAD_Org_ID(dataLogin.get("ad_org_id"));
						line.setAD_Client_ID(dataLogin.get("ad_client_id"));
						line.setIsActive(true);
						line.setC_Order_ID(orderId);
						line.setC_BPartner_ID(dataLogin.get("c_bpartner_id"));
						line.setC_BPartner_Location_ID(dataLogin.get("locationId"));
						line.setDateOrdered(new Timestamp(dateInvoice.getTime()));
						line.setDatePromised(new Timestamp(dateInvoice.getTime()));
						
						line.setM_Warehouse_ID(dataLogin.get("M_Warehouse_ID"));
						
						String unid = (String) data.get("unitID");
						Integer unidId = -1;
						if (!(unid == null || unid.trim().equals("null"))){
							unidId = Integer.parseInt(unid);
							line.setC_UOM_ID(unidId);
						}
						BigDecimal divideRate =  new BigDecimal(0);
						BigDecimal qty2 =  new BigDecimal((String) data.get("multiply"));
						
						if (product.trim().equals("propina")){
							Integer charge = Integer.parseInt(Msg.getMsg(Env.getCtx(), "c_charge_id").trim());
							line.setC_Charge_ID(charge);
						}else{
							productId = Integer.parseInt(product);
							HashMap<String, BigDecimal> priceList = getPriceList(productId);  
							line.setM_Product_ID(productId);
							line.setPriceList(priceList.get("pricelist"));
							line.setPriceLimit(priceList.get("pricelimit"));
							data.put("priceList", priceList);
							
							if(productId > -1 && unidId > -1){//proceso de conversion de unidades
								StringBuffer sql = new StringBuffer();
								sql.append("SELECT  umo.c_uom_conversion_id as umoCid");
								sql.append("  FROM  c_uom_conversion  as umo ");
								sql.append(" WHERE  m_product_id = "+productId+" and c_uom_to_id = "+unidId+"");
								
								pstmt = DB.prepareStatement (sql.toString(), trx.getTrxName());
								rs = pstmt.executeQuery ();
								
								if(rs.next()){
									int umoConversionId = rs.getInt("umoCid");
									MUOMConversion uomC = new MUOMConversion(Env.getCtx(), umoConversionId, null);
									divideRate = uomC.getDivideRate();
									qty2 = divideRate.multiply(qty2);
								}
							}
							
							
						}
						
						//quantity
						
						String qty = (String) data.get("multiply");     // original desde el pos. // cantidad
						line.setQtyEntered(new BigDecimal(qty));
						line.setQtyOrdered(qty2);
						line.setC_Currency_ID(dataLogin.get("currencyId"));
						
						TaxID = getTaxId((String) data.get("tax_name"));
						if (TaxID <= 0) TaxID = Integer.parseInt(Msg.getMsg(Env.getCtx(), "TaxID").trim());
						line.setC_Tax_ID(TaxID);
						
						//Price
						String price = (String) data.get("price");
						String discount = (String) data.get("discountValue");
						if (discount.trim()== null || discount.trim().equals("null")){
							discount = "0";
						}
						
						line.setPriceEntered(new BigDecimal(price));
						
						BigDecimal totalValue = new BigDecimal(price).multiply(qty2);
						// actual price = price of current UOM diveided by factor to know how much would be in the minimum UOM of the product
						if(divideRate.equals(new BigDecimal(0)))
							divideRate = new BigDecimal(1);
						line.setPriceActual(new BigDecimal(price).divide(divideRate,2,RoundingMode.HALF_UP));
						line.setLineNetAmt(totalValue);
						line.setIsDescription(false);
						line.setFreightAmt(Env.ZERO);
						line.setProcessed(true);
						line.setRRAmt(Env.ZERO);
						line.setQtyLostSales(Env.ZERO);
						line.setM_AttributeSetInstance_ID(0);
						Boolean lok = line.save();
						if (!lok){
							trx.rollback();
							trx.close();
							throw new IllegalStateException("Could not Create Order Line: "+line.toString());
	          			}else{
	          				data.put("lineOrderId", line.getC_OrderLine_ID());
	          			}
						listaOrder.add(data);
						}//while iterator
						
     					}//if/else
					pok = order.save();
					order.completeIt();

					order.setDocAction(DocAction.ACTION_Close);
					order.setDocStatus(DocAction.STATUS_Completed);
					pok = order.save();
					if (!pok ){
						trx.rollback();
						trx.close();
						throw new IllegalStateException("Could not Update order: "+order.toString());
					}
					trx.commit();
					trx.close();
					
				}catch (Exception e)	{
					log.log(Level.SEVERE, "Error en creacion de ordenes", e);
					e.printStackTrace();
					String msg = " Error Procesando Salida de inventario:: " + ((String) datos.get("m_iTicketId")).trim() +" - Message:: "+e.getMessage();
		    		msg = msg +" - Cause:: "+ e.getCause();
		    		sendError(msg, organization,"");
				}finally{					
					DB.close(rs, pstmt);
					rs = null; pstmt = null;
					if(trx != null && trx.isActive())
						trx.close();
				}
		
		return "Sincronizacion con POS exitosa !";
	}
    
    /**
     * sends Error message to POS
     * @param message
     * @param org
     * @param pcName
     */
    private void sendError(String message, String org,String pcName ){
    	try{
	    	StringBuffer auxXML = new StringBuffer();
			auxXML.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			auxXML.append("<entityDetail><type>SYNC-ERROR</type><detail>");
			auxXML.append("<Value>");
			auxXML.append(message);
			auxXML.append("</Value>");
			auxXML.append("</detail></entityDetail>");
			MQClient.sendMessage(auxXML.toString(),"POS-SYNC","", org, pcName);
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    
}//ERPPosListener