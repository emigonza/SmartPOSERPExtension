package com.smj.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

import org.compiere.model.MInOut;
import org.compiere.model.MInOutLine;
import org.compiere.model.MRMA;
import org.compiere.model.MRMALine;
import org.compiere.process.DocAction;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.compiere.util.Trx;

/**
 * Create s RMS from a a InOut (shipment)
 * @author pedrorozo
 *
 */
public class RmaFromInOut {
	protected CLogger log = CLogger.getCLogger(super.getClass());
	
	/**
 * It creates a RMA from a InOut (shipment) 
 * 
 *  
 */
	public void testRMA() {
	
		HashMap<String , String> datos = new HashMap<String, String>();
		int inOutId = 1000044;  
		datos.put("Name" , "Devolucion desde el POS");
		datos.put("Amount" , "1");    // total amount of items to return 
		datos.put("RmaTypeID" , "1000000");    // type of RMA  - tipo de devolucion
		datos.put("Qty" , "1");   // InOut Line ID  - liena entrega  asociada
		datos.put("DocTypeID" , "1000029");  // RMA  document type
		datos.put("productId" , "1000048");  // RMA  document type
		Trx trx = Trx.get(Trx.createTrxName("AL"), true);
        Integer rma = createRMAfromInOut(trx, datos, inOutId);
     // this section create a new Customer Return (INOUT document from a RMA)
		InOutFromRma r2i = new InOutFromRma();
    	InvoiceFromRma r2n = new InvoiceFromRma(); 
		//trx = Trx.get(Trx.createTrxName("AL"), true);
    	r2i.generateInOutFromRMA( rma,trx);	
		//trx = Trx.get(Trx.createTrxName("AL"), true);
    	r2n.generateInvoiceFromRMA(rma,trx);

    	trx.commit();
    	 trx.close();
	}
	/**
	 * Create Return of Material Autorizations (RMA) taking the parameters from the Hashmap and previous information from the InOut (entrega)
	 * to test mandatory fields see testRMA method;
	 * @param trx
	 * @param to
	 * @return
	 */
	public Integer createRMAfromInOut(Trx trx, HashMap <String,String> to, int inOutId){
		int code = 0;
		// retrieves data from previois inOut  //entregas
		MInOut pInOut = new MInOut(Env.getCtx(), inOutId, trx.getTrxName() );
		
		MRMA rma = new MRMA(Env.getCtx(), 0, trx.getTrxName());
		rma.setAD_Client_ID(Integer.parseInt(to.get("ad_client_id").trim()));   //Client id
		rma.setAD_Org_ID(Integer.parseInt(to.get("ad_org_id").trim()));   //org id
		rma.setIsActive(true);
		rma.setDocAction(DocAction.ACTION_Complete);
		rma.setDocStatus(DocAction.STATUS_Drafted);
		rma.setGenerateTo("N");
		rma.setIsSOTrx(true);
		rma.setProcessed(true);
		rma.setIsApproved(true);
		rma.setDescription(pInOut.getDescription());
		rma.setName(to.get("Name"));
		rma.setC_BPartner_ID(pInOut.getC_BPartner_ID());
		rma.setC_Currency_ID(pInOut.getC_Currency_ID());
		rma.setSalesRep_ID(pInOut.getSalesRep_ID());
		rma.setInOut_ID(inOutId); 
		rma.setM_RMAType_ID(Integer.parseInt(to.get("RmaTypeID")));
		
		//rma.setC_DocType_ID(Integer.parseInt(to.get("DocTypeID")));
		rma.setC_DocType_ID(getDocTypeIdFromClient(Integer.parseInt(to.get("ad_client_id").trim()),trx));
		
		rma.setAmt(new BigDecimal(to.get("Amount")).setScale(2, RoundingMode.HALF_UP));              
		Boolean ok = rma.save();    // it saves the RMA first as draft without lines

		if (ok){
			code = rma.get_ID();
			MInOutLine pInOutLines[] = pInOut.getLines();
			Boolean lok;
			for (int i = 0; i < pInOutLines.length;i++)
			{
				System.out.println("ProductID desde entrega original delticket: "+pInOutLines[i].getM_Product_ID());
				System.out.println("ProductID desde la dev. del POS: "+to.get("productId"));
				
				// validates if the Product ID == null - when the line is from discount or tip
				String productId = "0";
				if (!to.get("productId").equalsIgnoreCase("null"))
				{
					productId = to.get("productId");
				}
				
				
				if (Integer.parseInt(productId) == pInOutLines[i].getM_Product_ID() )  // only processs the return product id	
				{
					MRMALine line = new MRMALine(Env.getCtx(), 0, trx.getTrxName());
					line.setAD_Client_ID(Integer.parseInt(to.get("ad_client_id").trim())); 
					line.setAD_Org_ID(Integer.parseInt(to.get("ad_org_id").trim()));
					line.setIsActive(true);
					line.setM_RMA_ID(rma.get_ID());
					
					line.setQty(new BigDecimal(to.get("Qty")).setScale(2, RoundingMode.HALF_UP));
					line.setQtyDelivered(new BigDecimal(to.get("Qty")).setScale(2, RoundingMode.HALF_UP) );
					line.setProcessed(true);
					line.setLine(pInOutLines[i].getLine());
					line.setM_InOutLine_ID(pInOutLines[i].get_ID());
					String description = pInOut.getDescription();
					
					if(to.get("productName").toUpperCase().indexOf("PROPI") >=0){
						description += " \n "+to.get("productName"); 
					}
					line.setDescription(description);
					
					lok = line.save();
					if (!lok){
						trx.rollback();
						trx.close();
					
						throw new IllegalStateException("Could not Create RMA Lines: "+line.toString());
			     	}//if !lok
					break;
				} // if 
			}//for
			rma.setDocAction(DocAction.ACTION_Close);
			rma.setDocStatus(DocAction.STATUS_Completed);
			rma.save();            // now with the lines it saves the RMA with complete status
		} else{
			trx.rollback();
			trx.close();
			code = -1;
		}//if loc
		
			
	return code;
	}//createRMA

	private int getDocTypeIdFromClient(int clientId,Trx trx)
    {
        String docTypeSQl = "SELECT dt.c_doctype_id FROM C_DocType dt "
            + "WHERE dt.ad_client_id=? and name = 'Customer Return Material' and isactive='Y'";
        
        int docTypeId = DB.getSQLValue(trx.getTrxName(), docTypeSQl, clientId);
        
        return docTypeId;
    }
}
