package com.smj.model;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;

import org.compiere.model.MInOut;
import org.compiere.model.MInOutLine;
import org.compiere.model.MInvoice;
import org.compiere.model.MInvoiceLine;
import org.compiere.model.MRMA;
import org.compiere.model.MRMALine;
import org.compiere.process.DocAction;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.compiere.util.Trx;
import org.jboss.util.propertyeditor.BigDecimalEditor;

/**
 * Create an Invoice from a RMA
 * @author pedrorozo
 *
 */


public class InvoiceFromRma {
	protected CLogger log = CLogger.getCLogger(super.getClass());
	
    public void generateInvoice(Trx trx, HashMap <String,String> to, int inOutId)
    {
        MInvoice invoice = createInvoice(trx,to,inOutId);
        createInvoiceLines(inOutId, invoice, to, trx);
        
        if (!invoice.save())
        {
            throw new IllegalStateException("Could not update invoice");
        }
        invoice.completeIt();
        invoice.setDocStatus(DocAction.STATUS_Completed);
        invoice.setDocAction(DocAction.ACTION_Complete);
        invoice.save();
        log.log(Level.INFO,"***************** Crea factura desde RMA ******************** ");
    }
    
    public void generateInvoiceFromRMA(int M_RMA_ID, Trx trx)
    {
        MRMA rma = new MRMA(Env.getCtx(), M_RMA_ID, trx.getTrxName());
        
        MInvoice invoice = createInvoice(rma,trx);
        MInvoiceLine invoiceLines[] = createInvoiceLines(rma, invoice, trx);
        
        if (!invoice.save())
        {
            throw new IllegalStateException("Could not update invoice");
        }
        invoice.completeIt();
        invoice.setDocStatus(DocAction.STATUS_Completed);
        invoice.setDocAction(DocAction.ACTION_Complete);
        invoice.save();
        log.log(Level.INFO,"***************** Crea factura desde RMA ******************** ");
    }
    
    private MInvoice createInvoice(MRMA rma,Trx trx)
    {
        int docTypeId = getInvoiceDocTypeId(rma.get_ID(),trx);
            
        MInvoice invoice = new MInvoice(Env.getCtx(), 0, trx.getTrxName());
        invoice.setRMA(rma);
        invoice.setC_PaymentTerm_ID(Integer.parseInt(Msg.getMsg(Env.getCtx(), "PaymentTerm").trim()));
        invoice.setC_DocTypeTarget_ID(docTypeId);
        if (!invoice.save())
        {
            throw new IllegalStateException("Could not create invoice");
        }
        return invoice;
    }
    
    private MInvoiceLine[] createInvoiceLines(MRMA rma, MInvoice invoice,Trx trx)
    {
        ArrayList<MInvoiceLine> invLineList = new ArrayList<MInvoiceLine>();
        MRMALine rmaLines[] = rma.getLines(true);
        for (MRMALine rmaLine : rmaLines)
        {
            if (rmaLine.getM_InOutLine_ID() == 0)
            {
                throw new IllegalStateException("No customer return line - RMA = " 
                        + rma.getDocumentNo() + ", Line = " + rmaLine.getLine());
            }
            
            MInvoiceLine invLine = new MInvoiceLine(invoice);

            invLine.setRMALine(rmaLine);
            
            if(rmaLine.getDescription().toUpperCase().indexOf("PROPI") >= 0){
            	Integer charge = Integer.parseInt(Msg.getMsg(Env.getCtx(), "c_charge_id").trim());
            	invLine.setC_Charge_ID(charge);
            }
            
            if (!invLine.save())
            {
                throw new IllegalStateException("Could not create invoice line");
            }
            
            invLineList.add(invLine);
        }
        MInvoiceLine invLines[] = new MInvoiceLine[invLineList.size()];
        invLineList.toArray(invLines);
        return invLines;
    }
    
 
    private int getInvoiceDocTypeId(int M_RMA_ID,Trx trx)
    {
        String docTypeSQl = "SELECT dt.C_DocTypeInvoice_ID FROM C_DocType dt "
            + "INNER JOIN M_RMA rma ON dt.C_DocType_ID=rma.C_DocType_ID "
            + "WHERE rma.M_RMA_ID=?";
        
        int docTypeId = DB.getSQLValue(trx.getTrxName(), docTypeSQl, M_RMA_ID);
        
        return docTypeId;
    }
    
    private int getInvoiceDocTypeIdFromClient(int clientId,Trx trx)
    {
        String docTypeSQl = "SELECT dt.C_DocTypeInvoice_ID FROM C_DocType dt "
            + "WHERE dt.ad_client_id=? and name = 'Customer Return Material' and isactive='Y'";
        
        int docTypeId = DB.getSQLValue(trx.getTrxName(), docTypeSQl, clientId);
        
        return docTypeId;
    }
    
    private int getInvoiceStandarTax(int clientId,Trx trx)
    {
        String docTypeSQl = "SELECT c_tax_id FROM C_TAX "
            + " WHERE ad_client_id=? and rate=0 and isactive = 'Y' and isdefault ='Y'";
        
        int docTypeId = DB.getSQLValue(trx.getTrxName(), docTypeSQl, clientId);
        
        return docTypeId;
    }
    
    
    
    private MInvoice createInvoice(Trx trx, HashMap <String,String> to, int inOutId)
    {
    	MInOut pInOut = new MInOut(Env.getCtx(), inOutId, trx.getTrxName() );
    	
    	MInvoice data = new MInvoice(Env.getCtx(), 0, null);
    	data.setAD_Client_ID(Integer.parseInt(to.get("ad_client_id").trim()));
    	data.setAD_Org_ID(Integer.parseInt(to.get("ad_org_id").trim()));
		data.setIsActive(true);
		data.setDateInvoiced(new Timestamp((new Date().getTime())));
		data.setDateAcct(new Timestamp((new Date().getTime())));
		data.setC_BPartner_ID(pInOut.getC_BPartner_ID());
		data.setC_BPartner_Location_ID(Integer.parseInt(to.get("bpartner_location")));
		data.setC_DocType_ID(1000004);
		data.setC_Currency_ID(Integer.parseInt(Msg.getMsg(Env.getCtx(), "currencyId").trim()));
		data.setM_PriceList_ID(Integer.parseInt(to.get("PriceListID")));
		data.setSalesRep_ID(Integer.parseInt(to.get("ad_user_id")));
		data.setDescription(to.get("affects-stock-description"));
		data.setIsSOTrx(true);
		int docType = getInvoiceDocTypeIdFromClient(Integer.parseInt(to.get("ad_client_id").trim()), trx);
		data.setC_DocType_ID(docType);
		data.setC_DocTypeTarget_ID(docType);

		Boolean pok = data.save();
		if (!pok){
			throw new IllegalStateException("Could not Create Invoice Encounter: "+data.toString());
		}
		return data;
    }

    private void createInvoiceLines(int inOutId, MInvoice invoice,HashMap <String,String> to,Trx trx)
    {

    	MInOut pInOut = new MInOut(Env.getCtx(), inOutId, trx.getTrxName() );
    	MInOutLine pInOutLines[] = pInOut.getLines();
    	BigDecimal total = new BigDecimal(0); 
    	for (int i = 0; i < pInOutLines.length;i++)
		{
    		String productId = "0";
			if (!to.get("productId").equalsIgnoreCase("null"))
			{
				productId = to.get("productId");
			}
			
			if (Integer.parseInt(productId) == pInOutLines[i].getM_Product_ID() )  // only processs the return product id	
			{
				MInvoiceLine line = new MInvoiceLine(Env.getCtx(), 0, null);
				line.setAD_Org_ID(Integer.parseInt(to.get("ad_org_id").trim()));
				line.setAD_Client_ID(Integer.parseInt(to.get("ad_client_id").trim()));
				line.setIsActive(true);
				line.setC_Invoice_ID(invoice.getC_Invoice_ID());
				line.setQtyEntered((new BigDecimal(to.get("Qty"))));
				line.setM_Product_ID(Integer.parseInt(productId));
				line.setPrice(pInOutLines[i].getC_OrderLine().getPriceEntered());
				line.setPriceActual(pInOutLines[i].getC_OrderLine().getPriceEntered());
				line.setPriceList(new BigDecimal(0));
				line.setC_Tax_ID(pInOutLines[i].getC_OrderLine().getC_Tax_ID());
//				line.setC_Tax_ID(getInvoiceStandarTax(Integer.parseInt(to.get("ad_client_id").trim()), trx));
				line.setC_UOM_ID(pInOutLines[i].getC_OrderLine().getC_UOM_ID());
				line.setQtyInvoiced(new BigDecimal(to.get("Qty")));
				BigDecimal totalLine = line.getQtyEntered().multiply(pInOutLines[i].getC_OrderLine().getPriceEntered());
				line.setLineNetAmt(totalLine);
				total = total.add(totalLine);
				System.out.println("Price"+pInOutLines[i].getC_OrderLine().getPriceEntered());
				System.out.println("PriceActual"+pInOutLines[i].getC_OrderLine().getPriceActual());
				System.out.println("PriceList"+pInOutLines[i].getC_OrderLine().getPriceList());

				Boolean lok = line.save();
				
				if (!lok){
					throw new IllegalStateException("Could not Create Invoice Encounter: "+line.toString());
				}
			}
		}
    	
    	invoice.setTotalLines(total);
    }
}
