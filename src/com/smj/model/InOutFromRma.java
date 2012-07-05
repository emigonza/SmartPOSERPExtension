package com.smj.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;

import org.compiere.model.MInOut;
import org.compiere.model.MInOutLine;
import org.compiere.model.MRMA;
import org.compiere.model.MRMALine;
import org.compiere.process.DocAction;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Trx;

import com.smj.util.DataQueries;

/**
 * Creates a InOut (customer return) from a RMA
 * @author pedrorozo
 *
 */

public class InOutFromRma {

	
	protected CLogger log = CLogger.getCLogger(super.getClass());
	
	/**
	 * Creates a InOut (customer return) from a RMA
	 * @param M_RMA_ID
	 * @param trx
	 */
	public void generateInOutFromRMA(int M_RMA_ID,Trx trx)
    {
		
        MRMA rma = new MRMA(Env.getCtx(), M_RMA_ID, trx.getTrxName());
        
        MInOut inOut = createInOut(rma,trx);
        MInOutLine shipmentLines[] = createInOutLines(rma, inOut,trx);

        
        if (!inOut.save())
        {
            throw new IllegalStateException("Could not update shipment");
            
        }
        inOut.completeIt();
        
        inOut.setDocStatus(DocAction.STATUS_Completed);
		inOut.setDocAction(DocAction.ACTION_Complete);
		inOut.save();
    	
        log.log(Level.INFO,"***************** Crea InOut desde RMA ******************** ");
    }

	/**
	 * Creates a InOut (customer return) from a RMA
	 * @param M_RMA_ID
	 * @param trx
	 */
    private MInOut createInOut(MRMA rma,Trx trx)
    {
    	
    	int docTypeId = getInOutDocTypeId(rma.get_ID(),trx);
        
        if (docTypeId == -1)
        {
            throw new IllegalStateException("Could not get invoice document type for Vendor RMA");
        }
        
        MInOut originalReceipt = rma.getShipment();
        MInOut inOut = new MInOut(Env.getCtx() ,0, trx.getTrxName());
        inOut.setAD_Client_ID(originalReceipt.getAD_Client_ID());
        inOut.setDateOrdered(new Timestamp((new Date()).getTime()));
        inOut.setDateAcct(new Timestamp((new Date()).getTime()));
        inOut.setMovementDate(new Timestamp((new Date()).getTime()));
        
        inOut.setM_RMA_ID(rma.get_ID());
        inOut.setAD_Org_ID(rma.getAD_Org_ID());
        inOut.setAD_OrgTrx_ID(originalReceipt.getAD_OrgTrx_ID());
        inOut.setDescription(rma.getDescription());
        inOut.setC_BPartner_ID(rma.getC_BPartner_ID());
        inOut.setC_BPartner_Location_ID(originalReceipt.getC_BPartner_Location_ID());
        inOut.setIsSOTrx(rma.isSOTrx());
        inOut.setC_DocType_ID(docTypeId);       
        inOut.setM_Warehouse_ID(originalReceipt.getM_Warehouse_ID());
        inOut.setMovementType(MInOut.MOVEMENTTYPE_CustomerReturns);
        inOut.setDeliveryRule(MInOut.DELIVERYRULE_Force);
        inOut.setC_Project_ID(originalReceipt.getC_Project_ID());
        inOut.setC_Campaign_ID(originalReceipt.getC_Campaign_ID());
        inOut.setC_Activity_ID(originalReceipt.getC_Activity_ID());
        inOut.setUser1_ID(originalReceipt.getUser1_ID());
        inOut.setUser2_ID(originalReceipt.getUser2_ID());
        inOut.setSalesRep_ID(originalReceipt.getSalesRep_ID());
        
        if (!inOut.save())
        {
            throw new IllegalStateException("Could not create inOut");
        }
        return inOut;
    	
    }
    
    /**
	 * Creates  InOut Lines (customer return) from a RMA
	 * @param M_RMA_ID
	 * @param trx
	 */
    private MInOutLine[] createInOutLines(MRMA rma, MInOut inOut, Trx trx)
    {
       ArrayList<MInOutLine> inOutLineList = new ArrayList<MInOutLine>();
        
        MRMALine rmaLines[] = rma.getLines(true);
        for (MRMALine rmaLine : rmaLines)
        {
        	
        	// System.out.println("Product ID a insertar en la devolucion (MINOUT):"+rmaLine.getM_Product_ID());
        	
            if (rmaLine.getM_InOutLine_ID() != 0  && rmaLine.getM_Product_ID() != 0  )
            {
            	MInOutLine inOutLine = new MInOutLine(Env.getCtx() ,0, trx.getTrxName());
                inOutLine.setAD_Client_ID(rmaLine.getAD_Client_ID());
                inOutLine.setAD_Org_ID(rmaLine.getAD_Org_ID());
                inOutLine.setM_InOut_ID(inOut.getM_InOut_ID());
                
                inOutLine.setM_RMALine_ID(rmaLine.get_ID());
                inOutLine.setLine(rmaLine.getLine());
                inOutLine.setDescription(rmaLine.getDescription());
                inOutLine.setM_Product_ID(rmaLine.getM_Product_ID());
                inOutLine.setM_AttributeSetInstance_ID(rmaLine.getM_AttributeSetInstance_ID());
                inOutLine.setC_UOM_ID(rmaLine.getC_UOM_ID());
                
                // Calculates the right quantity using the conversion factor provided by the UMO
                DataQueries q = new DataQueries();
                BigDecimal divideRate = q.getConversionFactorUOM( rmaLine.getM_Product_ID() , rmaLine.getC_UOM_ID(), trx).setScale(2, RoundingMode.HALF_UP);
        		BigDecimal qty2 = divideRate.multiply(rmaLine.getQty());   // multiplies original quantity by factor
				// 	
                
                inOutLine.setQty(rmaLine.getQty().abs());
                inOutLine.setMovementQty(qty2.abs());
                inOutLine.setM_Locator_ID(rmaLine.getM_Locator_ID());
                inOutLine.setC_Project_ID(rmaLine.getC_Project_ID());
                inOutLine.setC_Campaign_ID(rmaLine.getC_Campaign_ID());
                inOutLine.setC_Activity_ID(rmaLine.getC_Activity_ID());
                inOutLine.setC_ProjectPhase_ID(rmaLine.getC_ProjectPhase_ID());
                inOutLine.setC_ProjectTask_ID(rmaLine.getC_ProjectTask_ID());
                inOutLine.setUser1_ID(rmaLine.getUser1_ID());
                inOutLine.setUser2_ID(rmaLine.getUser2_ID());
                inOutLine.save();
                inOutLineList.add(inOutLine);

            }
        }
        
        MInOutLine inOutLines[] = new MInOutLine[inOutLineList.size()];
        inOutLineList.toArray(inOutLines);
        
        return inOutLines;
    }
	
    /**
     * Returns InOut DocType Id  
     * @param M_RMA_ID
     * @param trx
     * @return
     */
	private int getInOutDocTypeId(int M_RMA_ID,Trx trx)
    {
        String docTypeSQl = "SELECT dt.C_DocTypeShipment_ID FROM C_DocType dt "
            + "INNER JOIN M_RMA rma ON dt.C_DocType_ID=rma.C_DocType_ID "
            + "WHERE rma.M_RMA_ID=?";
        
        int docTypeId = DB.getSQLValue(trx.getTrxName(), docTypeSQl, M_RMA_ID);
        
        return docTypeId;
    }


}
