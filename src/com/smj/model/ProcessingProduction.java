package com.smj.model;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import org.compiere.apps.form.Allocation;
import org.compiere.model.MClient;
import org.compiere.model.MLocator;
import org.compiere.model.MProduct;
import org.compiere.model.MStorage;
import org.compiere.model.MTransaction;
import org.compiere.model.Query;
import org.compiere.model.X_M_Production;
import org.compiere.model.X_M_ProductionLine;
import org.compiere.model.X_M_ProductionPlan;
import org.compiere.process.DocumentEngine;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Trx;
import org.eevolution.model.MPPProductBOM;
import org.eevolution.model.MPPProductBOMLine;

public class ProcessingProduction {
	private int m_level = 0;
	private boolean mustBeStocked = false;
	public static CLogger log = CLogger.getCLogger(ProcessingProduction.class);
	
	public boolean isProductCreatedByProductionProcess(int productId, Trx trx,Properties ctx) {
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select count(*) as c from PP_Product_BOMLine as ppl, PP_Product_BOM as pp where ppl.PP_Product_BOM_id = pp.PP_Product_BOM_id and pp.m_product_id = "+productId+" ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		

		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			if (rs.next ()){
				if(rs.getInt("c") > 0)
					return true;
				else
					return false;
			} else
				return false;
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
			return false;
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
	}
	
public int getProductBomId(int productId, Trx trx,Properties ctx) {
		
		StringBuffer sql = new StringBuffer();
		sql.append(" Select pp.PP_Product_BOM_id as id  from PP_Product_BOM as pp where pp.m_product_id = "+productId+" ");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		

		try	{
			pstmt = DB.prepareStatement (sql.toString(), null);
			rs = pstmt.executeQuery ();
			rs.next();
			return rs.getInt("id");
					
		}catch (Exception e)	{
			log.log (Level.SEVERE, sql.toString(), e);
			return 0;
		}
		finally	{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
	}

	public X_M_Production createProductionProcessFromProducto(Trx trx,
			HashMap<String, Object> to, Properties ctx) {
		X_M_Production producction = new X_M_Production(ctx, 0, trx.getTrxName());

		producction.setAD_Client_ID((Integer)to.get("ad_client_id"));
		producction.setAD_Org_ID((Integer)to.get("ad_org_id"));
		producction.setIsActive(true);
		producction.setName((String)to.get("productID") + " " + (new Date()));
		producction.setMovementDate(new Timestamp (System.currentTimeMillis()));
		
		producction.saveEx();
		
		return producction;
	}

	public List<X_M_ProductionPlan> createProductionPlan(Trx trx,
			HashMap<String, Object> to, X_M_Production production, Properties ctx, int productId, BigDecimal qty) {
		
		List<X_M_ProductionPlan> ppList = new ArrayList<X_M_ProductionPlan>();
		
		X_M_ProductionPlan pp = new X_M_ProductionPlan(ctx, 0, trx.getTrxName());
		
		pp.setLine(10);
		pp.setAD_Client_ID((Integer)to.get("ad_client_id"));
		pp.setAD_Org_ID((Integer)to.get("ad_org_id"));
		pp.setM_Product_ID( productId);
		pp.setProductionQty(qty);
		pp.setM_Locator_ID((Integer)to.get("locatorID"));
		pp.setM_Production_ID(production.get_ID());
		
		pp.save();
		
		ppList.add(pp);
		
		return ppList;
	}

	public boolean doProsses(Trx trx, HashMap<String, Object> to,
			X_M_Production production, List<X_M_ProductionPlan> lines, Properties ctx) throws Exception {

		/**
		 * No Action
		 */
		if (production.isProcessed()) {
			return false;
		}

		for (X_M_ProductionPlan pp : lines) {

			if (!production.isCreated()) {
				int line = 100;
				int no = DB.executeUpdateEx(
								"DELETE M_ProductionLine WHERE M_ProductionPlan_ID = ?",
								new Object[] { pp.getM_ProductionPlan_ID() },
								trx.getTrxName());


				MProduct product = MProduct.get(ctx, pp.getM_Product_ID());

				X_M_ProductionLine pl = new X_M_ProductionLine(ctx, 0,trx.getTrxName());
				pl.setAD_Org_ID(pp.getAD_Org_ID());
				pl.setLine(line);
				pl.setDescription(pp.getDescription());
				pl.setM_Product_ID(pp.getM_Product_ID());
				pl.setM_Locator_ID(pp.getM_Locator_ID());
				pl.setM_ProductionPlan_ID(pp.getM_ProductionPlan_ID());
				pl.setMovementQty(pp.getProductionQty());
				pl.save();
				if (explosion(pp, product, pp.getProductionQty(), line, ctx, trx,(Integer)to.get("ad_org_id")) == 0)
					return false;

			} else {
				String whereClause = "M_ProductionPlan_ID= ? ";
				List<X_M_ProductionLine> production_lines = new Query(ctx,
						X_M_ProductionLine.Table_Name, whereClause,
						trx.getTrxName())
						.setParameters(pp.getM_ProductionPlan_ID())
						.setOrderBy("Line").list();

				for (X_M_ProductionLine pline : production_lines) {
					MLocator locator = MLocator.get(ctx,
							pline.getM_Locator_ID());
					String MovementType = MTransaction.MOVEMENTTYPE_ProductionPlus;
					BigDecimal MovementQty = pline.getMovementQty();
					if (MovementQty.signum() == 0)
						continue;
					else if (MovementQty.signum() < 0) {
						BigDecimal QtyAvailable = MStorage.getQtyAvailable(
								locator.getM_Warehouse_ID(),
								locator.getM_Locator_ID(),
								pline.getM_Product_ID(),
								pline.getM_AttributeSetInstance_ID(),
								trx.getTrxName());

						if (mustBeStocked
								&& QtyAvailable.add(MovementQty).signum() < 0) {
							return false;
						}

						MovementType = MTransaction.MOVEMENTTYPE_Production_;
					}

					if (!MStorage.add(ctx, locator.getM_Warehouse_ID(),
							locator.getM_Locator_ID(), pline.getM_Product_ID(),
							pline.getM_AttributeSetInstance_ID(), 0,
							MovementQty, Env.ZERO, Env.ZERO, trx.getTrxName())) {
						return false;
					}

					// Create Transaction
					MTransaction mtrx = new MTransaction(ctx,
							pline.getAD_Org_ID(), MovementType,
							locator.getM_Locator_ID(), pline.getM_Product_ID(),
							pline.getM_AttributeSetInstance_ID(), MovementQty,
							production.getMovementDate(), trx.getTrxName());
					mtrx.setM_ProductionLine_ID(pline.getM_ProductionLine_ID());
					mtrx.saveEx();

					pline.setProcessed(true);
					pline.saveEx();
				} // Production Line

				pp.setProcessed(true);
				pp.saveEx();
			}
		} // Production Plan

		if (!production.isCreated()) {
			production.setIsCreated(true);
			production.saveEx();
		} else {
			production.setProcessed(true);
			production.saveEx();

			/* Immediate accounting */
			if (MClient.isClientAccountingImmediate()) {
				String ignoreError = DocumentEngine.postImmediate(ctx,
						(Integer)to.get("ad_client_id"), production.get_Table_ID(),
						production.get_ID(), true, trx.getTrxName());
			}

		}
		
		return true;

	}
	
	private int explosion(X_M_ProductionPlan pp , MProduct product , BigDecimal qty , int line,Properties ctx, Trx trx, int orgId ) throws Exception
	{
//		MPPProductBOM bom = MPPProductBOM.getDefault(product, trx.getTrxName());
		int bomId = getProductBomId(product.get_ID(),trx,ctx);
		MPPProductBOM bom = new MPPProductBOM(ctx,bomId,trx.getTrxName());
		if(bom == null )
		{	
			throw new IllegalStateException("Ticket Exists : ");
			
		}				
		MPPProductBOMLine[] bom_lines = bom.getLines(new Timestamp (System.currentTimeMillis()));
		m_level += 1;
		int components = 0;
		line = line * m_level;
		for(MPPProductBOMLine bomline : bom_lines)
		{
			MProduct component = MProduct.get(ctx, bomline.getM_Product_ID());
			
			if(component.isBOM() && !component.isStocked())
			{	
				explosion(pp, component, bomline.getQtyBOM() , line, ctx,trx, orgId);
			}
			else
			{	
				line += 1;
				X_M_ProductionLine pl = new X_M_ProductionLine(ctx, 0 , trx.getTrxName());
				pl.setAD_Org_ID(pp.getAD_Org_ID());
				pl.setLine(line);
				pl.setDescription(bomline.getDescription());
				pl.setM_Product_ID(bomline.getM_Product_ID());
				pl.setM_Locator_ID(pp.getM_Locator_ID());
				pl.setM_ProductionPlan_ID(pp.getM_ProductionPlan_ID());
				pl.setMovementQty(bomline.getQtyBOM().multiply(qty).negate());
				pl.saveEx();
				components += 1;
				
			}
		
		}
		return  components;
	}
}
