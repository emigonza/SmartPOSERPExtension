package com.smj.util;

import java.util.logging.Level;

import javax.jms.JMSException;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.compiere.util.CLogger;

/**
 * Servlet implementation class ServletPOSListener
 */
public class ServletPOSListener extends HttpServlet {
	private static final long serialVersionUID = 1L;
	protected CLogger log = CLogger.getCLogger(super.getClass());
    /**
     * Default constructor. 
     */
    public ServletPOSListener() {
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		 System.out.print("***************************** Inicializando Listener de POS ********************************");
	        ERPPosListener posl = new ERPPosListener();
			
			try {
				posl.run();
			} catch (JMSException e) {
				log.log (Level.SEVERE, null, e);
				e.printStackTrace();
			}
				
	}//init

}//ServletPOSListener