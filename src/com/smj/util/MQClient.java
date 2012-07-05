package com.smj.util;

import java.util.logging.Level;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.compiere.util.CLogger;
import org.compiere.util.Env;
import org.compiere.util.Msg;

public class MQClient {

	protected static CLogger log = CLogger.getCLogger("com.smj.util.MQClient");
	
    public static void sendMessage(String messageText,String messageType,String valor, String orgId,String pcName){
    	String subject = orgId;
    	String url = Msg.getMsg(Env.getCtx(), "urlqueue").trim();
    	String mquser = Msg.getMsg(Env.getCtx(), "userqueue").trim();
    	String mqpasswd = Msg.getMsg(Env.getCtx(), "pwdqueue").trim();
    	ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(mquser ,mqpasswd ,url); 
//        ConnectionFactory connectionFactory =  new ActiveMQConnectionFactory(url); // simple connections
        Connection connection;
		try {
			connection = connectionFactory.createConnection();
	        connection.start();
	        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
	        Destination destination = session.createQueue(subject);    // for queues
//	        Destination destination = session.createTopic(subject);    // for topics
	        MessageProducer producer = session.createProducer(destination);
	        //TextMessage message = session.createTextMessage(new String(messageText.getBytes(),"UTF-8"));
	        TextMessage message = session.createTextMessage(messageText.toString());
	        message.setStringProperty("Type", messageType);
	        message.setStringProperty("Value", valor);
	        if(pcName != null && !pcName.equalsIgnoreCase(""))
	        	message.setStringProperty("PCName", pcName);
	        producer.send(message);
	        System.out.println("Mensaje enviado a ActiveMQ:'" + message.getText() + "'");
	        connection.close();
		} catch (Exception e) {
			log.log (Level.SEVERE, null, e);
			e.printStackTrace();
		
		}
    }
    
    public static void sendMessageBlob(byte[] barray,String valor, String orgId){
    	String subject = orgId;
    	String url = Msg.getMsg(Env.getCtx(), "urlqueue").trim();
    	String mquser = Msg.getMsg(Env.getCtx(), "userqueue").trim();
    	String mqpasswd = Msg.getMsg(Env.getCtx(), "pwdqueue").trim();
    	ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(mquser ,mqpasswd ,url); 
//        ConnectionFactory connectionFactory =  new ActiveMQConnectionFactory(url); // simple connections
        Connection connection;
		try {
			connection = connectionFactory.createConnection();
	        connection.start();
	        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
	        Destination destination = session.createQueue(subject);    // for queues
//	        Destination destination = session.createTopic(subject);    // for topics
	        MessageProducer producer = session.createProducer(destination);
	        
//	        ByteArrayInputStream bis = new ByteArrayInputStream(barray);
//	        ObjectInput in = new ObjectInputStream(bis);
	        
	        
	        
	        ObjectMessage message = session.createObjectMessage();
	        message.setObject(barray);
	        message.setStringProperty("Value", valor);

	        producer.send(message);

//	        bis.close();
//	        in.close();
	        connection.close();
		} catch (Exception e) {
			log.log (Level.SEVERE, null, e);
			e.printStackTrace();
		
		}
    }
    	
}
