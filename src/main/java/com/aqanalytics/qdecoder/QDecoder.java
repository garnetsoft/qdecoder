package com.aqanalytics.qdecoder;

import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kx.*;
import kx.c.KException;

public class QDecoder {
	private static final Logger log = LoggerFactory.getLogger(QDecoder.class);
	
	public static void main(String[] args) {
		System.out.printf("starting...%s %n", new Date());		
		log.info("logger: {}, {}", log.getName(), new Date());
		
		c conn = null;
		
		try {
			conn = new c("localhost", 5001);
			
			Object status = conn.k("tables[]");
			log.info("xxxx status: {}", status);
			
			while (true) {
				log.info("......wating for upd ->");
				Object data = conn.k2();  // this is a blocking call
				
				log.info("upd type:[{}], data:{}", data.getClass(), data);
			}
			
		} catch (KException | IOException e) {
			e.printStackTrace();
			
			// reconnect logic
		}
	}

	public static void printKdbObject(Object data) {
		
	}
}
