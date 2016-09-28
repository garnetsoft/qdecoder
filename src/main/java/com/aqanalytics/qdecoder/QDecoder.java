package com.aqanalytics.qdecoder;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kx.*;
import kx.c.Dict;
import kx.c.Flip;
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
			
			// try kdb_sub func
			// Object sub = conn.k("upd[10]");
			// kdb_sub:{[id;table;subq;upf]
			Object sub = conn.k(new Object[] {"kdb_sub".toCharArray(), "gfeng", "dailybars", "select from dailybars", "{select from dailybars where Sym=`IBM}"});
			log.info("xxxx sub: {}", sub);
			printKdbObject(sub);
			
			while (true) {
				log.info("......wating for upd ->");
				Object data = conn.k2();  // this is a blocking call
				
				log.info("upd type:[{}], data:{}", data.getClass(), data);
				printKdbObject(data);
			}
			
		} catch (KException | IOException e) {
			e.printStackTrace();
			
			// reconnect logic
		}
	}

	public static void printKdbObject(Object data) {
		log.debug("xxxx type: {}", data.getClass());
		
		if (data instanceof Boolean) {
			log.info("type Boolean:{}", data);
		}
		else if (data instanceof Byte) {
			log.info("type byte:{}", Byte.valueOf((Byte)data));
		}
		else if (data instanceof Short) {
			log.info("type short:{}", data);
		}
		else if (data instanceof Integer) {
			log.info("type int:{}", data);
		}
		else if (data instanceof Long) {
			log.info("type Long:{}", data);
		}
		else if (data instanceof Float) {
			log.info("type float:{}", data);
		}
		else if (data instanceof Double) {
			log.info("type double:{}", data);
		}
		else if (data instanceof Character) {
			log.info("type char:{}", data);
		}
		else if (data instanceof String) {
			log.info("type symbol/String:{}", data);
		}
		else if (data instanceof java.sql.Timestamp) {
			log.info("type java.sql.Timestamp/.z.P:{}", data);
		}
		else if (data instanceof java.sql.Date) {
			log.info("type java.sql.Date/.z.D:{}", data);
		}
		else if (data instanceof java.sql.Time) {
			log.info("type java.sql.Time/.z.T:{}", data);
		}		
		else if (data instanceof Date) {
			log.info("type java.util.Data/.z.Z:{}", data);
		}
		else if (data instanceof String[]) {
			log.info("type String[]:{}", data);
		}
		else if (data instanceof int[]) {
			log.info("type int[]:{}", data);
		}		
		else if (data instanceof long[]) {
			log.info("type long[]:{}", data);
		}		
		else if (data instanceof double[]) {
			log.info("type double[]:{}", data);
		}
		
		else if (data instanceof Object[]) {
			log.info("type Object[]/list:{}", Arrays.toString((Object[])data));
			
			for (Object o : (Object[]) data) {
				printKdbObject(o);
			}
		}
		else if (data instanceof Dict) {
			log.info("type Dict:{}", data);
			Object x = ((Dict) data).x;
			Object y = ((Dict) data).y;
			
			printKdbObject(x);
			printKdbObject(y);
		
		}
		else if (data instanceof Flip) {
			log.info("type Flip/Table:{}", data);
			
			String[] cnames = ((Flip) data).x;
			Object[] values = ((Flip) data).y;
			
			printKdbObject(cnames);
			printKdbObject(values);
		}		
		else {
			log.warn("Unknown type: {}", data);
		}
	}
}
