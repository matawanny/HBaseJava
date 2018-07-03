package com.yieldbook.mortgage.spring.dao;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import com.yieldbook.mortgage.spring.bean.Servicer;

public class App {

	public static void main(String[] args) {

		ApplicationContext context = new ClassPathXmlApplicationContext(
				"com/yieldbook/mortgage/spring/bean/beans.xml");

		ServicersDAO servicersDao = (ServicersDAO) context.getBean("servicersDao");
		
		try {
			List<Servicer> servicers = servicersDao.getServicers();

			for (Servicer servicer : servicers) {
				System.out.println(servicer);
			}
			
			Servicer servicer = servicersDao.getServicerById(1000404);
			
			System.out.println("getServiceById(), service_id=1000404, should be: " + servicer);
			
			Servicer servicer0 = servicersDao.getServicerByName("SOUNDMORTGAGE,INCRES  WA 98063");
			
			System.out.println("getServicerByName(), the result is " + servicer0);
			
			
/*			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			Servicer servicer1 = new Servicer("phillipxiao", timestamp);
			if(servicersDao.create(servicer1)) {
				System.out.println("Create Servicer object.");
			}
			
			Servicer servicer2 = new Servicer(9999, "phillipxiao1", timestamp);
			if(servicersDao.update(servicer2)){
				System.out.println("Object update");
			}*/
			
		} 
		catch(CannotGetJdbcConnectionException ex) {
			System.out.println("Cannot get database connection.");
		}
		catch (DataAccessException ex) {
			System.out.println(ex.getMessage());
			System.out.println(ex.getClass());
		}
		

		((ClassPathXmlApplicationContext) context).close();
	}

}
