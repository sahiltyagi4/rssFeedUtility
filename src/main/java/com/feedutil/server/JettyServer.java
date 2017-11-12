package com.feedutil.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.glassfish.jersey.model.internal.FeatureContextWrapper;

import com.feedutil.utils.DashboardUtils;

public class JettyServer {
	
	public static void main(String[] args) {
		//main class to run the dashboard api....running jetty webserver on port 8082
		Server server = new Server(8082);
		ServletHandler handler = new ServletHandler();
		server.setHandler(handler);
		//servlet to add an event and it's associated tag
		handler.addServletWithMapping(EventTagServlet.class, "/addevent");
		//servlet to fetch data corresponding to a particular event
		handler.addServletWithMapping(FetchEventServlet.class, "/fetchevent");
		
		try {
			server.start();
			server.join();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
