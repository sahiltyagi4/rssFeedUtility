package com.feedutil.server;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedutil.utils.DashboardUtils;

public class EventTagServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setStatus(HttpServletResponse.SC_OK);
		response.setHeader("Access-Control-Allow-Origin", "*");
		
		PrintWriter writer = response.getWriter();
		String event = request.getParameter("event");
		String eventTags = request.getParameter("tags");
		
		//the method which saves the data into back-end
		JSONObject json = DashboardUtils.addEventsToProcess(event, eventTags);
		
		writer.print(json.toJSONString());
		writer.close();
	}
}
