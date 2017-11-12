package com.feedutil.server;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedutil.utils.DashboardUtils;

public class FetchEventServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setStatus(HttpServletResponse.SC_OK);
		response.setHeader("Access-Control-Allow-Origin", "*");
		
		String curatedEvent = request.getParameter("event").trim();
		int numRecords = Integer.parseInt(request.getParameter("records"));
		
		PrintWriter writer = response.getWriter();
		//fetches the records for the requested event
		JSONArray jsonArray = DashboardUtils.fetchEventArticles(curatedEvent, numRecords);
		
		writer.write(jsonArray.toJSONString());
		writer.close();
	}
}
