package uk.ac.mmu.advprog.hackathon;

import static spark.Spark.get;
import static spark.Spark.port;

import spark.Request;
import spark.Response;
import spark.Route;

/**
 * Handles the setting up and starting of the web service You will be adding
 * additional routes to this class, and it might get quite large Feel free to
 * distribute some of the work to additional child classes, like I did with DB
 * 
 * @author You, Mainly!
 */
public class AMIWebService {

	/**
	 * Main program entry point, starts the web service
	 * 
	 * @param args not used
	 */
	public static void main(String[] args) {
		port(8088);

		// Simple route so you can check things are working...
		// Accessible via http://localhost:8088/test in your browser
		get("/test", new Route() {
			@Override
			public Object handle(Request request, Response response) throws Exception {
				try (DB db = new DB()) {
					return "Number of Entries: " + db.getNumberOfEntries();
				}
			}
		});

		get("/lastsignal", new Route() {
			@Override
			public Object handle(Request request, Response response) throws Exception {
				try (DB db = new DB()) {
					String name = request.queryParams("signal_id");
					return db.getLastSignal(name);
				}
			}
		});

		get("/frequency", new Route() {
			@Override
			public Object handle(Request request, Response response) throws Exception {
				try (DB db = new DB()) {
					String signal_group = request.queryParams("motorway");
					return db.getFrequency(signal_group + "%");
				}

			}

		});

		get("/groups", new Route() {
			@Override
			public Object handle(Request request, Response response) throws Exception {

				try (DB db = new DB()) {
					//changes response type to be viewed in xml format
					response.type("application/xml");
					return db.getGroups();
				}
			}
		});

		get("/signalsattime", new Route() {
			@Override
			public Object handle(Request request, Response response) throws Exception {
				try (DB db = new DB()) {
					String Groups = request.queryParams("group");
					String Times = request.queryParams("time");
					
					//changes response type to be viewed in xml format
					response.type("application/xml");
					Times.replace("+", " ");

					return db.getGroupsTimes(Groups, Times);
				}
			}
		});

	}

}
