package uk.ac.mmu.advprog.hackathon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Handles database access from within your web service
 * 
 * @author You, Mainly!
 */
public class DB implements AutoCloseable {

	// allows us to easily change the database used
	private static final String JDBC_CONNECTION_STRING = "jdbc:sqlite:./data/AMI.db";

	// allows us to re-use the connection between queries if desired
	private Connection connection = null;

	/**
	 * Creates an instance of the DB object and connects to the database
	 */
	public DB() {
		try {
			connection = DriverManager.getConnection(JDBC_CONNECTION_STRING);
		} catch (SQLException sqle) {
			error(sqle);
		}
	}

	/**
	 * Returns the number of entries in the database, by counting rows
	 * 
	 * @return The number of entries in the database, or -1 if empty
	 */
	// executes a query statement to find out the number of entries specific to
	// certain motorway names from your database with try and catch block
	public int getNumberOfEntries() {
		int result = -1;
		try {
			// declares an connection for a new sql query
			Statement s = connection.createStatement();
			// executes your sql query
			ResultSet results = s.executeQuery("SELECT COUNT(*) AS count FROM ami_data");
			while (results.next()) { // will only execute once, because SELECT COUNT(*) returns just 1 number
				result = results.getInt(results.findColumn("count"));
			}
		} catch (SQLException sqle) {
			error(sqle);

		}
		return result;
	}

	// executes a query statement to find out the last signal displayed for a
	// specific motorway before its turned off or diplaying "NR" and finally not
	// displaying "BLNK"
	public String getLastSignal(String id) {
		String T = "";
		PreparedStatement s;
		try {
			s = connection.prepareStatement(
					"SELECT signal_value FROM ami_data WHERE signal_id = ? AND NOT signal_value = \"OFF\" AND NOT signal_value = \"NR\" AND NOT signal_value = \"BLNK\" ORDER BY datetime DESC LIMIT 1;");
			s.setString(1, id);
			ResultSet sT = s.executeQuery();
			// gets signal-value from data base format
			T = sT.getString("signal_value");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return T;
	}

	// gets the frequency of the signals for each motorway in the given timeframe
	// provided by the database
	public String getFrequency(String SignalID) {
		String json = "[]";
		
		PreparedStatement J;
		// creates an instance of anew json array
		JSONArray root = new JSONArray();

		try {
			J = connection.prepareStatement(
					"SELECT COUNT(signal_value) AS frequency, signal_value FROM ami_data WHERE signal_id LIKE ? GROUP BY signal_value ORDER BY frequency DESC;");
			J.setString(1, SignalID + "%");
			ResultSet JT = J.executeQuery();

			while (JT.next()) {
				// creates an instance of anew json object
				JSONObject Signal = new JSONObject();
				Signal.put("value", JT.getString("signal_value"));
				Signal.put("frequency", JT.getString(JT.findColumn("frequency")));
				// sets the root of your json array to your signal values
				root.put(Signal);
			}
			json = root.toString();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return json;

	}

	// returns the motorway group starting with a specific characters
	public String getGroups() {
		PreparedStatement G;

		try {
			G = connection.prepareStatement("SELECT DISTINCT signal_group FROM ami_data;");
			
			//execute your query statement
			ResultSet GT = G.executeQuery();
			
			//creates a new insatnce xml doc builder
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			Document document = dbf.newDocumentBuilder().newDocument();
			//create a new xml element named "GROUPS"
			Element Groups = document.createElement("Groups");
			//append your child node to xml doc
			document.appendChild(Groups);
			String Texts = "";
			
			while (GT.next()) {
				//run through your data base with the sql query inside the while loop while appending the elements onto your xml doc
				Texts = GT.getString("signal_group");
				Element Group = document.createElement("Group");
				Group.setTextContent(Texts);
				Groups.appendChild(Group);
			}
			//creates a new string writer to convert your xml to string 
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			StringWriter output = new StringWriter();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(new DOMSource(document), new StreamResult(output));

			return output.toString();
		} catch (SQLException | ParserConfigurationException | TransformerException
				| TransformerFactoryConfigurationError e) {
			e.printStackTrace();
		}
		return null;
	}

	public Writer getGroupsTimes(String GroupId, String Time) {
		PreparedStatement T;
		try {

			T = connection.prepareStatement(
					"SELECT datetime, signal_id, signal_value FROM ami_data WHERE signal_group = ? AND datetime < ? AND (datetime, signal_id) IN (SELECT MAX(datetime) AS datetime, signal_id FROM ami_data WHERE signal_group = ? AND datetime < ? GROUP BY signal_id) ORDER BY signal_id;");
			//sets your sql query statement to values of group id and time depending on their occurrences
			T.setString(1, GroupId);
			T.setString(2, Time);
			T.setString(3, GroupId);
			T.setString(4, Time);
			
			//execute your query statement
			ResultSet TT = T.executeQuery();
			
			//creates a new insatnce xml doc builder
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			Document doc = dbf.newDocumentBuilder().newDocument();
			Element Signals = doc.createElement("Signals");
			doc.appendChild(Signals);
			
			//run through your data base with the sql query inside the while loop while appending the elements onto your xml doc
			while (TT.next()) {
				Element Signal = doc.createElement("Signal");
				Signals.appendChild(Signal);
				Element ID = doc.createElement("ID");
				ID.setTextContent(TT.getString("signal_id"));
				Signal.appendChild(ID);
				Element DateSet = doc.createElement("DateSet");
				DateSet.setTextContent(TT.getString("datetime"));
				Signal.appendChild(DateSet);
				Element value = doc.createElement("value");
				value.setTextContent(TT.getString("signal_value"));
				Signal.appendChild(value);
			}
			
			//creates a new string writer to convert your xml to string 
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			Writer output = new StringWriter();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(new DOMSource(doc), new StreamResult(output));

			return output;
		} catch (SQLException | ParserConfigurationException | TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * Closes the connection to the database, required by AutoCloseable interface.
	 */
	@Override
	public void close() {
		try {
			if (!connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException sqle) {
			error(sqle);
		}
	}

	/**
	 * Prints out the details of the SQL error that has occurred, and exits the
	 * programme
	 * 
	 * @param sqle Exception representing the error that occurred
	 */
	private void error(SQLException sqle) {
		System.err.println("Problem Opening Database! " + sqle.getClass().getName());
		sqle.printStackTrace();
		System.exit(1);
	}
}
