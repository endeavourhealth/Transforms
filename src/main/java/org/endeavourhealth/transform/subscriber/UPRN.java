package org.endeavourhealth.transform.subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.URL;
import java.net.URLEncoder;

import org.json.*;

public class UPRN {
	public static String uprn_token = "";

	public static String getAdrec(String adrec, String token, String token_endpoint) throws Exception {
		String response = "";

		//String url= "https://uprnapi.discoverydataservice.net:8443/api/getinfo?adrec="+adrec;

		String url = token_endpoint+"api/getcsv?adrec=" + URLEncoder.encode(adrec, "UTF-8")+"&delim=~";

		try {
			URL obj = new URL(url);

			HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

			con.setRequestMethod("GET");
			con.setRequestProperty("Authorization", "Bearer " + token);

			int responseCode = con.getResponseCode();

			String output;
			BufferedReader in = new BufferedReader(
					new InputStreamReader(con.getInputStream()));

			while ((output = in.readLine()) != null) {
				response = response + output;
			}
			in.close();


		} catch (IOException e) {
			e.printStackTrace();
		}
		return response;
	}

	public static String getToken(String password, String username, String clientid, String token_endpoint) {
		String token = "";

		try {
			
			String encoded = "password=" + password + "&username=" + username + "&client_id=" + clientid + "&grant_type=password";

			// URL obj = new URL("https://www.discoverydataservice.net/auth/realms/endeavour-machine/protocol/openid-connect/token");
			URL obj = new URL(token_endpoint);
			HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

			con.setRequestMethod("POST");
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

			con.setDoOutput(true);

			BufferedOutputStream bos = new BufferedOutputStream(con.getOutputStream());

			BufferedInputStream bis = new BufferedInputStream(new StringBufferInputStream(encoded));

			int i;
			// read byte by byte until end of stream
			while ((i = bis.read()) > 0) {
				bos.write(i);
			}
			bis.close();
			bos.close();

			InputStream inputStream;
			int responseCode = con.getResponseCode();

			String response = "";

			if ((responseCode >= 200) && (responseCode <= 202)) {
				inputStream = con.getInputStream();
				//System.out.println(con.getHeaderField("location"));
				int j;
				while ((j = inputStream.read()) > 0) {
					//System.out.print((char) j);
					response = response + (char) j;
				}

			} else {
				inputStream = con.getErrorStream();
			}
			con.disconnect();

			JSONObject json = new JSONObject(response.toString());

			token = json.getString("access_token");


		} catch (Exception e) {
			e.printStackTrace();
		}

		return token;
	}
	public static String getUPRNToken(String password, String username, String clientid, Logger LOG, String token_endpoint) {
		if (uprn_token.isEmpty()) {
			LOG.debug("UPRN: refreshing token");
			uprn_token = getToken(password, username, clientid, token_endpoint);
		}
		return uprn_token;
	}
}

