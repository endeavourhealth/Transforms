package org.endeavourhealth.transform.subscriber;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.common.utility.MetricsHelper;
import org.endeavourhealth.common.utility.MetricsTimer;
import org.endeavourhealth.im.client.IMClient;
import org.glassfish.jersey.uri.UriComponent;
import org.keycloak.representations.AccessTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.*;

public class UPRN {
	private static final Logger LOG = LoggerFactory.getLogger(UPRN.class);

	//public static String uprnToken = "";
	private static KeycloakClient kcClient = null;

	private static JsonNode getConfig() throws Exception {
		return ConfigManager.getConfigurationAsJson("uprn");
	}

	public static boolean isConfigured() throws Exception {
		return getConfig() != null;
	}


	public static String getAdrec(String adrec, String ids) throws Exception {

		//in case of any one-off error, give it a few attempts
		int lives = 5;

		while (true) {
			lives--;
			try {
				return tryGetAdrec(adrec, ids);

			} catch (Exception ex) {
				if (lives <= 0) {
					throw ex;
				}

				LOG.warn("Exception " + ex.getMessage() + " calling into URPN API - will try " + lives + " more times");
				Thread.sleep(2000); //small delay
			}
		}
	}

	private static String tryGetAdrec(String adrec, String ids) throws Exception {

		JsonNode config = getConfig();
		if (config == null) {
			return null;
		}

		try (MetricsTimer timer = MetricsHelper.recordTime("UPRN.getcsv")) {

			Map<String, String> params = new HashMap<>();
			params.put("adrec", adrec);
			params.put("delim", "~");
			params.put("ids", ids);

			String baseUrl = config.get("uprn_endpoint").asText();

			Response response = get(baseUrl, "api/getcsv", params);

			if (response.getStatus() == 200) {
				Object o = response.getEntity();
				LOG.trace("Entity = " + o);
				LOG.trace("Cls " + o.getClass());
				return (String)o;
				//return response.readEntity(String.class);

			} else {
				throw new IOException(response.readEntity(String.class));
			}
		}
	}

	private static Response get(String baseUrl, String path, Map<String, String> params) throws Exception {
		Client client = ClientBuilder.newClient();
		WebTarget target = client.target(baseUrl).path(path);

		if (params != null && !params.isEmpty()) {
			for (Map.Entry<String, String> entry: params.entrySet()) {
				if (entry.getValue() != null) {
					String encoded = UriComponent.encode(entry.getValue(), UriComponent.Type.QUERY_PARAM_SPACE_ENCODED);
					target = target.queryParam(entry.getKey(), encoded);
				}
			}
		}

		return target
				.request()
				.accept(MediaType.TEXT_PLAIN_TYPE)
				.header("Authorization", "Bearer " + getUPRNToken())
				.get();
	}

	/*private static String getToken(String password, String username, String clientid, String token_endpoint) {
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
			//changed to log via the logging framework, so it's captured in log files
			LOG.error("Error getting Keycloak token for UPRN call", e);
			//e.printStackTrace();
		}

		return token;
	}

	// synchronized
	public static String getUPRNToken(String password, String username, String clientid, Logger LOG, String token_endpoint) {
		if (uprnToken.isEmpty()) {
			LOG.debug("UPRN: refreshing token");
			uprnToken = getToken(password, username, clientid, token_endpoint);
		}
		return uprnToken;
	}*/

	private static String getUPRNToken() throws Exception {
		if (kcClient == null) {

			JsonNode config = getConfig();
			if (config == null) {
				return null;
			}

			String url = config.get("auth_server_url").asText();
			String clientId = config.get("client_id").asText();
			String realm = config.get("realm").asText();
			String password = config.get("password").asText();
			String username = config.get("username").asText();
			kcClient = new KeycloakClient(url, realm, username, password, clientId);
		}

		AccessTokenResponse token = kcClient.getToken();
		return token.getToken();
	}


	public static boolean isActivated(String subscriberConfigName) throws Exception {

		JsonNode config = getConfig();
		if (config == null) {
			return false;
		}

		ArrayNode arrNode = (ArrayNode)config.get("subscribers");
		for (JsonNode objNode : arrNode) {
			String zc = objNode.asText();
			if (zc.equalsIgnoreCase(subscriberConfigName)) {
				return true;
			}
		}
		return false;
	}
}

