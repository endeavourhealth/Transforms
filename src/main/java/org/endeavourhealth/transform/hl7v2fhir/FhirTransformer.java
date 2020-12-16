package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.EncodingNotSupportedException;
import ca.uhn.hl7v2.parser.PipeParser;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(FhirTransformer.class);

    /**
     *
     * @param exchange
     * @param fhirResourceFiler
     * @param version
     * @throws Exception
     */
    public static void transform(Exchange exchange, FhirResourceFiler fhirResourceFiler, String version) throws Exception {
        String HL7Message = exchange.getBody();
        //get HL7 message from the table based on id
        /*Connection connection = ConnectionManager.getHL7v2InboundConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * from imperial where payload_id=?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, String.valueOf(exchange.getId()));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if(resultSet.next()) {
                        HL7Message = resultSet.getString("hl7_message");
                    }
                }
            }
            connection.commit();
        } catch (Exception ex) {
            LOG.error("", ex);
            SlackHelper.sendSlackMessage(SlackHelper.Channel.EnterpriseAgeUpdaterAlerts, "Exception in transform for exchange id (" + exchange.getId() + ")", ex);
        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }*/
        //get HL7 message from the table based on id

        String bodyStr= HL7Message.replace("\r\n","#@#@#@");
        bodyStr= bodyStr.replace("\n","\r\n");
        bodyStr= bodyStr.replace("#@#@#@","\r\n");
        Message hapiMsg = parseHL7Message(bodyStr);
        String msgType = (hapiMsg.printStructure()).substring(0, 3);

        if("ADT".equalsIgnoreCase(msgType)) {
            ImperialHL7FhirADTTransformer.transform(fhirResourceFiler, version, hapiMsg);
        } else if("ORU".equalsIgnoreCase(msgType)) {
            ImperialHL7FhirORUTransformer.transform(fhirResourceFiler, version, hapiMsg);
        }
    }

    /**
     *
     * @param hl7Message
     * @return
     * @throws Exception
     */
    private static Message parseHL7Message(String hl7Message) throws Exception {
        HapiContext context = new DefaultHapiContext();
        context.getParserConfiguration().setValidating(false);

        PipeParser pipeParser = new PipeParser(context) {
            public String getVersion(String message) throws HL7Exception {
                return "2.3";
            }
        };
        Message hapiMsg = null;
        try {
            // The parse method performs the actual parsing
            hapiMsg = pipeParser.parse(hl7Message);
        } catch (EncodingNotSupportedException e) {
            e.printStackTrace();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.EnterpriseAgeUpdaterAlerts, "Exception in parseHL7Message", e);
        } catch (HL7Exception e) {
            e.printStackTrace();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.EnterpriseAgeUpdaterAlerts, "Exception in parseHL7Message", e);
        }
        return hapiMsg;
    }

}
