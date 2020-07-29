package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.EncodingNotSupportedException;
import ca.uhn.hl7v2.parser.PipeParser;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
        String HL7Message = null;
        /*FileInputStream iS = new FileInputStream("C:\\Users\\USER\\Desktop\\Examples\\A01");
        HL7Message = IOUtils.toString(iS);*/
        //get HL7 message from the table based on id
        Connection connection = ConnectionManager.getHL7v2InboundConnection();
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
        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
        //get HL7 message from the table based on id

        Message hapiMsg = parseHL7Message(HL7Message);
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
        } catch (HL7Exception e) {
            e.printStackTrace();
        }
        return hapiMsg;
    }

}