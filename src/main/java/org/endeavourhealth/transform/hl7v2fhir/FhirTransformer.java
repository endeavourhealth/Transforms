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
        String sourceSystem= exchange.getHeader("SenderLocalIdentifier");

        Message hapiMsg = parseHL7Message(HL7Message);
        if(hapiMsg != null) {
            String msgType = (hapiMsg.printStructure()).substring(0, 3);

            if ("ADT".equalsIgnoreCase(msgType)) {
                AcuteHL7FhirADTTransformer.transform(fhirResourceFiler, sourceSystem, hapiMsg);
            } else if ("ORU".equalsIgnoreCase(msgType)) {
                LOG.info("Sending slack alert for : ORU message  received");
                SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "ORU message  received.");
              //  AcuteHL7FhirORUTransformer.transform(fhirResourceFiler, version, hapiMsg);
            } else {
                LOG.info("Sending slack alert for : Unknown message type received");
                SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Unknown message type received.");
            }
        }
        else{
            LOG.info("Sending slack alert for : Unknown message format received");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Unknown message format received.");
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
            SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Exception in parseHL7Message", e);
        } catch (HL7Exception e) {
            e.printStackTrace();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Exception in parseHL7Message", e);
        }
        return hapiMsg;
    }

}
