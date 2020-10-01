package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.model.v23.datatype.CE;
import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.PID;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Observation;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ObservationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    /**
     * @param pid
     * @param obr
     * @param orderObserv
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    public static void createObservation(PID pid, MSH msh, OBR obr, ORU_R01_ORDER_OBSERVATION orderObserv, FhirResourceFiler fhirResourceFiler,
                                         ImperialHL7Helper imperialHL7Helper) throws Exception {

        ObservationBuilder observationBuilder = new ObservationBuilder();
        String uniqueId = String.valueOf(obr.getFillerOrderNumber().getEntityIdentifier()) + orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier();
        observationBuilder.setId(uniqueId);

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(observationBuilder);
        identifierBuilder.setSystem("https://fhir.hl7.org.uk/rad/id/ryj");
        identifierBuilder.setValue(uniqueId);

        //patient reference
        CX[] patientIdList = pid.getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());

        String observationGuid = String.valueOf(obr.getFillerOrderNumber());
        ImperialHL7Helper.setUniqueId(observationBuilder, patientGuid, observationGuid);

        Reference patientReference = ImperialHL7Helper.createPatientReference(patientGuid);
        observationBuilder.setPatient(patientReference);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String observationDate = String.valueOf(orderObserv.getOBSERVATION().getOBX().getDateTimeOfTheObservation().getTimeOfAnEvent());
        Date date = null;
        if (observationDate != null) {
            date = formatter.parse(observationDate.substring(0, 4) + "-" + observationDate.substring(4, 6) + "-" + observationDate.substring(6, 8));

            DateTimeType eventPerformedDateTime = new DateTimeType(date);
            observationBuilder.setEffectiveDate(eventPerformedDateTime);
        }

        String issued = String.valueOf(msh.getDateTimeOfMessage().getTimeOfAnEvent());
        Date issuedDate = null;
        if (issued != null) {
            issuedDate = formatter.parse(issued.substring(0, 4) + "-" + issued.substring(4, 6) + "-" + issued.substring(6, 8));
            observationBuilder.setIssued(issuedDate);
        }

        StringBuilder obxVal = new StringBuilder();
        List<ORU_R01_OBSERVATION> obserVals = orderObserv.getOBSERVATIONAll();
        String patientDelay = null;
        for (ORU_R01_OBSERVATION val : obserVals) {
            Varies[] value = val.getOBX().getObservationValue();
            String delayDays = val.getOBX().getUserDefinedAccessChecks().getValue();
            if (patientDelay== null && delayDays != null && date != null) {
                patientDelay = delayDays;
                observationBuilder.addPatientDelayDays(calculateDate(delayDays, date));
            }
            if (value != null && value.length > 0) {
                for (int resultCount = 0; resultCount < value.length; resultCount++) {
                    obxVal.append(value[resultCount].getData() + "\r\n");
                }
            }

            String sendingApplication = msh.getSendingApplication().getNamespaceID().getValue();
            if("RYJ_PATH".equalsIgnoreCase(sendingApplication)) {
                Varies[] valQuantityVal = val.getOBX().getObservationValue();
                if (valQuantityVal != null && valQuantityVal.length > 0) {
                    for (int count = 0; count < value.length; count++) {
                        observationBuilder.setValueNumber(Double.valueOf(String.valueOf(valQuantityVal[count].getData())));
                    }
                }

                CE valQuantityUnit = val.getOBX().getUnits();
                observationBuilder.setValueNumberUnits(String.valueOf(valQuantityUnit.getIdentifier()));
                fhirResourceFiler.savePatientResource(null, observationBuilder);
            }
        }
        observationBuilder.setValueString(obxVal.toString());

        // status is always final as we check the status above
        observationBuilder.setStatus(Observation.ObservationStatus.FINAL);

        // coded concept
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

        // All codes are cerner codes??
        codeableConceptBuilder.addCoding("http://loinc.org");
        codeableConceptBuilder.setCodingCode(String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier()));
        codeableConceptBuilder.setText(String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getText()));
        codeableConceptBuilder.setCodingDisplay(String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getText()));

        //save resource
        fhirResourceFiler.savePatientResource(null, observationBuilder);
    }

    private static Date calculateDate(String delayDays, Date observationDate) {
        try {
        JsonParser parser = new JsonParser();
        JsonElement jsonTree = parser.parse(delayDays);
        JsonObject jsonObject = jsonTree.getAsJsonObject();
        String value = jsonObject.get("patientDelay").toString();
        int days = 0;
        if (value.length() > 2) {
            days = Integer.parseInt(value.substring(1, 3));
        }
        Calendar c = Calendar.getInstance();
        c.setTime(observationDate);
        c.add(Calendar.DATE, days);
        return c.getTime(); }
        catch (Exception e) {
            LOG.error("Problem in parsing ObservationDate :" + observationDate + " delay days:" + delayDays);
            return null;
        }
    }


}
