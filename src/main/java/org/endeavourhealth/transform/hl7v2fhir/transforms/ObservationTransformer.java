package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.PID;
import com.google.common.base.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ObservationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    /**
     *
     * @param pid
     * @param observationBuilder
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @return
     * @throws Exception
     */
    public static ObservationBuilder transformPIDToObservation(PID pid, ObservationBuilder observationBuilder, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper, String msgType) throws Exception {
        CX[] patientIdList = pid.getPatientIDInternalID();
        String id = String.valueOf(patientIdList[0].getID());

        String religion = pid.getReligion().getValue();
        if (!Strings.isNullOrEmpty(religion) && !religion.equalsIgnoreCase("\"\"")) {
            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                    "religion"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                    "religion", religion, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = observationBuilder.createNewCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code,false);
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme()).setDisplay("Religion");
        }
        return observationBuilder;
    }

    /**
     * @param pid
     * @param obr
     * @param orderObserv
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    public static void createObservation(PID pid, MSH msh, OBR obr, ORU_R01_ORDER_OBSERVATION orderObserv, FhirResourceFiler fhirResourceFiler,
                                         ImperialHL7Helper imperialHL7Helper, ORU_R01_OBSERVATION loopVal) throws Exception {

        ObservationBuilder observationBuilder = new ObservationBuilder();
        String uniqueId = obr.getFillerOrderNumber().getEntityIdentifier().getValue()+loopVal.getOBX().getObservationIdentifier().getIdentifier().getValue();
        observationBuilder.setId(uniqueId);

        String sendingApplication = msh.getSendingApplication().getNamespaceID().getValue();
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(observationBuilder);
        if("RYJ_PATH".equalsIgnoreCase(sendingApplication)) {
            identifierBuilder.setSystem("https://fhir.hl7.org.uk/path/id/ryj");
        } else {
            identifierBuilder.setSystem("https://fhir.hl7.org.uk/rad/id/ryj");
        }
        identifierBuilder.setValue(uniqueId);

        //patient reference
        CX[] patientIdList = pid.getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());

        /*String observationGuid = String.valueOf(obr.getFillerOrderNumber());
        ImperialHL7Helper.setUniqueId(observationBuilder, patientGuid, observationGuid);*/

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

        // status is always final as we check the status above
        observationBuilder.setStatus(Observation.ObservationStatus.FINAL);

        // coded concept
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

        // All codes are cerner codes??
        codeableConceptBuilder.addCoding("http://loinc.org");
        codeableConceptBuilder.setCodingCode(String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier()));
        codeableConceptBuilder.setText(String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getText()));
        codeableConceptBuilder.setCodingDisplay(String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getText()));

        if("RYJ_PATH".equalsIgnoreCase(sendingApplication)) {
            Varies[] valQuantityVal = loopVal.getOBX().getObservationValue();
            if (valQuantityVal != null && valQuantityVal.length > 0) {
                observationBuilder.setValueNumber(Double.valueOf(String.valueOf(valQuantityVal[0].getData())));
            }

            CE valQuantityUnit = loopVal.getOBX().getUnits();
            observationBuilder.setValueNumberUnits(String.valueOf(valQuantityUnit.getIdentifier()));

            String unitsDesc = null;
            if(loopVal.getOBX().getAbnormalFlags().length > 0) {
                unitsDesc = String.valueOf(loopVal.getOBX().getAbnormalFlags()[0].getValue());
                if (unitsDesc != null) {
                    observationBuilder.setValueNumberUnits(unitsDesc);
                }
            }

            try {
                String[] referencesRange = (loopVal.getOBX().getReferencesRange().getValue()).split("-");
                Double lowRange = Double.valueOf(referencesRange[0]);
                Double highRange = Double.valueOf(referencesRange[1]);
                if (lowRange != null && highRange != null) {
                    observationBuilder.setRecommendedRangeLow(lowRange, unitsDesc, Quantity.QuantityComparator.GREATER_OR_EQUAL);
                    observationBuilder.setRecommendedRangeHigh(highRange, unitsDesc, Quantity.QuantityComparator.LESS_OR_EQUAL);

                } else if (lowRange != null) {
                    observationBuilder.setRecommendedRangeLow(lowRange, unitsDesc, Quantity.QuantityComparator.GREATER_THAN);

                } else if (highRange != null){
                    observationBuilder.setRecommendedRangeHigh(highRange, unitsDesc, Quantity.QuantityComparator.LESS_THAN);
                }
            }
            catch (NumberFormatException ex) {
                // LOG.warn("Range not set for Clinical Event " + parser.getEventId().getString() + " due to invalid reference range");
                TransformWarnings.log(LOG, imperialHL7Helper, "Range not set for clinical event due to invalid reference range. Id:{}", uniqueId);

            }

        } else {
            StringBuilder obxVal = new StringBuilder();
            String patientDelay = null;

            Varies[] value = loopVal.getOBX().getObservationValue();
            String delayDays = loopVal.getOBX().getUserDefinedAccessChecks().getValue();
            if (patientDelay== null && delayDays != null && date != null) {
                patientDelay = delayDays;
                observationBuilder.addPatientDelayDays(calculateDate(delayDays, date));
            }
            if (value != null && value.length > 0) {
                for (int resultCount = 0; resultCount < value.length; resultCount++) {
                    obxVal.append(value[resultCount].getData() + "\r\n");
                }
            }
            observationBuilder.setValueString(obxVal.toString());
        }

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
