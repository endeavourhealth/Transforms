package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.OBX;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Observation;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.TemporalPrecisionEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ObservationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    /**
     *
     * @param pid
     * @param obr
     * @param orderObserv
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    public static void createObservation(PID pid, OBR obr, ORU_R01_ORDER_OBSERVATION orderObserv, FhirResourceFiler fhirResourceFiler,
                                         ImperialHL7Helper imperialHL7Helper) throws Exception {

        ObservationBuilder observationBuilder = new ObservationBuilder();
        String uniqueId = String.valueOf(obr.getFillerOrderNumber()) + String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier());
        observationBuilder.setId(uniqueId);

        //patient reference
        CX[] patientIdList = pid.getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());

        String observationGuid = String.valueOf(obr.getFillerOrderNumber());
        ImperialHL7Helper.setUniqueId(observationBuilder, patientGuid, observationGuid);

        Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
        observationBuilder.setPatient(patientReference);

        String observationDate = String.valueOf(orderObserv.getOBSERVATION().getOBX().getDateTimeOfTheObservation().getTimeOfAnEvent());
        if (observationDate != null) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(observationDate.substring(0,4)+"-"+observationDate.substring(4,6)+"-"+observationDate.substring(6,8));

            DateTimeType eventPerformedDateTime = new DateTimeType(date, TemporalPrecisionEnum.YEAR);
            observationBuilder.setEffectiveDate(eventPerformedDateTime);
        }

        StringBuilder obxVal = new StringBuilder();
        List<ORU_R01_OBSERVATION> obserVals = orderObserv.getOBSERVATIONAll();
        for(ORU_R01_OBSERVATION val : obserVals) {
            Varies[] value = val.getOBX().getObservationValue();
            if(value != null && value.length > 0) {
                obxVal.append(val.getOBX().getObservationValue()[0].getData()+"\r\n");
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

}
