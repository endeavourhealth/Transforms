package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.ORC;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.DiagnosticReportBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DiagnosticReportTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticReportTransformer.class);

    /**
     *
     * @param pid
     * @param orc
     * @param obr
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    public static void createDiagnosticReport(PID pid, ORC orc, OBR obr, ORU_R01_ORDER_OBSERVATION orderObserv, FhirResourceFiler fhirResourceFiler,
                                              ImperialHL7Helper imperialHL7Helper, MSH msh ) throws Exception {
        DiagnosticReportBuilder diagnosticReportBuilder = new DiagnosticReportBuilder();

        String observationGuid = String.valueOf(obr.getFillerOrderNumber().getEntityIdentifier());

        CX[] patientIdList = pid.getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());

        ImperialHL7Helper.setUniqueId(diagnosticReportBuilder, patientGuid, observationGuid);

        Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
        diagnosticReportBuilder.setPatient(patientReference);

        XCN[] orderingProvider = obr.getOrderingProvider();
        if(orderingProvider != null && orderingProvider.length > 0) {
            ST idNumber = orderingProvider[0].getIDNumber();
            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(idNumber.toString());
            diagnosticReportBuilder.setRecordedBy(practitionerReference);
        }

        //assume that any report already filed into Imperial HL7 is a final report
        diagnosticReportBuilder.setStatus(DiagnosticReport.DiagnosticReportStatus.FINAL);

        String sendingApplication = msh.getSendingApplication().getNamespaceID().getValue();
        if("RYJ_PATH".equalsIgnoreCase(sendingApplication)) {
            List<ORU_R01_OBSERVATION> obserVals = orderObserv.getOBSERVATIONAll();
            for (ORU_R01_OBSERVATION val : obserVals) {
                String uniqueId = obr.getFillerOrderNumber().getEntityIdentifier().getValue()+val.getOBX().getObservationIdentifier().getIdentifier().getValue();
                Reference observationReference = imperialHL7Helper.createObservationReference(uniqueId);
                diagnosticReportBuilder.addResult(observationReference);
            }

        } else {
            String uniqueId = obr.getFillerOrderNumber().getEntityIdentifier().getValue()+orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier().getValue();
            Reference observationReference = imperialHL7Helper.createObservationReference(uniqueId);
            diagnosticReportBuilder.addResult(observationReference);
        }

        String observationDate = String.valueOf(obr.getObservationDateTime().getTimeOfAnEvent());
        if (observationDate != null) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(observationDate.substring(0,4)+"-"+observationDate.substring(4,6)+"-"+observationDate.substring(6,8));

            DateTimeType dateTimeType = new DateTimeType(date);
            diagnosticReportBuilder.setEffectiveDate(dateTimeType);
            diagnosticReportBuilder.setRecordedDate(date);
        }

        // coded concept
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(diagnosticReportBuilder, CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code);

        codeableConceptBuilder.addCoding("http://loinc.org");
        codeableConceptBuilder.setCodingCode(String.valueOf(obr.getUniversalServiceIdentifier().getIdentifier()));
        codeableConceptBuilder.setText(String.valueOf(obr.getUniversalServiceIdentifier().getText()));
        codeableConceptBuilder.setCodingDisplay(String.valueOf(obr.getUniversalServiceIdentifier().getText()));

        CodeableConcept codeableConcept = new CodeableConcept();
        Coding coding = codeableConcept.addCoding();
        coding.setSystem("http://hl7.org/fhir/v2/0074");
        coding.setCode("RAD");
        diagnosticReportBuilder.setCategory(codeableConcept);

        fhirResourceFiler.savePatientResource(null, diagnosticReportBuilder);
    }

}
