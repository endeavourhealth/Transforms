package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.ORC;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.DiagnosticReportBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.DiagnosticReport;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

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
                                              ImperialHL7Helper imperialHL7Helper) throws Exception {
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

        String uniqueId = String.valueOf(obr.getFillerOrderNumber().getEntityIdentifier()) + orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier();
        Reference observationReference = imperialHL7Helper.createObservationReference(uniqueId);
        diagnosticReportBuilder.addResult(observationReference);

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

        fhirResourceFiler.savePatientResource(null, diagnosticReportBuilder);
    }

}
