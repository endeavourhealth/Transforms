package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.ID;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.OBX;
import ca.uhn.hl7v2.model.v23.segment.ORC;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.DiagnosticReportBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.DiagnosticReport;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.TemporalPrecisionEnum;
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

        String observationGuid = String.valueOf(obr.getFillerOrderNumber());

        CX[] patientIdList = pid.getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());

        ImperialHL7Helper.setUniqueId(diagnosticReportBuilder, patientGuid, observationGuid);

        Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
        diagnosticReportBuilder.setPatient(patientReference);

        XCN[] orderingProvider = obr.getOrderingProvider();
        ST idNumber = orderingProvider[0].getIDNumber();
        Reference practitionerReference = imperialHL7Helper.createPractitionerReference(idNumber.toString());
        diagnosticReportBuilder.setRecordedBy(practitionerReference);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        /*if (deletedCell.getBoolean()) {
            diagnosticReportBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), diagnosticReportBuilder);
            return;
        }*/

        //assume that any report already filed into Imperial HL7 is a final report
        diagnosticReportBuilder.setStatus(DiagnosticReport.DiagnosticReportStatus.FINAL);

        /*ID codeId = obr.getUniversalServiceIdentifier().getIdentifier();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(diagnosticReportBuilder, false, codeId, CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code, csvHelper);*/

        /*ReferenceList childObservations = imperialHL7Helper.getAndRemoveObservationParentRelationships(diagnosticReportBuilder.getResourceId());
        if (childObservations != null) {
            for (int i=0; i<childObservations.size(); i++) {
                Reference reference = childObservations.getReference(i);
                CsvCell[] sourceCells = childObservations.getSourceCells(i);
                diagnosticReportBuilder.addResult(reference, sourceCells);
            }
        }*/

        /*Reference reference = childObservations.getReference(i);
        diag*/
        String uniqueId = String.valueOf(obr.getFillerOrderNumber()) + String.valueOf(orderObserv.getOBSERVATION().getOBX().getObservationIdentifier().getIdentifier());
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

        fhirResourceFiler.savePatientResource(null, diagnosticReportBuilder);
    }

}
