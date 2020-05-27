package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Alerts;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.FlagBuilder;
import org.hl7.fhir.instance.model.Flag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AlertsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AlertsTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Alerts.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((Alerts) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(Alerts parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        FlagBuilder flagBuilder = new FlagBuilder();
        CsvCell alertIdCell = parser.getId();
        CsvCell patientIdCell = parser.getPasId();

        flagBuilder.setId(alertIdCell.getString(), alertIdCell);
        flagBuilder.setSubject(csvHelper.createPatientReference(patientIdCell), patientIdCell);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell actionCell = parser.getLinestatus();
        if (actionCell.getString().equalsIgnoreCase("Delete")) {
            flagBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), flagBuilder);
            return;
        }

        CsvCell startDateTimeCell = parser.getStartDttm();
        if (!startDateTimeCell.isEmpty()) {
            flagBuilder.setStartDate(startDateTimeCell.getDate(), startDateTimeCell);
        }

        //also use the end date as the active indicator
        CsvCell endDateTimeCell = parser.getEndDttm();
        if (!endDateTimeCell.isEmpty()) {

            flagBuilder.setEndDate(endDateTimeCell.getDate(), endDateTimeCell);
            flagBuilder.setStatus(Flag.FlagStatus.INACTIVE);
        } else {

            flagBuilder.setStatus(Flag.FlagStatus.ACTIVE);
        }

        CsvCell alertTypeDescCell = parser.getAlertTypeDescription();
        if (!alertTypeDescCell.isEmpty()) {

            flagBuilder.setCategory(alertTypeDescCell.getString(), alertTypeDescCell);
        }

        //build up the flag text from whats available in the alert
        StringBuilder flagTextBuilder = new StringBuilder();

        CsvCell alertDescriptionCell = parser.getAlertDescription();
        if (!alertDescriptionCell.isEmpty()) {

            flagTextBuilder.append("Description: ").append(alertDescriptionCell.getString()).append(". ");
        }
        CsvCell alertRiskLevelCell = parser.getRiskLevel();
        if (!alertRiskLevelCell.isEmpty()) {

            flagTextBuilder.append("Risk level: ").append(alertRiskLevelCell.getString()).append(". ");
        }
        CsvCell alertCommentsCell = parser.getAlertComments();
        if (!alertCommentsCell.isEmpty()) {

            flagTextBuilder.append("Comments: ").append(alertCommentsCell.getString()).append(". ");
        }

        flagBuilder.setCode(flagTextBuilder.toString().trim(), alertDescriptionCell, alertRiskLevelCell, alertCommentsCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);
    }
}
