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
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        if (dataUpdateStatusCell.getString().equalsIgnoreCase("Deleted")) {

            flagBuilder.setDeletedAudit(dataUpdateStatusCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), flagBuilder);
            return;
        }

        CsvCell startDateTimeCell = parser.getStartDttm();
        if (!startDateTimeCell.isEmpty()) {

            flagBuilder.setStartDate(startDateTimeCell.getDateTime(), startDateTimeCell);
        }

        CsvCell appliedDateTimeCell = parser.getAppliedDttm();
        if (!appliedDateTimeCell.isEmpty()) {

            flagBuilder.setRecordedDate(appliedDateTimeCell.getDateTime(), appliedDateTimeCell);
        }

        //build up the flag text from what text is available in the alert record
        StringBuilder flagTextBuilder = new StringBuilder();

        //also use the end date as the active indicator
        CsvCell endDateTimeCell = parser.getClosedDttm();
        CsvCell closedNoteCell = parser.getClosedNote();
        if (!endDateTimeCell.isEmpty()) {

            flagBuilder.setEndDate(endDateTimeCell.getDateTime(), endDateTimeCell);
            flagBuilder.setStatus(Flag.FlagStatus.INACTIVE);

            if (!closedNoteCell.isEmpty()) {
                flagTextBuilder.append("Closed note: ").append(closedNoteCell.getString()).append(". ");
            }

        } else {

            flagBuilder.setStatus(Flag.FlagStatus.ACTIVE);
        }

        CsvCell alertTypeDescCell = parser.getAlertTypeDescription();
        if (!alertTypeDescCell.isEmpty()) {

            flagBuilder.setCategory(alertTypeDescCell.getString(), alertTypeDescCell);
        }

        CsvCell alertCommentsCell = parser.getAlertComment();
        if (!alertCommentsCell.isEmpty()) {

            flagTextBuilder.append("Comments: ").append(alertCommentsCell.getString()).append(". ");
        }

        flagBuilder.setCode(flagTextBuilder.toString().trim(), closedNoteCell, alertCommentsCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);
    }
}
