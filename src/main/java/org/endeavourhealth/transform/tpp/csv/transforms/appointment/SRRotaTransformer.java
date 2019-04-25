package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ScheduleBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRRota;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class SRRotaTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRota.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRRota) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRRota parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        ScheduleBuilder scheduleBuilder = new ScheduleBuilder();

        CsvCell sessionId = parser.getRowIdentifier();
        scheduleBuilder.setId(sessionId.getString(), sessionId);

        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {

            scheduleBuilder.setDeletedAudit(removedCell);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), scheduleBuilder);
            return;
        }

        CsvCell locationBranchId = parser.getIDBranch();
        if (!locationBranchId.isEmpty() && locationBranchId.getLong() > 0) {
            Reference fhirReference = csvHelper.createLocationReference(locationBranchId);
            scheduleBuilder.setLocation(fhirReference, locationBranchId);
        }

        CsvCell sessionType = parser.getRotaType();
        if (!sessionType.isEmpty()) {
            scheduleBuilder.setTypeFreeText(sessionType.getString(), sessionType);
        }

        CsvCell sessionName = parser.getName();
        if (!sessionName.isEmpty()) {
            //the FHIR description of "Comment" seems appropriate to store the category
            //scheduleBuilder.addComment(sessionName.getString(), sessionName);
            scheduleBuilder.setScheduleName(sessionName.getString(), sessionName);
        }

        CsvCell profileIdOwner = parser.getIDProfileOwner();
        if (!profileIdOwner.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReferenceForProfileId(profileIdOwner);
            scheduleBuilder.addActor(practitionerReference, profileIdOwner);
        }

        //added missing transforms
        CsvCell locationTypeIdCell = parser.getLocation();
        if (!locationTypeIdCell.isEmpty()
                && locationTypeIdCell.getLong() > 0) {

            //the location type links to a configured list item
            TppConfigListOption configuredListItem = csvHelper.lookUpTppConfigListOption(locationTypeIdCell, parser);
            String locationTypeDesc = configuredListItem.getListOptionName();
            scheduleBuilder.setLocationType(locationTypeDesc, locationTypeIdCell);
        }

        CsvCell createdDateCell = parser.getDateCreation();
        if (!createdDateCell.isEmpty()) {
            Date d = createdDateCell.getDateTime();
            scheduleBuilder.setRecordedDate(d, createdDateCell);
        }

        CsvCell createdByCell = parser.getIDProfileCreatedBy();
        if (!createdByCell.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReferenceForProfileId(profileIdOwner);
            scheduleBuilder.setRecordedBy(practitionerReference, profileIdOwner);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), scheduleBuilder);
    }
}
