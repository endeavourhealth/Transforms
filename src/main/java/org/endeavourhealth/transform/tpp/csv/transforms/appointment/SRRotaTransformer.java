package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.core.database.dal.publisherCommon.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ScheduleBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.RotaDetailsObject;
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

        CsvCell rotaIdCell = parser.getRowIdentifier();
        scheduleBuilder.setId(rotaIdCell.getString(), rotaIdCell);

        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {

            scheduleBuilder.setDeletedAudit(removedCell);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), scheduleBuilder);
            return;
        }

        CsvCell locationBranchIdCell = parser.getIDBranch();
        if (!TppCsvHelper.isEmptyOrNegative(locationBranchIdCell)) {
            Reference fhirReference = csvHelper.createLocationReference(locationBranchIdCell);
            scheduleBuilder.setLocation(fhirReference, locationBranchIdCell);
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

        //added missing transforms
        CsvCell locationTypeIdCell = parser.getLocation();
        if (!TppCsvHelper.isEmptyOrNegative(locationTypeIdCell)) {

            //the location type links to a configured list item
            TppConfigListOption configuredListItem = TppCsvHelper.lookUpTppConfigListOption(locationTypeIdCell);
            String locationTypeDesc = configuredListItem.getListOptionName();
            scheduleBuilder.setLocationType(locationTypeDesc, locationTypeIdCell);
        }

        CsvCell createdDateCell = parser.getDateCreation();
        if (!createdDateCell.isEmpty()) {
            Date d = createdDateCell.getDateTime();
            scheduleBuilder.setRecordedDate(d, createdDateCell);
        }

        CsvCell profileCreatedByCell = parser.getIDProfileCreatedBy();
        Reference createdByReference = csvHelper.createPractitionerReferenceForProfileId(profileCreatedByCell);
        if (createdByReference != null) {
            scheduleBuilder.setRecordedBy(createdByReference, profileCreatedByCell);
        }

        //SD-281 - the profile owner column is garbage and should be ignored (see below for correct code)
        /*CsvCell profileIdOwnerCell = parser.getIDProfileOwner();
        Reference ownerReference = csvHelper.createPractitionerReferenceForProfileId(profileIdOwnerCell);
        if (ownerReference != null) {
            scheduleBuilder.addActor(ownerReference, profileIdOwnerCell);
        }*/

        //newer versions of SRRota do have the start datetime as a proper column, although if this isn't present,
        //it'll try to calculate the start date from the SRAppointment file data (see below)
        CsvCell trueStartDateCell = parser.getDateStart();
        if (trueStartDateCell != null) { //not present in older versions of the file
            scheduleBuilder.setPlanningHorizonStart(trueStartDateCell.getDateTime(), trueStartDateCell);
        }

        //some of the fields we need aren't available in the SRRota file, so have been pre-cached from SRAppointment (and existing Schedule resources)
        RotaDetailsObject details = csvHelper.getRotaDateAndStaffCache().getCachedDetails(rotaIdCell);
        if (details != null) {

            CsvCell profileClinicianCell = details.getClinicianProfileIdCell();
            if (profileClinicianCell != null) {
                Reference clinicianReference = csvHelper.createPractitionerReferenceForProfileId(profileClinicianCell);
                scheduleBuilder.addActor(clinicianReference, profileClinicianCell);
            }

            //only use the cached start date if our file doesn't have the start date column
            if (trueStartDateCell == null) {
                Date cachedStartDate = details.getStartDate();
                if (cachedStartDate != null) {
                    scheduleBuilder.setPlanningHorizonStart(cachedStartDate);
                }
            }
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), scheduleBuilder);
    }
}
