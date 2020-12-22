package org.endeavourhealth.transform.emis.csv.transforms.appointment;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.ScheduleHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ScheduleBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Session;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.Schedule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SessionTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Session.class);
        while (parser != null && parser.nextRecord()) {

            try {
                createResource((Session)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(Session parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        ScheduleBuilder scheduleBuilder = new ScheduleBuilder();

        CsvCell sessionGuid = parser.getAppointmnetSessionGuid();
        scheduleBuilder.setId(sessionGuid.getString(), sessionGuid);

        //handle deleted sessions
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {

            //we will also have received a "delete" for all the linked users to this session, which we don't
            //need to process because we're deleting the resource itself, so just get them passing in true to stop them being processed
            csvHelper.findSessionPractitionersToSave(sessionGuid);

            scheduleBuilder.setDeletedAudit(deleted);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), scheduleBuilder);
            return;
        }

        CsvCell locationGuid = parser.getLocationGuid();
        if (!locationGuid.isEmpty()) {
            Reference fhirReference = csvHelper.createLocationReference(locationGuid);
            scheduleBuilder.setLocation(fhirReference, locationGuid);
        }

        CsvCell startDate = parser.getStartDate();
        CsvCell startTime = parser.getStartTime();
        Date startDateTime = CsvCell.getDateTimeFromTwoCells(startDate, startTime);
        if (startDateTime != null) {
            scheduleBuilder.setPlanningHorizonStart(startDateTime, startDate, startTime);
        }

        CsvCell endDate = parser.getEndDate();
        CsvCell endTime = parser.getEndTime();
        Date endDateTime = CsvCell.getDateTimeFromTwoCells(endDate, endTime);
        if (endDateTime != null) {
            scheduleBuilder.setPlanningHorizonEnd(endDateTime, endDate, endTime);
        }

        CsvCell sessionType = parser.getSessionTypeDescription();
        if (!sessionType.isEmpty()) {
            scheduleBuilder.setTypeFreeText(sessionType.getString(), sessionType);
        }

        CsvCell category = parser.getSessionCategoryDisplayName();
        if (!category.isEmpty()) {
            //the FHIR description of "Comment" seems approproate to store the category
            scheduleBuilder.addComment(category.getString(), category);
        }

        CsvCell description = parser.getDescription();
        if (!description.isEmpty()) {
            scheduleBuilder.addComment(description.getString(), description);
        }

        //if just the session has changed, so won't receive the session_user records again, so we need
        //to retrieve the existing instance of the schedule from the DB and carry over the users from that instance into our new one
        List<CsvCell> newUsersToSave = csvHelper.findSessionPractitionersToSave(sessionGuid);
        List<CsvCell> newUsersToDelete = csvHelper.findSessionPractitionersToDelete(sessionGuid);
        List<CsvCell> userGuidCells = retrieveExistingSessionUsers(sessionGuid, csvHelper, fhirResourceFiler);

        CsvCell.addAnyMissingByValue(userGuidCells, newUsersToSave);
        CsvCell.removeAnyByValue(userGuidCells, newUsersToDelete);

        //not strictly necessary, since we're creating a NEW schedule resource (even if it's a delta), but good practice
        scheduleBuilder.clearActors();

        //apply the users to the FHIR resource
        for (CsvCell userGuidCell : userGuidCells) {
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, userGuidCell.getString());
            scheduleBuilder.addActor(practitionerReference, userGuidCell);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), scheduleBuilder);
    }

    private static List<CsvCell> retrieveExistingSessionUsers(CsvCell sessionGuidCell, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        List<CsvCell> ret = new ArrayList<>();

        String sessionGuid = sessionGuidCell.getString();
        Schedule fhirScheduleOld = (Schedule)csvHelper.retrieveResource(sessionGuid, ResourceType.Schedule);
        if (fhirScheduleOld != null) {

            List<Reference> edsReferences = ScheduleHelper.getAllActors(fhirScheduleOld);

            //due to past oddness in this transform, we have existing Schedules with the practitioner references duplicated,
            //which causes the below function to throw an exception, since it doesn't handle duplicates. So filter out any duplicates first.
            ReferenceHelper.removeDuplicates(edsReferences);

            //the existing resource will have been through the mapping process, so we need to reverse-lookup the source EMIS user GUIDs from the EDS UUIDs
            List<Reference> rawReferences = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, edsReferences);

            for (Reference rawReference : rawReferences) {
                String emisUserGuid = ReferenceHelper.getReferenceId(rawReference);

                //our list expects CsvCells, so create a dummy cell with a row audit of -1, which will automatically be ignored
                CsvCell wrapperCell = CsvCell.factoryDummyWrapper(emisUserGuid);
                ret.add(wrapperCell);

                //SD-284 - when appointments are booked into pre-exisitng sessions, we get an update to the session
                //but don't get the session user data again. To ensure that the practitioner is set on the appointment properly
                //we need to cache the existing session practitioners here.
                csvHelper.cacheSessionPractitionerMap(sessionGuidCell, wrapperCell, false);
            }
        }

        return ret;
    }

}
