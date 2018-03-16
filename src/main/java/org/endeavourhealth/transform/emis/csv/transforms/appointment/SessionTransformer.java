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

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Session.class);
        while (parser.nextRecord()) {

            try {
                createResource((Session)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
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
            csvHelper.findSessionPractionersToSave(sessionGuid);

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
        List<CsvCell> userGuidCells = retrieveExistingSessionUsers(sessionGuid, csvHelper, fhirResourceFiler);

        List<CsvCell> newUsersToSave = csvHelper.findSessionPractionersToSave(sessionGuid);
        CsvCell.addAnyMissingByValue(userGuidCells, newUsersToSave);

        List<CsvCell> newUsersToDelete = csvHelper.findSessionPractionersToDelete(sessionGuid);
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
        Schedule fhirScheduleOld = (Schedule)csvHelper.retrieveResource(sessionGuid, ResourceType.Schedule, fhirResourceFiler);
        if (fhirScheduleOld != null) {

            List<Reference> edsReferences = ScheduleHelper.getAllActors(fhirScheduleOld);

            //the existing resource will have been through the mapping process, so we need to reverse-lookup the source EMIS user GUIDs from the EDS UUIDs
            List<Reference> rawReferences = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, edsReferences);

            for (Reference rawReference : rawReferences) {
                String emisUserGuid = ReferenceHelper.getReferenceId(rawReference);

                //our list expects CsvCells, so create a dummy cell with a row audit of -1, which will automatically be ignored
                CsvCell wrapperCell = CsvCell.factoryDummyWrapper(emisUserGuid);
                ret.add(wrapperCell);
            }
        }

        return ret;
    }

    /*private static void createResource(Session parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        Schedule fhirSchedule = new Schedule();
        fhirSchedule.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_SCHEDULE));

        String sessionGuid = parser.getAppointmnetSessionGuid();
        fhirSchedule.setId(sessionGuid);

        //handle deleted sessions
        if (parser.getDeleted()) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), fhirSchedule);
            return;
        }

        String locationGuid = parser.getLocationGuid();
        Reference fhirReference = csvHelper.createLocationReference(locationGuid);
        fhirSchedule.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.SCHEDULE_LOCATION, fhirReference));

        Date start = parser.getStartDateTime();
        Date end = parser.getEndDateTime();
        Period fhirPeriod = PeriodHelper.createPeriod(start, end);
        fhirSchedule.setPlanningHorizon(fhirPeriod);

        String sessionType = parser.getSessionTypeDescription();
        fhirSchedule.addType(CodeableConceptHelper.createCodeableConcept(sessionType));

        String category = parser.getSessionCategoryDisplayName();
        fhirSchedule.setComment(category); //the FHIR description of "Comment" seems approproate to store the category

        String description = parser.getDescription();
        fhirSchedule.setComment(description);

        List<String> userGuids = csvHelper.findSessionPractionersToSave(sessionGuid, true);

        //if we don't have any practitioners in the helper, then this may be a DELTA record from EMIS,
        //and we need to carry over the practitioners from our previous instance
        if (userGuids.isEmpty()) {
            try {
                Schedule fhirScheduleOld = (Schedule)csvHelper.retrieveResource(sessionGuid, ResourceType.Schedule, fhirResourceFiler);

                List<Reference> edsReferences = new ArrayList<>();

                if (fhirScheduleOld.hasActor()) {
                    Reference actorReference = fhirScheduleOld.getActor();
                    edsReferences.add(actorReference);
                }

                if (fhirScheduleOld.hasExtension()) {
                    for (Extension extension: fhirScheduleOld.getExtension()) {
                        if (extension.getUrl().equals(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR)) {
                            Reference oldAdditionalActor = (Reference)extension.getValue();
                            edsReferences.add(oldAdditionalActor);
                        }
                    }
                }

                //then existing resource will have been through the mapping process, so we need to reverse-lookup the source EMIS user GUID from the EDS ID
                List<Reference> rawReferences = IdHelper.convertEdsReferencesToLocallyUniqueReferences(fhirResourceFiler.getServiceId(), edsReferences);
                for (Reference rawReference: rawReferences) {
                    String emisUserGuid = ReferenceHelper.getReferenceId(rawReference);
                    userGuids.add(emisUserGuid);
                }

            } catch (Exception ex) {
                //in production data, there should always be at least one practitioner for each session, but the
                //test data contains at least one session that doesn't, so we can end up here because we're
                //trying to find a previous instance of a resource that never existed before
            }
        }

        //add the user GUIDs to the FHIR resource
        if (!userGuids.isEmpty()) {

            //treat the first reference as the primary actor
            Reference first = ReferenceHelper.createReference(ResourceType.Practitioner, userGuids.get(0));
            fhirSchedule.setActor(first);

            //add any additional references as additional actors
            for (int i=1; i<userGuids.size(); i++) {
                Reference additional = ReferenceHelper.createReference(ResourceType.Practitioner, userGuids.get(i));
                fhirSchedule.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR, additional));
            }
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirSchedule);
    }*/
}
