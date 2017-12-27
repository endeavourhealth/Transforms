package org.endeavourhealth.transform.emis.csv.transforms.appointment;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Session;
import org.hl7.fhir.instance.model.*;

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

        Schedule fhirSchedule = new Schedule();
        fhirSchedule.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_SCHEDULE));

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
    }
}
