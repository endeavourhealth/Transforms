package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ScheduleBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRRota;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

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

        CsvCell locationBranchId = parser.getIDBranch();
        if (!locationBranchId.isEmpty() && locationBranchId.getLong() > 0) {
            Reference fhirReference = csvHelper.createLocationReference(locationBranchId);
            if (scheduleBuilder.isIdMapped()) {
                fhirReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(fhirReference,fhirResourceFiler);
            }
            scheduleBuilder.setLocation(fhirReference, locationBranchId);
        }

        CsvCell sessionType = parser.getRotaType();
        if (!sessionType.isEmpty()) {
            scheduleBuilder.setTypeFreeText(sessionType.getString(), sessionType);
        }

        CsvCell sessionName = parser.getName();
        if (!sessionName.isEmpty()) {
            //the FHIR description of "Comment" seems appropriate to store the category
            scheduleBuilder.addComment(sessionName.getString(), sessionName);
        }

        //not strictly necessary, since we're creating a NEW schedule resource (even if it's a delta), but good practice
        scheduleBuilder.clearActors();

        CsvCell sessionActorStaffProfileId = parser.getIDProfileOwner();
        if (!sessionActorStaffProfileId.isEmpty() && sessionActorStaffProfileId.getString() != "-1") {

            String staffMemberId = csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                    sessionActorStaffProfileId.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference practitionerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, staffMemberId);
                if (scheduleBuilder.isIdMapped()) {
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference,fhirResourceFiler);
                }
                scheduleBuilder.addActor(practitionerReference, sessionActorStaffProfileId);
            }
        }
        boolean mapIds = !scheduleBuilder.isIdMapped();
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), mapIds, scheduleBuilder);
    }
}
