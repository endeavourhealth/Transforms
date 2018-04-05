package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
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
        while (parser.nextRecord()) {

            try {
                createResource((SRRota)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRRota parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        ScheduleBuilder scheduleBuilder = new ScheduleBuilder();

        CsvCell sessionId = parser.getRowIdentifier();
        scheduleBuilder.setId(sessionId.getString(), sessionId);

        CsvCell locationBranchId = parser.getIDBranch();
        if (!locationBranchId.isEmpty()) {
            Reference fhirReference = csvHelper.createLocationReference(locationBranchId);
            scheduleBuilder.setLocation(fhirReference, locationBranchId);
        }

        CsvCell sessionType = parser.getRotaType();
        if (!sessionType.isEmpty()) {
            scheduleBuilder.setTypeFreeText(sessionType.getString(), sessionType);
        }

        CsvCell sessionName = parser.getName();
        if (!sessionName.isEmpty()) {
            //the FHIR description of "Comment" seems approproate to store the category
            scheduleBuilder.addComment(sessionName.getString(), sessionName);
        }

        //not strictly necessary, since we're creating a NEW schedule resource (even if it's a delta), but good practice
        scheduleBuilder.clearActors();

        CsvCell sessionActorStaffProfileId = parser.getIDProfileCreatedBy();
        if (!sessionActorStaffProfileId.isEmpty()) {
            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, sessionActorStaffProfileId.getString());
            scheduleBuilder.addActor(practitionerReference, sessionActorStaffProfileId);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), scheduleBuilder);
    }
}
