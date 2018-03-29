package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRVisit;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;

import java.util.Map;

public class SRVisitTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRVisit.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRVisit)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRVisit parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell visitId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        TppCsvHelper.setUniqueId(encounterBuilder, patientId, visitId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        encounterBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            encounterBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell visitDate = parser.getDateBooked();
        encounterBuilder.setPeriodStart(visitDate.getDate(), visitDate);

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {
            //TODO:  this links to SRStaffMemberProfile -> how get staff reference?
            //Reference staffReference = csvHelper.createPractitionerReference("TODO");
            //encounterBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell visitStaffAssigned = parser.getIDProfileAssigned();
        if (!visitStaffAssigned.isEmpty()) {
            //TODO:  this links to SRStaffMemberProfile -> how get staff reference?
            //Reference staffReference = csvHelper.createPractitionerReference("TODO");
            //encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PRIMARY_PERFORMER, visitStaffAssigned);
        }

        CsvCell visitStatus = parser.getCurrentStatus();
        if (!visitStatus.isEmpty()) {
            if (visitStatus.getString().equalsIgnoreCase("cancelled")) {
                encounterBuilder.setStatus(Encounter.EncounterState.CANCELLED);
            } else if (visitStatus.getString().equalsIgnoreCase("deferred")) {
                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED);
            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }

        CsvCell visitDuration = parser.getDuration();
        if (!visitDuration.isEmpty()) {
            encounterBuilder.setDuration(QuantityHelper.createDuration(Integer.valueOf(visitDuration.getInt()), "minutes"));
        }

        CsvCell visitOrg = parser.getIDOrganisation();
        if (!visitOrg.isEmpty()) {
            Reference orgReference = csvHelper.createOrganisationReference(visitOrg);
            encounterBuilder.setServiceProvider(orgReference, visitOrg);
        }

        CsvCell followUpDetails = parser.getFollowUpDetails();
        if (!followUpDetails.isEmpty()) {
            //TODO - where store follow up text?
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }
}
