package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.AEATT;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class AEATTTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AEATTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createAandEAttendance((AEATT)parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createAandEAttendance(AEATT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            //if the record is non-active (i.e. deleted) then we don't get any other columns. But we can also expect that our linked
            //ENCNT record will be deleted too, so we don't need to do anything extra here
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPersonId();

        //first we need to check if our encounter has been duplicated, so as to preserve separate FHIR
        //encounters for A&E and Inpatient admission
        boolean isDuplicateEmergencyEncounter;
        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowDuplicateEmergencyEncounterBuilder(encounterIdCell, personIdCell);
        if (encounterBuilder != null) {
            isDuplicateEmergencyEncounter = true;
        } else {
            encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell);
            isDuplicateEmergencyEncounter = false;
        }


        CsvCell decisionToAdmitDateTimeCell = parser.getDecisionToAdmitDateTime();
        CsvCell beginDateCell = parser.getCheckInDateTime();
        CsvCell endDateCell = parser.getCheckOutDateTime();
        CsvCell currentLocationCell = parser.getLastLocCode();
        CsvCell reasonForVisit = parser.getPresentingCompTxt();
        CsvCell triageStartCell = parser.getTriageStartDateTime();
        CsvCell triageEndCell = parser.getTriageCompleteDateTime();
        CsvCell triagePersonIdCell = parser.getTriagePersonId();
        CsvCell hcpFirstAssignedPersonIdCell = parser.getHcpFirstAssignedPersonId();
        CsvCell firstSpecPersonIdCell = parser.getFirstSpecPersonId();
        CsvCell respHcpPersonIdCell = parser.getRespHcpPersonId();
        CsvCell referralPersonIdCell = parser.getReferralPersonId();


        // Encounter start and end
        Date beginDate = null;
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(beginDateCell)) {
            beginDate = BartsCsvHelper.parseDate(beginDateCell);
        }

        Date endDate = null;
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {
            endDate = BartsCsvHelper.parseDate(endDateCell);
        }

        // Triage start and end
        Date triageBeginDate = null;
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(triageStartCell)) {
            triageBeginDate = BartsCsvHelper.parseDate(triageStartCell);
        }

        Date triageEndDate = null;
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(triageEndCell)) {
            triageEndDate = BartsCsvHelper.parseDate(triageEndCell);
        }

        //if it's an A&E attenance, then it's emergency
        encounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        // Using checkin/out date as they largely cover the whole period
        if (beginDate != null) {
            // Start date
            encounterBuilder.setPeriodStart(beginDate);

            // End date
            if (endDate != null) {
                encounterBuilder.setPeriodEnd(endDate);
                encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            } else if (beginDate.before(new Date())) {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, beginDateCell);
            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED, beginDateCell);
            }
        } else {
            encounterBuilder.setStatus(Encounter.EncounterState.PLANNED);
        }

        // Triage person
        if (!BartsCsvHelper.isEmptyOrIsZero(triagePersonIdCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(triagePersonIdCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            Period triagePeriod = new Period();
            triagePeriod.setStart(triageBeginDate);
            if (triageEndDate != null) {
                triagePeriod.setEnd(triageEndDate);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PARTICIPANT, triagePeriod, true, triagePersonIdCell);
        }

        if (!BartsCsvHelper.isEmptyOrIsZero(hcpFirstAssignedPersonIdCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(hcpFirstAssignedPersonIdCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.ATTENDER, true, hcpFirstAssignedPersonIdCell);
        }

        if (!BartsCsvHelper.isEmptyOrIsZero(firstSpecPersonIdCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(firstSpecPersonIdCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, true, firstSpecPersonIdCell);
        }

        if (!BartsCsvHelper.isEmptyOrIsZero(respHcpPersonIdCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(respHcpPersonIdCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.CONSULTANT, true, respHcpPersonIdCell);
        }

        if (!BartsCsvHelper.isEmptyOrIsZero(referralPersonIdCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(referralPersonIdCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.REFERRER, true, referralPersonIdCell);
        }

        // Location
        if (!BartsCsvHelper.isEmptyOrIsZero(currentLocationCell)) {

            Reference locationReference = csvHelper.createLocationReference(currentLocationCell);
            if (encounterBuilder.isIdMapped()) {
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
            }

            encounterBuilder.addLocation(locationReference, true, currentLocationCell);
        }

        //Reason - this is duplicated from ENCNT, but calling addReason(..) ignores duplicates anyway
        if (!reasonForVisit.isEmpty()) {
            encounterBuilder.addReason(reasonForVisit.getString(), reasonForVisit);
        }

        //TODO - process parser.getDischargeDispositionCode()
        //TODO - process parser.getAttendanceDisposalCode()

        // Retrieve or create EpisodeOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(parser);
        if (episodeOfCareBuilder != null) {

            csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, encounterBuilder, fhirResourceFiler);

            // Using checkin/out date as they largely cover the whole period
            if (beginDate != null) {

                if (episodeOfCareBuilder.getRegistrationStartDate() == null || beginDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                    episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                }

                // End date
                if (endDate != null) {

                    if (episodeOfCareBuilder.getRegistrationEndDate() == null || endDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                        episodeOfCareBuilder.setRegistrationEndDate(endDate, endDateCell);
                    }
                }
            } else {
                if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
                }
            }

            // Check whether to Finish EpisodeOfCare
            // If the patient has left AE (checkout-time/enddatetime) and not been admitted (decisionToAdmitDateTime empty) complete EpisodeOfCare
            if (endDate != null
                    && BartsCsvHelper.isEmptyOrIsEndOfTime(decisionToAdmitDateTimeCell)) {

                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED, endDateCell);
            }
        }

        //we don't save immediately, but return the Encounter builder to the cache
        if (isDuplicateEmergencyEncounter) {
            csvHelper.getEncounterCache().returnDuplicateEmergencyEncounterBuilder(encounterIdCell, encounterBuilder);

        } else {
            csvHelper.getEncounterCache().returnEncounterBuilder(encounterIdCell, encounterBuilder);
        }
    }
}



