package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.OPATT;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
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

public class OPATTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OPATTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    createOutpatientAttendanceEvent((OPATT)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createOutpatientAttendanceEvent(OPATT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            //if the record is non-active (i.e. deleted) then we don't get any other columns. But we can also expect that our linked
            //ENCNT record will be deleted too, so we don't need to do anything extra here
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPersonId();

        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell, csvHelper);

        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

        Date beginDate = null;
        Date endDate = null;

        CsvCell beginDateCell = parser.getAppointmentDateTime();
        CsvCell apptLengthCell = parser.getExpectedAppointmentDuration();

        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(beginDateCell)) {
            beginDate = BartsCsvHelper.parseDate(beginDateCell);

            encounterBuilder.setPeriodStart(beginDate, beginDateCell);

            //there's no explicit end date, but we can work it out from the duration
            if (!apptLengthCell.isEmpty()) {
                endDate = new Date(beginDate.getTime() + (apptLengthCell.getInt() * 60 * 1000));

                encounterBuilder.setPeriodEnd(endDate, beginDateCell, apptLengthCell);
            }
        }


        //work out the Encounter status
        CsvCell outcomeCell = parser.getAttendanceOutcomeCode();
        if (!outcomeCell.isEmpty()) {
            //if we have an outcome, we know the encounter has ended
            encounterBuilder.setStatus(Encounter.EncounterState.FINISHED, outcomeCell);

        } else {
            //if we don't have an outcome, the Encounter is either in progress or in the future
            if (beginDate == null
                || beginDate.after(new Date())) {

                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED, beginDateCell);

            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, beginDateCell);
            }
        }


        // Location
        CsvCell currentLocationCell = parser.getLocationCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(currentLocationCell)) {

            Reference locationReference = csvHelper.createLocationReference(currentLocationCell);
            if (encounterBuilder.isIdMapped()) {
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
            }

            if (beginDate == null || endDate == null) {
                encounterBuilder.addLocation(locationReference, true, currentLocationCell);
            } else {
                Period apptPeriod = PeriodHelper.createPeriod(beginDate, endDate);
                Encounter.EncounterLocationComponent elc = new Encounter.EncounterLocationComponent();
                elc.setPeriod(apptPeriod);
                elc.setLocation(locationReference);
                encounterBuilder.addLocation(locationReference, true, currentLocationCell, beginDateCell, apptLengthCell);
            }
        }

        CsvCell reasonCell = parser.getReasonForVisitText();
        if (!reasonCell.isEmpty()) {
            String reason = reasonCell.getString();
            encounterBuilder.addReason(reason, reasonCell);
        }

        CsvCell typeCell = parser.getAppointmentTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(typeCell)) {

            //set the OPATT type at the consultation source. The content of this (e.g. Cardiology DeFib F/Up) is equivalent to the Emis consultation
            //source (e.g. asthma review)
            BartsCodeableConceptHelper.applyCodeDisplayTxt(typeCell, CodeValueSet.APPOINTMENT_TYPE, encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source, csvHelper);

            /*CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.APPOINTMENT_TYPE, typeCell);
            if (codeRef != null) {
                String typeDesc = codeRef.getCodeDispTxt();
                encounterBuilder.addType(typeDesc, typeCell);
            }*/
        }

        CsvCell createdByPersonnelIdCell = parser.getEncounterCreatedByPersonnelId();
        if (!BartsCsvHelper.isEmptyOrIsZero(createdByPersonnelIdCell)) {
            Reference practitionerReference = csvHelper.createPractitionerReference(createdByPersonnelIdCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }
            encounterBuilder.setRecordedBy(practitionerReference);
        }

        //EpisodOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(parser, csvHelper);
        if (episodeOfCareBuilder != null) {

            csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, encounterBuilder, fhirResourceFiler);

            //we may have missed the original referral, so our episode of care may have the wrong start date, so adjust that now
            if (beginDate != null) {
                if (episodeOfCareBuilder.getRegistrationStartDate() == null
                        || beginDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {

                    episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                }
            }

            // Check whether to Finish EpisodeOfCare
            //outcome corresponds to NHS Data Dictionary: https://www.datadictionary.nhs.uk/data_dictionary/attributes/o/out/outcome_of_attendance_de.asp?shownav=1
            // Outcome = 1 means discharged from care
            if (!outcomeCell.isEmpty()) {
                int outcomeCode = outcomeCell.getInt();
                if (outcomeCode == 1) { //	Discharged from CONSULTANT's care (last attendance)

                    //make sure to set the status AFTER setting the end date, as setting the end date
                    //will auto-calculate the status and we want to just overwrite that because we KNOW the episode is ended
                    episodeOfCareBuilder.setRegistrationEndDate(endDate, beginDateCell, apptLengthCell);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED, outcomeCell);
                }
            }
        }

        //we don't save immediately, but return the Encounter builder to the cache
        csvHelper.getEncounterCache().returnEncounterBuilder(encounterIdCell, encounterBuilder);
    }
}
