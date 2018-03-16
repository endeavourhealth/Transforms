package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.AEATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class AEATTTransformer extends BartsBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AEATTTransformer.class);
    private static InternalIdDalI internalIdDAL = null;
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");


    /*
     *
     */
    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry((AEATT)parser);
                if (valStr == null) {
                    createAandEAttendance((AEATT) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    TransformWarnings.log(LOG, parser, "Validation error: {}", valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(AEATT parser) {
        return null;
    }


    /*
     *
     */
    public static void createAandEAttendance(AEATT parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell beginDateCell = parser.getCheckInDateTime();
        CsvCell endDateCell = parser.getCheckOutDateTime();
        CsvCell currentLocationCell = parser.getLastLocCode();
        CsvCell reasonForVisit = parser.getPresentingCompTxt();

        Date beginDate = null;
        try {
            beginDate = formatDaily.parse(beginDateCell.getString());
        } catch (ParseException ex) {
            beginDate = formatBulk.parse(beginDateCell.getString());
        }
        Date endDate = null;
        if (endDateCell != null && !endDateCell.isEmpty()) {
            try {
                endDate = formatDaily.parse(endDateCell.getString());
            } catch (ParseException ex) {
                endDate = formatBulk.parse(endDateCell.getString());
            }
        }

        if (personIdCell != null) {
            LOG.debug("Current line " + parser.getCurrentLineNumber() + " personId is " + personIdCell.getString());
        } else {
            LOG.debug("Current line " + parser.getCurrentLineNumber() + " personId is null");
        }

        // Patient
        boolean changeOfPatient = false;
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping A&E attendance {} because no Person->MRN mapping {} could be found in file {}", encounterIdCell.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }
        LOG.debug("person UUID is " + patientUuid.toString());

        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());
        if (encounterBuilder == null
                && !activeCell.getIntAsBoolean()) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }

        // Retrieve or create EpisodeOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(null, null, parser.getEncounterId(),
                personIdCell, parser.getArrivalDateTime(), csvHelper, fhirResourceFiler, internalIdDAL);

        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
            identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);

        } else {
            // Delete existing encounter ?
            if (!activeCell.getIntAsBoolean()) {
                encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                //LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
                //fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
                EncounterResourceCache.deleteEncounterBuilder(encounterBuilder);
                return;
            }
        }
        encounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        // Using checkin/out date as they largely cover the whole period
        encounterBuilder.setPeriodStart(beginDate, parser.getCheckInDateTime());
        if (episodeOfCareBuilder.getRegistrationStartDate() == null || episodeOfCareBuilder.getRegistrationStartDate().after(beginDate)) {
            episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
        }

        if (endDate != null) {
            encounterBuilder.setPeriodEnd(endDate, parser.getCheckOutDateTime());
            if (episodeOfCareBuilder.getRegistrationEndDate() == null || episodeOfCareBuilder.getRegistrationEndDate().before(endDate)) {
                episodeOfCareBuilder.setRegistrationEndDate(endDate, endDateCell);
            }
        }

        // Has patient reference changed?
        String currentPatientUuid = ReferenceHelper.getReferenceId(encounterBuilder.getPatient());
        LOG.debug("currentPatientUuid is " + currentPatientUuid);
        if (currentPatientUuid!= null && currentPatientUuid.compareToIgnoreCase(patientUuid.toString()) != 0) {
            // As of 2018-03-02 we dont appear to get any further ENCNT entries for teh minor encounter in A35 and hence the EoC reference on an encounter cannot change
            // Patient reference on Encounter resources is handled below
            // Patient reference on EpisodeOfCare resources is handled below
            changeOfPatient = true;

            List<Resource> resourceList = csvHelper.retrieveResourceByPatient(UUID.fromString(currentPatientUuid));
            for (Resource resource : resourceList) {
                if (resource instanceof Condition) {
                    ConditionBuilder builder = new ConditionBuilder((Condition) resource);
                    builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                } else if (resource instanceof Procedure) {
                    ProcedureBuilder builder = new ProcedureBuilder((Procedure) resource);
                    builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                } else if (resource instanceof Observation) {
                    ObservationBuilder builder = new ObservationBuilder((Observation) resource);
                    builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                }
            }
        }

        //We have a number of potential events in the patient journey through A&E. We can't map all the states.
        /*
        Encounter.EncounterState encState = null;
        // Triage
        if (parser.getTriageStartDateTime().isEmpty()) {
                encState = Encounter.EncounterState.PLANNED;
            } else
        {
            if (parser.getTriageCompleteDateTime().isEmpty()) {
                encState = Encounter.EncounterState.INPROGRESS;
            } else {
                encState = Encounter.EncounterState.FINISHED;
            }
        }
        //Save triage status
        CsvCell triageStart = parser.getTriageStartDateTime();
        CsvCell triageEnd = parser.getTriageCompleteDateTime();

        encounterBuilder.setStatus(encState, triageStart.getDate(),
                triageEnd.getDate(), parser.getTriageCatNbr());
        Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, parser.getTriagePersonId().toString());
        Period triagePeriod = new Period();
        triagePeriod.setStart(triageStart.getDate());
        triagePeriod.setEnd(triageEnd.getDate());
        encounterBuilder.addParticipant(ref,EncounterParticipantType.PARTICIPANT,triagePeriod, parser.getTriageCatNbr());
        encState = null; // reset
        //  First medical assessment to conclusion
        CsvCell startDate  = parser.getFirstAssessDateTime();
        CsvCell conclusionDate =parser.getConclusionDateTime();
        if (startDate.isEmpty()) {
            encState = Encounter.EncounterState.PLANNED;
            } else
         {  // All the records I looked at had Conclusion date set. It should at least allow us to keep some kind of progress.
            if (conclusionDate.isEmpty()) {
                encState = Encounter.EncounterState.INPROGRESS;
            } else {
                encState = Encounter.EncounterState.FINISHED;
            }
        }
        encounterBuilder.setStatus(encState, startDate.getDate(), conclusionDate.getDate(), parser.getCdsBatchContentId());
        */

        // Location
        if (currentLocationCell != null && !currentLocationCell.isEmpty() && currentLocationCell.getLong() > 0) {
            UUID locationResourceUUID = csvHelper.lookupLocationUUID(currentLocationCell.getString(), fhirResourceFiler, parser);
            if (locationResourceUUID != null) {
                encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), currentLocationCell);
            } else {
                TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in AEATT record {} in file {}", currentLocationCell.getString(), parser.getCdsBatchContentId().getString(), parser.getFilePath());
            }
        }

        //Reason
        if (reasonForVisit != null && !reasonForVisit.isEmpty()) {
            CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(reasonForVisit.getString());
            encounterBuilder.addReason(reasonForVisitText, reasonForVisit);
        }

        // EoC reference
        encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()));

       }// end createAandEAttendance()



    }



