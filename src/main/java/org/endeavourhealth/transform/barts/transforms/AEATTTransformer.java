package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.AEATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
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
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");


    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    String valStr = validateEntry((AEATT) parser);
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

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell arrivalDateCell = parser.getArrivalDateTime();
        CsvCell beginDateCell = parser.getCheckInDateTime();
        CsvCell endDateCell = parser.getCheckOutDateTime();
        CsvCell currentLocationCell = parser.getLastLocCode();
        CsvCell reasonForVisit = parser.getPresentingCompTxt();
        CsvCell triageStartCell = parser.getTriageStartDateTime();
        CsvCell triageEndCell = parser.getTriageCompleteDateTime();
        CsvCell triagepersonIdCell = parser.getTriagePersonId();
        CsvCell firstAssessmentDateCell = parser.getFirstAssessDateTime();
        CsvCell conclusionDateCell =parser.getConclusionDateTime();

        // Encounter start and end
        Date beginDate = null;
        if (beginDateCell != null && !beginDateCell.isEmpty()) {
            try {
                beginDate = formatDaily.parse(beginDateCell.getString());
            } catch (ParseException ex) {
                beginDate = formatBulk.parse(beginDateCell.getString());
            }
        }
        Date endDate = null;
        if (endDateCell != null && !endDateCell.isEmpty()) {
            try {
                endDate = formatDaily.parse(endDateCell.getString());
            } catch (ParseException ex) {
                endDate = formatBulk.parse(endDateCell.getString());
            }
        }
        // Triage start and end
        Date triageBeginDate = null;
        if (triageStartCell != null && !triageStartCell.isEmpty()) {
            try {
                triageBeginDate = formatDaily.parse(triageStartCell.getString());
            } catch (ParseException ex) {
                triageBeginDate = formatBulk.parse(triageStartCell.getString());
            }
        }
        Date triageEndDate = null;
        if (triageEndCell != null && !triageEndCell.isEmpty()) {
            try {
                triageEndDate = formatDaily.parse(triageEndCell.getString());
            } catch (ParseException ex) {
                triageEndDate = formatBulk.parse(triageEndCell.getString());
            }
        }
        // Assessment start and end
        Date assessmentBeginDate = null;
        if (triageStartCell != null && !triageStartCell.isEmpty()) {
            try {
                assessmentBeginDate = formatDaily.parse(triageStartCell.getString());
            } catch (ParseException ex) {
                assessmentBeginDate = formatBulk.parse(triageStartCell.getString());
            }
        }
        Date assessmentEndDate = null;
        if (triageEndCell != null && !triageEndCell.isEmpty()) {
            try {
                assessmentEndDate = formatDaily.parse(triageEndCell.getString());
            } catch (ParseException ex) {
                assessmentEndDate = formatBulk.parse(triageEndCell.getString());
            }
        }

        /*
        if (personIdCell != null) {
            LOG.debug("Current line " + parser.getCurrentLineNumber() + " personId is " + personIdCell.getString());
        } else {
            LOG.debug("Current line " + parser.getCurrentLineNumber() + " personId is null");
        }*/

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
        EpisodeOfCareBuilder episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(null, null, encounterIdCell, personIdCell, patientUuid, csvHelper, fhirResourceFiler);
        LOG.debug("episodeOfCareBuilder:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
        if (encounterBuilder != null && episodeOfCareBuilder.getResourceId().compareToIgnoreCase(encounterBuilder.getEpisodeOfCare().get(0).getReference()) != 0) {
            LOG.debug("episodeOfCare reference has changed from " + encounterBuilder.getEpisodeOfCare().get(0).getReference() + " to " + episodeOfCareBuilder.getResourceId());
        }

        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell, null);

            encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), encounterIdCell);
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
        if (beginDate != null) {
            // Start date
            encounterBuilder.setPeriodStart(beginDate);

            if (episodeOfCareBuilder.getRegistrationStartDate() == null || beginDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
            }

            // End date
            if (endDate != null) {
                encounterBuilder.setPeriodEnd(endDate);

                encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

                if (episodeOfCareBuilder.getRegistrationEndDate() == null || endDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                    episodeOfCareBuilder.setRegistrationEndDateNoStatusUpdate(endDate, endDateCell);
                }

            } else if (beginDate.before(new Date())) {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, beginDateCell);
            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED, beginDateCell);
            }
        } else {
            encounterBuilder.setStatus(Encounter.EncounterState.PLANNED);
            if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
            }
        }

        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        //We have a number of potential events in the patient journey through A&E. We can't map all the states.

        // Triage
        /*
        if (triageStartCell != null) {
            LOG.debug("triageStartCell=" + triageStartCell.getString());
        } else {
            LOG.debug("triageStartCell=null");
        }
        if (triageEndCell != null) {
            LOG.debug("triageEndCell=" + triageEndCell.getString());
        } else {
            LOG.debug("triageEndCell=null");
        }
        if (triagepersonIdCell != null) {
            LOG.debug("triagepersonIdCell=" + triagepersonIdCell.getString());
        } else {
            LOG.debug("triagepersonIdCell=null");
        }


        if (triageBeginDate == null) {
            encState = Encounter.EncounterState.PLANNED;
        } else if (triageEndDate == null) {
            encState = Encounter.EncounterState.INPROGRESS;
        } else {
            encState = Encounter.EncounterState.FINISHED;
        }
        encounterBuilder.setStatus(encState, triageBeginDate, triageEndDate, triageStartCell, triageEndCell);
        */

        if (triagepersonIdCell != null && !triagepersonIdCell.isEmpty() && triagepersonIdCell.getLong() > 0) {
            ResourceId triagePersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, triagepersonIdCell);
            if (triagePersonResourceId != null) {
                Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, triagePersonResourceId.getResourceId().toString());
                Period triagePeriod = new Period();
                triagePeriod.setStart(triageBeginDate);
                if (triageEndDate != null) {
                    triagePeriod.setEnd(triageEndDate);
                }
                encounterBuilder.addParticipant(ref, EncounterParticipantType.PARTICIPANT, triagePeriod, true, triagepersonIdCell);
            }
        }

        //  First medical assessment to conclusion
        /*
        if (firstAssessmentDateCell != null) {
            LOG.debug("firstAssessmentDateCell=" + firstAssessmentDateCell.getString());
        } else {
            LOG.debug("firstAssessmentDateCell=null");
        }
        if (conclusionDateCell != null) {
            LOG.debug("conclusionDateCell=" + conclusionDateCell.getString());
        } else {
            LOG.debug("conclusionDateCell=null");
        }

        if (assessmentBeginDate == null) {
            encState = Encounter.EncounterState.PLANNED;
        } else if (assessmentEndDate == null) {
            encState = Encounter.EncounterState.INPROGRESS;
        } else {
            encState = Encounter.EncounterState.FINISHED;
        }
        encounterBuilder.setStatus(encState, assessmentBeginDate, assessmentEndDate, firstAssessmentDateCell, conclusionDateCell);
        */

        // Location
        if (currentLocationCell != null && !currentLocationCell.isEmpty() && currentLocationCell.getLong() > 0) {
            UUID locationResourceUUID = LocationResourceCache.getOrCreateLocationUUID(csvHelper, currentLocationCell);
            if (locationResourceUUID != null) {
                encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), true,currentLocationCell);
            } else {
                TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in AEATT record {} in file {}", currentLocationCell.getString(), parser.getCdsBatchContentId().getString(), parser.getFilePath());
            }
        }

        //Reason
        if (reasonForVisit != null && !reasonForVisit.isEmpty()) {
            CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(reasonForVisit.getString());
            encounterBuilder.addReason(reasonForVisitText, true, reasonForVisit);
        }

        // EoC reference
        if (encounterBuilder.getEpisodeOfCare() != null && encounterBuilder.getEpisodeOfCare().size() > 0) {
            encounterBuilder.getEpisodeOfCare().remove(0);
        }
        encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("episodeOfCare Complete:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
            LOG.debug("encounter complete:" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
        }

    }// end createAandEAttendance()



    }



