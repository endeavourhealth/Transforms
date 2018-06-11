package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
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
                    createAandEAttendance((AEATT) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
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
        CsvCell decisionToAdmitDateTimeCell = parser.getDecisionToAdmitDateTime();
        CsvCell beginDateCell = parser.getCheckInDateTime();
        CsvCell endDateCell = parser.getCheckOutDateTime();
        CsvCell currentLocationCell = parser.getLastLocCode();
        CsvCell reasonForVisit = parser.getPresentingCompTxt();
        CsvCell triageStartCell = parser.getTriageStartDateTime();
        CsvCell triageEndCell = parser.getTriageCompleteDateTime();
        CsvCell triagepersonIdCell = parser.getTriagePersonId();
        CsvCell hcpFirstAssignedPersonIdCell = parser.getHcpFirstAssignedPersonId();
        CsvCell firstSpecPersonIdCell = parser.getFirstSpecPersonId();
        CsvCell respHcpPersonIdCell = parser.getRespHcpPersonId();
        CsvCell referralPersonIdCell = parser.getReferralPersonId();

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

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Retrieve or create EpisodeOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(null, null, encounterIdCell, personIdCell, patientUuid, csvHelper, parser);
        LOG.debug("episodeOfCareBuilder:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
        if (encounterBuilder != null && episodeOfCareBuilder.getResourceId().compareToIgnoreCase(ReferenceHelper.getReferenceId(encounterBuilder.getEpisodeOfCare().get(0))) != 0) {
            LOG.debug("episodeOfCare reference has changed from " + encounterBuilder.getEpisodeOfCare().get(0).getReference() + " to " + episodeOfCareBuilder.getResourceId());
        }

        episodeOfCareBuilder.setManagingOrganisation((ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString())));

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

        // Check whether to Finish EpisodeOfCare
        // If the patient has left AE (checkout-time/enddatetime) and not been admitted (decisionToAdmitDateTime empty) complete EpisodeOfCare
        if (endDateCell != null && endDateCell.getString().trim().length() > 0 && (decisionToAdmitDateTimeCell == null || decisionToAdmitDateTimeCell.getString().trim().length() == 0)) {
            episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED, endDateCell);
        }

        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        // Triage person
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

        if (hcpFirstAssignedPersonIdCell != null && !hcpFirstAssignedPersonIdCell.isEmpty() && hcpFirstAssignedPersonIdCell.getLong() > 0) {
            ResourceId hcpFirstAssignedResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, hcpFirstAssignedPersonIdCell);
            if (hcpFirstAssignedResourceId != null) {
                Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, hcpFirstAssignedResourceId.getResourceId().toString());
                encounterBuilder.addParticipant(ref, EncounterParticipantType.ATTENDER,  true, hcpFirstAssignedPersonIdCell);
            }
        }

        if (firstSpecPersonIdCell != null && !firstSpecPersonIdCell.isEmpty() && firstSpecPersonIdCell.getLong() > 0) {
            ResourceId firstSpecResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, firstSpecPersonIdCell);
            if (firstSpecResourceId != null) {
                Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, firstSpecResourceId.getResourceId().toString());
                encounterBuilder.addParticipant(ref, EncounterParticipantType.PRIMARY_PERFORMER,  true, firstSpecPersonIdCell);
            }
        }

        if (respHcpPersonIdCell != null && !respHcpPersonIdCell.isEmpty() && respHcpPersonIdCell.getLong() > 0) {
            ResourceId respHcpResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, respHcpPersonIdCell);
            if (respHcpResourceId != null) {
                Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, respHcpResourceId.getResourceId().toString());
                encounterBuilder.addParticipant(ref, EncounterParticipantType.CONSULTANT,  true, respHcpPersonIdCell);
            }
        }

        if (referralPersonIdCell != null && !referralPersonIdCell.isEmpty() && referralPersonIdCell.getLong() > 0) {
            ResourceId referralResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, referralPersonIdCell);
            if (referralResourceId != null) {
                Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, referralResourceId.getResourceId().toString());
                encounterBuilder.addParticipant(ref, EncounterParticipantType.REFERRER,  true, referralPersonIdCell);
            }
        }

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
        if (!reasonForVisit.isEmpty()) {
            encounterBuilder.addReason(reasonForVisit.getString(), reasonForVisit);
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



