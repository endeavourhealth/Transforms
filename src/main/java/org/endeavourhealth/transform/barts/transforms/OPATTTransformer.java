package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.OPATT;
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

public class OPATTTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OPATTTransformer.class);
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");


    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createOutpatientAttendanceEvent((OPATT)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createOutpatientAttendanceEvent(OPATT parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        CsvCell finIdCell = parser.getFINNo();

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            //skip - inactive entries contains no useful data and the ENCOUNTER row will be inactive too
            return;
        }

        // get the associated encounter
        CsvCell encounterIdCell = parser.getEncounterId();
        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());

        // Patient
        CsvCell personIdCell = parser.getPatientId();
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping encounter {} because no Person->MRN mapping {} could be found in file {}", encounterIdCell.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        //EpisodOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(null, finIdCell, encounterIdCell, personIdCell, patientUuid, csvHelper, parser);

        // Create new encounter
        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell, finIdCell);

            encounterBuilder.setEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), finIdCell);
        }

        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

        episodeOfCareBuilder.setManagingOrganisation((ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString())));
        episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);


        Date beginDate = null;
        CsvCell beginDateCell = parser.getAppointmentDateTime();
        try {
            beginDate = formatDaily.parse(beginDateCell.getString());
        } catch (ParseException ex) {
            beginDate = formatBulk.parse(beginDateCell.getString());
        }
        encounterBuilder.setPeriodStart(beginDate, beginDateCell);

        //there's no explicit end date, but we can work it out from the duration
        Date endDate = null;
        CsvCell apptLengthCell = parser.getExpectedAppointmentDuration();
        if (!apptLengthCell.isEmpty()) {
            endDate = new Date(beginDate.getTime() + (apptLengthCell.getInt() * 60 * 1000));
        }

        // End date
        if (endDate != null) {
            encounterBuilder.setPeriodEnd(endDate, beginDateCell, apptLengthCell);
        }

        //work out the Encounter status
        CsvCell outcomeCell = parser.getAttendanceOutcomeCode();
        if (!outcomeCell.isEmpty()) {
            //if we have an outcome, we know the encounter has ended
            encounterBuilder.setStatus(Encounter.EncounterState.FINISHED, outcomeCell);

        } else {
            //if we don't have an outcome, the Encounter is either in progress or in the future
            if (beginDate.before(new Date())) {
                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED, beginDateCell);

            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, beginDateCell);
            }
        }

        //we may have missed the original referral, so our episode of care may have the wrong start date, so adjust that now
        if (episodeOfCareBuilder.getRegistrationStartDate() == null
                || beginDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {

            episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
            episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
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

        // Location
        CsvCell currentLocationCell = parser.getLocationCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(currentLocationCell)) {

            UUID locationResourceUUID = LocationResourceCache.getOrCreateLocationUUID(csvHelper, currentLocationCell);
            if (locationResourceUUID != null) {
                if (beginDate == null || endDate == null) {
                    encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), true, currentLocationCell);
                } else {
                    Period apptPeriod = PeriodHelper.createPeriod(beginDate, endDate);
                    Encounter.EncounterLocationComponent elc = new Encounter.EncounterLocationComponent();
                    elc.setPeriod(apptPeriod);
                    elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
                    encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), true, currentLocationCell, beginDateCell, apptLengthCell);
                }
            } else {
                TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in OPATT record {} in file {}", currentLocationCell.getString(), encounterIdCell.getString(), parser.getFilePath());
            }
        }

        CsvCell reasonCell = parser.getReasonForVisitText();
        if (!reasonCell.isEmpty()) {
            String reason = reasonCell.getString();
            encounterBuilder.addReason(reason, reasonCell);
        }

        CsvCell typeCell = parser.getAppointmentTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(typeCell)) {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.APPOINTMENT_TYPE, typeCell);
            if (codeRef != null) {
                String typeDesc = codeRef.getCodeDispTxt();
                encounterBuilder.addType(typeDesc, typeCell);
            }
        }


    }
}
