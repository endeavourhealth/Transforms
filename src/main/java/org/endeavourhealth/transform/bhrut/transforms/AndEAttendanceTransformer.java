package org.endeavourhealth.transform.bhrut.transforms;

import org.apache.commons.lang3.ObjectUtils;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.AandeAttendances;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.transform.bhrut.BhrutCsvHelper.addParmIfNotNullJson;
import static org.endeavourhealth.transform.bhrut.BhrutCsvHelper.addParmIfNotNullNhsdd;


public class AndEAttendanceTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(AndEAttendanceTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(AandeAttendances.class);

        if (parser != null) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                try {
                    AandeAttendances aAndeParser = (AandeAttendances) parser;
                    if (aAndeParser.getDataUpdateStatus().getString().equalsIgnoreCase("Deleted")) {

                        deleteResources(aAndeParser, fhirResourceFiler, csvHelper);
                    } else {
                        createResources(aAndeParser, fhirResourceFiler, csvHelper, version);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(AandeAttendances parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        if (patientIdCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "Patient id cell is empty for externalId {}.  Line ignored.", parser.getId());
            return;
        }

        //Create the episode of care is for the time in A&E
        createEpisodeOfCare(parser, fhirResourceFiler, csvHelper);

        //Create the Parent Encounter
        EncounterBuilder parentEncounterBuilder
                = createEncountersParentMinimum(parser, fhirResourceFiler, csvHelper);

        //the chief complaint needs capturing - it's not coded, so set as the reason for the encounter
        CsvCell complaintCell = parser.getComplaint();
        if (!complaintCell.isEmpty()) {
            parentEncounterBuilder.addReason(complaintCell.getString(), complaintCell);
        }

        //use RECORDED_OUTCOME to populate the discharge disposition
        CsvCell dischargeOutcomeCell = parser.getRecordedOutcome();
        if (!dischargeOutcomeCell.isEmpty()) {
            parentEncounterBuilder.setDischargeDisposition(dischargeOutcomeCell.getString(), dischargeOutcomeCell);
        }

        createEmergencySubEncounters(parser, parentEncounterBuilder, fhirResourceFiler, csvHelper);

        //save the top level parent encounter
        fhirResourceFiler.savePatientResource(null, !parentEncounterBuilder.isIdMapped(), parentEncounterBuilder);
    }

    private static void deleteResources(AandeAttendances parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        Encounter existingParentEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, parser.getId().getString());

        EncounterBuilder parentEncounterBuilder
                = new EncounterBuilder(existingParentEncounter);

        if (existingParentEncounter.hasContained()) {
            ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();

            for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {
                Reference ref = item.getItem();
                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                if (comps.getResourceType() != ResourceType.Encounter) {
                    continue;
                }
                Encounter childEncounter
                        = (Encounter) resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.Encounter, comps.getId());
                if (childEncounter != null) {
                    LOG.debug("Deleting child encounter " + childEncounter.getId());

                    fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                } else {

                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter: {} for deletion", childEncounter.getId());
                }
            }
            //finally, delete the top level parent
            fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

        } else {
            TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", parser.getId().getString());
        }
    }

    private static EncounterBuilder createEncountersParentMinimum(AandeAttendances parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);
        parentTopEncounterBuilder.setId(parser.getId().getString());
        parentTopEncounterBuilder.setPeriodStart(parser.getArrivalDttm().getDateTime(), parser.getArrivalDttm());

        CsvCell dischargeDateCell = parser.getDischargedDttm();
        if (!isNullDischargeDateCell(dischargeDateCell)) {

            parentTopEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime(), dischargeDateCell);
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Emergency");

        setCommonEncounterAttributes(parentTopEncounterBuilder, parser, csvHelper, false);

        return parentTopEncounterBuilder;
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder, AandeAttendances parser, BhrutCsvHelper csvHelper, boolean isChildEncounter) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();

        if (!patientIdCell.isEmpty()) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, patientIdCell.getString());
            if (builder.isIdMapped()) {
                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }

            builder.setPatient(patientReference, patientIdCell);
        }

        //episode of care references set using the emergency encounter Id
        if (!idCell.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, idCell.getString());
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }
            builder.setEpisodeOfCare(episodeReference, idCell);
        }

        //Note: no practitioner available

        CsvCell admissionHospitalCodeCell = parser.getHospitalCode();
        if (!admissionHospitalCodeCell.isEmpty()) {
            Reference organizationReference
                    = csvHelper.createOrganisationReference(admissionHospitalCodeCell.getString());
            if (builder.isIdMapped()) {
                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }

            builder.setServiceProvider(organizationReference, admissionHospitalCodeCell);
        }

        if (isChildEncounter) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            parentEncounter
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            builder.setPartOf(parentEncounter, idCell);
        }
    }

    private static void createEmergencySubEncounters(AandeAttendances parser, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        EncounterBuilder arrivalEncounterBuilder = new EncounterBuilder();
        arrivalEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        String arrivalEncounterId = parser.getId().getString() + ":01:EM";
        arrivalEncounterBuilder.setId(arrivalEncounterId);

        CsvCell arrivalDateCell = parser.getArrivalDttm();
        arrivalEncounterBuilder.setPeriodStart(arrivalDateCell.getDateTime(), arrivalDateCell);
        arrivalEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

        CodeableConceptBuilder codeableConceptBuilderAdmission
                = new CodeableConceptBuilder(arrivalEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilderAdmission.setText("Emergency Arrival");

        setCommonEncounterAttributes(arrivalEncounterBuilder, parser, csvHelper, true);

        //and link the parent to this new child encounter
        Reference childArrivalRef = ReferenceHelper.createReference(ResourceType.Encounter, arrivalEncounterId);
        if (existingParentEncounterBuilder.isIdMapped()) {
            childArrivalRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childArrivalRef, csvHelper);
        }
        existingEncounterList.addReference(childArrivalRef);

        ContainedParametersBuilder containedParametersBuilderArrival
                = new ContainedParametersBuilder(arrivalEncounterBuilder);
        containedParametersBuilderArrival.removeContainedParameters();

        CsvCell arrivalModeCell = parser.getArrivalMode();
        if (!arrivalModeCell.isEmpty()) {
            String arrivalMode = "2"; //Default i.e "Other"
            if (arrivalModeCell.getString().toLowerCase().contains("ambulance")) {
                arrivalMode = "1";
            }
            addParmIfNotNullNhsdd("ARRIVAL_MODE", arrivalMode,
                    arrivalModeCell, containedParametersBuilderArrival, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
        }

        CsvCell attendanceTypeCell = parser.getAttendanceType();
        if (!attendanceTypeCell.isEmpty()) {
            addParmIfNotNullNhsdd("ATTENDANCE_TYPE", attendanceTypeCell.getString(), attendanceTypeCell,
                    containedParametersBuilderArrival, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
        }

        CsvCell referralSourceCell = parser.getReferralSource();
        if (!referralSourceCell.isEmpty()) {
            addParmIfNotNullJson("REFERRAL_SOURCE", referralSourceCell.getString(), referralSourceCell,
                    containedParametersBuilderArrival, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
        }

        CsvCell triageDateCell = parser.getTriageDttm();
        CsvCell invAndTreatmentsDateCell = parser.getSeenByAeDoctorDttm();
        CsvCell leftDepartmentDateCell = parser.getLeftDepartmentDttm();
        CsvCell dischargeDateCell = parser.getDischargedDttm();

        Date aeEndDate
                = ObjectUtils.firstNonNull(triageDateCell.getDateTime(), invAndTreatmentsDateCell.getDateTime(),
                dischargeDateCell.getDateTime(), leftDepartmentDateCell.getDateTime());
        if (aeEndDate != null) {

            arrivalEncounterBuilder.setPeriodEnd(aeEndDate);
            arrivalEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        }
        //save the A&E arrival encounter
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), arrivalEncounterBuilder);

        //Is there an initial assessment encounter?
        EncounterBuilder assessmentEncounterBuilder = null;
        if (!triageDateCell.isEmpty()) {

            assessmentEncounterBuilder = new EncounterBuilder();
            assessmentEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String assessmentEncounterId = parser.getId().getString() + ":02:EM";
            assessmentEncounterBuilder.setId(assessmentEncounterId);
            assessmentEncounterBuilder.setPeriodStart(triageDateCell.getDateTime(), triageDateCell);
            assessmentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderAssessment
                    = new CodeableConceptBuilder(assessmentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAssessment.setText("Emergency Assessment");

            setCommonEncounterAttributes(assessmentEncounterBuilder, parser, csvHelper, true);

            //and link the parent to this new child encounter
            Reference childAssessmentRef = ReferenceHelper.createReference(ResourceType.Encounter, assessmentEncounterId);

            if (existingParentEncounterBuilder.isIdMapped()) {
                childAssessmentRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAssessmentRef, csvHelper);
            }
            existingEncounterList.addReference(childAssessmentRef);

            Date aeAssessmentEndDate
                    = ObjectUtils.firstNonNull(invAndTreatmentsDateCell.getDateTime(), dischargeDateCell.getDateTime(), leftDepartmentDateCell.getDateTime());
            if (aeAssessmentEndDate != null) {

                assessmentEncounterBuilder.setPeriodEnd(aeAssessmentEndDate);
                assessmentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
            //save the A&E assessment encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), assessmentEncounterBuilder);
        }

        //Is there a treatments encounter?
        EncounterBuilder treatmentsEncounterBuilder = null;
        if (!invAndTreatmentsDateCell.isEmpty()) {

            treatmentsEncounterBuilder = new EncounterBuilder();
            treatmentsEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String treatmentsEncounterId = parser.getId().getString() + ":03:EM";
            treatmentsEncounterBuilder.setId(treatmentsEncounterId);
            treatmentsEncounterBuilder.setPeriodStart(invAndTreatmentsDateCell.getDateTime(), invAndTreatmentsDateCell);
            treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderTreatments
                    = new CodeableConceptBuilder(treatmentsEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderTreatments.setText(CommonStrings.ENCOUNTER_EMERGENCY_TREATMENT);

            setCommonEncounterAttributes(treatmentsEncounterBuilder, parser, csvHelper, true);

            //and link the parent to this new child encounter
            Reference childTreatmentsRef = ReferenceHelper.createReference(ResourceType.Encounter, treatmentsEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {

                childTreatmentsRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childTreatmentsRef, csvHelper);
            }
            existingEncounterList.addReference(childTreatmentsRef);

            //if you are treated, you get discharged at some point following treatment
            if (!isNullDischargeDateCell(dischargeDateCell)) {

                treatmentsEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime(), dischargeDateCell);
                treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }

            //save the A&E treatments encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), treatmentsEncounterBuilder);
        }

        //Is there a discharge/conclusion Encounter ?
        EncounterBuilder dischargeEncounterBuilder = null;
        CsvCell leftDeptDate = parser.getLeftDepartmentDttm();
        if ((!isNullDischargeDateCell(dischargeDateCell)) || (!leftDeptDate.isEmpty())) {

            dischargeEncounterBuilder = new EncounterBuilder();
            dischargeEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String dischargeEncounterId = parser.getId().getString() + ":04:EM";
            dischargeEncounterBuilder.setId(dischargeEncounterId);
            dischargeEncounterBuilder.setPeriodStart(ObjectUtils.firstNonNull(dischargeDateCell.getDateTime(), leftDeptDate.getDateTime()));
            dischargeEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderDischarge
                    = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderDischarge.setText(CommonStrings.ENCOUNTER_EMERGENCY_END);

            setCommonEncounterAttributes(dischargeEncounterBuilder, parser, csvHelper, true);

            //and link the parent to this new child encounter
            Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {
                childDischargeRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, csvHelper);
            }
            existingEncounterList.addReference(childDischargeRef);

            //add in additional extended data as Parameters resource with additional extension
            ContainedParametersBuilder containedParametersBuilderDischarge
                    = new ContainedParametersBuilder(dischargeEncounterBuilder);
            containedParametersBuilderDischarge.removeContainedParameters();

            CsvCell dischargeDestinationCell = parser.getDischargeDestination();
            if (!dischargeDestinationCell.isEmpty()) {

                BhrutCsvHelper.addParmIfNotNullJson( "DISCHARGE_DESTINATION",
                        dischargeDestinationCell.getString(),dischargeDestinationCell,
                        containedParametersBuilderDischarge, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
            }
            CsvCell recordedOutComeCell = parser.getRecordedOutcome();
            if (!recordedOutComeCell.isEmpty()) {

                BhrutCsvHelper.addParmIfNotNullJson( "RECORDED_OUTCOME",
                        recordedOutComeCell.getString(),recordedOutComeCell,
                        containedParametersBuilderDischarge, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
            }

            //the left department date/time is usually after the discharge date/time so set that as the
            //end date of the discharge
            Date aeDischargeEndDate
                    = ObjectUtils.firstNonNull(leftDepartmentDateCell.getDateTime(), dischargeDateCell.getDateTime());
            if (aeDischargeEndDate != null) {

                dischargeEncounterBuilder.setPeriodEnd(aeDischargeEndDate);
                dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }

            //save the A&E discharge encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), dischargeEncounterBuilder);
        }
    }

    private static void createEpisodeOfCare(AandeAttendances parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();
        EpisodeOfCareBuilder episodeOfCareBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(idCell, csvHelper, fhirResourceFiler);

        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        if (episodeOfCareBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        episodeOfCareBuilder.setPatient(patientReference, patientIdCell);

        CsvCell arrivalDateCell = parser.getArrivalDttm();
        CsvCell dischargeDateCell = parser.getDischargedDttm();

        //the episode begins when they arrive
        if (arrivalDateCell != null) {

            if (episodeOfCareBuilder.getRegistrationStartDate() == null ||
                    arrivalDateCell.getDateTime().before(episodeOfCareBuilder.getRegistrationStartDate())) {

                episodeOfCareBuilder.setRegistrationStartDate(arrivalDateCell.getDateTime(), arrivalDateCell);
                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
            }

            // End date is discharge date and time
            if (!isNullDischargeDateCell(dischargeDateCell)) {

                if (episodeOfCareBuilder.getRegistrationEndDate() == null ||
                        dischargeDateCell.getDateTime().after(episodeOfCareBuilder.getRegistrationEndDate())) {

                    episodeOfCareBuilder.setRegistrationEndDate(dischargeDateCell.getDateTime(), dischargeDateCell);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
                }
            }
        } else {
            if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
            }
        }

        CsvCell odsCodeCell = parser.getHospitalCode();
        Reference organisationReference;
        if (!odsCodeCell.isEmpty()) {
            organisationReference = csvHelper.createOrganisationReference(odsCodeCell.getString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeOfCareBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setManagingOrganisation(organisationReference, odsCodeCell);
        } else {
            //v1 uses service details
            UUID serviceId = parser.getServiceId();
            organisationReference = csvHelper.createOrganisationReference(serviceId.toString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeOfCareBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setManagingOrganisation(organisationReference);
        }

        //NOTE: no coded practitioner available for A&E

        csvHelper.getEpisodeOfCareCache().cacheEpisodeOfCareBuilder(idCell, episodeOfCareBuilder);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(),!episodeOfCareBuilder.isIdMapped(),episodeOfCareBuilder);
    }

    private static boolean isNullDischargeDateCell (CsvCell dischargeDateCell) throws Exception {

        if (dischargeDateCell.isEmpty()) {
            return true;
        }

        //Bhrut A&E data uses 01/01/2020 00:00:00 discharge date as null place holders
        Date nullDischargeDate = new SimpleDateFormat("yyyy-MM-dd").parse("2200-01-01");
        Date d = dischargeDateCell.getDate();

        if (d.equals(nullDischargeDate)) {
            return true;
        }

        return false;
    }
}