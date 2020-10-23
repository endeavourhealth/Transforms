package org.endeavourhealth.transform.bhrut.transforms;

import org.apache.commons.lang3.ObjectUtils;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.AandeAttendances;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.endeavourhealth.transform.bhrut.BhrutCsvHelper.addParmIfNotNull;
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
                    AandeAttendances andeParser = (AandeAttendances) parser;

                    if (andeParser.getDataUpdateStatus().getString().equalsIgnoreCase("Deleted")) {
                        deleteResource(andeParser, fhirResourceFiler, csvHelper, version);
                        deleteEmergencyEncounterAndChildren(andeParser, fhirResourceFiler, csvHelper);
                    } else {
                        createResources(andeParser, fhirResourceFiler, csvHelper, version);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void deleteResource(AandeAttendances parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().getString());
        CsvCell patientIdCell = parser.getPasId();
        if (patientIdCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "Patient id cell is empty for externalId {}.  Line ignored.", parser.getId());
            return;
        }
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        encounterBuilder.setDeletedAudit(dataUpdateStatusCell);
        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);

    }

    public static void createResources(AandeAttendances parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        CsvCell idCell = parser.getId();
        CsvCell patientIdCell = parser.getPasId();
        if (patientIdCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "Patient id cell is empty for externalId {}.  Line ignored.", parser.getId());
            return;
        }
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        //Create EncounterBuilder object
        EncounterBuilder encounterBuilder = createEncountersParentMinimum(parser, fhirResourceFiler, csvHelper);

        encounterBuilder.setPatient(patientReference, patientIdCell);
        CsvCell org = parser.getHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);

        //set the encounter extensions
       ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(encounterBuilder);



        //the chief complaint needs capturing - it's not coded, so set as the reason for the encounter
        CsvCell complaintCell = parser.getComplaint();
        if (!complaintCell.isEmpty()) {
            encounterBuilder.addReason(complaintCell.getString(), complaintCell);
        }

        //use RECORDED_OUTCOME to populate the discharge disposition
        CsvCell dischargeOutcomeCell = parser.getRecordedOutcome();
        if (!dischargeOutcomeCell.isEmpty()) {
            encounterBuilder.setDischargeDisposition(dischargeOutcomeCell.getString());
        }

        //List<ResourceBuilderBase> bases =
        createEmergencyEncounters(parser, encounterBuilder, fhirResourceFiler, csvHelper);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);

    }




    private static void deleteEmergencyEncounterAndChildren(AandeAttendances parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

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
        if (!dischargeDateCell.isEmpty()) {

            parentTopEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime());
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

            builder.setPatient(patientReference);
        }

        if (!idCell.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, idCell.getString());
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }

            builder.setEpisodeOfCare(episodeReference);
        }

        if (!idCell.isEmpty()) {
            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, idCell.getString());
            if (builder.isIdMapped()) {
                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }

            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }

        CsvCell admissionHospitalCode = parser.getHospitalCode();
        if (!admissionHospitalCode.isEmpty()) {
            Reference organizationReference
                    = csvHelper.createOrganisationReference(admissionHospitalCode.getString());
            if (builder.isIdMapped()) {
                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }

            builder.setServiceProvider(organizationReference);
        }

        if (isChildEncounter) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
           // if (builder.isIdMapped()) {
                parentEncounter
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            //}
            builder.setPartOf(parentEncounter);
        }

    }

    private static List<ResourceBuilderBase> createEmergencyEncounters(AandeAttendances parser, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);
        List<ResourceBuilderBase> ret = new ArrayList<>();
        ret.add(existingParentEncounterBuilder);
        EncounterBuilder arrivalEncounterBuilder = new EncounterBuilder();
        arrivalEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);
        CsvCell arrivalModeCell = parser.getArrivalMode();
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(arrivalEncounterBuilder);
        if (!arrivalModeCell.isEmpty()) {
            String arrivalMode="2"; //Default i.e "Other"
            if (arrivalModeCell.getString().toLowerCase().contains("ambulance")) {
                arrivalMode="1";
            }
            addParmIfNotNullNhsdd("ARRIVAL_MODE", arrivalMode,
                    arrivalModeCell, containedParametersBuilder, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
        }

        String arrivalEncounterId = parser.getId().getString() + ":01:EM";
        arrivalEncounterBuilder.setId(arrivalEncounterId);

        arrivalEncounterBuilder.setPeriodStart(parser.getArrivalDttm().getDateTime(), parser.getArrivalDttm());
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

        CsvCell attendanceTypeCell = parser.getAttendanceType();
        if (!attendanceTypeCell.isEmpty()) {
            if (attendanceTypeCell.getString().equalsIgnoreCase("1")) {
                addParmIfNotNullNhsdd("ATTENDANCE_TYPE", "1", attendanceTypeCell,
                        containedParametersBuilder, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
            } else if (attendanceTypeCell.getString().equalsIgnoreCase("2")) {
                addParmIfNotNullNhsdd("ATTENDANCE_TYPE", "2", attendanceTypeCell,
                        containedParametersBuilder, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
            } else if (attendanceTypeCell.getString().equalsIgnoreCase("3")) {
                addParmIfNotNullNhsdd("ATTENDANCE_TYPE", "3", attendanceTypeCell,
                        containedParametersBuilder, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
            } else {
                throw new TransformException("Unexpected attendance type/category code: [" + attendanceTypeCell.getString() + "]");
            }
        }

        CsvCell referralSourceCell = parser.getReferralSource();
        if (!referralSourceCell.isEmpty()) {
            addParmIfNotNullNhsdd("REFERRAL_SOURCE", referralSourceCell.getString(), referralSourceCell,
                    containedParametersBuilder, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
        }


        CsvCell triageDateCell = parser.getTriageDttm();
        CsvCell invAndTreatmentsDateCell = parser.getSeenByAeDoctorDttm();
        CsvCell arrivalDateCell = parser.getArrivalDttm();
        CsvCell conclusionDate = parser.getLeftDepartmentDttm();
        CsvCell dischargeDateCell = parser.getDischargedDttm();

        Date aeEndDate
                = ObjectUtils.firstNonNull(triageDateCell.getDateTime(), invAndTreatmentsDateCell.getDateTime(),
                arrivalDateCell.getDateTime(), conclusionDate.getDateTime(), dischargeDateCell.getDateTime());
        if (aeEndDate != null) {

            arrivalEncounterBuilder.setPeriodEnd(aeEndDate);
            arrivalEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        }
        ret.add(arrivalEncounterBuilder);
        //save the A&E arrival encounter

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), arrivalEncounterBuilder);
              //Is there an initial assessment encounter?
        EncounterBuilder assessmentEncounterBuilder = null;
        if (!triageDateCell.isEmpty()) {

            assessmentEncounterBuilder = new EncounterBuilder();
            assessmentEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String assessmentEncounterId = parser.getId().getString() + ":02:EM";
            assessmentEncounterBuilder.setId(assessmentEncounterId);
            assessmentEncounterBuilder.setPeriodStart(triageDateCell.getDateTime());
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
                    = ObjectUtils.firstNonNull(invAndTreatmentsDateCell.getDateTime(), arrivalDateCell.getDateTime(), conclusionDate.getDateTime(), dischargeDateCell.getDateTime());
            if (aeAssessmentEndDate != null) {

                assessmentEncounterBuilder.setPeriodEnd(aeAssessmentEndDate);
                assessmentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
            //save the A&E assessment encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), assessmentEncounterBuilder);

            ret.add(assessmentEncounterBuilder);

        }

        //Is there a treatments encounter?
        EncounterBuilder treatmentsEncounterBuilder = null;
        if (!invAndTreatmentsDateCell.isEmpty()) {

            treatmentsEncounterBuilder = new EncounterBuilder();
            treatmentsEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String treatmentsEncounterId = parser.getId().getString() + ":03:EM";
            treatmentsEncounterBuilder.setId(treatmentsEncounterId);
            treatmentsEncounterBuilder.setPeriodStart(invAndTreatmentsDateCell.getDateTime());
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

            Date aeTreatmentsEndDate
                    = ObjectUtils.firstNonNull(conclusionDate.getDateTime(), dischargeDateCell.getDateTime());
            if (aeTreatmentsEndDate != null) {

                treatmentsEncounterBuilder.setPeriodEnd(aeTreatmentsEndDate);
                treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }

            //save the A&E treatments encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), treatmentsEncounterBuilder);
            ret.add(treatmentsEncounterBuilder);
        }

        //Is there a dischargeEncounter ?
        EncounterBuilder dischargeEncounterBuilder = null;
        CsvCell leftDeptDate = parser.getLeftDepartmentDttm();
        if ((!dischargeDateCell.isEmpty()) || (!leftDeptDate.isEmpty())) {

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

            CsvCell dischargeDestinationCell = parser.getDischargeDestination();
            if (!dischargeDestinationCell.isEmpty()) {
                //add in additional extended data as Parameters resource with additional extension
            ContainedParametersBuilder containedParametersBuilderDischarge
                    = new ContainedParametersBuilder(dischargeEncounterBuilder);
            containedParametersBuilderDischarge.removeContainedParameters();
                BhrutCsvHelper.addParmIfNotNullJson( "DISCHARGE_DESTINATION",
                        dischargeDestinationCell.getString(),dischargeDestinationCell,
                        containedParametersBuilderDischarge, BhrutCsvToFhirTransformer.IM_AEATTENDANCE_TABLE_NAME);
            }

            Date aeDischargeEndDate
                    = ObjectUtils.firstNonNull(conclusionDate.getDateTime(), dischargeDateCell.getDateTime());
            if (aeDischargeEndDate != null) {

                dischargeEncounterBuilder.setPeriodEnd(aeDischargeEndDate);
                dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }

            //save the A&E discharge encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), dischargeEncounterBuilder);
            ret.add(dischargeEncounterBuilder);
        }

        return ret;
    }
    private static ObservationBuilder createLinkedObservationbuilder(AandeAttendances parser, BhrutCsvHelper csvHelper,
                                                                     Reference encounterRef,
                                                                     boolean idMapped) throws Exception {
        ObservationBuilder observationBuilder = new ObservationBuilder();
        //Same id as parent Encounter but different ResourceType
        observationBuilder.setId(parser.getId().getString() + ":01:EM", parser.getId() );
        observationBuilder.setNotes(parser.getComplaint().getString());
        Reference patientReference
                = ReferenceHelper.createReference(ResourceType.Patient, parser.getPasId().getString());
        if (idMapped) {
            patientReference =
                IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference,csvHelper);
        }
        observationBuilder.setPatient(patientReference);
        DateTimeType dtt = new DateTimeType(parser.getArrivalDttm().getDateTime());
        observationBuilder.setEffectiveDate(dtt,parser.getArrivalDttm());
        observationBuilder.setEncounter(encounterRef);
        return observationBuilder;
    }

}
