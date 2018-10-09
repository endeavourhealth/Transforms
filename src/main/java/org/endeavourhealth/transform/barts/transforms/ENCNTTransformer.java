package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class ENCNTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createEncounter((ENCNT)parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createEncounter(ENCNT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // Check if encounter type should be excluded
        //not safe to skip encounters
        /*CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        if (excludeEncounterType(encounterTypeCodeCell, csvHelper)) {
            return;
        }*/

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveIndicator();

        //this will save a little bit of DB reading when we get to the PROCE and DIAGN transforms
        csvHelper.cacheEncounterIdToPersonId(encounterIdCell, personIdCell);

        //cache our extract date for later
        CsvCell extractDateTimeCell = parser.getExtractDateTime();
        csvHelper.cacheExtractDateTime(extractDateTimeCell);

        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell, csvHelper);

        //if inactive, we want to delete it
        if (!activeCell.getIntAsBoolean()) {
            csvHelper.getEncounterCache().deleteEncounter(encounterBuilder, encounterIdCell, personIdCell, csvHelper, fhirResourceFiler, parser.getCurrentState(), activeCell);
            return;
        }

        CsvCell finIdCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getVisitId();
        CsvCell treatmentFunctionCodeCell = parser.getCurrentTreatmentFunctionMillenniumCode();
        CsvCell mainSpecialtyCodeCell = parser.getMainSpecialtyMillenniumCode();

        //if the Encounter previously existed, see if we've changed the patient UUID, in which
        //case we'll need to update all other resources for the patient
        if (encounterBuilder.isIdMapped()) {

            UUID oldPatientUuid = csvHelper.getEncounterCache().getOriginalPatientUuid(encounterIdCell);
            if (oldPatientUuid != null) {

                // As of 2018-03-02 we dont appear to get any further ENCNT entries for teh minor encounter in A35 and hence the EoC reference on an encounter cannot change
                // PatientTable reference on EncounterTable resources is handled below
                // PatientTable reference on EpisodeOfCare resources is handled below
                String newPatientUuid = ReferenceHelper.getReferenceId(encounterBuilder.getPatient());
                LOG.debug("ENCNT has changed patient from " + oldPatientUuid + " to " + newPatientUuid);

                List<Resource> resourceList = csvHelper.retrieveResourceByPatient(oldPatientUuid);
                for (Resource resource : resourceList) {
                    if (resource instanceof Condition) {
                        ConditionBuilder builder = new ConditionBuilder((Condition) resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, newPatientUuid), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);

                    } else if (resource instanceof Procedure) {
                        ProcedureBuilder builder = new ProcedureBuilder((Procedure) resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, newPatientUuid), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);

                    } else if (resource instanceof Observation) {
                        ObservationBuilder builder = new ObservationBuilder((Observation) resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, newPatientUuid), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);

                    } else if (resource instanceof FamilyMemberHistory) {
                        FamilyMemberHistoryBuilder builder = new FamilyMemberHistoryBuilder((FamilyMemberHistory)resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, newPatientUuid), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);

                    } else if (resource instanceof Encounter) {
                        //encounters will be updated with deltas to the ENCNT file

                    } else if (resource instanceof EpisodeOfCare) {
                        //episodes will be automatically updated by the EpisodeOfCareResourceCache

                    } else if (resource instanceof Patient) {
                        //leave the old patient resource alone

                    } else {
                        throw new TransformException("Unsupported resource type " + resource.getClass());
                    }
                }
            }
        }

        // Save visit-id to encounter-id link
        //removed as we never look up this mapping
        /*if (!visitIdCell.isEmpty()) {
            csvHelper.saveInternalId(InternalIdMap.TYPE_VISIT_ID_TO_ENCOUNTER_ID, visitIdCell.getString(), encounterIdCell.getString());
        }*/

        // Identifiers
        if (!finIdCell.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        if (!visitIdCell.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setValue(visitIdCell.getString(), visitIdCell);
        }

        if (!encounterIdCell.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);
        }

        // class
        CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        Encounter.EncounterClass cls = getEncounterClass(encounterTypeCodeCell, encounterIdCell, csvHelper);
        if (cls != null) {
            encounterBuilder.setClass(cls, encounterTypeCodeCell);
        }

        // status
        //Date d = null;
        CsvCell statusCell = parser.getEncounterStatusMillenniumCode();
        Encounter.EncounterState status = getEncounterStatus(statusCell.getString(), encounterIdCell, parser);
        encounterBuilder.setStatus(status, statusCell);

        //Reason
        CsvCell reasonForVisit = parser.getReasonForVisitText();
        if (!reasonForVisit.isEmpty()) {
            encounterBuilder.addReason(reasonForVisit.getString(), reasonForVisit);
        }

        // EncounterTable type
        if (!BartsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
            if (codeRef != null) {
                String typeDesc = codeRef.getCodeDispTxt();
                encounterBuilder.setType(typeDesc, encounterTypeCodeCell);
            }
        }

        // treatment function
        BartsCodeableConceptHelper.applyCodeDisplayTxt(treatmentFunctionCodeCell, CodeValueSet.TREATMENT_FUNCTION, encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Treatment_Function, csvHelper);

        // Referrer
        CsvCell referrerPersonnelIdentifier = parser.getReferrerMillenniumPersonnelIdentifier();
        if (!BartsCsvHelper.isEmptyOrIsZero(referrerPersonnelIdentifier)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(referrerPersonnelIdentifier);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.REFERRER, true, referrerPersonnelIdentifier);
        }

        // responsible person
        CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
        if (!BartsCsvHelper.isEmptyOrIsZero(responsibleHCPCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(responsibleHCPCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, true, responsibleHCPCell);
        }

        // registering person
        CsvCell registeringPersonnelIdentifierCell = parser.getRegisteringMillenniumPersonnelIdentifier();
        if (!BartsCsvHelper.isEmptyOrIsZero(registeringPersonnelIdentifierCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(registeringPersonnelIdentifierCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PARTICIPANT, true, registeringPersonnelIdentifierCell);
        }

        // Location
        // Field maintained from OPATT, AEATT, IPEPI and IPWDS

        if (!BartsCsvHelper.isEmptyOrIsZero(mainSpecialtyCodeCell)) {
            //the specialty code points to an entry in CVREF. The CVREF transformer creates Organization
            //resources for specialty records, using a slightly different source ID to avoid conflicts with
            //other numeric source IDs
            Reference organisationReference = csvHelper.createSpecialtyOrganisationReference(mainSpecialtyCodeCell);
            if (encounterBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            encounterBuilder.setServiceProvider(organisationReference);
        }

        CsvCell recordedDateCell = parser.getEncounterCreateDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(recordedDateCell)) {

            Date d = BartsCsvHelper.parseDate(recordedDateCell);
            encounterBuilder.setRecordedDate(d, recordedDateCell);

            //if we've just created the Encounter, then set the creation date as the start date too, because
            //not all encounters have records in OPATT, AEATT etc. (e.g. results-only encounters), so it's best we have some date on there
            if (!encounterBuilder.isIdMapped()) {
                encounterBuilder.setPeriodStart(d, recordedDateCell);
            }
        }

        // Maintain EpisodeOfCare
        // Field maintained from OPATT, AEATT, IPEPI and IPWDS

        /*EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(parser, csvHelper);
        if (episodeOfCareBuilder != null) {
            csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, encounterBuilder, fhirResourceFiler);
        }*/

        //we don't save immediately, but return the Encounter builder to the cache
        csvHelper.getEncounterCache().returnEncounterBuilder(encounterIdCell, encounterBuilder);
    }



    /**
     * cannot exclude encounters, no matter the type. e.g. by exluding "Outpatient Pre-Registration" we missed
     * details on a lot of Outpatient Appts
     */
    /*private static boolean excludeEncounterType(CsvCell encounterTypeCodeCell, BartsCsvHelper csvHelper) throws Exception {

        if (BartsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            return false;
        }

        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
        if (codeRef == null) {
            return false;
        }

        String desc = codeRef.getCodeDispTxt();

        return desc.equals("Inpatient Pre-Admission")
                || desc.equals("Inpatient Waiting List")
                || desc.equals("Day Case Waiting List")
                || desc.equals("Outpatient Referral")
                || desc.equals("Outpatient Pre-Registration")
                || desc.equals("Direct Referral");

        *//*if (millenniumCode.compareTo("309313") == 0) { return true; } // Inpatient Pre-Admission
        else if (millenniumCode.compareTo("3767801") == 0) { return true; } // Inpatient Waiting List
        else if (millenniumCode.compareTo("3767802") == 0) { return true; } // Day Case Waiting List
        else if (millenniumCode.compareTo("3767803") == 0) { return true; } // Outpatient Referral
        else if (millenniumCode.compareTo("3767806") == 0) { return true; } // Outpatient Pre-registration
        else if (millenniumCode.compareTo("3768747") == 0) { return true; } // Outpatient Services
        else {
            return false;
        }*//*
    }*/

    private static Encounter.EncounterClass getEncounterClass(CsvCell encounterTypeCodeCell, CsvCell encounterIdCell, BartsCsvHelper csvHelper) throws Exception {

        if (BartsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            return null;
        }

        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
        if (codeRef == null) {
            return null;
        }

        String desc = codeRef.getCodeDispTxt();
        if (desc.equals("Inpatient")
                || desc.equals("Psychiatric Inpatient")
                || desc.equals("Regular Day Admission")
                || desc.equals("Regular Night Admission")
                || desc.equals("Day Surgery")
                || desc.equals("Day Case")
                || desc.equals("Day Care")
                ) {

            return Encounter.EncounterClass.INPATIENT;

        } else if (desc.equals("Outpatient Message")
                || desc.equals("Outpatient")
                || desc.equals("Outpatient Referral")
                ) {

            return Encounter.EncounterClass.OUTPATIENT;

        } else if (desc.equals("Emergency")
                || desc.equals("ER Temp")
                || desc.equals("Emergency Department")) {

            return Encounter.EncounterClass.EMERGENCY;

        } else if (desc.equals("Research")
                || desc.equals("Inpatient Pre-Admission")
                || desc.equals("Results Only")
                || desc.equals("Observation")
                || desc.equals("Recurring")
                || desc.equals("Inpatient Waiting List")
                || desc.equals("Day Case Waiting List")
                || desc.equals("Ward Attender")
                || desc.equals("Outpatient Pre-Registration")
                || desc.equals("Newborn")
                || desc.equals("Direct Referral")
                || desc.equals("Maternity")
                || desc.equals("Mortuary")
                || desc.equals("HLA QC")
                || desc.equals("Community")
                || desc.equals("Blood Donation")) {

            return Encounter.EncounterClass.OTHER;

        } else {
            throw new TransformException("Unknown ENCNT type [" + desc + "]");
        }


        /*if (millenniumCode.compareTo("309308") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("309309") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("309313") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767801") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767802") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767803") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3767804") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767806") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3767807") == 0) { return Encounter.EncounterClass.EMERGENCY; }
        else if (millenniumCode.compareTo("3767808") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767809") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3767810") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767811") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767812") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3768747") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3768748") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else {
            TransformWarnings.log(LOG, parser, "Millennium encounter-type {} not found for Personnel-id {} in ENCNT record {} in file {}", millenniumCode, encounterIdCell.getString(), parser.getFilePath());
            return Encounter.EncounterClass.OTHER;
        }*/
    }

    /*
    *
     */
    private static Encounter.EncounterState getEncounterStatus(String millenniumCode, CsvCell encounterIdCell,  ParserI parser) throws Exception {
        if (millenniumCode.compareTo("666807") == 0) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.compareTo("666808") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("666809") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("854") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.compareTo("855") == 0) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.compareTo("856") == 0) { return Encounter.EncounterState.FINISHED; }
        else if (millenniumCode.compareTo("857") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.compareTo("858") == 0) { return Encounter.EncounterState.ARRIVED; }
        else if (millenniumCode.compareTo("859") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("860") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else {
            TransformWarnings.log(LOG, parser, "Millennium status-code {} not found for Personnel-id {} in ENCNT record {} in file {}. Status set to in-progress", millenniumCode, encounterIdCell.getString(), parser.getFilePath());
            return Encounter.EncounterState.INPROGRESS;
        }
    }

    /**
     * we add the encounter type to the "type" element of the Encounter, but the type changes over time,
     * so we need to replace any previous type before adding the new one. However, the OPATT transform
     * also uses the type field, so we need to ensure we're only removing a type that came from the code
     * set we use. There is no overlap between the ENCNT type and OPATT type codes.
     */
    /*private static void removePreviousEncounterType(EncounterBuilder builder, BartsCsvHelper csvHelper) throws Exception {

        Set<String> typeSet = new HashSet<>();
        List<CernerCodeValueRef> types = csvHelper.getCernerCodesForSet(CodeValueSet.ENCOUNTER_TYPE);
        for (CernerCodeValueRef type: types) {
            String typeDesc = type.getCodeDispTxt(); //make sure this is the same field as we use when adding the type
            typeSet.add(typeDesc);
        }

        builder.removeTypes(typeSet);
    }*/
}
