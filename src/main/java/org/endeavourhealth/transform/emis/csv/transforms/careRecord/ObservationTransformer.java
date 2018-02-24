package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.common.fhir.schema.ImmunizationStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.core.terminology.Read2;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.*;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ObservationTransformer {

    private static ResourceIdTransformDalI idMapRepository = DalProvider.factoryResourceIdTransformDal();

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Observation.class);
        while (parser.nextRecord()) {

            try {

                Observation observationParser = (Observation)parser;

                //if it's deleted we need to look up what the original resource type was before we can do the delete
                CsvCell deleted = observationParser.getDeleted();
                if (deleted.getBoolean()) {
                    deleteResource(observationParser, fhirResourceFiler, csvHelper, version);
                } else {
                    createResource(observationParser, fhirResourceFiler, csvHelper, version);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void deleteResource(Observation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version) throws Exception {

        ResourceType resourceType = findOriginalTargetResourceType(fhirResourceFiler, parser);
        if (resourceType != null) {
            switch (resourceType) {
                case Observation:
                    createOrDeleteObservation(parser, fhirResourceFiler, csvHelper);
                    break;
                //checked below, as this is a special case
                /*case Condition:
                    createOrDeleteCondition(parser, csvProcessor, csvHelper);
                    break;*/
                case Procedure:
                    createOrDeleteProcedure(parser, fhirResourceFiler, csvHelper);
                    break;
                case AllergyIntolerance:
                    createOrDeleteAllergy(parser, fhirResourceFiler, csvHelper);
                    break;
                case FamilyMemberHistory:
                    createOrDeleteFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                    break;
                case Immunization:
                    createOrDeleteImmunization(parser, fhirResourceFiler, csvHelper);
                    break;
                case DiagnosticOrder:
                    createOrDeleteDiagnosticOrder(parser, fhirResourceFiler, csvHelper);
                    break;
                case DiagnosticReport:
                    createOrDeleteDiagnosticReport(parser, fhirResourceFiler, csvHelper);
                    break;
                case Specimen:
                    createOrDeleteSpecimen(parser, fhirResourceFiler, csvHelper);
                    break;
                case ReferralRequest:
                    createOrDeleteReferralRequest(parser, fhirResourceFiler, csvHelper);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
            }
        }

        //if EMIS has a non-Condition code (e.g. family history) that's flagged as a problem, we'll create
        //a FHIR Condition (for the problem) as well as the FHIR FamilyMemberHistory. The above code will
        //sort out deleting the FamilyMemberHistory, so we also need to see if the same EMIS observation
        //was saved as a condition too
        if (wasSavedAsResourceType(fhirResourceFiler, parser.getPatientGuid(), parser.getObservationGuid(), ResourceType.Condition)) {
            createOrDeleteCondition(parser, fhirResourceFiler, csvHelper, true);
        }
    }


    /**
     * finds out what resource type an EMIS observation was previously saved as
     */
    private static ResourceType findOriginalTargetResourceType(FhirResourceFiler fhirResourceFiler, Observation parser) throws Exception {
        return findOriginalTargetResourceType(fhirResourceFiler, parser.getPatientGuid(), parser.getObservationGuid());
    }

    private static ResourceType findOriginalTargetResourceType(FhirResourceFiler fhirResourceFiler, CsvCell patientGuid, CsvCell observationGuid) throws Exception {

        List<ResourceType> potentialResourceTypes = new ArrayList<>();
        potentialResourceTypes.add(ResourceType.Observation);
        //potentialResourceTypes.add(ResourceType.Condition); //don't check this here - as conditions are handled differently
        potentialResourceTypes.add(ResourceType.Procedure);
        potentialResourceTypes.add(ResourceType.AllergyIntolerance);
        potentialResourceTypes.add(ResourceType.FamilyMemberHistory);
        potentialResourceTypes.add(ResourceType.Immunization);
        potentialResourceTypes.add(ResourceType.DiagnosticOrder);
        potentialResourceTypes.add(ResourceType.Specimen);
        potentialResourceTypes.add(ResourceType.DiagnosticReport);
        potentialResourceTypes.add(ResourceType.ReferralRequest);
        
        for (ResourceType resourceType: potentialResourceTypes) {
            if (wasSavedAsResourceType(fhirResourceFiler, patientGuid, observationGuid, resourceType)) {
                return resourceType;
            }
        }
        return null;
    }

    private static boolean wasSavedAsResourceType(FhirResourceFiler fhirResourceFiler, CsvCell patientGuid, CsvCell observationGuid, ResourceType resourceType) throws Exception {
        String sourceId = EmisCsvHelper.createUniqueId(patientGuid, observationGuid);
        Reference sourceReference = ReferenceHelper.createReference(resourceType, sourceId);
        Reference edsReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(sourceReference, fhirResourceFiler);
        return edsReference != null;
    }
    

    public static void createResource(Observation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version) throws Exception {

        //the code ID should NEVER be null, but the test data has nulls, so adding this to handle those rows gracefully
        if ((version.equalsIgnoreCase(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equalsIgnoreCase(EmisCsvToFhirTransformer.VERSION_5_1))
                && parser.getCodeId().isEmpty()) {
            return;
        }

        ResourceType resourceType = getTargetResourceType(parser, csvHelper);
        switch (resourceType) {
            case Observation:
                createOrDeleteObservation(parser, fhirResourceFiler, csvHelper);
                break;
            case Condition:
                createOrDeleteCondition(parser, fhirResourceFiler, csvHelper, true);
                break;
            case Procedure:
                createOrDeleteProcedure(parser, fhirResourceFiler, csvHelper);
                break;
            case AllergyIntolerance:
                createOrDeleteAllergy(parser, fhirResourceFiler, csvHelper);
                break;
            case FamilyMemberHistory:
                createOrDeleteFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                break;
            case Immunization:
                createOrDeleteImmunization(parser, fhirResourceFiler, csvHelper);
                break;
            case DiagnosticOrder:
                createOrDeleteDiagnosticOrder(parser, fhirResourceFiler, csvHelper);
                break;
            case DiagnosticReport:
                createOrDeleteDiagnosticReport(parser, fhirResourceFiler, csvHelper);
                break;
            case Specimen:
                createOrDeleteSpecimen(parser, fhirResourceFiler, csvHelper);
                break;
            case ReferralRequest:
                createOrDeleteReferralRequest(parser, fhirResourceFiler, csvHelper);
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        //if we didn't transform our record into a Condition, but the Problem CSV had a row for
        //it, then we'll also have part-created a Condition resource for it, which we need to finish populating
        if (resourceType != ResourceType.Condition
                && csvHelper.existsProblem(observationGuid, patientGuid)) {
            createOrDeleteCondition(parser, fhirResourceFiler, csvHelper, false);
        }

        //remove any cached links of child observations that link to the row we just processed. If the row used
        //the links, they'll already have been removed. If not, then we can't use them anyway.
        //csvHelper.getAndRemoveObservationParentRelationships(observationGuid, patientGuid);
    }

    /**
     * the FHIR resource type is roughly derived from the code category primarily, although the Value and ReadCode
     * are also used as it's not a perfect match.
     */
    public static ResourceType getTargetResourceType(Observation parser,
                                                     EmisCsvHelper csvHelper) throws Exception {

        CsvCell codeId = parser.getCodeId();
        ClinicalCodeType codeType = csvHelper.findClinicalCodeType(codeId);
        CsvCell value = parser.getValue();

        if (!value.isEmpty()
            || codeType == ClinicalCodeType.Biochemistry
            || codeType == ClinicalCodeType.Biological_Values
            || codeType == ClinicalCodeType.Cyology_Histology
            || codeType == ClinicalCodeType.Haematology
            || codeType == ClinicalCodeType.Health_Management
            || codeType == ClinicalCodeType.Immunology
            || codeType == ClinicalCodeType.Microbiology
            || codeType == ClinicalCodeType.Radiology
            || codeType == ClinicalCodeType.Symptoms_Findings
            || codeType == ClinicalCodeType.Procedure //note, the codes is this category aren't actually "procedures"
            || codeType == ClinicalCodeType.Adminisation_Documents_Attachments
            || codeType == ClinicalCodeType.Body_Structure //dental structures
            || codeType == ClinicalCodeType.Care_Episode_Outcome
            || codeType == ClinicalCodeType.Dental_Finding
            || codeType == ClinicalCodeType.Diagnostics
            || codeType == ClinicalCodeType.Discharged_From_Service
            || codeType == ClinicalCodeType.EMIS_Qualifier //not really suited to any FHIR resource, but they have to go somewhere
            || codeType == ClinicalCodeType.Ethnicity
            || codeType == ClinicalCodeType.HMP //not necessarily suited to FHIR observations, but seems the best match
            || codeType == ClinicalCodeType.Intervention_Category
            || codeType == ClinicalCodeType.Intervention_Target
            || codeType == ClinicalCodeType.KC60 //looks like these codes are a mix of procedures, conditions and vaccinations, but no way to distinguish them apart
            || codeType == ClinicalCodeType.Marital_Status
            || codeType == ClinicalCodeType.Nationality
            || codeType == ClinicalCodeType.Nursing_Problem
            || codeType == ClinicalCodeType.Nursing_Problem_Domain
            || codeType == ClinicalCodeType.Obsteterics_Birth
            || codeType == ClinicalCodeType.Person_Health_Social
            || codeType == ClinicalCodeType.Planned_Dental
            || codeType == ClinicalCodeType.Problem_Rating_Scale
            || codeType == ClinicalCodeType.Reason_For_Care
            || codeType == ClinicalCodeType.Referral_Activity
            || codeType == ClinicalCodeType.Referral_Rejected
            || codeType == ClinicalCodeType.Referral_Withdrawn
            || codeType == ClinicalCodeType.Regiment
            || codeType == ClinicalCodeType.Religion
            || codeType == ClinicalCodeType.Trade_Branch
            || codeType == ClinicalCodeType.Unset) {

            if (isDiagnosticReport(parser, csvHelper)) {
                return ResourceType.DiagnosticReport;
            } else {
                return ResourceType.Observation;
            }

        } else if (codeType == ClinicalCodeType.Conditions_Operations_Procedures) {

            if (isProcedure(codeId, csvHelper)) {
                return ResourceType.Procedure;
            } else if (isDisorder(codeId, csvHelper)) {
                return ResourceType.Condition;
            } else {
                return ResourceType.Observation;
            }

        } else if (codeType == ClinicalCodeType.Allergy_Adverse_Drug_Reations
            || codeType == ClinicalCodeType.Allergy_Adverse_Reations) {

            return ResourceType.AllergyIntolerance;

        } else if (codeType == ClinicalCodeType.Dental_Disorder) {

            return ResourceType.Condition;

        } else if (codeType == ClinicalCodeType.Dental_Procedure) {

            return ResourceType.Condition;

        } else if (codeType == ClinicalCodeType.Family_History) {

            return ResourceType.FamilyMemberHistory;

        } else if (codeType == ClinicalCodeType.Immunisations) {

            return ResourceType.Immunization;

        } else if (codeType == ClinicalCodeType.Investigation_Requests) {

            return ResourceType.DiagnosticOrder;

        } else if (codeType == ClinicalCodeType.Pathology_Specimen) {

            return ResourceType.Specimen;

        } else if (codeType == ClinicalCodeType.Referral) {

            return ResourceType.ReferralRequest;

        } else {
            throw new IllegalArgumentException("Unhandled codeType " + codeType);
        }
    }

    private static boolean isDisorder(CsvCell codeIdCell, EmisCsvHelper csvHelper) throws Exception {

        EmisCsvCodeMap codeMapping = csvHelper.findClinicalCode(codeIdCell);
        String system = EmisCodeHelper.getClinicalCodeSystemForReadCode(codeMapping);
        if (system.equals(FhirCodeUri.CODE_SYSTEM_READ2)) {
            String readCode = EmisCodeHelper.removeSynonymAndPadRead2Code(codeMapping);
            return Read2.isDisorder(readCode);
        }

        return false;
    }

    /*private static boolean isDisorder(CsvCell codeIdCell, EmisCsvHelper csvHelper) throws Exception {

        CodeableConcept fhirConcept = csvHelper.findClinicalCode(codeIdCell);
        for (Coding coding: fhirConcept.getCoding()) {

            //would prefer to check for procedures using Snomed, but this Read2 is simple and works
            if (coding.getSystem().equals(FhirCodeUri.CODE_SYSTEM_READ2)) {
                return Read2.isDisorder(coding.getCode());
            }

        }

        return false;
    }*/

    /*public static ResourceType getTargetResourceType(Observation parser,
                                                     CsvProcessor csvProcessor,
                                                     EmisCsvHelper csvHelper) throws Exception {

        String observationTypeString = parser.getObservationType();
        ObservationType observationType = ObservationType.fromValue(observationTypeString);
        Double value = parser.getValue();

        if (observationType == ObservationType.VALUE
                || observationType == ObservationType.INVESTIGATION
                || value != null) { //anything with a value, even if not labelled as a Value has to go into an Observation resource
            if (isDiagnosticReport(parser, csvProcessor, csvHelper)) {
                return ResourceType.DiagnosticReport;
            } else {
                return ResourceType.Observation;
            }

        } else if (observationType == ObservationType.ALLERGY) {
            return ResourceType.AllergyIntolerance;

        } else if (observationType == ObservationType.TEST_REQUEST) {
            return ResourceType.DiagnosticOrder;

        } else if (observationType == ObservationType.IMMUNISATION) {
            return ResourceType.Immunization;

        } else if (observationType == ObservationType.FAMILY_HISTORY) {
            return ResourceType.FamilyMemberHistory;

        } else if (observationType == ObservationType.REFERRAL) {
            return ResourceType.ReferralRequest;

        } else if (observationType == ObservationType.DOCUMENT) {
            return ResourceType.Observation;

        } else if (observationType == ObservationType.ANNOTATED_IMAGE) {
            return ResourceType.Observation;

        } else if (observationType == ObservationType.OBSERVATION) {
            if (isProcedure(parser, csvProcessor, csvHelper)) {
                return ResourceType.Procedure;
            } else {
                return ResourceType.Condition;
            }

        } else {
            throw new IllegalArgumentException("Unhandled ObservationType " + observationType);
        }
    }
*/
    private static boolean isDiagnosticReport(Observation parser,
                                              EmisCsvHelper csvHelper) throws Exception {

        //if it's got a value, it's not a diagnostic report, as it'll be an investigation within a report
        if (!parser.getValue().isEmpty()) {
            return false;
        }

        //if it doesn't have any child observations linking to it, then don't store as a report
        if (!csvHelper.hasChildObservations(parser.getObservationGuid(), parser.getPatientGuid())) {
            return false;
        }

        //if we pass the above checks, then check what kind of code it is. If one of the below types, then store as a report.
        CsvCell codeIdCell = parser.getCodeId();
        ClinicalCodeType codeType = csvHelper.findClinicalCodeType(codeIdCell);
        return codeType == ClinicalCodeType.Biochemistry
            || codeType == ClinicalCodeType.Cyology_Histology
            || codeType == ClinicalCodeType.Haematology
            || codeType == ClinicalCodeType.Immunology
            || codeType == ClinicalCodeType.Microbiology
            || codeType == ClinicalCodeType.Radiology
            || codeType == ClinicalCodeType.Health_Management;
    }

    private static boolean isProcedure(CsvCell codeIdCell,
                                       EmisCsvHelper csvHelper) throws Exception {

        EmisCsvCodeMap codeMapping = csvHelper.findClinicalCode(codeIdCell);

        String system = EmisCodeHelper.getClinicalCodeSystemForReadCode(codeMapping);
        if (system.equals(FhirCodeUri.CODE_SYSTEM_READ2)) {
            String readCode = EmisCodeHelper.removeSynonymAndPadRead2Code(codeMapping);
            return Read2.isProcedure(readCode);
        }

        return false;
    }

    private static void createOrDeleteReferralRequest(Observation parser,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      EmisCsvHelper csvHelper) throws Exception {

        //we have already parsed the ObservationReferral file, and will have created ReferralRequest
        //resources for all records in that file. So, first find any pre-created ReferralRequest for our record
        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        //as well as processing the Observation row into a FHIR resource, we
        //may also have a row in the Referral file that we've previously processed into
        //a FHIR ReferralRequest that we need to complete
        ReferralRequestBuilder referralRequestBuilder = csvHelper.findReferral(observationGuid, patientGuid);

        //if there's no existing builder, we've just got a record in the observation file without a corresponding one in the referrals file
        if (referralRequestBuilder == null) {
            //if we didn't have a record in the ObservationReferral file, we need to create a new one
            referralRequestBuilder = new ReferralRequestBuilder();

            EmisCsvHelper.setUniqueId(referralRequestBuilder, patientGuid, observationGuid);

            Reference patientReference = csvHelper.createPatientReference(patientGuid);
            referralRequestBuilder.setPatient(patientReference, patientGuid);
        }

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
            return;
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            referralRequestBuilder.setDate(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            referralRequestBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(referralRequestBuilder, false, codeId, null, csvHelper);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);

            //depending on what's alredy set on the referral from the separate referrals file, our clinician
            //may be the requester OR the recipient
            Reference existingRequester = referralRequestBuilder.getRequester();
            if (existingRequester == null) {
                //if no known requester already set, then our clinician is the requester
                referralRequestBuilder.setRequester(practitionerReference, clinicianGuid);

            } else {
                //in the referral file transform we set the requester to a reference to an organisation
                Reference ourOrgReference = csvHelper.createOrganisationReference(parser.getOrganisationGuid());

                //if the existing requester is OUR organisation, then replace the requester with the specific clinician we have
                if (ReferenceHelper.equals(existingRequester, ourOrgReference)) {
                    referralRequestBuilder.setRequester(practitionerReference, clinicianGuid);
                } else {
                    //if the referral didn't come FROM our organisation, then our clinician should be the recipient
                    referralRequestBuilder.addRecipient(practitionerReference, clinicianGuid);
                }
            }
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            referralRequestBuilder.setDescription(associatedText.getString(), associatedText);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            referralRequestBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date entererDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (entererDateTime != null) {
            referralRequestBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            referralRequestBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            referralRequestBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            referralRequestBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            referralRequestBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these fields are empty, as we don't stored them in this resource type,
        assertValueEmpty(referralRequestBuilder, parser);
        assertNumericUnitEmpty(referralRequestBuilder, parser);
        assertNumericRangeLowEmpty(referralRequestBuilder, parser);
        assertNumericRangeHighEmpty(referralRequestBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), referralRequestBuilder);

    }

    private static ResourceType findObservationType(EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, CsvCell patientGuidCell, CsvCell parentObservationCell) throws Exception {
        ResourceType parentResourceType = csvHelper.getCachedParentObservationResourceType(patientGuidCell, parentObservationCell);
        if (parentResourceType == null) {
            parentResourceType = findOriginalTargetResourceType(fhirResourceFiler, patientGuidCell, parentObservationCell);
        }
        return parentResourceType;
    }

    /*private static void createOrDeleteReferralRequest(Observation parser,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      EmisCsvHelper csvHelper) throws Exception {

        //we have already parsed the ObservationReferral file, and will have created ReferralRequest
        //resources for all records in that file. So, first find any pre-created ReferralRequest for our record
        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        //as well as processing the Observation row into a FHIR resource, we
        //may also have a row in the Referral file that we've previously processed into
        //a FHIR ReferralRequest that we need to complete
        ReferralRequest fhirReferral = csvHelper.findReferral(observationGuid, patientGuid);
        if (fhirReferral == null) {

            //if we didn't have a record in the ObservationReferral file, we need to create a new one
            fhirReferral = new ReferralRequest();
            fhirReferral.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_REFERRAL_REQUEST));

            EmisCsvHelper.setUniqueId(fhirReferral, patientGuid, observationGuid);

            fhirReferral.setPatient(csvHelper.createPatientReference(patientGuid));
        }

        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirReferral);
            return;
        }

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirReferral.setDateElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirReferral.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        Long codeId = parser.getCodeId();
        //after discussion, the observation code should go into the service requested field
        CodeableConcept codeableConcept = csvHelper.findClinicalCode(codeId);
        fhirReferral.addServiceRequested(codeableConcept);
        //fhirReferral.setType(csvHelper.findClinicalCode(codeId));

        String clinicianGuid = parser.getClinicianUserInRoleGuid();

        Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
        if (!fhirReferral.hasRequester()) {
            fhirReferral.setRequester(practitionerReference);
        } else {
            //in the referral file transform we set the requester to a reference to an organisation
            Reference requesterReference = fhirReferral.getRequester();
            Reference ourOrgReference = csvHelper.createOrganisationReference(parser.getOrganisationGuid());

            //if that requester is OUR organisation, then replace the requester with the specific clinician we have
            if (requesterReference.equalsShallow(ourOrgReference)) {
                fhirReferral.setRequester(practitionerReference);
            } else {
                //if the referral didn't come FROM our organisation, then our clinician should be the recipient
                fhirReferral.addRecipient(practitionerReference);
            }
        }

        String associatedText = parser.getAssociatedText();
        fhirReferral.setDescription(associatedText);

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirReferral, parser, csvHelper);
        addRecordedDateExtension(fhirReferral, parser);
        addDocumentExtension(fhirReferral, parser);
        addReviewExtension(fhirReferral, codeableConcept, parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirReferral, parser);

        //assert that these fields are empty, as we don't stored them in this resource type,
        assertValueEmpty(fhirReferral, parser);
        assertNumericUnitEmpty(fhirReferral, parser);
        assertNumericRangeLowEmpty(fhirReferral, parser);
        assertNumericRangeHighEmpty(fhirReferral, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirReferral);

    }*/

    private static void createOrDeleteDiagnosticOrder(Observation parser,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      EmisCsvHelper csvHelper) throws Exception {

        DiagnosticOrderBuilder diagnosticOrderBuilder = new DiagnosticOrderBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(diagnosticOrderBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        diagnosticOrderBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), diagnosticOrderBuilder);
            return;
        }

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            diagnosticOrderBuilder.setOrderedBy(practitionerReference, clinicianGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            diagnosticOrderBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(diagnosticOrderBuilder, false, codeId, null, csvHelper);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            diagnosticOrderBuilder.setNote(associatedText.getString(), associatedText);
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        diagnosticOrderBuilder.setDateTime(dateTimeType, effectiveDate, effectiveDatePrecision);

        //we don't know anything other than it was requested
        diagnosticOrderBuilder.setStatus(DiagnosticOrder.DiagnosticOrderStatus.REQUESTED);

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            diagnosticOrderBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (enteredDateTime != null) {
            diagnosticOrderBuilder.setRecordedDate(enteredDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            diagnosticOrderBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            diagnosticOrderBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            diagnosticOrderBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            diagnosticOrderBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(diagnosticOrderBuilder, parser);
        assertNumericUnitEmpty(diagnosticOrderBuilder, parser);
        assertNumericRangeLowEmpty(diagnosticOrderBuilder, parser);
        assertNumericRangeHighEmpty(diagnosticOrderBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), diagnosticOrderBuilder);
    }

    /*private static void createOrDeleteDiagnosticOrder(Observation parser,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      EmisCsvHelper csvHelper) throws Exception {
        DiagnosticOrder fhirOrder = new DiagnosticOrder();
        fhirOrder.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_DIAGNOSTIC_ORDER));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirOrder, patientGuid, observationGuid);

        fhirOrder.setSubject(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirOrder);
            return;
        }

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        fhirOrder.setOrderer(csvHelper.createPractitionerReference(clinicianGuid));

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirOrder.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        Long codeId = parser.getCodeId();
        DiagnosticOrder.DiagnosticOrderItemComponent diagnosticOrderItemComponent = fhirOrder.addItem();
        diagnosticOrderItemComponent.setCode(csvHelper.findClinicalCode(codeId));

        String associatedText = parser.getAssociatedText();
        fhirOrder.addNote(AnnotationHelper.createAnnotation(associatedText));

        DiagnosticOrder.DiagnosticOrderEventComponent diagnosticOrderEventComponent = fhirOrder.addEvent();
        diagnosticOrderEventComponent.setStatus(DiagnosticOrder.DiagnosticOrderStatus.REQUESTED);

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        diagnosticOrderEventComponent.setDateTimeElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirOrder, parser, csvHelper);
        addRecordedDateExtension(fhirOrder, parser);
        addDocumentExtension(fhirOrder, parser);
        addReviewExtension(fhirOrder, diagnosticOrderItemComponent.getCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirOrder, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirOrder, parser);
        assertNumericUnitEmpty(fhirOrder, parser);
        assertNumericRangeLowEmpty(fhirOrder, parser);
        assertNumericRangeHighEmpty(fhirOrder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirOrder);
    }*/

    private static void createOrDeleteSpecimen(Observation parser, FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        SpecimenBuilder specimenBuilder = new SpecimenBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(specimenBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        specimenBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), specimenBuilder);
            return;
        }

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            specimenBuilder.setCollectedBy(practitionerReference, clinicianGuid);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(specimenBuilder, false, codeId, null, csvHelper);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            specimenBuilder.setNotes(associatedText.getString(), associatedText);
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            specimenBuilder.setCollectedDate(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (enteredDateTime != null) {
            specimenBuilder.setRecordedDate(enteredDateTime, enteredDate, enteredTime);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            specimenBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference reference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            specimenBuilder.setEncounter(reference, consultationGuid);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            specimenBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            specimenBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            specimenBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            specimenBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(specimenBuilder, parser);
        assertNumericUnitEmpty(specimenBuilder, parser);
        assertNumericRangeLowEmpty(specimenBuilder, parser);
        assertNumericRangeHighEmpty(specimenBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), specimenBuilder);
    }

    /*private static void createOrDeleteSpecimen(Observation parser, FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        Specimen fhirSpecimen = new Specimen();
        fhirSpecimen.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_SPECIMIN));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirSpecimen, patientGuid, observationGuid);

        fhirSpecimen.setSubject(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirSpecimen);
            return;
        }

        Specimen.SpecimenCollectionComponent fhirCollection = new Specimen.SpecimenCollectionComponent();
        fhirSpecimen.setCollection(fhirCollection);

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        fhirCollection.setCollector(csvHelper.createPractitionerReference(clinicianGuid));

        Long codeId = parser.getCodeId();
        fhirSpecimen.setType(csvHelper.findClinicalCode(codeId));

        String associatedText = parser.getAssociatedText();
        fhirCollection.addComment(associatedText);

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirCollection.setCollected(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        //the entered date and person are stored in extensions
        addEncounterExtension(fhirSpecimen, parser, csvHelper, patientGuid);
        addRecordedByExtension(fhirSpecimen, parser, csvHelper);
        addRecordedDateExtension(fhirSpecimen, parser);
        addDocumentExtension(fhirSpecimen, parser);
        addReviewExtension(fhirSpecimen, fhirSpecimen.getType(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirSpecimen, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirSpecimen, parser);
        assertNumericUnitEmpty(fhirSpecimen, parser);
        assertNumericRangeLowEmpty(fhirSpecimen, parser);
        assertNumericRangeHighEmpty(fhirSpecimen, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirSpecimen);
    }*/

    private static void createOrDeleteAllergy(Observation parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              EmisCsvHelper csvHelper) throws Exception {

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(allergyIntoleranceBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        allergyIntoleranceBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
            return;
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        allergyIntoleranceBuilder.setRecordedDate(enteredDateTime, enteredDate, enteredTime);

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        Reference enteredByReference = csvHelper.createPractitionerReference(enteredByGuid);
        allergyIntoleranceBuilder.setRecordedBy(enteredByReference, enteredByGuid);

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        allergyIntoleranceBuilder.setOnsetDate(dateTimeType, effectiveDate, effectiveDatePrecision);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
        allergyIntoleranceBuilder.setClinician(practitionerReference, clinicianGuid);

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(allergyIntoleranceBuilder, false, codeId, null, csvHelper);

        CsvCell associatedText = parser.getAssociatedText();
        allergyIntoleranceBuilder.setNote(associatedText.getString(), associatedText);

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            allergyIntoleranceBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference reference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            allergyIntoleranceBuilder.setEncounter(reference, consultationGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            allergyIntoleranceBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            allergyIntoleranceBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            allergyIntoleranceBuilder.setParentResource(parentReference, parentObservationCell);
        }

        assertValueEmpty(allergyIntoleranceBuilder, parser);
        assertNumericUnitEmpty(allergyIntoleranceBuilder, parser);
        assertNumericRangeLowEmpty(allergyIntoleranceBuilder, parser);
        assertNumericRangeHighEmpty(allergyIntoleranceBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }
    /*private static void createOrDeleteAllergy(Observation parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              EmisCsvHelper csvHelper) throws Exception {

        AllergyIntolerance fhirAllergy = new AllergyIntolerance();
        fhirAllergy.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ALLERGY_INTOLERANCE));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirAllergy, patientGuid, observationGuid);

        fhirAllergy.setPatient(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirAllergy);
            return;
        }

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        fhirAllergy.setRecorder(csvHelper.createPractitionerReference(clinicianGuid));

        Date enteredDate = parser.getEnteredDateTime();
        fhirAllergy.setRecordedDate(enteredDate);

        addRecordedByExtension(fhirAllergy, parser, csvHelper);
        addDocumentExtension(fhirAllergy, parser);

        Long codeId = parser.getCodeId();
        fhirAllergy.setSubstance(csvHelper.findClinicalCode(codeId));

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirAllergy.setOnsetElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String associatedText = parser.getAssociatedText();
        fhirAllergy.setNote(AnnotationHelper.createAnnotation(associatedText));

        addEncounterExtension(fhirAllergy, parser, csvHelper, patientGuid);
        addReviewExtension(fhirAllergy, fhirAllergy.getSubstance(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirAllergy, parser);

        assertValueEmpty(fhirAllergy, parser);
        assertNumericUnitEmpty(fhirAllergy, parser);
        assertNumericRangeLowEmpty(fhirAllergy, parser);
        assertNumericRangeHighEmpty(fhirAllergy, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirAllergy);
    }*/

    private static void createOrDeleteDiagnosticReport(Observation parser,
                                                       FhirResourceFiler fhirResourceFiler,
                                                       EmisCsvHelper csvHelper) throws Exception {
        DiagnosticReportBuilder diagnosticReportBuilder = new DiagnosticReportBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(diagnosticReportBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        diagnosticReportBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), diagnosticReportBuilder);
            return;
        }

        //assume that any report already filed into Emis Web is a final report
        diagnosticReportBuilder.setStatus(DiagnosticReport.DiagnosticReportStatus.FINAL);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(clinicianGuid);
            diagnosticReportBuilder.setFiledBy(reference, clinicianGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            diagnosticReportBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(diagnosticReportBuilder, false, codeId, null, csvHelper);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            diagnosticReportBuilder.setConclusion(associatedText.getString(), associatedText);
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType effectiveDateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (effectiveDateTimeType != null) {
            diagnosticReportBuilder.setEffectiveDate(effectiveDateTimeType, effectiveDate, effectiveDatePrecision);
        }

        ReferenceList childObservations = csvHelper.getAndRemoveObservationParentRelationships(diagnosticReportBuilder.getResourceId());
        if (childObservations != null) {
            for (int i=0; i<childObservations.size(); i++) {
                Reference reference = childObservations.getReference(i);
                CsvCell[] sourceCells = childObservations.getSourceCells(i);
                diagnosticReportBuilder.addResult(reference, sourceCells);
            }
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            diagnosticReportBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date entererDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (entererDateTime != null) {
            diagnosticReportBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            diagnosticReportBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            diagnosticReportBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            diagnosticReportBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            diagnosticReportBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(diagnosticReportBuilder, parser);
        assertNumericUnitEmpty(diagnosticReportBuilder, parser);
        assertNumericRangeLowEmpty(diagnosticReportBuilder, parser);
        assertNumericRangeHighEmpty(diagnosticReportBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), diagnosticReportBuilder);

    }

    /*private static void createOrDeleteDiagnosticReport(Observation parser,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      EmisCsvHelper csvHelper) throws Exception {
        DiagnosticReport fhirReport = new DiagnosticReport();
        fhirReport.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_DIAGNOSTIC_REPORT));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirReport, patientGuid, observationGuid);

        fhirReport.setSubject(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirReport);
            return;
        }

        fhirReport.setStatus(DiagnosticReport.DiagnosticReportStatus.FINAL);

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!Strings.isNullOrEmpty(clinicianGuid)) {
            Reference reference = csvHelper.createPractitionerReference(clinicianGuid);
            fhirReport.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.DIAGNOSTIC_REPORT_FILED_BY, reference));
        }

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirReport.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        Long codeId = parser.getCodeId();
        fhirReport.setCode(csvHelper.findClinicalCode(codeId));

        String associatedText = parser.getAssociatedText();
        if (!Strings.isNullOrEmpty(associatedText)) {
            fhirReport.setConclusion(associatedText);
        }

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirReport.setEffective(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        List<String> childObservations = csvHelper.getAndRemoveObservationParentRelationships(observationGuid, patientGuid);
        if (childObservations != null) {
            List<Reference> references = ReferenceHelper.createReferences(childObservations);
            for (Reference reference : references) {
                fhirReport.getResult().add(reference);
            }
        }

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirReport, parser, csvHelper);
        addRecordedDateExtension(fhirReport, parser);
        addDocumentExtension(fhirReport, parser);
        addReviewExtension(fhirReport, fhirReport.getCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirReport, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirReport, parser);
        assertNumericUnitEmpty(fhirReport, parser);
        assertNumericRangeLowEmpty(fhirReport, parser);
        assertNumericRangeHighEmpty(fhirReport, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirReport);

    }*/

    private static void createOrDeleteProcedure(Observation parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                EmisCsvHelper csvHelper) throws Exception {

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(procedureBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        procedureBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
            return;
        }

        //by definition, we only receive procedures that are completed
        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(procedureBuilder, false, codeId, ProcedureBuilder.TAG_CODEABLE_CONCEPT_CODE, csvHelper);

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            procedureBuilder.setPerformed(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            procedureBuilder.addPerformer(practitionerReference, clinicianGuid);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            procedureBuilder.addNotes(associatedText.getString());
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference reference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            procedureBuilder.setEncounter(reference, consultationGuid);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            procedureBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date entererDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (entererDateTime != null) {
            procedureBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            procedureBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            procedureBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            procedureBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            procedureBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(procedureBuilder, parser);
        assertNumericUnitEmpty(procedureBuilder, parser);
        assertNumericRangeLowEmpty(procedureBuilder, parser);
        assertNumericRangeHighEmpty(procedureBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }



    /*private static void createOrDeleteProcedure(Observation parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                EmisCsvHelper csvHelper) throws Exception {

        Procedure fhirProcedure = new Procedure();
        fhirProcedure.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PROCEDURE));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirProcedure, patientGuid, observationGuid);

        fhirProcedure.setSubject(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirProcedure);
            return;
        }

        fhirProcedure.setStatus(Procedure.ProcedureStatus.COMPLETED);

        Long codeId = parser.getCodeId();
        fhirProcedure.setCode(csvHelper.findClinicalCode(codeId));

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirProcedure.setPerformed(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        Procedure.ProcedurePerformerComponent fhirPerformer = fhirProcedure.addPerformer();
        fhirPerformer.setActor(csvHelper.createPractitionerReference(clinicianGuid));

        String associatedText = parser.getAssociatedText();
        fhirProcedure.addNotes(AnnotationHelper.createAnnotation(associatedText));

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirProcedure.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirProcedure, parser, csvHelper);
        addRecordedDateExtension(fhirProcedure, parser);
        addDocumentExtension(fhirProcedure, parser);
        addReviewExtension(fhirProcedure, fhirProcedure.getCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirProcedure, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirProcedure, parser);
        assertNumericUnitEmpty(fhirProcedure, parser);
        assertNumericRangeLowEmpty(fhirProcedure, parser);
        assertNumericRangeHighEmpty(fhirProcedure, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirProcedure);
    }*/

    private static void createOrDeleteCondition(Observation parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                EmisCsvHelper csvHelper,
                                                boolean validateUnusedFields) throws Exception {

        //we have already parsed the Problem file, and will have created Condition
        //resources for all records in that file. So, first find any pre-created Condition for our record
        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        ConditionBuilder conditionBuilder = csvHelper.findProblem(observationGuid, patientGuid);

        //if we didn't find a Condtion from the problem map, then it's not a problem and should be
        //treated just as a standalone condition resource
        if (conditionBuilder == null) {
            //if we didn't have a record in the Problem file, we need to create a new one
            conditionBuilder = new ConditionBuilder();
            conditionBuilder.setAsProblem(false); //but it's NOT a problem

            EmisCsvHelper.setUniqueId(conditionBuilder, patientGuid, observationGuid);

            Reference patientReference = csvHelper.createPatientReference(patientGuid);
            conditionBuilder.setPatient(patientReference, patientGuid);
        }

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(clinicianGuid);
            conditionBuilder.setClinician(reference, clinicianGuid);
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            conditionBuilder.setOnset(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        conditionBuilder.setRecordedDate(enteredDateTime, enteredDate, enteredTime);

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(conditionBuilder, false, codeId, ConditionBuilder.TAG_CODEABLE_CONCEPT_CODE, csvHelper);

        //we don't have enough information to set this accurately, so taking out
        //fhirCondition.setClinicalStatus("active"); //if we have a Problem record for this condition, this status may be changed

        //can't be confident this is true, so don't set
        //fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            conditionBuilder.setNotes(associatedText.getString(), associatedText);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            conditionBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell problemGuid = parser.getProblemGuid();
        if (!problemGuid.isEmpty()) {
            Reference problemReference = csvHelper.createProblemReference(problemGuid, patientGuid);
            conditionBuilder.setPartOfProblem(problemReference, problemGuid);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            conditionBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            conditionBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            conditionBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            conditionBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            conditionBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        //but only if we've passed in the boolean to say so - if this is false, we've already processed this
        //row in the CSV into a different resource type, so will have used the values and don't need to worry we're ignoring them now
        if (validateUnusedFields) {
            assertValueEmpty(conditionBuilder, parser);
            assertNumericUnitEmpty(conditionBuilder, parser);
            assertNumericRangeLowEmpty(conditionBuilder, parser);
            assertNumericRangeHighEmpty(conditionBuilder, parser);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }

    /*private static void createOrDeleteCondition(Observation parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                EmisCsvHelper csvHelper,
                                                boolean validateUnusedFields) throws Exception {

        //we have already parsed the Problem file, and will have created Condition
        //resources for all records in that file. So, first find any pre-created Condition for our record
        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        //as well as processing the Observation row into a FHIR resource, we
        //may also have a row in the Problem file that we've previously processed into
        //a FHIR Condition that we need to complete
        Condition fhirCondition = csvHelper.findProblem(observationGuid, patientGuid);

        //if we didn't find a Condtion from the problem map, then it's not a problem and should be
        //treated just as a standalone condition resource
        if (fhirCondition == null) {

            //if we didn't have a record in the Problem file, we need to create a new one
            fhirCondition = new Condition();
            fhirCondition.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_CONDITION));

            EmisCsvHelper.setUniqueId(fhirCondition, patientGuid, observationGuid);

            fhirCondition.setPatient(csvHelper.createPatientReference(patientGuid));
        }

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirCondition);
            return;
        }

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        fhirCondition.setAsserter(csvHelper.createPractitionerReference(clinicianGuid));

        Date enteredDate = parser.getEnteredDateTime();
        fhirCondition.setDateRecorded(enteredDate);

        Long codeId = parser.getCodeId();
        fhirCondition.setCode(csvHelper.findClinicalCode(codeId));

        //we don't have enough information to set this accurately, so taking out
        //fhirCondition.setClinicalStatus("active"); //if we have a Problem record for this condition, this status may be changed

        fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirCondition.setOnset(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String associatedText = parser.getAssociatedText();
        //if the condition is a problem, there may already be text in the note variable, from the Comment field on the problem table
        if (fhirCondition.hasNotes()) {
            fhirCondition.setNotes(associatedText + "\n" + fhirCondition.getNotes());
        } else {
            fhirCondition.setNotes(associatedText);
        }

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirCondition.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        String problemGuid = parser.getProblemGuid();
        if (!Strings.isNullOrEmpty(problemGuid)) {
            Reference problemReference = csvHelper.createProblemReference(problemGuid, patientGuid);
            fhirCondition.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.CONDITION_PART_OF_PROBLEM, problemReference));
        }

        //the entered by is stored in an extension
        addRecordedByExtension(fhirCondition, parser, csvHelper);
        addDocumentExtension(fhirCondition, parser);
        addReviewExtension(fhirCondition, fhirCondition.getCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirCondition, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        //but only if we've passed in the boolean to say so - if this is false, we've already processed this
        //row in the CSV into a different resource type, so will have used the values and don't need to worry we're ignoring them now
        if (validateUnusedFields) {
            assertValueEmpty(fhirCondition, parser);
            assertNumericUnitEmpty(fhirCondition, parser);
            assertNumericRangeLowEmpty(fhirCondition, parser);
            assertNumericRangeHighEmpty(fhirCondition, parser);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirCondition);
    }*/

    private static void createOrDeleteObservation(Observation parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  EmisCsvHelper csvHelper) throws Exception {

        ObservationBuilder observationBuilder = new ObservationBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(observationBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        observationBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), observationBuilder);
            return;
        }

        //status is mandatory, so set the only value we can
        observationBuilder.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(observationBuilder, false, codeId, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT, csvHelper);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            observationBuilder.setClinician(practitionerReference, clinicianGuid);
        }

        CsvCell value = parser.getValue();
        if (!value.isEmpty()) {
            observationBuilder.setValue(value.getDouble(), value);
        }

        CsvCell units = parser.getNumericUnit();
        if (!units.isEmpty()) {
            observationBuilder.setUnits(units.getString(), units);
        }

        CsvCell low = parser.getNumericRangeLow();
        CsvCell high = parser.getNumericRangeHigh();

        if (!low.isEmpty() || !high.isEmpty()) {

            //going by how lab results were defined in the pathology spec, if we have upper and lower bounds,
            //it's an inclusive range. If we only have one bound, then it's non-inclusive.
            if (!low.isEmpty() && !high.isEmpty()) {
                observationBuilder.setRecommendedRangeLow(low.getDouble(), units.getString(), Quantity.QuantityComparator.GREATER_OR_EQUAL, low, units);
                observationBuilder.setRecommendedRangeHigh(high.getDouble(), units.getString(), Quantity.QuantityComparator.LESS_OR_EQUAL, high, units);

            } else if (!low.isEmpty()) {
                observationBuilder.setRecommendedRangeLow(low.getDouble(), units.getString(), Quantity.QuantityComparator.GREATER_THAN, low, units);

            } else {
                observationBuilder.setRecommendedRangeHigh(high.getDouble(), units.getString(), Quantity.QuantityComparator.LESS_THAN, high, units);
            }
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            observationBuilder.setNotes(associatedText.getString(), associatedText);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            observationBuilder.setEncounter(encounterReference, consultationGuid);
        }

        ReferenceList childObservations = csvHelper.getAndRemoveObservationParentRelationships(observationBuilder.getResourceId());
        if (childObservations != null) {
            for (int i=0; i<childObservations.size(); i++) {
                Reference reference = childObservations.getReference(i);
                CsvCell[] sourceCells = childObservations.getSourceCells(i);

                observationBuilder.addChildObservation(reference, sourceCells);
            }
        }

        //if we have BP readings from child observations, include them in the components for this observation too
        List<BpComponent> bpComponents = csvHelper.findBpComponents(observationGuid, patientGuid);
        if (bpComponents != null) {
            for (BpComponent bpComponent: bpComponents) {
                CsvCell bpCodeId = bpComponent.getCodeId();
                CsvCell bpValue = bpComponent.getValue();
                CsvCell bpUnit = bpComponent.getUnit();

                observationBuilder.addComponent();
                observationBuilder.setComponentValue(bpValue.getDouble(), bpValue);
                observationBuilder.setComponentUnit(bpUnit.getString(), bpUnit);

                EmisCodeHelper.createCodeableConcept(observationBuilder, false, bpCodeId, ObservationBuilder.TAG_COMPONENT_CODEABLE_CONCEPT, csvHelper);
            }
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            observationBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date entererDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (entererDateTime != null) {
            observationBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            observationBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT, parser, csvHelper, fhirResourceFiler)) {
            observationBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            observationBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            observationBuilder.setParentResource(parentReference, parentObservationCell);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
    }

    /*private static void createOrDeleteObservation(Observation parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  EmisCsvHelper csvHelper) throws Exception {

        org.hl7.fhir.instance.model.Observation fhirObservation = new org.hl7.fhir.instance.model.Observation();
        fhirObservation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_OBSERVATION));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirObservation, patientGuid, observationGuid);

        fhirObservation.setSubject(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirObservation);
            return;
        }

        fhirObservation.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        Long codeId = parser.getCodeId();
        fhirObservation.setCode(csvHelper.findClinicalCode(codeId));

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        fhirObservation.addPerformer(csvHelper.createPractitionerReference(clinicianGuid));

        Double value = parser.getValue();
        String units = parser.getNumericUnit();
        fhirObservation.setValue(QuantityHelper.createQuantity(value, units));

        Double low = parser.getNumericRangeLow();
        Double high = parser.getNumericRangeHigh();

        if (low != null || high != null) {

            org.hl7.fhir.instance.model.Observation.ObservationReferenceRangeComponent fhirRange = fhirObservation.addReferenceRange();
            if (low != null && high != null) {
                fhirRange.setLow(QuantityHelper.createSimpleQuantity(low, units, Quantity.QuantityComparator.GREATER_OR_EQUAL));
                fhirRange.setHigh(QuantityHelper.createSimpleQuantity(high, units, Quantity.QuantityComparator.LESS_OR_EQUAL));
            } else if (low != null) {
                fhirRange.setLow(QuantityHelper.createSimpleQuantity(low, units, Quantity.QuantityComparator.GREATER_THAN));
            } else {
                fhirRange.setHigh(QuantityHelper.createSimpleQuantity(high, units, Quantity.QuantityComparator.LESS_THAN));
            }
        }

        String associatedText = parser.getAssociatedText();
        fhirObservation.setComments(associatedText);

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirObservation.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        List<String> childObservations = csvHelper.getAndRemoveObservationParentRelationships(fhirObservation.getId());
        if (childObservations != null) {
            List<Reference> references = ReferenceHelper.createReferences(childObservations);
            for (Reference reference : references) {
                org.hl7.fhir.instance.model.Observation.ObservationRelatedComponent fhirRelation = fhirObservation.addRelated();
                fhirRelation.setType(org.hl7.fhir.instance.model.Observation.ObservationRelationshipType.HASMEMBER);
                fhirRelation.setTarget(reference);
            }
        }

        //if we have BP readings from child observations, include them in the components for this observation too
        List<org.hl7.fhir.instance.model.Observation.ObservationComponentComponent> observationComponents = csvHelper.findBpComponents(observationGuid, patientGuid);
        if (observationComponents != null) {
            for (org.hl7.fhir.instance.model.Observation.ObservationComponentComponent component: observationComponents) {
                fhirObservation.getComponent().add(component);
            }
        }

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirObservation, parser, csvHelper);
        addRecordedDateExtension(fhirObservation, parser);
        addDocumentExtension(fhirObservation, parser);
        addReviewExtension(fhirObservation, fhirObservation.getCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirObservation, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirObservation);
    }*/

    private static void createOrDeleteFamilyMemberHistory(Observation parser,
                                                          FhirResourceFiler fhirResourceFiler,
                                                          EmisCsvHelper csvHelper) throws Exception {

        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(familyMemberHistoryBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        familyMemberHistoryBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
            return;
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            familyMemberHistoryBuilder.setDate(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        //status is mandatory, so set the only possible status we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        familyMemberHistoryBuilder.setRelationship(FamilyMember.FAMILY_MEMBER);

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(familyMemberHistoryBuilder, false, codeId, null, csvHelper);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            familyMemberHistoryBuilder.setNotes(associatedText.getString(), associatedText);
        }

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            familyMemberHistoryBuilder.setClinician(practitionerReference, clinicianGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference reference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            familyMemberHistoryBuilder.setEncounter(reference, consultationGuid);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            familyMemberHistoryBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date entererDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (entererDateTime != null) {
            familyMemberHistoryBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            familyMemberHistoryBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            familyMemberHistoryBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            familyMemberHistoryBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            familyMemberHistoryBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(familyMemberHistoryBuilder, parser);
        assertNumericUnitEmpty(familyMemberHistoryBuilder, parser);
        assertNumericRangeLowEmpty(familyMemberHistoryBuilder, parser);
        assertNumericRangeHighEmpty(familyMemberHistoryBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }

    /*private static void createOrDeleteFamilyMemberHistory(Observation parser,
                                                          FhirResourceFiler fhirResourceFiler,
                                                          EmisCsvHelper csvHelper) throws Exception {

        FamilyMemberHistory fhirFamilyHistory = new FamilyMemberHistory();
        fhirFamilyHistory.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_FAMILY_MEMBER_HISTORY));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirFamilyHistory, patientGuid, observationGuid);

        fhirFamilyHistory.setPatient(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirFamilyHistory);
            return;
        }

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirFamilyHistory.setDateElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        fhirFamilyHistory.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        fhirFamilyHistory.setRelationship(CodeableConceptHelper.createCodeableConcept(FamilyMember.FAMILY_MEMBER));

        FamilyMemberHistory.FamilyMemberHistoryConditionComponent fhirCondition = fhirFamilyHistory.addCondition();

        Long codeId = parser.getCodeId();
        fhirCondition.setCode(csvHelper.findClinicalCode(codeId));

        String associatedText = parser.getAssociatedText();
        fhirCondition.setNote(AnnotationHelper.createAnnotation(associatedText));

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        Reference reference = csvHelper.createPractitionerReference(clinicianGuid);
        fhirFamilyHistory.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.FAMILY_MEMBER_HISTORY_REPORTED_BY, reference));

        //the entered date and person are stored in extensions
        addEncounterExtension(fhirFamilyHistory, parser, csvHelper, patientGuid);
        addRecordedByExtension(fhirFamilyHistory, parser, csvHelper);
        addRecordedDateExtension(fhirFamilyHistory, parser);
        addDocumentExtension(fhirFamilyHistory, parser);
        addReviewExtension(fhirFamilyHistory, fhirCondition.getCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirFamilyHistory, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirFamilyHistory, parser);
        assertNumericUnitEmpty(fhirFamilyHistory, parser);
        assertNumericRangeLowEmpty(fhirFamilyHistory, parser);
        assertNumericRangeHighEmpty(fhirFamilyHistory, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirFamilyHistory);
    }*/

    private static void createOrDeleteImmunization(Observation parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   EmisCsvHelper csvHelper) throws Exception {

        ImmunizationBuilder immunizationBuilder = new ImmunizationBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(immunizationBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        immunizationBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), immunizationBuilder);
            return;
        }

        //these fields are mandatory so set to what we know
        immunizationBuilder.setStatus(ImmunizationStatus.COMPLETED.getCode()); //we know it was given
        immunizationBuilder.setWasNotGiven(false); //we know it was given
        immunizationBuilder.setReported(false); //assume it was adminsitered by the practice

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (dateTimeType != null) {
            immunizationBuilder.setPerformedDate(dateTimeType, effectiveDate, effectiveDatePrecision);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(immunizationBuilder, false, codeId, null, csvHelper);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            immunizationBuilder.setPerformer(practitionerReference, clinicianGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            immunizationBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            immunizationBuilder.setNote(associatedText.getString(), associatedText);
        }


        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            immunizationBuilder.setRecordedBy(reference, enteredByGuid);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date entererDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (entererDateTime != null) {
            immunizationBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            immunizationBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, null, parser, csvHelper, fhirResourceFiler)) {
            immunizationBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            immunizationBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
            immunizationBuilder.setParentResource(parentReference, parentObservationCell);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(immunizationBuilder, parser);
        assertNumericUnitEmpty(immunizationBuilder, parser);
        assertNumericRangeLowEmpty(immunizationBuilder, parser);
        assertNumericRangeHighEmpty(immunizationBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), immunizationBuilder);
    }

    /*private static void createOrDeleteImmunization(Observation parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   EmisCsvHelper csvHelper) throws Exception {

        Immunization fhirImmunisation = new Immunization();
        fhirImmunisation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_IMMUNIZATION));

        String observationGuid = parser.getObservationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirImmunisation, patientGuid, observationGuid);

        fhirImmunisation.setPatient(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirImmunisation);
            return;
        }

        fhirImmunisation.setStatus(ImmunizationStatus.COMPLETED.getCode());
        fhirImmunisation.setWasNotGiven(false);
        fhirImmunisation.setReported(false);

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirImmunisation.setDateElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        Long codeId = parser.getCodeId();
        fhirImmunisation.setVaccineCode(csvHelper.findClinicalCode(codeId));

        String clinicianGuid = parser.getClinicianUserInRoleGuid();
        Reference reference = csvHelper.createPractitionerReference(clinicianGuid);
        fhirImmunisation.setPerformer(reference);

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            reference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            fhirImmunisation.setEncounter(reference);
        }

        String associatedText = parser.getAssociatedText();
        fhirImmunisation.addNote(AnnotationHelper.createAnnotation(associatedText));

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirImmunisation, parser, csvHelper);
        addRecordedDateExtension(fhirImmunisation, parser);
        addDocumentExtension(fhirImmunisation, parser);
        addReviewExtension(fhirImmunisation, fhirImmunisation.getVaccineCode(), parser, csvHelper, fhirResourceFiler);
        addConfidentialExtension(fhirImmunisation, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirImmunisation, parser);
        assertNumericUnitEmpty(fhirImmunisation, parser);
        assertNumericRangeLowEmpty(fhirImmunisation, parser);
        assertNumericRangeHighEmpty(fhirImmunisation, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirImmunisation);
    }*/

    /*private static void addDocumentExtension(DomainResource resource, Observation parser) {

        String documentGuid = parser.getDocumentGuid();
        if (Strings.isNullOrEmpty(documentGuid)) {
            return;
        }

        Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid);
        resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.EXTERNAL_DOCUMENT, fhirIdentifier));
    }

    private static void addRecordedByExtension(DomainResource resource, Observation parser, EmisCsvHelper emisCsvHelper) throws Exception {
        String enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (Strings.isNullOrEmpty(enteredByGuid)) {
            return;
        }

        Reference reference = emisCsvHelper.createPractitionerReference(enteredByGuid);
        resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
    }

    private static void addRecordedDateExtension(DomainResource resource, Observation parser) throws Exception {
        Date enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime == null) {
            return;
        }

        resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
    }

    private static void addReviewExtension(DomainResource resource, CodeableConcept codeableConcept, Observation parser,
																					 EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        String problemGuid = parser.getProblemGuid();
        if (Strings.isNullOrEmpty(problemGuid)) {
            return;
        }

        //find the original code our problem was coded with
        String patientGuid = parser.getPatientGuid();
        String problemReadCode = csvHelper.findProblemObservationReadCode(patientGuid, problemGuid, fhirResourceFiler);
        if (Strings.isNullOrEmpty(problemReadCode)) {
            return;
        }

        //find the original code our current observation is coded with
        String observationReadCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
        if (!problemReadCode.equals(observationReadCode)) {
            //if the codes differ, then return out
            return;
        }

        //if the codes are the same, our current observation is a review of the problem
        Extension extension = ExtensionConverter.createExtension(FhirExtensionUri.IS_REVIEW, new BooleanType(true));
        resource.addExtension(extension);
    }

    private static void addConfidentialExtension(DomainResource resource, Observation parser) {
        if (parser.getIsConfidential()) {
            resource.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
        }
    }

    private static boolean isReview(CodeableConcept codeableConcept, Observation parser,
                                    EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        String problemGuid = parser.getProblemGuid();
        if (Strings.isNullOrEmpty(problemGuid)) {
            return false;
        }

        //find the original code our problem was coded with
        String patientGuid = parser.getPatientGuid();
        String problemReadCode = csvHelper.findProblemObservationReadCode(patientGuid, problemGuid, fhirResourceFiler);
        if (Strings.isNullOrEmpty(problemReadCode)) {
            return false;
        }

        //find the original code our current observation is coded with
        String observationReadCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
        if (!problemReadCode.equals(observationReadCode)) {
            //if the codes differ, then return out
            return false;
        }

        return true;
    }

    private static void addEncounterExtension(DomainResource resource, Observation parser, EmisCsvHelper csvHelper, String patientGuid) throws Exception {

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {

            Reference reference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ASSOCIATED_ENCOUNTER, reference));
        }
    }

    private static void assertValueEmpty(Resource destinationResource, Observation parser) throws Exception {
        if (parser.getValue() != null) {
            throw new FieldNotEmptyException("Value", destinationResource);
        }
    }
    private static void assertNumericRangeLowEmpty(Resource destinationResource, Observation parser) throws Exception {
        if (parser.getNumericRangeLow() != null) {
            throw new FieldNotEmptyException("NumericRangeLow", destinationResource);
        }
    }
    private static void assertNumericRangeHighEmpty(Resource destinationResource, Observation parser) throws Exception {
        if (parser.getNumericRangeHigh() != null) {
            throw new FieldNotEmptyException("NumericRangeHigh", destinationResource);
        }
    }
    private static void assertNumericUnitEmpty(Resource destinationResource, Observation parser) throws Exception {
        if (!Strings.isNullOrEmpty(parser.getNumericUnit())) {
            throw new FieldNotEmptyException("NumericUnit", destinationResource);
        }
    }*/

    private static void assertValueEmpty(ResourceBuilderBase resourceBuilder, Observation parser) throws Exception {
        CsvCell value = parser.getValue();
        if (!value.isEmpty()) {
            throw new FieldNotEmptyException("Value", resourceBuilder.getResource());
        }
    }
    private static void assertNumericRangeLowEmpty(ResourceBuilderBase resourceBuilder, Observation parser) throws Exception {
        CsvCell value = parser.getNumericRangeLow();
        if (!value.isEmpty()) {
            throw new FieldNotEmptyException("NumericRangeLow", resourceBuilder.getResource());
        }
    }
    private static void assertNumericRangeHighEmpty(ResourceBuilderBase resourceBuilder, Observation parser) throws Exception {
        CsvCell value = parser.getNumericRangeHigh();
        if (!value.isEmpty()) {
            throw new FieldNotEmptyException("NumericRangeHigh", resourceBuilder.getResource());
        }
    }
    private static void assertNumericUnitEmpty(ResourceBuilderBase resourceBuilder, Observation parser) throws Exception {
        CsvCell value = parser.getNumericUnit();
        if (!value.isEmpty()) {
            throw new FieldNotEmptyException("NumericUnit", resourceBuilder.getResource());
        }
    }



    private static boolean isReview(CodeableConceptBuilder codeableConceptBuilder, String codeableConceptTag, Observation parser,
                                    EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        CsvCell problemGuid = parser.getProblemGuid();
        if (problemGuid.isEmpty()) {
            return false;
        }

        //find the original code our problem was coded with
        CsvCell patientGuid = parser.getPatientGuid();
        String problemReadCode = csvHelper.findProblemObservationReadCode(patientGuid, problemGuid, fhirResourceFiler);
        if (Strings.isNullOrEmpty(problemReadCode)) {
            return false;
        }

        //if we don't have a clinical code, then it can't be a review
        if (codeableConceptBuilder == null) {
            return false;
        }

        //find the original code our current observation is coded with
        CodeableConcept codeableConcept = codeableConceptBuilder.getCodeableConcept();
        String observationReadCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
        if (!problemReadCode.equals(observationReadCode)) {
            //if the codes differ, then return out
            return false;
        }

        return true;
    }




}
