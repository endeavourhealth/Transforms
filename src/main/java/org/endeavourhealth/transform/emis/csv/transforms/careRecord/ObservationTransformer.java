package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.common.fhir.schema.ImmunizationStatus;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.core.terminology.Read2;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.helpers.BpComponent;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ObservationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Observation parser = (Observation)parsers.get(Observation.class);
        while (parser != null && parser.nextRecord()) {
            try {
                //if it's deleted we need to look up what the original resource type was before we can do the delete
                CsvCell deleted = parser.getDeleted();
                if (deleted.getBoolean()) {
                    deleteResource(parser, fhirResourceFiler, csvHelper);

                } else {
                    if (csvHelper.shouldProcessRecord(parser)) {
                        createResource(parser, fhirResourceFiler, csvHelper);
                    }
                }

            } catch (EmisCodeNotFoundException ex) {
                csvHelper.logMissingCode(ex, parser.getPatientGuid(), parser.getObservationGuid(), parser);

            } catch (Exception ex) {
                //log any record-level exception and continue
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void deleteResource(Observation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        Set<ResourceType> resourceTypes = findOriginalTargetResourceTypes(fhirResourceFiler, parser);
        for (ResourceType resourceType: resourceTypes) {

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
        }
    }


    /**
     * finds out what resource type an EMIS observation was previously saved as
     */
    private static Set<ResourceType> findOriginalTargetResourceTypes(HasServiceSystemAndExchangeIdI hasServiceId, Observation parser) throws Exception {
        return findOriginalTargetResourceTypes(hasServiceId, parser.getPatientGuid(), parser.getObservationGuid());
    }

    public static Set<ResourceType> findOriginalTargetResourceTypes(HasServiceSystemAndExchangeIdI hasServiceId, CsvCell patientGuid, CsvCell observationGuid) throws Exception {

        List<ResourceType> potentialResourceTypes = new ArrayList<>();
        potentialResourceTypes.add(ResourceType.Procedure);
        potentialResourceTypes.add(ResourceType.AllergyIntolerance);
        potentialResourceTypes.add(ResourceType.FamilyMemberHistory);
        potentialResourceTypes.add(ResourceType.Immunization);
        potentialResourceTypes.add(ResourceType.DiagnosticOrder);
        potentialResourceTypes.add(ResourceType.Specimen);
        potentialResourceTypes.add(ResourceType.DiagnosticReport);
        potentialResourceTypes.add(ResourceType.ReferralRequest);
        potentialResourceTypes.add(ResourceType.Condition);
        potentialResourceTypes.add(ResourceType.Observation);

        String sourceId = EmisCsvHelper.createUniqueId(patientGuid, observationGuid);

        Set<Reference> sourceReferences = new HashSet<>();
        for (ResourceType resourceType: potentialResourceTypes) {
            Reference ref = ReferenceHelper.createReference(resourceType, sourceId);
            sourceReferences.add(ref);
        }

        Map<Reference, UUID> idMap = IdHelper.getEdsResourceIds(hasServiceId.getServiceId(), sourceReferences);

        Set<ResourceType> ret = new HashSet<>();

        for (Reference ref: sourceReferences) {
            UUID id = idMap.get(ref);
            if (id != null) {
                ResourceType resourceType = ReferenceHelper.getResourceType(ref);
                ret.add(resourceType);
            }
        }

        return ret;
    }

    /*public static Set<ResourceType> findOriginalTargetResourceTypes(HasServiceSystemAndExchangeIdI hasServiceId, CsvCell patientGuid, CsvCell observationGuid) throws Exception {

        List<ResourceType> potentialResourceTypes = new ArrayList<>();
        potentialResourceTypes.add(ResourceType.Procedure);
        potentialResourceTypes.add(ResourceType.AllergyIntolerance);
        potentialResourceTypes.add(ResourceType.FamilyMemberHistory);
        potentialResourceTypes.add(ResourceType.Immunization);
        potentialResourceTypes.add(ResourceType.DiagnosticOrder);
        potentialResourceTypes.add(ResourceType.Specimen);
        potentialResourceTypes.add(ResourceType.DiagnosticReport);
        potentialResourceTypes.add(ResourceType.ReferralRequest);
        potentialResourceTypes.add(ResourceType.Condition);
        potentialResourceTypes.add(ResourceType.Observation);

        Set<ResourceType> ret = new HashSet<>();
        
        for (ResourceType resourceType: potentialResourceTypes) {
            if (wasSavedAsResourceType(hasServiceId, patientGuid, observationGuid, resourceType)) {
                ret.add(resourceType);
            }
        }

        return ret;
    }

    private static boolean wasSavedAsResourceType(HasServiceSystemAndExchangeIdI hasServiceId, CsvCell patientGuid, CsvCell observationGuid, ResourceType resourceType) throws Exception {
        String sourceId = EmisCsvHelper.createUniqueId(patientGuid, observationGuid);
        UUID uuid = IdHelper.getEdsResourceId(hasServiceId.getServiceId(), resourceType, sourceId);
        return uuid != null;
    }*/
    

    public static void createResource(Observation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        //the code ID should NEVER be null, but the test data has nulls, so adding this to handle those rows gracefully
        if ((parser.getVersion().equalsIgnoreCase(EmisCsvToFhirTransformer.VERSION_5_0)
                || parser.getVersion().equalsIgnoreCase(EmisCsvToFhirTransformer.VERSION_5_1))
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
        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell observationGuid = parser.getObservationGuid();
        ClinicalCodeType codeType = EmisCodeHelper.findClinicalCodeType(codeId);
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

            if (isProcedure(codeId)) {
                return ResourceType.Procedure;
            } else if (isDisorder(codeId)) {
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

    private static boolean isDisorder(CsvCell codeIdCell) throws Exception {

        EmisClinicalCode codeMapping = EmisCodeHelper.findClinicalCodeOrParentRead2Code(codeIdCell);
        if (codeMapping == null) {
            //if we can't find a Read2 code in the hierarchy we don't know
            return false;
        }

        String readCode = codeMapping.getAdjustedCode(); //use the adjusted code as it's padded to five chars
        return Read2.isDisorder(readCode);
    }



    private static boolean isDiagnosticReport(Observation parser,
                                              EmisCsvHelper csvHelper) throws Exception {

        //if it's got a value, it's not a diagnostic report, as it'll be an investigation within a report
        if (!parser.getValue().isEmpty()) {
            return false;
        }

        //if it doesn't have any child observations linking to it, then don't store as a report
        //no, don't test this. Depending on the order of data, we may get this wrong when called from the ObservationPreTransformer,
        //since we may not have cached the child observations yet. So be consistent and use the code type to check.
        /*if (!csvHelper.hasChildObservations(parser.getObservationGuid(), parser.getPatientGuid())) {
            return false;
        }*/

        //if we pass the above checks, then check what kind of code it is. If one of the below types, then store as a report.
        CsvCell codeIdCell = parser.getCodeId();
        ClinicalCodeType codeType = EmisCodeHelper.findClinicalCodeType(codeIdCell);

        return codeType == ClinicalCodeType.Biochemistry
            || codeType == ClinicalCodeType.Cyology_Histology
            || codeType == ClinicalCodeType.Haematology
            || codeType == ClinicalCodeType.Immunology
            || codeType == ClinicalCodeType.Microbiology
            || codeType == ClinicalCodeType.Radiology
            || codeType == ClinicalCodeType.Health_Management;
    }

    private static boolean isProcedure(CsvCell codeIdCell) throws Exception {

        EmisClinicalCode codeMapping = EmisCodeHelper.findClinicalCodeOrParentRead2Code(codeIdCell);
        if (codeMapping == null) {
            //if we can't find a Read2 code in the hierarchy we don't know
            return false;
        }

        String readCode = codeMapping.getAdjustedCode(); //use the adjusted code as it's padded to five chars
        return Read2.isProcedure(readCode);
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

        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            referralRequestBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(referralRequestBuilder, false, codeId, CodeableConceptBuilder.Tag.Referral_Request_Service, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            referralRequestBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            referralRequestBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                referralRequestBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these fields are empty, as we don't stored them in this resource type,
        assertValueEmpty(referralRequestBuilder, parser);
        assertNumericUnitEmpty(referralRequestBuilder, parser);
        assertNumericRangeLowEmpty(referralRequestBuilder, parser);
        assertNumericRangeHighEmpty(referralRequestBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), referralRequestBuilder);

    }

    private static ResourceType findParentObservationType(EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, CsvCell patientGuidCell, CsvCell parentObservationCell) throws Exception {

        ResourceType parentResourceType = csvHelper.getCachedParentObservationResourceType(patientGuidCell, parentObservationCell);
        if (parentResourceType == null) {
            Set<ResourceType> resourceTypes = findOriginalTargetResourceTypes(fhirResourceFiler, patientGuidCell, parentObservationCell);

            //the Emis test pack includes child observations that refer to parents that never existed
            if (resourceTypes.isEmpty()) {
                return null;
                //throw new TransformException("Didn't find parent resource type for patient " + patientGuidCell.getString() + " and observation " + parentObservationCell.getString());

            } else if (resourceTypes.size() > 1) {

                //due to a past bug, we will have generated ID mappings for some Observation records to the true destination
                //resource type (e.g. Immunization) plus Observation and Condition, so the above function may return between
                //one and three resource types. So if we find multiple resource types, we need to find out which one actually has a resource
                String sourceId = EmisCsvHelper.createUniqueId(patientGuidCell, parentObservationCell);
                for (ResourceType resourceType: resourceTypes) {
                    Resource resource = csvHelper.retrieveResource(sourceId, resourceType);
                    if (resource != null) {
                        parentResourceType = resourceType;
                        break;
                    }
                }
                //throw new TransformException("Found " + resourceTypes.size() + " mapped from patient " + patientGuidCell.getString() + " and observation " + parentObservationCell.getString());

            } else {
                parentResourceType = resourceTypes.iterator().next();
            }
        }
        return parentResourceType;
    }


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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            diagnosticOrderBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(diagnosticOrderBuilder, false, codeId, CodeableConceptBuilder.Tag.Diagnostic_Order_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            diagnosticOrderBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            diagnosticOrderBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                diagnosticOrderBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(diagnosticOrderBuilder, parser);
        assertNumericUnitEmpty(diagnosticOrderBuilder, parser);
        assertNumericRangeLowEmpty(diagnosticOrderBuilder, parser);
        assertNumericRangeHighEmpty(diagnosticOrderBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), diagnosticOrderBuilder);
    }


    private static void createOrDeleteSpecimen(Observation parser, FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        SpecimenBuilder specimenBuilder = new SpecimenBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(specimenBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        specimenBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            specimenBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), specimenBuilder);
            return;
        }

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            specimenBuilder.setCollectedBy(practitionerReference, clinicianGuid);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(specimenBuilder, false, codeId, CodeableConceptBuilder.Tag.Specimen_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            specimenBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            specimenBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                specimenBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(specimenBuilder, parser);
        assertNumericUnitEmpty(specimenBuilder, parser);
        assertNumericRangeLowEmpty(specimenBuilder, parser);
        assertNumericRangeHighEmpty(specimenBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), specimenBuilder);
    }


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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            allergyIntoleranceBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(allergyIntoleranceBuilder, false, codeId, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            allergyIntoleranceBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            allergyIntoleranceBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                allergyIntoleranceBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        assertValueEmpty(allergyIntoleranceBuilder, parser);
        assertNumericUnitEmpty(allergyIntoleranceBuilder, parser);
        assertNumericRangeLowEmpty(allergyIntoleranceBuilder, parser);
        assertNumericRangeHighEmpty(allergyIntoleranceBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }


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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            diagnosticReportBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(diagnosticReportBuilder, false, codeId, CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            diagnosticReportBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            diagnosticReportBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                diagnosticReportBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(diagnosticReportBuilder, parser);
        //in the Emis Left & Dead extracts have contained a number of records that are report headers (that transform into DiagnosticReport resources)
        //but have weird values in the min and max range fields, but no value. So continue to assert that there's
        //no value, but ignore non-empty range values
        /*assertNumericUnitEmpty(diagnosticReportBuilder, parser);
        assertNumericRangeLowEmpty(diagnosticReportBuilder, parser);
        assertNumericRangeHighEmpty(diagnosticReportBuilder, parser);*/

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), diagnosticReportBuilder);

    }


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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            procedureBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
            return;
        }

        //by definition, we only receive procedures that are completed
        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(procedureBuilder, false, codeId, CodeableConceptBuilder.Tag.Procedure_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            procedureBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            procedureBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                procedureBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(procedureBuilder, parser);
        assertNumericUnitEmpty(procedureBuilder, parser);
        assertNumericRangeLowEmpty(procedureBuilder, parser);
        assertNumericRangeHighEmpty(procedureBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }

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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            conditionBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(conditionBuilder, false, codeId, CodeableConceptBuilder.Tag.Condition_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            conditionBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            conditionBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                conditionBuilder.setParentResource(parentReference, parentObservationCell);
            }
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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            observationBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(observationBuilder, false, codeId, CodeableConceptBuilder.Tag.Observation_Main_Code, csvHelper);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            observationBuilder.setClinician(practitionerReference, clinicianGuid);
        }

        CsvCell value = parser.getValue();
        if (!value.isEmpty()) {
            observationBuilder.setValueNumber(value.getDouble(), value);
        }

        CsvCell units = parser.getNumericUnit();
        if (!units.isEmpty()) {
            observationBuilder.setValueNumberUnits(units.getString(), units);
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

                EmisCodeHelper.createCodeableConcept(observationBuilder, false, bpCodeId, CodeableConceptBuilder.Tag.Observation_Component_Code, csvHelper);
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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            observationBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            observationBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                observationBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
    }

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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            familyMemberHistoryBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(familyMemberHistoryBuilder, false, codeId, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            familyMemberHistoryBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            familyMemberHistoryBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                familyMemberHistoryBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(familyMemberHistoryBuilder, parser);
        assertNumericUnitEmpty(familyMemberHistoryBuilder, parser);
        assertNumericRangeLowEmpty(familyMemberHistoryBuilder, parser);
        assertNumericRangeHighEmpty(familyMemberHistoryBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }


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
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            immunizationBuilder.setDeletedAudit(deletedCell);
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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(immunizationBuilder, false, codeId, CodeableConceptBuilder.Tag.Immunization_Main_Code, csvHelper);

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

        if (isReview(codeableConceptBuilder, parser, csvHelper, fhirResourceFiler)) {
            immunizationBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            immunizationBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, csvHelper.createUniqueId(patientGuid, parentObservationCell));
                immunizationBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(immunizationBuilder, parser);
        assertNumericUnitEmpty(immunizationBuilder, parser);
        assertNumericRangeLowEmpty(immunizationBuilder, parser);
        assertNumericRangeHighEmpty(immunizationBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), immunizationBuilder);
    }



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



    private static boolean isReview(CodeableConceptBuilder codeableConceptBuilder, Observation parser,
                                    EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        CsvCell problemGuid = parser.getProblemGuid();
        if (problemGuid.isEmpty()) {
            return false;
        }

        //find the original code our problem was coded with
        CsvCell patientGuid = parser.getPatientGuid();
        String problemReadCode = csvHelper.findProblemObservationReadCode(patientGuid, problemGuid);
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
