package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ReferralRequestTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReferralRequestTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        ReferralRequest fhir = (ReferralRequest)resource;

        long id;
        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        // Long snomedConceptId = null;
        Long requesterOrganizationId = null;
        Long recipientOrganizationId = null;
        Integer referralRequestPriorityConceptId = null;
        Integer referralRequestTypeConceptId = null;
        String mode = null;
        Boolean outgoingReferral = null;
        // String originalCode = null;
        // String originalTerm = null;
        boolean isReview = false;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;
        Boolean isPrimary = null;

        id = enterpriseId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);
        }

        //moved to lower down since this isn't correct for incoming referrals
        /*if (fhir.hasRequester()) {
            Reference practitionerReference = fhir.getRequester();
            practitionerId = findEnterpriseId(data.getPractitioners(), practitionerReference);
            if (practitionerId == null) {
                practitionerId = transformOnDemand(practitionerReference, data, otherResources, enterpriseOrganisationId, enterprisePatientId, enterprisePersonId, configName, protocolId);
            }
        }*/

        if (fhir.hasDateElement()) {
            DateTimeType dt = fhir.getDateElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }


        //changed where the observation code is stored
        if (fhir.hasServiceRequested()) {

            if (fhir.getServiceRequested().size() > 1) {
                throw new TransformException("Transform doesn't support referrals with multiple service codes " + fhir.getId());
            }

            CodeableConcept fhirServiceRequested = fhir.getServiceRequested().get(0);
            ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhirServiceRequested);
            if (codes == null) {
                return;
            }
            //snomedConceptId = codes.getSnomedConceptId();
            //originalCode = codes.getOriginalCode();
            //originalTerm = codes.getOriginalTerm();
            
            Coding coding = CodeableConceptHelper.findOriginalCoding(fhirServiceRequested);
            String codingSystem = coding.getSystem();
            String scheme = getScheme(codingSystem);
            coreConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(scheme, codes.getOriginalCode());
            if (coreConceptId == null) {
                throw new org.endeavourhealth.core.exceptions.TransformException("coreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
            }

            nonCoreConceptId = IMClient.getConceptIdForSchemeCode(scheme, codes.getOriginalCode());
            if (nonCoreConceptId == null) {
                throw new org.endeavourhealth.core.exceptions.TransformException("nonCoreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }
        /*Long snomedConceptId = findSnomedConceptId(fhir.getType());
        model.setSnomedConceptId(snomedConceptId);*/

        if (fhir.hasRequester()) {
            Reference requesterReference = fhir.getRequester();
            ResourceType resourceType = ReferenceHelper.getResourceType(requesterReference);

            //the requester can be an organisation or practitioner
            if (resourceType == ResourceType.Organization) {
                requesterOrganizationId = transformOnDemandAndMapId(requesterReference, params);

            } else if (resourceType == ResourceType.Practitioner) {
                requesterOrganizationId = findOrganisationEnterpriseIdFromPractitioner(requesterReference, fhir, params);
            }
        }

        if (fhir.hasRecipient()) {

            //there may be two recipients, one for the organisation and one for the practitioner
            for (Reference recipientReference: fhir.getRecipient()) {
                if (ReferenceHelper.isResourceType(recipientReference, ResourceType.Organization)) {
                    //the EMIS test pack contains referrals that point to recipient organisations that don't exist,
                    //so we need to handle the failure to find the organisation
                    recipientOrganizationId = transformOnDemandAndMapId(recipientReference, params);
                }
            }

            //if we didn't find an organisation reference, look for a practitioner one
            //just rely on the organisation ID, so we don't accidentally infer that the referral is to
            //and organisation when it's not because the Emis data contains referrals with BOTH sender and recipient
            //organsiation GUIDs being to unknown orgs, and we should let that be carried through into the subscriber DB
            /*if (recipientOrganizationId == null) {
                for (Reference recipientReference : fhir.getRecipient()) {
                    if (ReferenceHelper.isResourceType(recipientReference, ResourceType.Practitioner)) {
                        recipientOrganizationId = findOrganisationEnterpriseIdFromPractictioner(recipientReference, fhir, params);
                    }
                }
            }*/
        }

        Reference practitionerReference = null;

        if (requesterOrganizationId != null) {
            outgoingReferral = requesterOrganizationId.longValue() == organizationId;

        } else if (recipientOrganizationId != null) {
            outgoingReferral = recipientOrganizationId.longValue() != organizationId;
        }

        //if we're an outgoing referral, then populate the practitioner ID from the practitioner in the requester field
        if (outgoingReferral != null
            && outgoingReferral.booleanValue()
            && fhir.hasRequester()) {

            Reference requesterReference = fhir.getRequester();
            if (ReferenceHelper.isResourceType(requesterReference, ResourceType.Practitioner)) {
                practitionerReference = requesterReference;
            }
        }

        //if we're an incoming referral then populate the practitioner ID using the practitioner in the recipient field
        if (outgoingReferral != null
                && !outgoingReferral.booleanValue()
                && fhir.hasRecipient()) {

            for (Reference recipientReference : fhir.getRecipient()) {
                if (ReferenceHelper.isResourceType(recipientReference, ResourceType.Practitioner)) {
                    practitionerReference = recipientReference;
                }
            }
        }

        if (practitionerReference != null) {
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        // TODO Code needs to be reviewed to use the IM for
        //  Referral Request Priority
        if (fhir.hasPriority()) {
            CodeableConcept codeableConcept = fhir.getPriority();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralPriority fhirReferralPriority = ReferralPriority.fromCode(coding.getCode());
                Integer referralRequestPriorityId = fhirReferralPriority.ordinal();

                referralRequestPriorityConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(
                        IMConstant.FHIR_REFERRAL_PRIORITY, referralRequestPriorityId.toString());
                if (referralRequestPriorityConceptId == null) {
                    throw new TransformException("referralRequestPriorityConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
                }

            }
        }

        // TODO Code needs to be reviewed to use the IM for
        //  Referral Request Type
        if (fhir.hasType()) {
            CodeableConcept codeableConcept = fhir.getType();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralType fhirReferralType = ReferralType.fromCode(coding.getCode());
                Integer referralRequestTypeId = fhirReferralType.ordinal();

                referralRequestTypeConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(
                        IMConstant.FHIR_REFERRAL_TYPE, referralRequestTypeId.toString());
                if (referralRequestTypeConceptId == null) {
                    throw new TransformException("referralRequestTypeConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
                }

            }
        }

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {
                if (extension.getUrl().equals(FhirExtensionUri.REFERRAL_REQUEST_SEND_MODE)) {
                    CodeableConcept cc = (CodeableConcept)extension.getValue();
                    if (!Strings.isNullOrEmpty(cc.getText())) {
                        mode = cc.getText();
                    } else {
                        Coding coding = cc.getCoding().get(0);
                        mode = coding.getDisplay();
                    }
                }
            }
        }

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType)reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue();
            }
        }

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        org.endeavourhealth.transform.subscriber.outputModels.ReferralRequest model
                = (org.endeavourhealth.transform.subscriber.outputModels.ReferralRequest)csvWriter;
        model.writeUpsert(id,
            organizationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionId,
            requesterOrganizationId,
            recipientOrganizationId,
            referralRequestPriorityConceptId,
            referralRequestTypeConceptId,
            mode,
            outgoingReferral,
            isReview,
            coreConceptId,
            nonCoreConceptId,
            ageAtEvent,
            isPrimary);
    }

    private Long findOrganisationEnterpriseIdFromPractitioner(Reference practitionerReference,
                                                               ReferralRequest fhir,
                                                               SubscriberTransformParams params) throws Exception {

        Practitioner fhirPractitioner = (Practitioner)findResource(practitionerReference, params);
        if (fhirPractitioner == null) {
            //we have a number of examples of Emis data where the practitioner doesn't exist, so handle this not being found
            LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to a Practitioner that doesn't exist");
            return null;
        }
        Practitioner.PractitionerPractitionerRoleComponent role = fhirPractitioner.getPractitionerRole().get(0);
        Reference organisationReference = role.getManagingOrganization();

        Long ret = transformOnDemandAndMapId(organisationReference, params);
        return ret;
    }

}
