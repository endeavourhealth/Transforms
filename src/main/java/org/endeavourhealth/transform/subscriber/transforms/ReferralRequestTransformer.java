package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ReferralRequestTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReferralRequestTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.ReferralRequest;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.ReferralRequest model = params.getOutputContainer().getReferralRequests();

        ReferralRequest fhir = (ReferralRequest)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                || isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionConceptId = null;
        // Long snomedConceptId = null;
        Long requesterOrganizationId = null;
        Long recipientOrganizationId = null;
        Integer referralRequestPriorityConceptId = null;
        Integer referralRequestTypeConceptId = null;
        String mode = null;
        Boolean outgoingReferral = null;
        boolean isReview = false;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = transformOnDemandAndMapId(encounterReference, SubscriberTableId.ENCOUNTER, params);
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
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), clinicalEffectiveDate.toString());
        }


        //changed where the observation code is stored
        if (fhir.hasServiceRequested()) {

            if (fhir.getServiceRequested().size() > 1) {
                throw new TransformException("Transform doesn't support referrals with multiple service codes " + fhir.getId());
            }

            CodeableConcept fhirServiceRequested = fhir.getServiceRequested().get(0);
            Coding originalCoding = CodeableConceptHelper.findOriginalCoding(fhirServiceRequested);
            if (originalCoding == null) {
                TransformWarnings.log(LOG, params, "No suitable Coding found for {} {}", fhir.getResourceType(), fhir.getId());
                return;
            }
            String originalCode = originalCoding.getCode();


            String conceptScheme = getScheme(originalCoding.getSystem());
            coreConceptId = IMHelper.getIMMappedConcept(params, fhir, conceptScheme, originalCode);
            nonCoreConceptId = IMHelper.getIMConcept(params, fhir, conceptScheme, originalCode, originalCoding.getDisplay());
        }
        /*Long snomedConceptId = findSnomedConceptId(fhir.getType());
        model.setSnomedConceptId(snomedConceptId);*/

        if (fhir.hasRequester()) {
            Reference requesterReference = fhir.getRequester();
            ResourceType resourceType = ReferenceHelper.getResourceType(requesterReference);

            //the requester can be an organisation or practitioner
            if (resourceType == ResourceType.Organization) {
                requesterOrganizationId = transformOnDemandAndMapId(requesterReference, SubscriberTableId.ORGANIZATION, params);

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
                    recipientOrganizationId = transformOnDemandAndMapId(recipientReference, SubscriberTableId.ORGANIZATION, params);
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
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasPriority()) {
            CodeableConcept codeableConcept = fhir.getPriority();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralPriority fhirReferralPriority = ReferralPriority.fromCode(coding.getCode());
                Integer referralRequestPriorityId = fhirReferralPriority.ordinal();
                referralRequestPriorityConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_REFERRAL_PRIORITY,
                        referralRequestPriorityId.toString(), coding.getDisplay());
            }
        }

        if (fhir.hasType()) {
            CodeableConcept codeableConcept = fhir.getType();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralType fhirReferralType = ReferralType.fromCode(coding.getCode());
                referralRequestTypeConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_REFERRAL_TYPE,
                        fhirReferralType.getCode(), coding.getDisplay());
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

        if (fhir.getPatient() != null) {
            Reference ref = fhir.getPatient();
            Patient patient = params.getCachedPatient(ref);
            ageAtEvent = getPatientAgeInDecimalYears(patient, clinicalEffectiveDate);
        }

        model.writeUpsert(
                subscriberId,
                organizationId,
                patientId,
                personId,
                encounterId,
                practitionerId,
                clinicalEffectiveDate,
                datePrecisionConceptId,
                requesterOrganizationId,
                recipientOrganizationId,
                referralRequestPriorityConceptId,
                referralRequestTypeConceptId,
                mode,
                outgoingReferral,
                isReview,
                coreConceptId,
                nonCoreConceptId,
                ageAtEvent);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.REFERRAL_REQUEST;
    }

    private Long findOrganisationEnterpriseIdFromPractitioner(Reference practitionerReference,
                                                               ReferralRequest fhir,
                                                               SubscriberTransformHelper params) throws Exception {

        Practitioner fhirPractitioner = (Practitioner)params.findOrRetrieveResource(practitionerReference);
        if (fhirPractitioner == null) {
            //we have a number of examples of Emis data where the practitioner doesn't exist, so handle this not being found
            LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to a Practitioner that doesn't exist");
            return null;
        }
        Practitioner.PractitionerPractitionerRoleComponent role = fhirPractitioner.getPractitionerRole().get(0);
        if (!role.hasManagingOrganization()) {
            //From TPP we sometimes don't get a practitioner with a role encoded.
            return null;
        }
        Reference organisationReference = role.getManagingOrganization();

        Long ret = transformOnDemandAndMapId(organisationReference, SubscriberTableId.ORGANIZATION, params);
        return ret;
    }

}
