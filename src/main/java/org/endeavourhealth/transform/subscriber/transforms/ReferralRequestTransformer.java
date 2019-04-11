package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.exceptions.TransformException;
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
        Long snomedConceptId = null;
        Long requesterOrganizationId = null;
        Long recipientOrganizationId = null;
        Integer priorityId = null;
        Integer typeId = null;
        String mode = null;
        Boolean outgoing = null;
        String originalCode = null;
        String originalTerm = null;
        boolean isReview = false;

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
            snomedConceptId = codes.getSnomedConceptId();
            originalCode = codes.getOriginalCode();
            originalTerm = codes.getOriginalTerm();
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
                requesterOrganizationId = findOrganisationEnterpriseIdFromPractictioner(requesterReference, fhir, params);
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
            outgoing = requesterOrganizationId.longValue() == organizationId;

        } else if (recipientOrganizationId != null) {
            outgoing = recipientOrganizationId.longValue() != organizationId;
        }

        //if we're an outgoing referral, then populate the practitioner ID from the practitioner in the requester field
        if (outgoing != null
            && outgoing.booleanValue()
            && fhir.hasRequester()) {

            Reference requesterReference = fhir.getRequester();
            if (ReferenceHelper.isResourceType(requesterReference, ResourceType.Practitioner)) {
                practitionerReference = requesterReference;
            }
        }

        //if we're an incoming referral then populate the practitioner ID using the practitioner in the recipient field
        if (outgoing != null
                && !outgoing.booleanValue()
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

        if (fhir.hasPriority()) {
            CodeableConcept codeableConcept = fhir.getPriority();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralPriority fhirReferralPriority = ReferralPriority.fromCode(coding.getCode());
                priorityId = fhirReferralPriority.ordinal();
            }
        }

        if (fhir.hasType()) {
            CodeableConcept codeableConcept = fhir.getType();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralType fhirReferralType = ReferralType.fromCode(coding.getCode());
                typeId = fhirReferralType.ordinal();
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
            snomedConceptId,
            requesterOrganizationId,
            recipientOrganizationId,
            priorityId,
            typeId,
            mode,
            outgoing,
            originalCode,
            originalTerm,
            isReview);
    }

    private Long findOrganisationEnterpriseIdFromPractictioner(Reference practitionerReference,
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

