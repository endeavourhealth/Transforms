package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ReferralRequestEnterpriseTransformer extends AbstractEnterpriseTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReferralRequestEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.ReferralRequest;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        ReferralRequest fhir = (ReferralRequest)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

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
        String ubrn = null;
        String specialty = null;
        Date dateRecorded = null;

        id = enterpriseId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = transformOnDemandAndMapId(encounterReference, params);
        }

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
                priorityId = new Integer(fhirReferralPriority.ordinal());

            } else if (codeableConcept.hasText()) {
                //we can't always map the inbound free-text referral priority to one of the hard-coded valueset, so may
                //carry it through as free-text, in which case just look up using that text, but we have no way to use that in Compass v2
            }
        }

        if (fhir.hasType()) {
            CodeableConcept codeableConcept = fhir.getType();
            if (codeableConcept.hasCoding()) {
                Coding coding = codeableConcept.getCoding().get(0);
                ReferralType fhirReferralType = ReferralType.fromCode(coding.getCode());
                typeId = new Integer(fhirReferralType.ordinal());

            } else if (codeableConcept.hasText()) {
                //we can't always map the inbound free-text referral priority to one of the hard-coded valueset, so may
                //carry it through as free-text, in which case just look up using that text, but we have no way to use that in Compass v2
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

        if (fhir.hasSpecialty()) {

            specialty = fhir.getSpecialty().getText();
        }

        if (fhir.hasIdentifier()) {

            List<Identifier> identifiers = fhir.getIdentifier();
            ubrn = IdentifierHelper.findIdentifierValue(identifiers, FhirIdentifierUri.IDENTIFIER_SYSTEM_UBRN);
        }

        dateRecorded = params.includeDateRecorded(fhir);

        org.endeavourhealth.transform.enterprise.outputModels.ReferralRequest model = (org.endeavourhealth.transform.enterprise.outputModels.ReferralRequest)csvWriter;
        model.setIncludeDateRecorded(params.isIncludeDateRecorded());
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
            isReview,
            specialty,
            ubrn,
            dateRecorded);
    }

    private Long findOrganisationEnterpriseIdFromPractictioner(Reference practitionerReference,
                                                               ReferralRequest fhir,
                                                               EnterpriseTransformHelper params) throws Exception {

        ResourceWrapper wrapper = params.findOrRetrieveResource(practitionerReference);
        if (wrapper == null) {
            //we have a number of examples of Emis data where the practitioner doesn't exist, so handle this not being found
            LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to a Practitioner that doesn't exist");
            return null;
        }
        Practitioner fhirPractitioner = (Practitioner) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
        Practitioner.PractitionerPractitionerRoleComponent role = fhirPractitioner.getPractitionerRole().get(0);
        Reference organisationReference = role.getManagingOrganization();

        if (!organisationReference.isEmpty()) {
            Long ret = transformOnDemandAndMapId(organisationReference, params);
            return ret;
        }
        return null;
    }

}

