package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
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

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatementTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationStatementTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.MedicationStatement;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.MedicationStatement model = params.getOutputContainer().getMedicationStatements();

        MedicationStatement fhir = (MedicationStatement)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        long id;
        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionConceptId = null;
        // Long dmdId = null;
        boolean isActive;
        Date cancellationDate = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        Integer authorisationTypeConceptId;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        // String originalTerm = null;
        String bnfReference = null;
        Double ageAtEvent = null;
        String issueMethod = null;

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasInformationSource()) {
            Reference practitionerReference = fhir.getInformationSource();
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasDateAssertedElement()) {
            DateTimeType dt = fhir.getDateAssertedElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), clinicalEffectiveDate.toString());
        }

        /*
        dmdId = CodeableConceptHelper.findSnomedConceptId(fhir.getMedicationCodeableConcept());
        */

        /*
        //add term too, for easy display of results
        originalTerm = fhir.getMedicationCodeableConcept().getText();
        //if we failed to find one, it's because of a change in how the CodeableConcept was generated, so find the term differently
        if (Strings.isNullOrEmpty(originalTerm)) {
            originalTerm = CodeableConceptHelper.findSnomedConceptText(fhir.getMedicationCodeableConcept());
        }*/

        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(fhir.getMedicationCodeableConcept());
        if (originalCoding == null) {
            TransformWarnings.log(LOG, params, "No suitable Coding found for {} {}", fhir.getResourceType(), fhir.getId());
            return;
        }
        String originalCode = originalCoding.getCode();


        String conceptScheme = getScheme(originalCoding.getSystem());
        coreConceptId = IMHelper.getIMMappedConcept(params, fhir, conceptScheme, originalCode);
        nonCoreConceptId = IMHelper.getIMConcept(params, fhir, conceptScheme, originalCode, originalCoding.getDisplay());

        isActive = fhir.hasStatus()
                && fhir.getStatus() == MedicationStatement.MedicationStatementStatus.ACTIVE;

        if (fhir.hasDosage()) {
            if (fhir.getDosage().size() > 1) {
                throw new TransformException("Cannot support MedicationStatements with more than one dose " + fhir.getId());
            }

            MedicationStatement.MedicationStatementDosageComponent doseage = fhir.getDosage().get(0);
            dose = doseage.getText();

            //one of the Emis test packs includes the unicode \u0001 character in the dose. This should be handled
            //during the inbound transform, but the data is already in the DB now, so needs handling here
            char[] chars = dose.toCharArray();
            for (int i=0; i<chars.length; i++) {
                char c = chars[i];
                if (c == 1) {
                    chars[i] = '?'; //just replace with ?
                }
            }
            dose = new String(chars);
        }

        MedicationAuthorisationType authorisationType = null;
        String originalTerm = null;

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_AUTHORISATION_CANCELLATION)) {
                    //this extension is a compound one, with one or two inner extensions, giving us the date and performer
                    if (extension.hasExtension()) {
                        for (Extension innerExtension: extension.getExtension()) {
                            if (innerExtension.getValue() instanceof DateType) {
                                DateType d = (DateType)innerExtension.getValue();
                                cancellationDate = d.getValue();
                            }
                        }
                    }

                } else if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY)) {
                    Quantity q = (Quantity)extension.getValue();
                    quantityValue = q.getValue();
                    quantityUnit = q.getUnit();

                } else if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE)) {
                    Coding c = (Coding)extension.getValue();
                    authorisationType = MedicationAuthorisationType.fromCode(c.getCode());
                    originalTerm = c.getDisplay();
                }
            }
        }

        authorisationTypeConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_MED_STATEMENT_AUTH_TYPE,
                authorisationType.getCode(), originalTerm);

        String snomedCodeString = null;

        if (originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)){

            snomedCodeString = originalCoding.getCode();
        }
        else {
            snomedCodeString = IMHelper.getIMSnomedCodeForConceptId(params, fhir, coreConceptId);
        }

        if (snomedCodeString != null) {
            bnfReference = params.getSnomedToBnfChapter(snomedCodeString);
            //LOG.info("bnfReference: " + bnfReference);
        }

        if (fhir.getPatient() != null) {
            Reference ref = fhir.getPatient();
            Patient patient = params.getCachedPatient(ref);
            ageAtEvent = getPatientAgeInDecimalYears(patient, clinicalEffectiveDate);
        }

        //TODO - not sure what issueMethod is supposed to contain, and MedicationStatement note field is never used in DDS (Drew)
        if (fhir.getNote() != null && fhir.getNote().length() > 0) {
            issueMethod = fhir.getNote();
        }


        model.writeUpsert(subscriberId,
            organizationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionConceptId,
            isActive,
            cancellationDate,
            dose,
            quantityValue,
            quantityUnit,
            authorisationTypeConceptId,
            coreConceptId,
            nonCoreConceptId,
            bnfReference,
            ageAtEvent,
            issueMethod);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.MEDICATION_STATEMENT;
    }

}
