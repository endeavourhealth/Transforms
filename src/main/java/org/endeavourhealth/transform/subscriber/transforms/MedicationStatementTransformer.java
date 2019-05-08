package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatementTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationStatementTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.MedicationStatement model = params.getOutputContainer().getMedicationStatements();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);

            return;
        }


        MedicationStatement fhir = (MedicationStatement) FhirResourceHelper.deserialiseResouce(resourceWrapper);

        long id;
        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionConceptId = null;
        // Long dmdId = null;
        Boolean isActive = null;
        Date cancellationDate = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        Integer medicationStatementAuthorisationTypeConceptId;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        // String originalTerm = null;
        Integer bnfReference = null;
        Double ageAtEvent = null;
        String issueMethod = null;

        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasInformationSource()) {
            Reference practitionerReference = fhir.getInformationSource();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasDateAssertedElement()) {
            DateTimeType dt = fhir.getDateAssertedElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(dt.getPrecision());
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

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getMedicationCodeableConcept());
        if (codes == null) {
            return;
        }
        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(fhir.getMedicationCodeableConcept());
        String originalCode = codes.getOriginalCode();
        if (originalCoding == null) {
            originalCoding = fhir.getMedicationCodeableConcept().getCoding().get(0);
            originalCode = fhir.getMedicationCodeableConcept().getCoding().get(0).getCode();
        }

        coreConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(getScheme(originalCoding.getSystem()), originalCode);
        if (coreConceptId == null) {
            LOG.warn("coreConceptId is null using scheme: " + getScheme(originalCoding.getSystem()) + " code: " + originalCode);
            throw new TransformException("coreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        nonCoreConceptId = IMClient.getConceptIdForSchemeCode(getScheme(originalCoding.getSystem()), originalCode);
        if (nonCoreConceptId == null) {
            LOG.warn("nonCoreConceptId is null using scheme: " + getScheme(originalCoding.getSystem()) + " code: " + originalCode);
            throw new TransformException("nonCoreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        if (fhir.hasStatus()) {
            MedicationStatement.MedicationStatementStatus fhirStatus = fhir.getStatus();
            isActive = Boolean.valueOf(fhirStatus == MedicationStatement.MedicationStatementStatus.ACTIVE);
        }

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
                }
            }
        }

        medicationStatementAuthorisationTypeConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(
                IMConstant.FHIR_MED_STATEMENT_AUTH_TYPE, authorisationType.getCode());
        if (medicationStatementAuthorisationTypeConceptId == null) {
            throw new TransformException("medicationStatementAuthorisationTypeConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        //  TODO Finalised the use of coreConceptId and the IM in order to look up the BNF
        //  Chapter in that table in the reference DB, by using the actual Snomed code
        /*
        String snomedCodeString = IMClient.getCodeForConceptId(coreConceptId);

        SnomedToBnfChapterDalI snomedToBnfChapterDal = DalProvider.factorySnomedToBnfChapter();
        String fullBnfChapterCodeString = snomedToBnfChapterDal.lookupSnomedCode(snomedCodeString);
        if (fullBnfChapterCodeString != null && fullBnfChapterCodeString.length() > 7) {
            bnfReference = Integer.parseInt(fullBnfChapterCodeString.substring(0,6));
        }
        */

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

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
            medicationStatementAuthorisationTypeConceptId,
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
