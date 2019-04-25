package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.SnomedToBnfChapterDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatementTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationStatementTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        MedicationStatement fhir = (MedicationStatement)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        Long dmdId = null;
        Boolean isActive = null;
        Date cancellationDate = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        int authorisationTypeId;
        String originalTerm = null;
        Integer bnfReference = null;
        Double ageAtEvent = null;
        String issueMethod = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasInformationSource()) {
            Reference practitionerReference = fhir.getInformationSource();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasDateAssertedElement()) {
            DateTimeType dt = fhir.getDateAssertedElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        dmdId = CodeableConceptHelper.findSnomedConceptId(fhir.getMedicationCodeableConcept());

        //add term too, for easy display of results
        originalTerm = fhir.getMedicationCodeableConcept().getText();
        //if we failed to find one, it's because of a change in how the CodeableConcept was generated, so find the term differently
        if (Strings.isNullOrEmpty(originalTerm)) {
            originalTerm = CodeableConceptHelper.findSnomedConceptText(fhir.getMedicationCodeableConcept());
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

        authorisationTypeId = authorisationType.ordinal();

        // TODO Finalise the use of core_concept_id and the IM (rather than dmdId) in order to look
        //  up the BNF Chapter in that table in the reference DB, by using the ACTUAL Snomed code

        // This line will need changing
        Long snomedCode = dmdId;

        // These lines are fine
        SnomedToBnfChapterDalI snomedToBnfChapterDal = DalProvider.factorySnomedToBnfChapter();
        String fullBnfChapterCodeString = snomedToBnfChapterDal.lookupSnomedCode(snomedCode.toString());
        bnfReference = Integer.parseInt(fullBnfChapterCodeString.substring(0,6));

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

        if (fhir.getNote() != null && fhir.getNote().length() > 0) {
            issueMethod = fhir.getNote();
        }

        org.endeavourhealth.transform.subscriber.outputModels.MedicationStatement model
                = (org.endeavourhealth.transform.subscriber.outputModels.MedicationStatement)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionId,
            dmdId,
            isActive,
            cancellationDate,
            dose,
            quantityValue,
            quantityUnit,
            authorisationTypeId,
            originalTerm,
            bnfReference,
            ageAtEvent,
            issueMethod);
    }
}
