package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationOrderTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationOrderTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        MedicationOrder fhir = (MedicationOrder)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        Long dmdId = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        Integer durationDays = null;
        BigDecimal estimatedCost = null;
        Long medicationStatementId = null;
        String originalTerm = null;
        Double ageAtEvent = null;
        String issueMethod = null;
        Boolean isPrimary = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasPrescriber()) {
            Reference practitionerReference = fhir.getPrescriber();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);
        }

        if (fhir.hasDateWrittenElement()) {
            DateTimeType dt = fhir.getDateWrittenElement();
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

        if (fhir.hasDosageInstruction()) {
            if (fhir.getDosageInstruction().size() > 1) {
                throw new TransformException("Cannot support MedicationStatements with more than one dose " + fhir.getId());
            }

            MedicationOrder.MedicationOrderDosageInstructionComponent doseage = fhir.getDosageInstruction().get(0);
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

        if (fhir.hasDispenseRequest()) {
            MedicationOrder.MedicationOrderDispenseRequestComponent dispenseRequestComponent = fhir.getDispenseRequest();
            Quantity q = dispenseRequestComponent.getQuantity();
            quantityValue = q.getValue();
            quantityUnit = q.getUnit();

            if (dispenseRequestComponent.getExpectedSupplyDuration() != null) {
                Duration duration = dispenseRequestComponent.getExpectedSupplyDuration();
                if (duration != null && duration.getUnit() != null) {
                    if (!duration.getUnit().equalsIgnoreCase("days")) {
                        throw new TransformException("Unsupported medication order duration type [" + duration.getUnit() + "] for " + fhir.getId());
                    }
                    int days = duration.getValue().intValue();
                    durationDays = Integer.valueOf(days);
                }
            }
        }

        if (fhir.hasExtension()) {
            for (Extension extension : fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_ORDER_ESTIMATED_COST)) {
                    DecimalType d = (DecimalType)extension.getValue();
                    estimatedCost = d.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_ORDER_AUTHORISATION)) {
                    Reference medicationStatementReference = (Reference)extension.getValue();
                    medicationStatementId = findEnterpriseId(params, medicationStatementReference);

                    //the test pack contains medication orders (i.e. issueRecords) that point to medication statements (i.e. drugRecords)
                    //that don't exist, so log it out and just skip this bad record
                    if (medicationStatementId == null) {
                        LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " as it refers to a MedicationStatement that doesn't exist");
                    }
                }
            }
        }

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

        if (fhir.getNote() != null && fhir.getNote().length() > 0) {
            issueMethod = fhir.getNote();
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        org.endeavourhealth.transform.subscriber.outputModels.MedicationOrder model
                = (org.endeavourhealth.transform.subscriber.outputModels.MedicationOrder)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionId,
            dmdId,
            dose,
            quantityValue,
            quantityUnit,
            durationDays,
            estimatedCost,
            medicationStatementId,
            originalTerm,
            ageAtEvent,
            issueMethod,
            isPrimary);
    }
}
