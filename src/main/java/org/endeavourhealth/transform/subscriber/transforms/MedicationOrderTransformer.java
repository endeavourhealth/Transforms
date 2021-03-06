package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationOrderTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationOrderTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.MedicationOrder;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.MedicationOrder model = params.getOutputContainer().getMedicationOrders();

        MedicationOrder fhir = (MedicationOrder)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
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
        // Long dmdId = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        Integer durationDays = null;
        BigDecimal estimatedCost = null;
        Long medicationStatementId = null;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        // String originalTerm = null;
        String bnfReference = null;
        Double ageAtEvent = null;
        String issueMethod = null;

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasPrescriber()) {
            Reference practitionerReference = fhir.getPrescriber();
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = transformOnDemandAndMapId(encounterReference, SubscriberTableId.ENCOUNTER, params);
        }

        if (fhir.hasDateWrittenElement()) {
            DateTimeType dt = fhir.getDateWrittenElement();
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

        Coding originalCoding = ObservationCodeHelper.findOriginalCoding(fhir.getMedicationCodeableConcept());
        if (originalCoding == null) {
            //we get lots of free-text medication from TPP, so removed this as there's no point logging it when there's no fix
            //TransformWarnings.log(LOG, params, "No suitable Coding found for {} {}", fhir.getResourceType(), fhir.getId());
            return;
        }
        String originalCode = originalCoding.getCode();

        String conceptScheme = ObservationCodeHelper.mapCodingSystemToImScheme(originalCoding);
        coreConceptId = IMHelper.getIMMappedConcept(params, fhir, conceptScheme, originalCode);
        nonCoreConceptId = IMHelper.getIMConcept(params, fhir, conceptScheme, originalCode, originalCoding.getDisplay());

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
        if (durationDays == null) { durationDays = 0;} // Not nullable in SQL
        if (fhir.hasExtension()) {
            for (Extension extension : fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_ORDER_ESTIMATED_COST)) {
                    DecimalType d = (DecimalType)extension.getValue();
                    estimatedCost = d.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.MEDICATION_ORDER_AUTHORISATION)) {
                    Reference medicationStatementReference = (Reference)extension.getValue();
                    medicationStatementId = transformOnDemandAndMapId(medicationStatementReference, SubscriberTableId.MEDICATION_STATEMENT, params);

                    //the test pack contains medication orders (i.e. issueRecords) that point to medication statements (i.e. drugRecords)
                    //that don't exist, so log it out and just skip this bad record
                    if (medicationStatementId == null) {
                        LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " as it refers to a MedicationStatement that doesn't exist");
                    }
                }
            }
        }

        String snomedCodeString = null;

         if (originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)){
            snomedCodeString = originalCoding.getCode();
            // LOG.info("originalCode snomedCodeString: " + snomedCodeString);
        }
        else {
            snomedCodeString = IMHelper.getSnomedConceptIdForCoreDBID(params, fhir, coreConceptId);
            // LOG.info("IM snomedCodeString: " + snomedCodeString);
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

        //TODO - not sure what issueMethod is supposed to contain, and MedicationOrder note field is never used in DDS (Drew)
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
                dose,
                quantityValue,
                quantityUnit,
                durationDays,
                estimatedCost,
                medicationStatementId,
                coreConceptId,
                nonCoreConceptId,
                bnfReference,
                ageAtEvent,
                issueMethod);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.MEDICATION_ORDER;
    }

}
