package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.Immunisation;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ImmunisationTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ImmunisationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Immunization fhir = (Immunization)resource;

        long id;
        Integer owningOrganisationId;
        Integer patientId;

        Long encounterId = null;
        Integer effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = null;

        id = enterpriseId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().intValue();
        patientId = params.getEnterprisePatientId().intValue();

        //personId = params.getEnterprisePersonId().longValue();   //TODO: track down source and remove for PCR

        Integer conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Integer enteredByPractitionerId = null;
        Long careActivityId = null;
        Integer careActivityHeadingConceptId = null;
        Integer statusConceptId = null;
        boolean confidential = false;
        String dose = null;
        Integer bodyLocationConceptId = null;
        Integer methodConceptId = null;
        String batchNumber = null;
        Date expiryDate = null;
        Integer doseOrdinal = null;
        Integer dosesRequired = null;
        boolean isConsent = false;

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }

        if (fhir.hasPerformer()) {
            Reference practitionerReference = fhir.getPerformer();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params).intValue();
        }

        //effective date
        if (fhir.hasDateElement()) {
            DateTimeType dt = fhir.getDateElement();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        //recorded/entered date
        Extension enteredDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
        if (enteredDateExtension != null) {

            DateTimeType enteredDateTimeType = (DateTimeType)enteredDateExtension.getValue();
            enteredDate = enteredDateTimeType.getValue();
        }

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference)enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params).intValue();
        }

        ObservationCodeHelper vaccineCode = ObservationCodeHelper.extractCodeFields(fhir.getVaccineCode());
        if (vaccineCode != null) {

            snomedConceptId = vaccineCode.getSnomedConceptId();
            //TODO: map to IM conceptId
            //conceptId = ??
        }

        //TODO: where get heading from?
        //careActivityHeadingConceptId

        //immunisation status
        if (fhir.hasStatus()) {

            String status = fhir.getStatus();
            //statusConceptId = ??    //TODO: map to IM concept
        }

        //confidential?
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }

        if (fhir.hasDoseQuantity()) {

            SimpleQuantity qty = fhir.getDoseQuantity();
            if (qty != null) {
                dose = (qty.getValue().toString() + " " +qty.getUnit()).trim();
            }
        }

        ObservationCodeHelper bodyLocationCode = ObservationCodeHelper.extractCodeFields(fhir.getSite());
        if (bodyLocationCode != null) {

            String site = bodyLocationCode.getOriginalTerm();

            //bodyLocationConceptId = ?? //TODO: map to IM concept
        }

        ObservationCodeHelper methodCode = ObservationCodeHelper.extractCodeFields(fhir.getRoute());
        if (methodCode != null) {

            String route = methodCode.getOriginalTerm();

            //methodConceptId = ?? //TODO: map to IM concept
        }

        //lot/batch number
        if (fhir.hasLotNumber()) {

            batchNumber = fhir.getLotNumber();
        }

        //expiry date
        if (fhir.hasExpirationDate()) {

            expiryDate = fhir.getExpirationDate();
        }

        if (fhir.hasVaccinationProtocol()) {

            Immunization.ImmunizationVaccinationProtocolComponent proto = fhir.getVaccinationProtocol().get(0);
            doseOrdinal = proto.getDoseSequence();
            dosesRequired = proto.getSeriesDoses();
        }

        Immunisation model = (Immunisation)csvWriter;
        model.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                insertDate,
                enteredDate,
                enteredByPractitionerId,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                statusConceptId,
                confidential,
                dose,
                bodyLocationConceptId,
                methodConceptId,
                batchNumber,
                expiryDate,
                doseOrdinal,
                dosesRequired,
                isConsent);
    }
}

