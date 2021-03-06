package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatementEnterpriseTransformer extends AbstractEnterpriseTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationStatementEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.MedicationStatement;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        MedicationStatement fhir = (MedicationStatement)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        Long dmdId = null;
        //boolean isActive;
        Date cancellationDate = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        int authorisationTypeId;
        String originalTerm = null;

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

        try {
            dmdId = CodeableConceptHelper.findSnomedConceptId(fhir.getMedicationCodeableConcept());
        } catch (NumberFormatException nfe) {
            //deal with the error
            MedicationOrderEnterpriseTransformer.logInvalidDMDId(nfe, fhir, params);
            return;
        }

        //add term too, for easy display of results
        originalTerm = fhir.getMedicationCodeableConcept().getText();
        //if we failed to find one, it's because of a change in how the CodeableConcept was generated, so find the term differently
        if (Strings.isNullOrEmpty(originalTerm)) {
            originalTerm = CodeableConceptHelper.findSnomedConceptText(fhir.getMedicationCodeableConcept());
        }

//        isActive = fhir.hasStatus()
//                && fhir.getStatus() == MedicationStatement.MedicationStatementStatus.ACTIVE;

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

        org.endeavourhealth.transform.enterprise.outputModels.MedicationStatement model = (org.endeavourhealth.transform.enterprise.outputModels.MedicationStatement)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionId,
            dmdId,
            //isActive,
            cancellationDate,
            dose,
            quantityValue,
            quantityUnit,
            authorisationTypeId,
            originalTerm);
    }
}

