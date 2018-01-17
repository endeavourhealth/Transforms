package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ClinicalEvent;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.EmisDateTimeHelper;
import org.endeavourhealth.transform.terminology.TerminologyService;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ClinicalEventTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ClinicalEventTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 ClinicalEvent parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createObservation(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(ClinicalEvent parser) {
        return null;
    }


    /*
     *
     */
    public static void createObservation(ClinicalEvent parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getPatientId()))};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getPatientId(), null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        // Encounter
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId());
            createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getEffectiveDateTime(), parser.getEffectiveDateTime(), null, Encounter.EncounterClass.OTHER);
        }
        // this Observation resource id
        ResourceId observationResourceId = getObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getPatientId(), parser.getEffectiveDateTimeAsString(), parser.getEventCode());

        //Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        String observationID = parser.getEventId();
        Observation fhirObservation = new Observation();
        fhirObservation.setId(observationResourceId.getResourceId().toString());
        fhirObservation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_OBSERVATION));
        fhirObservation.addIdentifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_OBSERVATION_ID).setValue(observationID);
        fhirObservation.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        fhirObservation.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        fhirObservation.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));

        String clinicianID = parser.getClinicianID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            //TODO - need to map to person resources
            //fhirObservation.addPerformer(csvHelper.createPractitionerReference(clinicianID));
        }

        String term = parser.getEventTitleText();
        if (Strings.isNullOrEmpty(term))
            term = parser.getEventTag();
        String code = parser.getEventCode();

        //TODO - establish code mapping for millenium / Snomed
        CodeableConcept obsCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, code);
        TerminologyService.translateToSnomed(obsCode);
        if (obsCode != null) {
            fhirObservation.setCode(obsCode);
        } else {
            LOG.warn("Unable to create codeableConcept for Observation ID: "+observationID);
            return;
        }

        //set value and units
        String value = parser.getEventResultAsText();
        if (!Strings.isNullOrEmpty(value)) {
            Double value1 = Double.parseDouble(value);
            String unitsCode = parser.getEventUnitsCode();
            String units = "";   //TODO - map units from unitsCode
            fhirObservation.setValue(QuantityHelper.createQuantity(value1, units));
        }

        //TODO - set comments
        //fhirObservation.setComments("");

        // save resource
        if (parser.isActive()) {
            LOG.debug("Save Observation (PatId=" + parser.getPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirObservation));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirObservation);
        } else {
            LOG.debug("Delete Observation (PatId=" + parser.getPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirObservation));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirObservation);
        }

    }

}
