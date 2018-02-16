package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;

import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Date;

public class CLEVETransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVETransformer.class);
    //TODO  - kludge -relies on given order
    private static final String[] comparators = {"<=", "<", ">=", ">"};
    private static final String RESULT_STATUS_AUTHORIZED = "25";
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;

    /*
     *
     */
    public static void transform(String version,
                                 CLEVE parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }
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
    public static String validateEntry(CLEVE parser) {
        //TODO later we need more results but focus on numerics for now
        try {
            if (parser.getEventResultClassCode().equals(RESULT_STATUS_AUTHORIZED)) {
                return null;
            }
        } catch (Exception ex) {
            LOG.error("Problem parsing Event Result Status Code" + parser.toString());
        }
        return "Non-numeric result ignored for now";
    }


    /*
     *
     */
    public static void createObservation(CLEVE parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BartsCsvHelper csvHelper,
                                         String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getPatientId()))};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getPatientId(), null, null, null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        // Performer
        //TODO Is IDENTIFIER_SYSTEM_BARTS_PERSONNEL_ID the right one?
        //  ResourceId clinicianResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getPatientId(), null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);
        ResourceId clinicianResourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Practitioner", parser.getClinicianID());


        // Encounter
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId());
            createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, null, encounterResourceId, Encounter.EncounterState.FINISHED, parser.getEffectiveDateTime(), parser.getEffectiveDateTime(), null, Encounter.EncounterClass.OTHER);
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
        //TODO we need to filter out any records that are not final
        fhirObservation.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.FINAL);
        fhirObservation.addPerformer(ReferenceHelper.createReference(ResourceType.Practitioner, clinicianResourceId.getResourceId().toString()));

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        fhirObservation.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));

//        String clinicianID = parser.getClinicianID();
//        if (!Strings.isNullOrEmpty(clinicianID)) {
//            //TODO - need to map to person resources
//            //fhirObservation.addPerformer(csvHelper.createPractitionerReference(clinicianID));
//        }

        String term = parser.getEventTitleText();
        if (Strings.isNullOrEmpty(term)) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.CLINICAL_CODE_TYPE,
                    Long.parseLong(parser.getEventCode()),
                    fhirResourceFiler.getServiceId());

            term = cernerCodeValueRef.getCodeDispTxt();

        }

        String code = parser.getEventCode();

        //TODO - establish code mapping for millenium / FHIR
        // Just staying with FHIR code for now - valid approach?
        CodeableConcept obsCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_CERNER_CODE_ID, term, code);
        //TerminologyService.translateToSnomed(obsCode);
        // Previous null test wouldn't work. Specific errors should get an exception
        if (obsCode.getCoding().isEmpty()) {
            LOG.warn("Unable to create codeableConcept for Observation ID: " + observationID);
            return;
        } else {
            fhirObservation.setCode(obsCode);
        }

        //set value and units
        //TODO need to check getEventResultClassCode()
        // Need to store comparator separately  - new method in Quantityhelper
        // if it looks like a plain number it's simple.
        // if it contains  valid comparator symbol, use it
        String value = parser.getEventResultAsText();
        if (!Strings.isNullOrEmpty(value)) {
            Number num = null;
            num = NumberFormat.getInstance().parse(value);
            if (num == null) {
                //TODO we need to cater for some non-numeric results
                LOG.debug("Not proceeding with non-numeric value for now");
                return;
            }
            Double value1 = Double.parseDouble(value);
            String unitsCode = parser.getEventUnitsCode();
            String units = "";   //TODO - map units from unitsCode

            for (String comp : comparators) {
                if (value.contains(comp)) {
                    fhirObservation.setValue(QuantityHelper.createSimpleQuantity(value1,
                            units,
                            Quantity.QuantityComparator.fromCode(comp),
                            FhirUri.CODE_SYSTEM_CERNER_CODE_ID,
                            unitsCode));
                } else {
                    fhirObservation.setValue(QuantityHelper.createSimpleQuantity(value1, units, FhirUri.CODE_SYSTEM_CERNER_CODE_ID, unitsCode));
                }
            }

            //TODO - set comments
            fhirObservation.setComments(parser.getEventTag());

            // Reference range if supplied
            if (parser.getEventNormalRangeLow() != null && parser.getEventNormalRangeHigh() != null) {
                Double rangeLow = Double.parseDouble(parser.getEventNormalRangeLow());
                SimpleQuantity low = QuantityHelper.createSimpleQuantity(rangeLow, units);
                Double rangeHigh = Double.parseDouble(parser.getEventNormalRangeHigh());
                SimpleQuantity high = QuantityHelper.createSimpleQuantity(rangeHigh, units);
                //TODO I think I need a new createcodeableconcept method without term ???
                // I think the fhir page suggests this but may be wrong
                CodeableConcept normCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_CERNER_CODE_ID, "normal", parser.getEventNormalcyCode());
                Observation.ObservationReferenceRangeComponent obsRange = new Observation.ObservationReferenceRangeComponent();
                obsRange.setHigh(high);
                obsRange.setLow(low);
                obsRange.setMeaning(normCode);
                fhirObservation.addReferenceRange(obsRange);
            }
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

}
