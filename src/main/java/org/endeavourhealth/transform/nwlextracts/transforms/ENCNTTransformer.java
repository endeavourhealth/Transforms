package org.endeavourhealth.transform.nwlextracts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.helpers.NwlExtractsHelper;
import org.endeavourhealth.transform.nwlextracts.schema.ENCNT;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ENCNTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        NwlExtractsHelper nwlextractsHelper = new NwlExtractsHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), null);

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createEncounter((ENCNT)parser, fhirResourceFiler, csvHelper,nwlextractsHelper,sourceSystem);
            }
        }
    }

    public static void createEncounter(ENCNT parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, NwlExtractsHelper nwlextractsHelper, String sourceSystem) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveIndicator();

        //cache our extract date for later
        CsvCell extractDateTimeCell = parser.getExtractDateTime();
        csvHelper.cacheExtractDateTime(extractDateTimeCell);

        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell);

        Patient existingPatient = (Patient) csvHelper.retrieveResource(personIdCell.getString(), ResourceType.Patient);

        if (existingPatient == null) {
            TransformWarnings.log(LOG, csvHelper, "Encounter ID {} has either no associated Patient already existed or deleted the patient for person ID {}", encounterIdCell, personIdCell);
        }

        //if inactive, we want to delete it
        if (!activeCell.getIntAsBoolean()) {
            csvHelper.getEncounterCache().deleteEncounter(encounterBuilder, encounterIdCell, personIdCell, fhirResourceFiler, parser.getCurrentState(), activeCell);
            return;
        }

        CsvCell finIdCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getVisitId();
        CsvCell treatmentFunctionCodeCell = parser.getCurrentTreatmentFunctionMillenniumCode();
        CsvCell mainSpecialtyCodeCell = parser.getMainSpecialtyMillenniumCode();

        /* Parent Encounter As in ADT */

        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        // class
        CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        String encounterType=null;

        Encounter.EncounterClass cls = getEncounterClass(encounterTypeCodeCell, csvHelper);

        if (cls == null) {
            return;
        } else {
            encounterBuilder.setClass(cls, encounterTypeCodeCell);
        }

        // EncounterTable type
        if (!NwlExtractsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
            if (codeRef != null) {
                String typeDesc = codeRef.getCodeDispTxt();
                encounterBuilder.setType(typeDesc, encounterTypeCodeCell);
            }
        }

        CsvCell enctCreationDateCell = parser.getEncounterCreateDateTime();
        CsvCell recordedDateCell = parser.getEncounterEffectiveDateTime();

        if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(enctCreationDateCell)) {

            Date effectiveDt = NwlExtractsCsvHelper.parseDate(enctCreationDateCell);
            encounterBuilder.setPeriodStart(effectiveDt, enctCreationDateCell);
        } else {
            TransformWarnings.log(LOG, parser, "Encounter Created Date is not found  in ENCNT for the Encounter ID {} and Patient ID {}", encounterIdCell,personIdCell);
        }

        if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(recordedDateCell)) {

            Date recordedDt = NwlExtractsCsvHelper.parseDate(recordedDateCell);
            encounterBuilder.setRecordedDate(recordedDt, recordedDateCell);
        } else {
            TransformWarnings.log(LOG, parser, "Encounter Effective/Recorded Date is not found in ENCNT for the Encounter ID {} and Patient ID {}", encounterIdCell,personIdCell);
        }

        // status
        CsvCell statusCell = parser.getEncounterStatusMillenniumCode();
        Encounter.EncounterState status = getEncounterStatus(statusCell.getString(), encounterIdCell, parser);
        encounterBuilder.setStatus(status, statusCell);

        /* Set Common Attributes as in ADT */

        if (null!=visitIdCell && !visitIdCell.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, visitIdCell.getString());
            if (encounterBuilder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, fhirResourceFiler);
            }
            encounterBuilder.setEpisodeOfCare(episodeReference);
        }

        // Location
        CsvCell locationRefCell = parser.getCurrentLocationIdentifier();

        if (NwlExtractsCsvHelper.isEmptyOrIsZero(locationRefCell)  ) {
            TransformWarnings.log(LOG, parser, "Encounter Location  is not found in ENCNT for the Encounter ID {} and Patient ID {}", encounterIdCell,personIdCell);

        } else {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.LOCATION_NAME, locationRefCell);

            if (codeRef != null) {
                String codeDesc = codeRef.getCodeDescTxt();
                String codeDisp = codeRef.getCodeDispTxt();

                if(Strings.isNullOrEmpty(codeDesc) && Strings.isNullOrEmpty(codeDisp)) {
                    TransformWarnings.log(LOG, parser, "Encounter Location Name is NULL in ENCNT for the Encounter ID {} and Location ID {} ", encounterIdCell,locationRefCell);
                }
            }

            Reference patientAssignedLocReference
                    = ReferenceHelper.createReference(ResourceType.Location, locationRefCell.getString());
            if (encounterBuilder.isIdMapped()) {
                patientAssignedLocReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientAssignedLocReference, nwlextractsHelper);
            }

            if(encounterBuilder.getLocation().size() > 0) {
                for (int i = 0; i < encounterBuilder.getLocation().size(); i++) {
                    encounterBuilder.getLocation().remove(i);
                }
            }

            encounterBuilder.addLocation(patientAssignedLocReference);
            encounterBuilder.getLocation().get(encounterBuilder.getLocation().size() - 1).setStatus(Encounter.EncounterLocationStatus.ACTIVE);
        }

        // responsible person
        CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
        if (NwlExtractsCsvHelper.isEmptyOrIsZero(responsibleHCPCell)) {
            TransformWarnings.log(LOG, parser, "ResponsibleHCPCell Code is not found in ENCNT for the encounter ID {} and Patient ID {} ", encounterIdCell,personIdCell);

        } else {

            Reference practitionerReference = csvHelper.createPractitionerReference(responsibleHCPCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, true, responsibleHCPCell);
        }

        if (!NwlExtractsCsvHelper.isEmptyOrIsZero(mainSpecialtyCodeCell)) {
            Reference organisationReference = csvHelper.createSpecialtyOrganisationReference(mainSpecialtyCodeCell);
            if (encounterBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            encounterBuilder.setServiceProvider(organisationReference);
        }

        //add in additional extended data as Parameters resource with additional extension

            // Fin Number
            if (!finIdCell.isEmpty()) {
                MapResponse propertyResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem,"fin_number",null);
                String propertyCode = propertyResponse.getConcept().getCode();
                String propertyName = "TEXT_"+propertyCode;
                containedParametersBuilder.addParameter(propertyName, finIdCell.getString());
            }

           // Encounter Type
            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
                encounterType=encounterTypeCodeCell.getString();

                MapResponse propertyResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem,"encounter_type",null);
                MapResponse valueResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem,"encounter_type",encounterType);

                if(valueResponse.getWasCreated()) {
                    NwlExtractsHelper.raiseMissingAlert(encounterType, "encounter_type", sourceSystem, nwlextractsHelper.getExchangeId().toString(), personIdCell.getString(), nwlextractsHelper);
                }

                CodeableConcept ccValue = new CodeableConcept();
                ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                        .setSystem(valueResponse.getConcept().getScheme());

                containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
            }

            // treatment function

            if (NwlExtractsCsvHelper.isEmptyOrIsZero(treatmentFunctionCodeCell)) {
                TransformWarnings.log(LOG, parser, "Cerner treatmentFunctionCode is not found in ENCNT for the encounter ID {} and Patient ID {}", encounterIdCell,personIdCell);
                //  SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "treatmentFunctionCode Code not found for the Patient ID"+ personIdCell + "and encounter ID in ENCNTT" + encounterIdCell + "for ODS Code" +sourceSystem);

            } else {

                String treatmentFunctionCode = treatmentFunctionCodeCell.getString();

                MapResponse propertyResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "treatment_function_code", null);
                MapResponse valueResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "treatment_function_code", treatmentFunctionCode);

                if (valueResponse.getWasCreated()) {
                    NwlExtractsHelper.raiseMissingAlert(treatmentFunctionCode, "treatment_function_code", sourceSystem, nwlextractsHelper.getExchangeId().toString(), personIdCell.getString(), nwlextractsHelper);
                }
                CodeableConcept ccValue = new CodeableConcept();
                ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                        .setSystem(valueResponse.getConcept().getScheme());

                containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);

        }

        // Child Encounters or Encounter events
        String desc=null;
        if (!NwlExtractsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
            if (codeRef!=null) {
                desc = codeRef.getCodeDispTxt();
            }
        }
        if (desc.equalsIgnoreCase("Inpatient Waiting List") || desc.equalsIgnoreCase("Day Case Waiting List") || (cls.toString().equals("OUTPATIENT"))) {

                EncounterEventTransformer.createEncounterEvents(parser, encounterBuilder,fhirResourceFiler, csvHelper);
        }
        //we don't save immediately, but return the Encounter builder to the cache
        csvHelper.getEncounterCache().returnEncounterBuilder(encounterIdCell, encounterBuilder);
    }


    public static Encounter.EncounterClass getEncounterClass(CsvCell encounterTypeCodeCell, NwlExtractsCsvHelper csvHelper) throws Exception {

        if (NwlExtractsCsvHelper.isEmptyOrIsZero(encounterTypeCodeCell)) {
            return null;
        }

        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.ENCOUNTER_TYPE, encounterTypeCodeCell);
        if (codeRef == null) {
            return null;
        }

        String desc = codeRef.getCodeDispTxt();
        switch (desc) {
            case "Inpatient":
            case "Regular Day Admission":
            case "Regular Night Admission":
            case "Day Case":
            case "Day Case Waiting List":
            case "Newborn":
            case "Maternity":
            case "Inpatient Waiting List":
            case "Inpatient Pre-Admission":


                return Encounter.EncounterClass.INPATIENT;

            case "Outpatient":
            case "Outpatient Pre-Registration":
            case "Outpatient Referral":
            case "Outpatient Services":

                return Encounter.EncounterClass.OUTPATIENT;

            //  case "Emergency":
            case "Emergency Department":

                return Encounter.EncounterClass.EMERGENCY;

            default:
                TransformWarnings.log(LOG, csvHelper, "Unknown ENCNT type {}", desc);
                return null;
        }
    }

    private static Encounter.EncounterState getEncounterStatus(String millenniumCode, CsvCell encounterIdCell,  ParserI parser) throws Exception {
        if (millenniumCode.compareTo("666807") == 0) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.compareTo("666808") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("666809") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("854") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.compareTo("855") == 0) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.compareTo("856") == 0) { return Encounter.EncounterState.FINISHED; }
        else if (millenniumCode.compareTo("857") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.compareTo("858") == 0) { return Encounter.EncounterState.ARRIVED; }
        else if (millenniumCode.compareTo("859") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("860") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else {
            TransformWarnings.log(LOG, parser, "Millennium status-code {} is not found in ENCNT for Personnel-id {} record {} in file {}. Status set to in-progress", millenniumCode, encounterIdCell.getString(), parser.getFilePath());
            return Encounter.EncounterState.INPROGRESS;
        }
    }
}
