package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRImmunisation;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRImmunisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRImmunisationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRImmunisation.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRImmunisation)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRImmunisation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell eventId = parser.getIDEvent();

        ImmunizationBuilder immunizationBuilder = new ImmunizationBuilder();
        TppCsvHelper.setUniqueId(immunizationBuilder, patientId, rowId);

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        if (!eventId.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            immunizationBuilder.setEncounter(eventReference, eventId);
        }

        Reference patientReference = csvHelper.createPatientReference(patientId);
        immunizationBuilder.setPatient(patientReference, patientId);

        CsvCell deleteData = parser.getRemovedData();
        if (deleteData.getIntAsBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), immunizationBuilder);
            return;
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            immunizationBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell eventDate = parser.getDateEvent();
        immunizationBuilder.setPerformedDate(eventDate.getDateTimeType(eventDate.getDate(), "YMDT"), eventDate);

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            immunizationBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell encounterDoneBy = parser.getIDDoneBy();
        if (!encounterDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(encounterDoneBy);
            immunizationBuilder.setPerformer(staffReference, encounterDoneBy);
        }

        CsvCell orgDoneAt = parser.getIDOrganisation();
        if (!orgDoneAt.isEmpty()) {
            Reference locReference = csvHelper.createLocationReference(orgDoneAt);
            immunizationBuilder.setLocation(locReference, orgDoneAt);
        }

        CsvCell dose = parser.getDose();
        if (!dose.isEmpty()) {
            parseDose(dose, immunizationBuilder);
        }

        CsvCell siteLocation = parser.getLocation();
        if (!siteLocation.isEmpty()) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(siteLocation.getLong());
            String mappedTerm = tppMappingRef.getMappedTerm();
            immunizationBuilder.setSite(mappedTerm, siteLocation);
        }

        CsvCell method = parser.getMethod();
        if (!method.isEmpty()) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(method.getLong());
            String mappedTerm = tppMappingRef.getMappedTerm();
            immunizationBuilder.setRoute(mappedTerm, method);
        }

        CsvCell expiryDate = parser.getDateExpiry();
        if (!expiryDate.isEmpty()) {
            immunizationBuilder.setExpirationDate(expiryDate.getDate(), expiryDate);
        }

        CsvCell readV3Code = parser.getImmsReadCode();
        if (!readV3Code.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(immunizationBuilder, immunizationBuilder.TAG_VACCINE_CODEABLE_CONCEPT);

            // translate to Snomed
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
            if (snomedCode != null) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                codeableConceptBuilder.setText(snomedCode.getTerm());
            }

            immunizationBuilder.setVaccineCode(codeableConceptBuilder.getCodeableConcept());
        }

        Immunization.ImmunizationVaccinationProtocolComponent protocolComponent = new Immunization.ImmunizationVaccinationProtocolComponent();

        CsvCell vaccPart = parser.getVaccPart();
        if (!vaccPart.isEmpty()) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(vaccPart.getLong());
            String mappedTerm = tppMappingRef.getMappedTerm();
            protocolComponent.setDoseSequence(Integer.parseInt(mappedTerm));
            immunizationBuilder.setVaccinationProtocol(protocolComponent);
        }

        CsvCell batch = parser.getVaccBatchNumber();
        if (!vaccPart.isEmpty()) {
            immunizationBuilder.setLotNumber(batch.getString(), batch);
        }

        CsvCell idEvent = parser.getIDEvent();
        if (!idEvent.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            immunizationBuilder.setEncounter(eventReference, eventId);
        }
    }

    private static void parseDose(CsvCell dose, ImmunizationBuilder immunizationBuilder) {
        String[] strings = dose.getString().split(" ", 2);
        boolean success = false;
        Double value = 0.0;

        if (strings.length == 2) {
            try {
                value = Double.parseDouble(strings[0]);
                success = true;
            } catch (Exception e) {

            }
        }

        if (success) {
            immunizationBuilder.setDoseQuantity(QuantityHelper.createSimpleQuantity(value, strings[1]));
        } else {
            immunizationBuilder.setDoseQuantity(QuantityHelper.createSimpleQuantity(null, dose.getString()));
        }
    }
}

