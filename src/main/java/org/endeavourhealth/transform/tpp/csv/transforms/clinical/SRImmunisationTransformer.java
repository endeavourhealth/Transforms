package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppImmunisationContent;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ImmunizationBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRImmunisation;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Immunization;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRImmunisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRImmunisationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRImmunisation.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRImmunisation) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRImmunisation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            Immunization immunization = (Immunization) csvHelper.retrieveResource(rowId.getString(), ResourceType.Immunization);
            if (immunization != null) {
                ImmunizationBuilder immunizationBuilder = new ImmunizationBuilder(immunization);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, immunizationBuilder);
            }
            return;
        }

        ImmunizationBuilder immunizationBuilder = new ImmunizationBuilder();
        immunizationBuilder.setId(rowId.getString(), rowId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        immunizationBuilder.setPatient(patientReference, patientId);

        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId);
            immunizationBuilder.setEncounter(eventReference, eventId);
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            immunizationBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell eventDate = parser.getDateEvent();
        if (!eventDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(eventDate.getDate());
            immunizationBuilder.setPerformedDate(dateTimeType, eventDate);
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId = csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                immunizationBuilder.setRecordedBy(staffReference, recordedBy);
            }
        }

        CsvCell encounterDoneBy = parser.getIDDoneBy();
        if (!encounterDoneBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReference(encounterDoneBy);
            immunizationBuilder.setPerformer(staffReference, encounterDoneBy);
        }

        CsvCell dose = parser.getDose();
        if (!dose.isEmpty()) {
            parseDose(dose, immunizationBuilder);
        }

        CsvCell siteLocation = parser.getLocation();
        if (!siteLocation.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(siteLocation);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                immunizationBuilder.setSite(mappedTerm, siteLocation);
            }
        }

        CsvCell method = parser.getMethod();
        if (!method.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(method);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                immunizationBuilder.setRoute(mappedTerm, method);
            }
        }

        CsvCell expiryDate = parser.getDateExpiry();
        if (!expiryDate.isEmpty()) {
            immunizationBuilder.setExpirationDate(expiryDate.getDate(), expiryDate);
        }

        CsvCell readV3Code = parser.getImmsReadCode();
        if (!readV3Code.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code);

            // add Ctv3 coding
            TppCtv3Lookup ctv3Lookup = csvHelper.lookUpTppCtv3Code(readV3Code.getString(), parser);

            if (ctv3Lookup != null) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
                codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
                String readV3Term = ctv3Lookup.getCtv3Text();
                codeableConceptBuilder.setCodingDisplay(readV3Term, readV3Code);
                codeableConceptBuilder.setText(readV3Term, readV3Code);
            }

            // translate to Snomed if code does not start with "Y" as they are local TPP codes
            if (!readV3Code.getString().startsWith("Y")) {
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        CsvCell vaccPart = parser.getVaccPart();
        if (!vaccPart.isEmpty()) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(vaccPart);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (StringUtils.isNumeric(mappedTerm)) {
//TODO - confirm that this is the right FHIR field for this content
                    immunizationBuilder.setProtocolSequenceNumber(Integer.parseInt(mappedTerm), vaccPart);
                } else {
//TODO - confirm that this is the right FHIR field for this content
                    immunizationBuilder.setProtocolDescription(mappedTerm, vaccPart);
                }
            }
        }

        CsvCell immContent = parser.getIDImmunisationContent();
        if (!immContent.isEmpty()) {
            TppImmunisationContent tppImmunisationContent = csvHelper.lookUpTppImmunisationContent(immContent.getLong(), parser);
            if (tppImmunisationContent != null) {
                String contentName = tppImmunisationContent.getName();
//TODO - confirm that this is the right FHIR field for this content
                immunizationBuilder.setProtocolSeriesName(contentName, immContent);
            }
        }

        CsvCell batch = parser.getVaccBatchNumber();
        if (!vaccPart.isEmpty()) {
            immunizationBuilder.setLotNumber(batch.getString(), batch);
        }


        fhirResourceFiler.savePatientResource(parser.getCurrentState(), immunizationBuilder);
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

