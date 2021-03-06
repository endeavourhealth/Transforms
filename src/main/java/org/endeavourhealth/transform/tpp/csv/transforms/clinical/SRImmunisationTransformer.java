package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppImmunisationContent;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ImmunizationBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCodingHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
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
                immunizationBuilder.setDeletedAudit(deleteData);
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

        CsvCell dateRecorded = parser.getDateEventRecorded();
        if (!dateRecorded.isEmpty()) {
            immunizationBuilder.setRecordedDate(dateRecorded.getDateTime(), dateRecorded);
        }

        CsvCell eventDate = parser.getDateEvent();
        if (!eventDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(eventDate.getDateTime());
            immunizationBuilder.setPerformedDate(dateTimeType, eventDate);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            immunizationBuilder.setRecordedBy(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        Reference doneByReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, orgDoneAtCell);
        if (doneByReference != null) {
            immunizationBuilder.setPerformer(doneByReference, staffMemberIdDoneByCell, orgDoneAtCell);
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

                CodeableConceptBuilder immsSiteCodeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Site);
                immsSiteCodeableConceptBuilder.setText(mappedTerm, siteLocation);
            }
        }

        CsvCell method = parser.getMethod();
        if (!method.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(method);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Route, true);
                codeableConceptBuilder.setText(mappedTerm, method);
            }
        }

        CsvCell expiryDate = parser.getDateExpiry();
        if (!expiryDate.isEmpty()) {
            immunizationBuilder.setExpirationDate(expiryDate.getDate(), expiryDate);
        }

        CsvCell snomedCodeCell = parser.getImmsSNOMEDCode();
        CsvCell ctv3CodeCell = parser.getImmsReadCode();

        //only add a codeable concept if either a Snomed code or Ctv3 code is present
        if (!ctv3CodeCell.isEmpty() ||
                (snomedCodeCell != null //Snomed column not present in all versions
                        && !TppCsvHelper.isEmptyOrNegative(snomedCodeCell))) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code);
            TppCodingHelper.addCodes(codeableConceptBuilder, snomedCodeCell, null, ctv3CodeCell, null);
        }

        CsvCell vaccPart = parser.getVaccPart();
        if (!vaccPart.isEmpty()) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(vaccPart);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (StringUtils.isNumeric(mappedTerm)) {
                    immunizationBuilder.setProtocolSequenceNumber(Integer.parseInt(mappedTerm), vaccPart);
                } else {
                    immunizationBuilder.setProtocolDescription(mappedTerm, vaccPart);
                }
            }
        }

        CsvCell immContentCell = parser.getIDImmunisationContent();
        if (!immContentCell.isEmpty()) {
            TppImmunisationContent tppImmunisationContent = csvHelper.lookUpTppImmunisationContent(immContentCell);
            if (tppImmunisationContent != null) {
                String contentName = tppImmunisationContent.getName();
                immunizationBuilder.setProtocolSeriesName(contentName, immContentCell);
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
        Double value = new Double(0.0);

        if (strings.length == 2) {
            try {
                value = Double.valueOf(strings[0]);
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

