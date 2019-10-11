package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppImmunisationContent;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ImmunizationBuilder;
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

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            immunizationBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell eventDate = parser.getDateEvent();
        if (!eventDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(eventDate.getDateTime());
            immunizationBuilder.setPerformedDate(dateTimeType, eventDate);
        }

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            immunizationBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            if (staffReference != null) {
                immunizationBuilder.setPerformer(staffReference, staffMemberIdDoneBy);
            }
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

        CsvCell immsSNOMEDCodeCell = parser.getImmsSNOMEDCode();
        CsvCell readV3CodeCell = parser.getImmsReadCode();

        //only add a codeable concept if either a Snomed code or Ctv3 code is present
        if (!readV3CodeCell.isEmpty() ||
                (immsSNOMEDCodeCell != null && !immsSNOMEDCodeCell.isEmpty() && immsSNOMEDCodeCell.getLong() != -1)) {

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code);

            boolean addedSnomed = false;
            String snomedCodeDisplayText = "";

            if (immsSNOMEDCodeCell != null //might be null in older versions
                    && !immsSNOMEDCodeCell.isEmpty()
                    && immsSNOMEDCodeCell.getLong() != -1) {

                SnomedCode snomedCode = TerminologyService.lookupSnomedFromConceptId(immsSNOMEDCodeCell.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    snomedCodeDisplayText = snomedCode.getTerm();   //use to set display text later
                    addedSnomed = true;
                }
            }

            if (!readV3CodeCell.isEmpty()) {

                String code = readV3CodeCell.getString();
                if (!code.startsWith("Y")) {

                    //only add the Snomed translation if not already added Snomed
                    if (!addedSnomed) {
                        SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(code);
                        if (snomedCode != null) {

                            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                            codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                            codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                            snomedCodeDisplayText = snomedCode.getTerm();   //use to set display text later
                        }
                    }

                    // add Ctv3 coding
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);

                } else {

                    //this is a TPP Ctv3 local code
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_TPP_CTV3);
                }

                //set the code from the received code cell
                codeableConceptBuilder.setCodingCode(code, readV3CodeCell);

                //perform a ctv3 lookup to get the code term details as this is not supplied in the extract
                TppCtv3Lookup ctv3Lookup = csvHelper.lookUpTppCtv3Code(code, parser);
                if (ctv3Lookup != null) {
                    String readV3Term = ctv3Lookup.getCtv3Text();
                    codeableConceptBuilder.setCodingDisplay(readV3Term);
                    codeableConceptBuilder.setText(readV3Term);   //display text set here in-case no Snomed term derived
                }
            }

            // if Snomed code display text is set then use it for the display text (preferred over Ctv3 as no term supplied)
            if (!snomedCodeDisplayText.isEmpty()) {
                codeableConceptBuilder.setText(snomedCodeDisplayText);
            }
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

        CsvCell immContent = parser.getIDImmunisationContent();
        if (!immContent.isEmpty()) {
            TppImmunisationContent tppImmunisationContent = csvHelper.lookUpTppImmunisationContent(immContent.getLong(), parser);
            if (tppImmunisationContent != null) {
                String contentName = tppImmunisationContent.getName();
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

