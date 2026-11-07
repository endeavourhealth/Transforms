package org.endeavourhealth.transform.nwlextracts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCodeableConceptHelper;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPNAM;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPNAMTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //no try/catch as records in this file aren't independent and can't be re-processed on their own

                //filter on patients
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }
                createPatientName((PPNAM) parser, fhirResourceFiler, csvHelper, sourceSystem);
            }
        }
    }


    public static void createPatientName(PPNAM parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        CsvCell nameIdCell = parser.getMillenniumPersonNameId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //if non-active (i.e. deleted) we should REMOVE the name, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the name
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            //There are a small number of cases where all the fields are empty (including person ID) but in all examined
            //cases, we've never previously received a valid record, so can just ignore them
            if (!personIdCell.isEmpty()) {
                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
                if (patientBuilder != null) {
                    NameBuilder.removeExistingNameById(patientBuilder, nameIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
                }
            }
            TransformWarnings.log(LOG, parser, "Patient Person ID for the Name ID in PPNAM {} not found", nameIdCell);
            return;
        }

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
        if (patientBuilder == null) {
            //LOG.trace("No patient builder, so skipping");
            return;
        }

        try {

            CsvCell titleCell = parser.getTitle();
            CsvCell prefixCell = parser.getPrefix();
            CsvCell firstNameCell = parser.getFirstName();
            CsvCell middleNameCell = parser.getMiddleName();
            CsvCell lastNameCell = parser.getLastName();
            CsvCell suffixCell = parser.getSuffix();

            if (lastNameCell == null) {
                TransformWarnings.log(LOG, parser, "Cerner lastname {} for the Patient ID in PPNAM {} not found", lastNameCell,personIdCell);
            //    SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "ethnicity Code not found for the Patient ID in PPATI " + personIdCell+ "for ODS Code" + NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);
            }

            //by always removing and re-adding the name, we're constantly changing the order of the names in the resource
            //so attempt to reuse the HumanName by matching on the ID
            NameBuilder nameBuilder = NameBuilder.findOrCreateForId(patientBuilder, nameIdCell);
            nameBuilder.reset(); //we always re-populate the full record, so clear down

            if (!Strings.isNullOrEmpty(prefixCell.getString())) {
                    nameBuilder.addPrefix(prefixCell.getString(), prefixCell);
            } else {
                nameBuilder.addPrefix(titleCell.getString(), titleCell);
            }

            nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
            nameBuilder.addGiven(middleNameCell.getString(), middleNameCell);
            nameBuilder.addFamily(lastNameCell.getString(), lastNameCell);
            nameBuilder.addSuffix(suffixCell.getString(), suffixCell);

            CsvCell startDate = parser.getBeginEffectiveDate();
            if (!NwlExtractsCsvHelper.isEmptyOrIsStartOfTime(startDate)) { //possible to get empty start dates
                Date d = NwlExtractsCsvHelper.parseDate(startDate);
                nameBuilder.setStartDate(d, startDate);
            }

            CsvCell endDate = parser.getEndEffectiveDater();
            //use this function to test the endDate cell, since it will have the Cerner end of time content
            if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
                Date d = NwlExtractsCsvHelper.parseDate(endDate);
                nameBuilder.setEndDate(d, endDate);
            }

            boolean isActive = true;
            if (nameBuilder.getNameCreated().hasPeriod()) {
                isActive = PeriodHelper.isActive(nameBuilder.getNameCreated().getPeriod());
            }

            CsvCell nameTypeCell = parser.getNameTypeCode();
            CsvCell codeMeaningCell = NwlExtractsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.NAME_USE, nameTypeCell);
           if(null!=codeMeaningCell) {
               HumanName.NameUse nameUse = convertNameUse(codeMeaningCell.getString(), isActive);
               nameBuilder.setUse(nameUse, nameTypeCell, codeMeaningCell);

               //remove any duplicate pre-existing name that was added by the ADT feed
               HumanName humanNameAdded = nameBuilder.getNameCreated();
               removeExistingNameWithoutIdByValue(patientBuilder, humanNameAdded);
           }
        } finally {
            //no need to save the resource now, as all patient resources are saved at the end of the PP... files
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
        }
    }

    public static void removeExistingNameWithoutIdByValue(PatientBuilder patientBuilder, HumanName check) {

        Patient patient = (Patient) patientBuilder.getResource();
        if (!patient.hasName()) {
            return;
        }

        List<HumanName> names = patient.getName();
        List<HumanName> duplicates = NameHelper.findMatches(check, names);

        for (HumanName duplicate: duplicates) {

            //if this name has an ID it was created by this data warehouse feed, so don't try to remove it
            if (duplicate.hasId()) {
                continue;
            }

            //if we make it here, it's a duplicate and should be removed
            names.remove(duplicate);
        }
    }
    private static HumanName.NameUse convertNameUse(String statusCode, boolean isActive) {

        //FHIR spec states that any ended name should be flagged as OLD
        if (!isActive) {
            return HumanName.NameUse.OLD;
        }

        switch (statusCode) {
            case "ADOPTED":
            case "CURRENT":
            case "LEGAL":
                return HumanName.NameUse.OFFICIAL;
            case "ALTERNATE":
            case "ALT_CHAR_CUR":
                return HumanName.NameUse.NICKNAME;
            case "MAIDEN":
                return HumanName.NameUse.MAIDEN;
            case "OTHER":
            case "HEALTHCARD":
            case "PRSNL":
            case "NYSIIS":
                return HumanName.NameUse.TEMP;
            case "PREFERRED":
            case "USUAL":
                return HumanName.NameUse.USUAL;
            case "PREVIOUS":
            case "BACHELOR":
            case "BIRTH":
            case "NONHIST":
                return HumanName.NameUse.OLD;
            default:
                return null;
        }
    }
}