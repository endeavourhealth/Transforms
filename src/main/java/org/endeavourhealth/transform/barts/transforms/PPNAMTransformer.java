package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.openhr.transforms.common.NameConverter;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPNAMTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //no try/catch as records in this file aren't independent and can't be re-processed on their own

                //filter on patients
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }
                createPatientName((PPNAM) parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void createPatientName(PPNAM parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell nameIdCell = parser.getMillenniumPersonNameId();

        //if non-active (i.e. deleted) we should REMOVE the name, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the name
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPNAMPreTransformer.PPNAM_ID_TO_PERSON_ID, nameIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr));
                if (patientBuilder != null) {
                    NameBuilder.removeExistingNameById(patientBuilder, nameIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        LOG.trace("Processing PPNAM " + nameIdCell.getString() + " for Person ID " + personIdCell.getString());
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
        if (patientBuilder == null) {
            LOG.trace("No patient builder, so skipping");
            return;
        }

        LOG.trace("FHIR resource = " + patientBuilder.getResource().getResourceType() + " " + patientBuilder.getResource().getId());
        LOG.trace("FHIR starts with " + ((Patient) patientBuilder.getResource()).getName().size() + " names");
        CsvCell titleCell = parser.getTitle();
        CsvCell prefixCell = parser.getPrefix();
        CsvCell firstNameCell = parser.getFirstName();
        CsvCell middleNameCell = parser.getMiddleName();
        CsvCell lastNameCell = parser.getLastName();
        CsvCell suffixCell = parser.getSuffix();

        //since we're potentially updating an existing Patient resource, remove any existing name matching our ID
        boolean removedExisting = NameBuilder.removeExistingNameById(patientBuilder, nameIdCell.getString());
        LOG.trace("Removed existing = " + removedExisting + " leaving " + ((Patient) patientBuilder.getResource()).getName().size() + " names");

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setId(nameIdCell.getString(), nameIdCell);

        nameBuilder.addPrefix(titleCell.getString(), titleCell);
        nameBuilder.addPrefix(prefixCell.getString(), prefixCell);
        nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
        nameBuilder.addGiven(middleNameCell.getString(), middleNameCell);
        nameBuilder.addFamily(lastNameCell.getString(), lastNameCell);
        nameBuilder.addSuffix(suffixCell.getString(), suffixCell);

        CsvCell startDate = parser.getBeginEffectiveDate();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(startDate)) { //possible to get empty start dates
            Date d = BartsCsvHelper.parseDate(startDate);
            nameBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDater();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = BartsCsvHelper.parseDate(endDate);
            nameBuilder.setEndDate(d, endDate);
        }

        boolean isActive = true;
        if (nameBuilder.getNameCreated().hasPeriod()) {
            isActive = PeriodHelper.isActive(nameBuilder.getNameCreated().getPeriod());
        }

        CsvCell nameTypeCell = parser.getNameTypeCode();
        CsvCell codeMeaningCell = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.NAME_USE, nameTypeCell);
        HumanName.NameUse nameUse = convertNameUse(codeMeaningCell.getString(), isActive);
        nameBuilder.setUse(nameUse, nameTypeCell, codeMeaningCell);
        LOG.trace("Added all new name, FHIR now has " + ((Patient) patientBuilder.getResource()).getName().size() + " names");

        //remove any duplicate pre-existing name that was added by the ADT feed
        HumanName humanNameAdded = nameBuilder.getNameCreated();
        removeExistingNameWithoutIdByValue(patientBuilder, humanNameAdded);
        LOG.trace("Removed duplicate from ADT feed, and FHIR now has " + ((Patient) patientBuilder.getResource()).getName().size() + " names");

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
        LOG.trace("Returned to patient cache with person ID " + personIdCell);
    }

    public static void removeExistingNameWithoutIdByValue(PatientBuilder patientBuilder, HumanName check) {
        Patient patient = (Patient) patientBuilder.getResource();
        if (!patient.hasName()) {
            return;
        }

        List<HumanName> names = patient.getName();
        for (int i = names.size() - 1; i >= 0; i--) { //iterate backwards so we can remove
            HumanName name = names.get(i);

            //if this name has an ID it was created by this data warehouse feed, so don't try to remove it
            if (name.hasId()) {
                continue;
            }

            boolean matches = true;

            if (name.hasPrefix()) {
                for (StringType prefix : name.getPrefix()) {
                    if (!NameConverter.hasPrefix(check, prefix.toString())) {
                        matches = false;
                        break;
                    }
                }
            }

            if (name.hasGiven()) {
                for (StringType given : name.getGiven()) {
                    if (!NameConverter.hasGivenName(check, given.toString())) {
                        matches = false;
                        break;
                    }
                }
            }

            if (name.hasFamily()) {
                for (StringType family : name.getFamily()) {
                    if (!NameConverter.hasFamilyName(check, family.toString())) {
                        matches = false;
                        break;
                    }
                }
            }

            if (name.hasSuffix()) {
                for (StringType suffix : name.getSuffix()) {
                    if (!NameConverter.hasSuffix(check, suffix.toString())) {
                        matches = false;
                        break;
                    }
                }
            }

            if (matches) {
                //if we make it here, it's a duplicate and should be removed
                names.remove(i);
            }
        }
    }


    private static HumanName.NameUse convertNameUse(String statusCode, boolean isActive) {

        //FHIR spec states that any ended name should be flagged as OLD
        if (!isActive) {
            return HumanName.NameUse.OLD;
        }

        switch (statusCode) {
            case "ADOPTED":
                return HumanName.NameUse.OFFICIAL;
            case "ALTERNATE":
                return HumanName.NameUse.NICKNAME;
            case "CURRENT":
                return HumanName.NameUse.OFFICIAL;
            case "LEGAL":
                return HumanName.NameUse.OFFICIAL;
            case "MAIDEN":
                return HumanName.NameUse.MAIDEN;
            case "OTHER":
                return HumanName.NameUse.TEMP;
            case "PREFERRED":
                return HumanName.NameUse.USUAL;
            case "PREVIOUS":
                return HumanName.NameUse.OLD;
            case "PRSNL":
                return HumanName.NameUse.TEMP;
            case "NYSIIS":
                return HumanName.NameUse.TEMP;
            case "ALT_CHAR_CUR":
                return HumanName.NameUse.NICKNAME;
            case "USUAL":
                return HumanName.NameUse.USUAL;
            case "HEALTHCARD":
                return HumanName.NameUse.TEMP;
            case "BACHELOR":
                return HumanName.NameUse.OLD;
            case "BIRTH":
                return HumanName.NameUse.OLD;
            case "NONHIST":
                return HumanName.NameUse.OLD;
            default:
                return null;
        }
    }
}