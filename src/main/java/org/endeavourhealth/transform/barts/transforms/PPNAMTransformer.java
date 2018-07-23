package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPNAMTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createPatientName((PPNAM)parser, fhirResourceFiler, csvHelper);
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

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr), csvHelper);
                if (patientBuilder != null) {
                    NameBuilder.removeExistingNameById(patientBuilder, nameIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        HumanName.NameUse nameUse = null;

        CsvCell nameTypeCell = parser.getNameTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(nameTypeCell)) {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.NAME_USE, nameTypeCell);
            String codeDesc = codeRef.getCodeMeaningTxt();
            nameUse = convertNameUse(codeDesc);
        }

        CsvCell titleCell = parser.getTitle();
        CsvCell prefixCell = parser.getPrefix();
        CsvCell firstNameCell = parser.getFirstName();
        CsvCell middleNameCell = parser.getMiddleName();
        CsvCell lastNameCell = parser.getLastName();
        CsvCell suffixCell = parser.getSuffix();

        //since we're potentially updating an existing Patient resource, remove any existing name matching our ID
        NameBuilder.removeExistingNameById(patientBuilder, nameIdCell.getString());

        //and remove any pre-existing name that was added by the ADT feed
        removeExistingNameWithoutIdByValue(patientBuilder, titleCell, prefixCell, firstNameCell, middleNameCell, lastNameCell, suffixCell);

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setId(nameIdCell.getString(), nameIdCell);
        nameBuilder.setUse(nameUse, nameTypeCell);
        nameBuilder.addPrefix(titleCell.getString(), titleCell);
        nameBuilder.addPrefix(prefixCell.getString(), prefixCell);
        nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
        nameBuilder.addGiven(middleNameCell.getString(), middleNameCell);
        nameBuilder.addFamily(lastNameCell.getString(), lastNameCell);
        nameBuilder.addSuffix(suffixCell.getString(), suffixCell);

        CsvCell startDate = parser.getBeginEffectiveDate();
        if (!startDate.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(startDate);
            nameBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDater();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = BartsCsvHelper.parseDate(endDate);
            nameBuilder.setEndDate(d, endDate);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }

    private static void removeExistingNameWithoutIdByValue(PatientBuilder patientBuilder, CsvCell titleCell, CsvCell prefixCell, CsvCell firstNameCell, CsvCell middleNameCell, CsvCell lastNameCell, CsvCell suffixCell) {
        Patient patient = (Patient)patientBuilder.getResource();
        if (!patient.hasName()) {
            return;
        }

        List<HumanName> names = patient.getName();
        for (int i=names.size()-1; i>=0; i--) {
            HumanName name = names.get(i);

            //if this name has an ID it was created by this data warehouse feed, so don't try to remove it
            if (name.hasId()) {
                continue;
            }

            if (!titleCell.isEmpty()
                    && !NameConverter.hasPrefix(name, titleCell.getString())) {
                continue;
            }

            if (!prefixCell.isEmpty()
                    && !NameConverter.hasPrefix(name, prefixCell.getString())) {
                continue;
            }

            if (!firstNameCell.isEmpty()
                    && !NameConverter.hasGivenName(name, firstNameCell.getString())) {
                continue;
            }

            if (!middleNameCell.isEmpty()
                    && !NameConverter.hasGivenName(name, middleNameCell.getString())) {
                continue;
            }

            if (!lastNameCell.isEmpty()
                    && !NameConverter.hasFamilyName(name, lastNameCell.getString())) {
                continue;
            }

            if (!suffixCell.isEmpty()
                    && !NameConverter.hasSuffix(name, suffixCell.getString())) {
                continue;
            }

            //if we make it here, it's a duplicate and should be removed
            names.remove(i);
        }
    }


    private static HumanName.NameUse convertNameUse(String statusCode) {
        switch (statusCode) {
            case "ADOPTED": return HumanName.NameUse.OFFICIAL;
            case "ALTERNATE": return HumanName.NameUse.NICKNAME;
            case "CURRENT": return HumanName.NameUse.OFFICIAL;
            case "LEGAL": return HumanName.NameUse.OFFICIAL;
            case "MAIDEN": return HumanName.NameUse.MAIDEN;
            case "OTHER": return HumanName.NameUse.TEMP;
            case "PREFERRED": return HumanName.NameUse.USUAL;
            case "PREVIOUS": return HumanName.NameUse.OLD;
            case "PRSNL": return HumanName.NameUse.TEMP;
            case "NYSIIS": return HumanName.NameUse.TEMP;
            case "ALT_CHAR_CUR": return HumanName.NameUse.NICKNAME;
            case "USUAL": return HumanName.NameUse.USUAL;
            case "HEALTHCARD": return HumanName.NameUse.TEMP;
            case "BACHELOR": return HumanName.NameUse.OLD;
            case "BIRTH": return HumanName.NameUse.OLD;
            case "NONHIST": return HumanName.NameUse.OLD;
            default: return null;
        }
    }
}