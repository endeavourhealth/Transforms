package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPNAMTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformer.class);

    private static final String PPNAM_ID_TO_PERSON_ID = "PPNAM_ID_TO_PERSON_ID";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createPatientName((PPNAM)parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }


    public static void createPatientName(PPNAM parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell nameIdCell = parser.getMillenniumPersonNameId();

        //if non-active (i.e. deleted) we should REMOVE the name, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the name
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPNAM_ID_TO_PERSON_ID, nameIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(Long.valueOf(personIdStr), csvHelper);
                NameBuilder.removeExistingName(patientBuilder, nameIdCell.getString());
            }
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(personIdCell, csvHelper);

        //since we're potentially updating an existing Patient resource, remove any existing name matching our ID
        NameBuilder.removeExistingName(patientBuilder, nameIdCell.getString());

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

        //and we need to store the PPADD ID -> PERSON ID mapping so that if the address is ever deleted,
        //we can find the person it belonged to, since the deleted records only give us the ID
        csvHelper.saveInternalId(PPNAM_ID_TO_PERSON_ID, nameIdCell.getString(), personIdCell.getString());

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
    }


    /*private static String parseTitleAndPrefix(String title, String prefix) throws Exception {

        if (Strings.isNullOrEmpty(title) && Strings.isNullOrEmpty(prefix)) {
            return "";
        } else if (Strings.isNullOrEmpty(title) && !Strings.isNullOrEmpty(prefix)) {
            return prefix;
        } else if (!Strings.isNullOrEmpty(title) && Strings.isNullOrEmpty(prefix)) {
            return title;
        } else {
            if (title.toLowerCase().equals(prefix.toLowerCase())) {
                return prefix;
            } else {
                return processKnownDuplicateTitles(title, prefix);
            }

        }
    }

    private static String processKnownDuplicateTitles(String title, String prefix) {

        if (title.toLowerCase().replace(".", "").equals("master") && prefix.toLowerCase().replace(".", "").equals("mr")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("ms") && prefix.toLowerCase().replace(".", "").equals("miss")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("mst") && prefix.toLowerCase().replace(".", "").equals("mr")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("mister") && prefix.toLowerCase().replace(".", "").equals("mr")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("m")) {
            return prefix;
        }

        return prefix + " " + title ;
    }*/

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