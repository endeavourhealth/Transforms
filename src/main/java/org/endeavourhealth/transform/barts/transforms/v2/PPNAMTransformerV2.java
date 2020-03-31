package org.endeavourhealth.transform.barts.transforms.v2;

import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPNAMTransformerV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformerV2.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        if (TransformConfig.instance().isLive()) {
            //remove this check for go live
            return;
        }

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

        //ignore deletes as the latest record will update the deleted one
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        CsvCell endDate = parser.getEndEffectiveDater();
        boolean isActive = true;
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(endDate)) {
            isActive = false;
        }

        CsvCell nameTypeCell = parser.getNameTypeCode();
        CsvCell codeMeaningCell
                = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.NAME_USE, nameTypeCell);
        HumanName.NameUse nameUse = convertNameUse(codeMeaningCell.getString(), isActive);
        //only interested in usual and active name for Core v2 patient data
        if (nameUse != HumanName.NameUse.USUAL) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //get new/cached/from db Patient
        org.endeavourhealth.core.database.dal.ehr.models.Patient patient =  csvHelper.getPatientCache().borrowPatientV2Instance(personIdCell);
        if (patient == null) {
            return;
        }

        try {
            CsvCell titleCell = parser.getTitle();
            CsvCell prefixCell = parser.getPrefix();
            CsvCell firstNameCell = parser.getFirstName();
            CsvCell middleNameCell = parser.getMiddleName();
            CsvCell lastNameCell = parser.getLastName();

            String title = titleCell.getString();
            if (!prefixCell.isEmpty()) {
                title = title + " " + prefixCell.getString();
            }
            patient.setTitle(title);

            String firstNames = firstNameCell.getString();
            if (!middleNameCell.isEmpty()) {
                firstNames = firstNames + " "+ middleNameCell.getString();
            }
            patient.setFirstNames(firstNames);

            String lastName = lastNameCell.getString();
            patient.setLastName(lastName);

        } finally {
            csvHelper.getPatientCache().returnPatientV2Instance(personIdCell, patient);
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