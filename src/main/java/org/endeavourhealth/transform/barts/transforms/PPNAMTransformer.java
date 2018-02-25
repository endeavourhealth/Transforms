package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPNAMTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformer.class);


    public static void transform(String version,
                                 PPNAM parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatientName(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPNAM parser) {
        return null;
    }

    public static void createPatientName(PPNAM parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BartsCsvHelper csvHelper,
                                         String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        CsvCell millenniumPersonId = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(millenniumPersonId, csvHelper);

        CsvCell nameIdCell = parser.getMillenniumPersonNameId();
        NameBuilder.removeExistingName(patientBuilder, nameIdCell.getString());

        //if the name is no longer active, it's already been removed from the Patient so just return out
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        HumanName.NameUse nameUse = null;

        CsvCell nameTypeCell = parser.getNameTypeCode();
        if (!nameTypeCell.isEmpty() && nameTypeCell.getLong() > 0) {
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                                CernerCodeValueRef.NAME_USE,
                                                                                nameTypeCell.getLong());

            nameUse = convertNameUse(cernerCodeValueRef.getCodeMeaningTxt());
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
            nameBuilder.setStartDate(startDate.getDate(), startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDater();
        if (!endDate.isEmpty()) {
            nameBuilder.setEndDate(endDate.getDate(), endDate);
        }
    }


    private static String parseTitleAndPrefix(String title, String prefix) throws Exception {

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