package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPNAMTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMTransformer.class);
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;

    public static void transform(String version,
                                 PPNAM parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

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



        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        Patient fhirPatient = PatientResourceCache.getPatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()));
        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");

        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);


        // Patient Name

        HumanName.NameUse nameUse = HumanName.NameUse.OFFICIAL;

        if (parser.getNameTypeCode() != null) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.NAME_USE,
                    Long.parseLong(parser.getNameTypeCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                nameUse = convertNameUse(cernerCodeValueRef.getCodeMeaningTxt());
            } else {
                LOG.warn("Name Type code: " + parser.getNameTypeCode() + " not found in Code Value lookup");
            }
        }

        HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(
                nameUse,
                parser.getTitle(), parser.getFirstName(), parser.getMiddleName(),
                parser.getLastName(), parser.getSuffix());

        if (parser.getBeginEffectiveDate() != null || parser.getEndEffectiveDater() != null) {
            Period fhirPeriod = PeriodHelper.createPeriod(parser.getBeginEffectiveDate(), parser.getEndEffectiveDater());
            name.setPeriod(fhirPeriod);
        }

        fhirPatient.addName(name);

        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()), fhirPatient);

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