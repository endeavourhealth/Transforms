package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPPHO;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPPHOTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPPHOTransformer.class);
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;

    public static void transform(String version,
                                 PPPHO parser,
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
                    createPatientPhone(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPPHO parser) {
        return null;
    }

    public static void createPatientPhone(PPPHO parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          BartsCsvHelper csvHelper,
                                          String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {



        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        Patient fhirPatient = PatientResourceCache.getPatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()));

        // If we can't find a patient resource from a previous PPATI file, throw an exception but if the line is inactive then just ignore it
        if (fhirPatient == null) {
            if (parser.isActive()) {
                LOG.warn("Patient Resource Not Found In Cache: " + parser.getMillenniumPersonIdentifier());
            } else {
                return;
            }
        }

        // Patient Address
        if (parser.getPhoneNumber() != null && parser.getPhoneNumber().length() > 0 ) {
            String phoneNumber = parser.getPhoneNumber();
            if (parser.getExtension() != null && parser.getExtension().length() > 0 ) {
                phoneNumber += " " + parser.getExtension();
            }

            if (parser.getPhoneTypeCode() != null && parser.getPhoneTypeCode().length() > 0 ) {
                CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                        RdbmsCernerCodeValueRefDal.PHONE_TYPE,
                        Long.parseLong(parser.getPhoneTypeCode()),
                        fhirResourceFiler.getServiceId());

                ContactPoint.ContactPointUse use = ContactPoint.ContactPointUse.TEMP;
                ContactPoint.ContactPointSystem system = ContactPoint.ContactPointSystem.OTHER;

                if (cernerCodeValueRef != null) {
                    use = convertPhoneType(cernerCodeValueRef.getCodeMeaningTxt());

                    system = convertPhoneSystem(cernerCodeValueRef.getCodeMeaningTxt());
                } else {
                    LOG.warn("Phone Type code: " + parser.getPhoneTypeCode() + " not found in Code Value lookup");
                }

                ContactPoint contactPoint = ContactPointHelper.create(system, use, phoneNumber);

                fhirPatient.addTelecom(contactPoint);
            }
        }

        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()), fhirPatient);

    }

    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType) {
        switch (phoneType) {
            case "HOME":
            case "VHOME":
            case "PHOME":
            case "USUAL":
            case "FAX PERS":
            case "PAGER PERS":
                return ContactPoint.ContactPointUse.HOME;
            case "FAX BUS":
            case "PROFESSIONAL":
            case "SECBUSINESS":
                return ContactPoint.ContactPointUse.WORK;
            case "MOBILE":
                return ContactPoint.ContactPointUse.MOBILE;
            case "FAX PREV":
            case "PREVIOUS":
            case "PAGER PREV":
                return ContactPoint.ContactPointUse.OLD;
            case "FAX TEMP":
                return ContactPoint.ContactPointUse.TEMP;
            default: return ContactPoint.ContactPointUse.TEMP;
        }
    }

    private static ContactPoint.ContactPointSystem convertPhoneSystem(String phoneType) {
        switch (phoneType) {
            case "ALTERNATE":
            case "BILLING":
            case "EMC":
            case "PHONEEPRESCR":
            case "HOME":
            case "MOBILE":
            case "OS AFTERHOUR":
            case "OS PHONE":
            case "OS BK OFFICE":
            case "PREVIOUS":
            case "PHOME":
            case "PROFESSIONAL":
            case "CARETEAM":
            case "SECBUSINESS":
            case "TECHNICAL":
            case "USUAL":
            case "VHOME":
            case "VERIFY":
                return ContactPoint.ContactPointSystem.PHONE;
            case "FAX BUS":
            case "FAXEPRESCR":
            case "FAX ALT":
            case "FAX BILL":
            case "FAX PERS":
            case "FAX PREV":
            case "OS FAX":
                return ContactPoint.ContactPointSystem.FAX;
            case "PAGER ALT":
            case "PAGER PREV":
            case "PAGER PERS":
            case "OS PAGER":
            case "PAGER BILL":
            case "PAGING":
                return ContactPoint.ContactPointSystem.PAGER;
            default: return ContactPoint.ContactPointSystem.OTHER;
        }
    }

}

