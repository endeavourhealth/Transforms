package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PractitionerResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMember;
import org.hl7.fhir.instance.model.HumanName;

import java.util.Map;

public class SRStaffMemberTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMember.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRStaffMember)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRStaffMember parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell staffMemberId = parser.getRowIdentifier();
        PractitionerBuilder practitionerBuilder
                = PractitionerResourceCache.getPractitionerBuilder(staffMemberId, csvHelper, fhirResourceFiler);

        CsvCell fullName = parser.getStaffName();
        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addFullName(fullName.getString(), fullName);

        CsvCell userName = parser.getStaffUserName();
        if (!userName.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);
            identifierBuilder.setValue(userName.getString(), userName);
        }

        CsvCell nationalIdType = parser.getNationalIdType();
        if (!nationalIdType.isEmpty()) {
            CsvCell nationalId = parser.getIDNational();
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(getNationalIdTypeIdentifier(nationalIdType.toString()));
            identifierBuilder.setValue(nationalId.getString(), nationalId);
        }

        CsvCell smartCardId = parser.getIDSmartCard();
        if (!smartCardId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);
            identifierBuilder.setValue(smartCardId.getString(), smartCardId);
        }

        CsvCell obsolete = parser.getObsolete();
        Boolean active = !obsolete.getBoolean();
        practitionerBuilder.setActive(active, obsolete);

    }

    private static String getNationalIdTypeIdentifier (String nationalIdType) {

        switch (nationalIdType.toUpperCase()) {
            case "GMC": return FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER;
            default: return null;
        }
    }

//    public static String getJobCategoryName(String jobCategoryCode) {
//
//        switch (jobCategoryCode){
//            case "A": return "Principal GP";
//            case "B": return "Locum GP";
//            case "C": return "GP Registrar";
//            case "D": return "Other Practice staff";
//            case "D06": return "Practice Nurse";
//            case "D07": return "Dispenser";
//            case "D08": return "Physiotherapist";
//            case "D09": return "Chiropodist";
//            case "D10": return "Interpreter /Link Worker";
//            case "D11": return "Counsellor";
//            case "D12": return "Osteopath";
//            case "D13": return "Chiropractor";
//            case "D14": return "Acupuncturist";
//            case "D15": return "Homeopath";
//            case "D16": return "Health Visitor";
//            case "D17": return "District Nurse";
//            case "D18": return "Community Psychiatric Nurse";
//            case "D19": return "Mental Handicap Nurse";
//            default: return "None";
//        }
//    }
}
