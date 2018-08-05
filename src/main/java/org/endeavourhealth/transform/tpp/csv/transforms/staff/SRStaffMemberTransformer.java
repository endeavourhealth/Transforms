package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.StaffMemberProfileCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMember;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRStaffMemberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMember.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRStaffMember) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRStaffMember parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell staffMemberId = parser.getRowIdentifier();

        //both SRStaffMember and SRStaffMemberProfile feed into the same Practitioner resources,
        //and we can get one record updated without the other(s). So we need to re-retrieve our existing
        //Practitioner because we don't want to lose anything already on it.
        PractitionerBuilder practitionerBuilder = null;
        Practitioner practitioner = (Practitioner) csvHelper.retrieveResource(staffMemberId.getString(), ResourceType.Practitioner);
        if (practitioner == null) {
            practitionerBuilder = new PractitionerBuilder();
            practitionerBuilder.setId(staffMemberId.getString(), staffMemberId);

        } else {
            practitionerBuilder = new PractitionerBuilder(practitioner);
        }

        //remove any existing name before adding the new one
        NameBuilder.removeExistingNames(practitionerBuilder);

        CsvCell fullName = parser.getStaffName();
        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addFullName(fullName.getString(), fullName);

        CsvCell userName = parser.getStaffUserName();
        if (!userName.isEmpty()) {

            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);
            identifierBuilder.setValue(userName.getString(), userName);
        }

        CsvCell nationalIdType = parser.getNationalIdType();
        if (!nationalIdType.isEmpty()) {
            String nationalIdTypeSystem = getNationalIdTypeIdentifierSystem(nationalIdType.getString(), csvHelper);
            if (!Strings.isNullOrEmpty(nationalIdTypeSystem)) {

                IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, nationalIdTypeSystem);

                CsvCell nationalId = parser.getIDNational();
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.setSystem(nationalIdTypeSystem);
                identifierBuilder.setValue(nationalId.getString(), nationalId);
            }
        }

        CsvCell smartCardId = parser.getIDSmartCard();
        if (!smartCardId.isEmpty()) {

            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);
            identifierBuilder.setValue(smartCardId.getString(), smartCardId);
        }

        CsvCell obsolete = parser.getObsolete();
        if (!obsolete.isEmpty()) {

            Boolean isActive = !obsolete.getBoolean();
            practitionerBuilder.setActive(isActive, obsolete);
        } else {

            practitionerBuilder.setActive(true, obsolete);
        }

        // Get cached StaffMemberProfile records and apply them
        List<StaffMemberProfilePojo> pojoList = csvHelper.getStaffMemberProfileCache().getAndRemoveStaffMemberProfilePojo(staffMemberId);
        if (pojoList != null) {
            for (StaffMemberProfilePojo pojo : pojoList) {
                StaffMemberProfileCache.addOrReplaceProfileOnPractitioner(practitionerBuilder, pojo, csvHelper);
            }
        }

        //we don't retrieve from the DB and update so it will never be ID mapped
        boolean mapIds = !practitionerBuilder.isIdMapped();
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), mapIds, practitionerBuilder);
    }


    private static String getNationalIdTypeIdentifierSystem(String nationalIdType, TppCsvHelper csvHelper) throws Exception {

        switch (nationalIdType.toUpperCase()) {
            case "GMC":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER;
            case "NMC":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NMC_NUMBER;

            default:
                TransformWarnings.log(LOG, csvHelper, "TPP National ID type {} not mapped", nationalIdType);
                return null;
        }
    }

}
