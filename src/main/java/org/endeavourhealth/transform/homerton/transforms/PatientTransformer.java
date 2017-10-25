package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.homerton.schema.Patient;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PatientTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 Patient parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                patientCreateOrUpdate(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void patientCreateOrUpdate(Patient parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Patient
       // ResourceId patientResourceId = getPatientResourceId(parser.getCNN());
       // if (patientResourceId == null) {
       //     patientResourceId = createPatientResourceId(parser.getCNN());
       // }

        HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, null, parser.getFirstname(), "", parser.getSurname());

        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddressLine1(), parser.getAddressLine2(), parser.getAddressLine3(), parser.getCity(), parser.getCounty(), parser.getPostcode());

        // save resource
        //createPatient(parser.getCurrentState(), patientResourceId, fhirResourceFiler, patientResourceId.getResourceId().toString(), parser.getNHSNo(), name, fhirAddress, convertGenderToFHIR(parser.getGenderID()), parser.getDOB(), null, null);
    }



}
