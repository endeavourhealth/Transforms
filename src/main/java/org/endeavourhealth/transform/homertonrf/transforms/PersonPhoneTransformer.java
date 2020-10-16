package org.endeavourhealth.transform.homertonrf.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homerton.transforms.HomertonBasisTransformer;
import org.endeavourhealth.transform.homertonrf.HomertonRfCodeableConceptHelper;
import org.endeavourhealth.transform.homertonrf.HomertonRfCsvHelper;
import org.endeavourhealth.transform.homertonrf.schema.PersonPhone;
import org.hl7.fhir.instance.model.ContactPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonPhoneTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonPhoneTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonRfCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {
                    try {
                        transform((PersonPhone) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(PersonPhone parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonRfCsvHelper csvHelper) throws Exception {

        CsvCell personEmpiCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //if there is a sequence number, this can be used to create an Id for the phone number, i.e. 1,2,3
        CsvCell phoneSeqCell = parser.getPhoneSequence();
        if (!phoneSeqCell.isEmpty()) {

            //if it is 1, then it will already have been added during the Person transform
            if (phoneSeqCell.getString().equalsIgnoreCase("1")) {
                return;
            }
        }

        //by removing from the patient and re-adding, we're constantly changing the order of the
        //contact points, so attempt to reuse the existing one for the ID
        ContactPointBuilder contactPointBuilder
                = ContactPointBuilder.findOrCreateForId(patientBuilder, phoneSeqCell);
        contactPointBuilder.reset();

        CsvCell phoneTypeCell = parser.getPhoneTypeCode();
        CsvCell phoneTypeDescCell
                = HomertonRfCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.PHONE_TYPE, phoneTypeCell);
        String phoneTypeDesc = phoneTypeDescCell.getString();
        ContactPoint.ContactPointUse use = convertPhoneType(phoneTypeDesc);
        contactPointBuilder.setUse(use, phoneTypeCell, phoneTypeDescCell);

        CsvCell phoneNumberCell = parser.getPhoneNumber();
        String phoneNumber = phoneNumberCell.getString();

        //just append the extension on to the number
        CsvCell extensionCell = parser.getPhoneExt();
        if (!extensionCell.isEmpty()) {
            phoneNumber += " " + extensionCell.getString();
        }
        contactPointBuilder.setValue(phoneNumber, phoneNumberCell, extensionCell);


        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiCell, patientBuilder);
    }

    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType) throws Exception {

        //we're missing codes in the code ref table, so just handle by returning SOMETHING
        if (phoneType == null) {
            return null;
        }

        //this is based on the full list of types from CODE_REF where the set is 43
        switch (phoneType.toUpperCase()) {
            case "HOME":
            case "VHOME":
            case "PHOME":
            case "USUAL":
            case "PAGER PERS":
            case "FAX PERS":
            case "VERIFY":
                return ContactPoint.ContactPointUse.HOME;

            case "FAX BUS":
            case "PROFESSIONAL":
            case "SECBUSINESS":
            case "CARETEAM":
            case "PHONEEPRESCR":
            case "AAM": //Automated Answering Machine
            case "BILLING":
            case "PAGER ALT":
            case "PAGING":
            case "PAGER BILL":
            case "FAX BILL":
            case "FAX ALT":
            case "ALTERNATE":
            case "EXTSECEMAIL":
            case "INTSECEMAIL":
            case "FAXEPRESCR":
            case "EMC":  //Emergency Phone
            case "TECHNICAL":
            case "OS AFTERHOUR":
            case "OS PHONE":
            case "OS PAGER":
            case "OS BK OFFICE":
            case "OS FAX":
            case "BUSINESS":
                return ContactPoint.ContactPointUse.WORK;

            case "MOBILE":
                return ContactPoint.ContactPointUse.MOBILE;

            case "FAX PREV":
            case "PREVIOUS":
            case "PAGER PREV":
                return ContactPoint.ContactPointUse.OLD;

            case "FAX TEMP":
            case "TEMPORARY":
                return ContactPoint.ContactPointUse.TEMP;

            default:
                throw new TransformException("Unsupported phone type [" + phoneType + "]");
        }
    }
}
