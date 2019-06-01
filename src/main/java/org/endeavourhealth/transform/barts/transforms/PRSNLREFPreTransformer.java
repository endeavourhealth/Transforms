package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PRSNLREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PRSNLREFPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREFPreTransformer.class);

    public static final String MAPPING_ID_PERSONNEL_NAME_TO_ID = "PersonnelNameToId";
    public static final String MAPPING_ID_CONSULTANT_TO_ID = "ConsultantToId";


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    processLine((PRSNLREF) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processLine(PRSNLREF parser,
                                           FhirResourceFiler fhirResourceFiler,
                                           BartsCsvHelper csvHelper) throws Exception {


        CsvCell personnelIdCell = parser.getPersonnelID();

        //CsvCell titleCell = parser.getTitle();
        CsvCell givenNameCell = parser.getFirstName();
        CsvCell middleNameCell = parser.getMiddleName();
        CsvCell surnameCell = parser.getLastName();

        //we also need to save a lookup of free-text name to practitioner ID, because the fixed-width Procedure file
        //only gives us the name and not the ID. Note the way that this free-text name is built up is specifically
        //to mirror what Millennium does, so the weird extra spacing is intentional
        StringBuilder sb = new StringBuilder();
        sb.append(surnameCell.getString());
        sb.append(" , ");
        sb.append(givenNameCell.getString());
        if (!middleNameCell.isEmpty()) {
            sb.append(" ");
            sb.append(middleNameCell.getString());
        }

        String freeTextName = sb.toString();
        csvHelper.saveInternalId(MAPPING_ID_PERSONNEL_NAME_TO_ID, freeTextName, personnelIdCell.getString());

        CsvCell consultantNHSCode = parser.getConsultantNHSCode();
        if (!consultantNHSCode.isEmpty()) {
            csvHelper.saveInternalId(MAPPING_ID_CONSULTANT_TO_ID, consultantNHSCode.getString(), personnelIdCell.getString());
        }

        //if the HL7 Receiver has processed this person carry over the UUID that it used
        //so both the ADT feed and Data Warehouse feed map practitioners to the same UUID
        CsvCell gmpCodeCell = parser.getGmpCode();
        if (!gmpCodeCell.isEmpty()) {
            String localUniqueId = personnelIdCell.getString();
            String hl7ReceiverUniqueId = createHl7PractitionerId(givenNameCell, middleNameCell, surnameCell, gmpCodeCell);
            String hl7ReceiverScope = csvHelper.getHl7ReceiverScope(); //practitioners use local scope

            try {
                csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Practitioner, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope, false);
            } catch (Exception ex) {
                LOG.error("Failed to find/copy ID for personnel ID [" + localUniqueId + "]");
                LOG.error("HL7 ID [" + hl7ReceiverUniqueId + "]");
                LOG.error("Record = " + parser.getCurrentState());
                throw ex;
            }

        }
    }

    /**
     * the HL7 Receiver maps practitioners using a local ID in the format like this:
     * Surname=SINHA-Forename=SANTOSH-GmcCode=G8402174
     * Note that the names are caps without spaces
     */
    private static String createHl7PractitionerId(CsvCell givenNameCell, CsvCell middleNameCell, CsvCell surnameCell, CsvCell gmpCode) {
        StringBuilder sb = new StringBuilder();

        sb.append("Surname=");
        appendCaps(sb, surnameCell.getString());
        sb.append("Forename=");
        appendCaps(sb, givenNameCell.getString());
        appendCaps(sb, middleNameCell.getString());
        sb.append("GmcCode="); //NOTE, the code we have is a GMP code but the HL7 Receiver wrongly treats it as a GMC code, so we need to do the same
        appendCaps(sb, gmpCode.getString());

        return sb.toString();
    }

    private static void appendCaps(StringBuilder sb, String s) {
        if (Strings.isNullOrEmpty(s)) {
            return;
        }

        s = s.toUpperCase();
        s = s.replace(" ", "");
        sb.append(s);
    }

}

