package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRCode;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class SRCodePreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(SRCode.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processLine((SRCode) parser, csvHelper);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }


    private static void processLine(SRCode parser, TppCsvHelper csvHelper) throws Exception {

        //if this record is deleted, skip it
        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {
            return;
        }

        ResourceType resourceType = SRCodeTransformer.getTargetResourceType(parser, csvHelper);

        CsvCell observationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        // code linked consultation / encounter Id
        CsvCell eventLinkId = parser.getIDEvent();
        if (!eventLinkId.isEmpty()) {
            csvHelper.cacheNewConsultationChildRelationship(eventLinkId, observationId.getString(), resourceType);
        }

        // ethnicity and marital status lookup
        CsvCell readCode = parser.getCTV3Code();
        if (!readCode.isEmpty()) {

            DateTimeType dateTimeType = null;
            CsvCell eventDateCell = parser.getDateEvent();
            if (!eventDateCell.isEmpty()) {
                dateTimeType = new DateTimeType(parser.getDateEvent().getDateTime());
            }

            CsvCell codeIdCell = parser.getRowIdentifier();

            //try to get Ethnicity from code
            EthnicCategory ethnicCategory = csvHelper.getKnownEthnicCategory(readCode.getString());
            if (ethnicCategory != null) {
                csvHelper.cacheEthnicity(patientId, dateTimeType, ethnicCategory, codeIdCell);
            }

            //try to get Marital status from code
            MaritalStatus maritalStatus = findMaritalStatus(readCode.getString());
            if (maritalStatus != null) {
                csvHelper.cacheMaritalStatus(patientId, dateTimeType, maritalStatus, codeIdCell);
            }
        }

    }

    private static MaritalStatus findMaritalStatus(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.equals("XE0oZ")) {
            //single
            return MaritalStatus.NEVER_MARRIED;
        } else if (readCode.equals("XE0oa")) {
            return MaritalStatus.MARRIED;
        } else if (readCode.equals("1334.")) {
            return MaritalStatus.DIVORCED;
        } else if (readCode.equals("1335.")
                || readCode.equals("133C.")
                || readCode.equals("XaMz6")) {
            return MaritalStatus.WIDOWED;
        } else if (readCode.equals("XE0ob")) {
            return MaritalStatus.LEGALLY_SEPARATED;
        } else if (readCode.equals("1336.")
                || readCode.equals("Ua0HZ")) {
            return MaritalStatus.DOMESTIC_PARTNER;
        }

        return null;
    }

    //Map ethnic groups from Read code
    private static EthnicCategory findEthnicityCode(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.startsWith("XaFwD") || readCode.startsWith("XaQEa")) {
            return EthnicCategory.WHITE_BRITISH;
        } else if (readCode.startsWith("XaFwE")) {
            return EthnicCategory.WHITE_IRISH;
        } else if (readCode.startsWith("XaFwF")) {
            return EthnicCategory.OTHER_WHITE;
        } else if (readCode.startsWith("XactL") || readCode.startsWith("XacuS")) {
            return EthnicCategory.MIXED_CARIBBEAN;
        } else if (readCode.startsWith("XacuT") || readCode.startsWith("Xactd")) {
            return EthnicCategory.MIXED_AFRICAN;
        } else if (readCode.startsWith("XaJRN") || readCode.startsWith("XacuU") || readCode.startsWith("Xacte")) {
            return EthnicCategory.MIXED_ASIAN;
        } else if (readCode.startsWith("9SB.")) {
            return EthnicCategory.OTHER_MIXED;
        } else if (readCode.startsWith("Xacuc") || readCode.startsWith("Xactg") || readCode.startsWith("Xacv2")) {
            return EthnicCategory.ASIAN_INDIAN;
        } else if (readCode.startsWith("Xacv0") || readCode.startsWith("Xacui") || readCode.startsWith("Xacth")) {
            return EthnicCategory.ASIAN_PAKISTANI;
        } else if (readCode.startsWith("Xacti") || readCode.startsWith("Xacuj") || readCode.startsWith("Xacv5")) {
            return EthnicCategory.ASIAN_BANGLADESHI;
        } else if (readCode.startsWith("9SH") || readCode.startsWith("9T1E.") || readCode.startsWith("XaJR5")
                || readCode.startsWith("Xacul") || readCode.startsWith("Xactk") || readCode.startsWith("XaJRW")) {
            return EthnicCategory.OTHER_ASIAN;
        } else if (readCode.startsWith("9S2")) {
            return EthnicCategory.BLACK_CARIBBEAN;
        } else if (readCode.startsWith("9S3")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else if (readCode.startsWith("9S4") || readCode.startsWith("XaJR8") || readCode.startsWith("XaJRb")) {
            return EthnicCategory.OTHER_BLACK;
        } else if (readCode.startsWith("9S9") || readCode.startsWith("XaJR9")) {
            return EthnicCategory.CHINESE;
        } else if (readCode.startsWith("9SJ") || readCode.startsWith("XaFx1") || readCode.startsWith("XaJRA")) {
            return EthnicCategory.OTHER;
        } else if (readCode.startsWith("9SE") || readCode.startsWith("XaJRB")) {
            return EthnicCategory.NOT_STATED;
        } else {
            return null;
        }
    }


}


