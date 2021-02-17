package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.TppMappingHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRCode;
import org.endeavourhealth.transform.vision.helpers.VisionMappingHelper;
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
        CsvCell readCodeCell = parser.getCTV3Code();
        if (!readCodeCell.isEmpty()) {

            DateTimeType dateTimeType = null;
            CsvCell eventDateCell = parser.getDateEvent();
            if (!eventDateCell.isEmpty()) {
                dateTimeType = new DateTimeType(parser.getDateEvent().getDateTime());
            }

            CsvCell codeIdCell = parser.getRowIdentifier();

            //try to get Ethnicity from code
            if (TppMappingHelper.isEthnicityCode(readCodeCell.getString())) {
                EthnicCategory ethnicCategory = TppMappingHelper.findEthnicityCode(readCodeCell.getString());
                csvHelper.cacheEthnicity(patientId, dateTimeType, ethnicCategory, codeIdCell);
            }

            //try to get Marital status from code
            if (TppMappingHelper.isMaritalStatusCode(readCodeCell.getString())) {
                MaritalStatus maritalStatus = TppMappingHelper.findMaritalStatusCode(readCodeCell.getString());
                csvHelper.cacheMaritalStatus(patientId, dateTimeType, maritalStatus, codeIdCell);
            }

        }
    }


}


