package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.ORGREF;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ORGREFPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ORGREFPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFilerI fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //don't catch any errors and continue, since errors in this file mean we can't proceed
                processLine((ORGREF)parser, (FhirResourceFiler)fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void processLine(ORGREF parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper) throws Exception {

        CsvCell orgIdCell = parser.getOrgId();
        CsvCell odsCodeCell = parser.getNhsOrgAlias();

        if (odsCodeCell.isEmpty()) {
            return;
        }

        //save the ODS code to ID mapping, to ensure mappings from org ODS code to cerner ID are stored, so we can
        //look them up during the ORG transform proper. Needed because the file isn't in hierarchy
        //order, and rows refer to their parents using ODS code, so we need to know the ID for each
        //ODS code before we start creating resources
        String id = orgIdCell.getString();
        String odsCode = odsCodeCell.getString();

        //also ensure that if the HL7 Receiver has processed this ORG, we carry over the UUID that it used
        //so both the ADT feed and Data Warehouse feed map orgs to the same UUID
        String localUniqueId = orgIdCell.getString();
        String hl7ReceiverUniqueId = "OdsCode=" + odsCode;
        String hl7ReceiverScope = csvHelper.getHl7ReceiverGlobalScope();

        ArrayList<String> localUniqueIds = new ArrayList<String>();
        localUniqueIds.add(0,orgIdCell.getString());
        localUniqueIds.add(1,odsCode);

        csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Organization, localUniqueIds);
    }


}
