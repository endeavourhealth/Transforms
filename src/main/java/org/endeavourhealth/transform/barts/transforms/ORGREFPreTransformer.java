package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ORGREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ORGREFPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ORGREFPreTransformer.class);

    private static InternalIdDalI idDal = DalProvider.factoryInternalIdDal();

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFilerI fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //don't catch any errors and continue, since errors in this file mean we can't proceed
                processLine((ORGREF) parser, (FhirResourceFiler) fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            }
        }
    }

    public static void processLine(ORGREF parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BartsCsvHelper csvHelper,
                                      String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

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

        csvHelper.saveInternalId(InternalIdMap.TYPE_CERNER_ODS_CODE_TO_ORG_ID, odsCode, id);

        //also ensure that if the HL7 Receiver has processed this ORG, we
    }


}
