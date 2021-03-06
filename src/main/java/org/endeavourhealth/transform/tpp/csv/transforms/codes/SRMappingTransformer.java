package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppConfigListOptionDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppMappingRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class SRMappingTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRMappingTransformer.class);

    /*private static TppMappingRefDalI repository = DalProvider.factoryTppMappingRefDal();
    public static final String ROW_ID = "RowId";
    public static final String GROUP_ID = "groupId";
    public static final String MAPPED_TERM = "mappedTerm";*/

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRMapping.class);
        if (parser != null) {

            //just bulk load the file into the DB
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            TppMappingRefDalI dal = DalProvider.factoryTppMappingRefDal();
            dal.updateLookupTable(filePath, dataDate);
        }
    }

    /*public static void createResource(SRMapping parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell groupId = parser.getIDMappingGroup();
        CsvCell mappedTerm = parser.getMapping();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getPublishedFileId(), rowId.getRecordNumber(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(groupId.getPublishedFileId(), groupId.getRecordNumber(), groupId.getColIndex(), GROUP_ID);
        auditWrapper.auditValue(mappedTerm.getPublishedFileId(), mappedTerm.getRecordNumber(), mappedTerm.getColIndex(), MAPPED_TERM);

        TppMappingRef mapping = new TppMappingRef(rowId.getLong(),
                                    groupId.getLong(),
                                    mappedTerm.getString(),
                                    auditWrapper);
        //save to the DB
        repository.save(mapping);
    }*/
}
