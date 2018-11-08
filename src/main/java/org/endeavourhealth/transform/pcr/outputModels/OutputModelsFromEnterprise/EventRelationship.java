package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class EventRelationship extends AbstractPcrCsvWriter {

    public EventRelationship(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }
//TODO concept id needs IM
    public void writeUpsert(long id,
                            int itemType,
                            long linkedItemId,
                            int linkedItemRelationshopConceptId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + itemType,
                "" + linkedItemId,
                "" + linkedItemRelationshopConceptId);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "item_id",
                "item_type",
                "linked_item_id",
                "linked_item_relationship_concept_id"
                };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Integer.TYPE,
                Long.TYPE,
                Integer.TYPE};
    }
}
