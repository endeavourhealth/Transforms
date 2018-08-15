package org.endeavourhealth.transform.tpp.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRotaSlot extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRRotaSlot.class);

    public SRRotaSlot(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "IDRota",
                    "RotaSlotType",
                    "Duration",
                    "Quantity",
                    "EmbargoDuration",
                    "EmbargoExpiryTime",
                    "BlockedSlot",
                    "BookableCAndB",
                    "OldRowIdentifier",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "IDRota",
                    "RotaSlotType",
                    "Duration",
                    "Quantity",
                    "EmbargoDuration",
                    "EmbargoExpiryTime",
                    "BlockedSlot",
                    "BookableCAndB",
                    "OldRowIdentifier"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getIDRota() {
        return super.getCell("IDRota");
    }

    public CsvCell getRotaSlotType() {
        return super.getCell("RotaSlotType");
    }

    public CsvCell getDuration() {
        return super.getCell("Duration");
    }

    public CsvCell getQuantity() {
        return super.getCell("Quantity");
    }

    public CsvCell getEmbargoDuration() {
        return super.getCell("EmbargoDuration");
    }

    public CsvCell getEmbargoExpiryTime() {
        return super.getCell("EmbargoExpiryTime");
    }

    public CsvCell getBlockedSlot() {
        return super.getCell("BlockedSlot");
    }

    public CsvCell getBookableCAndB() {
        return super.getCell("BookableCAndB");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected String getFileTypeDescription() {
        return "TPP Rota Slot file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
