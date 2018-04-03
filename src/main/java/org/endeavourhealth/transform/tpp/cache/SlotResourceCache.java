package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.SlotBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SlotResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(SlotResourceCache.class);

    private static Map<Long, SlotBuilder> slotBuildersById = new HashMap<>();

    public static SlotBuilder getSlotBuilder(CsvCell slotIdCell,
                                                            TppCsvHelper csvHelper,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        SlotBuilder slotBuilder = slotBuildersById.get(slotIdCell.getLong());
        if (slotBuilder == null) {

            org.hl7.fhir.instance.model.Slot slot
                    = (org.hl7.fhir.instance.model.Slot)csvHelper.retrieveResource(slotIdCell.getString(), ResourceType.Slot, fhirResourceFiler);
            if (slot == null) {
                //if the Appointment Slot doesn't exist yet, create a new one
                slotBuilder = new SlotBuilder();
                slotBuilder.setId(slotIdCell.getString(), slotIdCell);
            } else {
                slotBuilder = new SlotBuilder(slot);
            }

            slotBuildersById.put(slotIdCell.getLong(), slotBuilder);
        }
        return slotBuilder;
    }

    public static void fileSlotResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long slotId: slotBuildersById.keySet()) {
            SlotBuilder slotBuilder = slotBuildersById.get(slotId);
            fhirResourceFiler.savePatientResource(null, slotBuilder);
        }

        //clear down as everything has been saved
        slotBuildersById.clear();
    }
}
