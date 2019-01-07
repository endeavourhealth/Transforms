package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class ProcedureResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureResourceCache.class);

    private final BartsCsvHelper csvHelper;
    private ResourceCache<String, ProcedureBuilder> procedureBuildersById = new ResourceCache<>();
    private Set<String> procedureIdsJustDeleted = new HashSet<>();

    public ProcedureResourceCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public ProcedureBuilder borrowProcedureBuilder(String uniqueId, CsvCell... uniqueIdCells) throws Exception {

        //if we know we've deleted it, return null
        if (procedureIdsJustDeleted.contains(uniqueId)) {
            return null;
        }

        //check the cache
        ProcedureBuilder cachedResource = procedureBuildersById.getAndRemoveFromCache(uniqueId);
        if (cachedResource != null) {
            return cachedResource;
        }


        ProcedureBuilder builder = null;

        //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
        Procedure procedure = (Procedure)csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, uniqueId.toString());
        if (procedure == null) {
            //if the patient doesn't exist yet, create a new one
            builder = new ProcedureBuilder();
            builder.setId(uniqueId.toString(), uniqueIdCells);

        } else {
            builder = new ProcedureBuilder(procedure);
        }

        return builder;
    }

    public void returnProcedureBuilder(String uniqueId, ProcedureBuilder builder) throws Exception {
        procedureBuildersById.addToCache(uniqueId, builder);
    }


    public void fileProcedureResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + procedureBuildersById.size() + " procedures to the DB");

        for (String uniqueId: procedureBuildersById.keySet()) {
            ProcedureBuilder builder = procedureBuildersById.getAndRemoveFromCache(uniqueId);

//TODO - validate if we want to save this resource???

            boolean performIdMapping = !builder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, performIdMapping, builder);
        }

        LOG.trace("Finishing saving procedures to the DB");

        //clear down as everything has been saved
        procedureBuildersById.clear();
    }

    public void deleteProcedure(ProcedureBuilder procedureBuilder, String uniqueId, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (procedureBuilder == null) {
            return;
        }

        //record that we know it's deleted
        procedureIdsJustDeleted.add(uniqueId);

        //remove from the cache
        procedureBuildersById.removeFromCache(uniqueId);

        boolean mapIds = !procedureBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, procedureBuilder);
    }

}
