package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.transforms.admin.PatientTransformer;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRRota;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SRRotaPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRRotaPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRota.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRRota) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    //since we're a pre-transformer, don't log and continue if we get a record-level
                    //exception, as failure to process anything in here must be treated as a critical failure
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }

        //essential that all the callables are complete before we continue
        csvHelper.waitUntilThreadPoolIsEmpty();
    }

    private static void processRecord(SRRota parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null //column not always present so need null check
                && removedCell.getIntAsBoolean()) {
            return;
        }

        CsvCell rotaIdCell = parser.getRowIdentifier();
        CsvCurrentState state = parser.getCurrentState();

        //since the required caching is retrieving data off the DB one record at a time,
        //use the common thread pool to do this in parallel
        CacheRotaDetailsTask task = new CacheRotaDetailsTask(state, rotaIdCell, csvHelper);
        csvHelper.submitToThreadPool(task);
    }

    static class CacheRotaDetailsTask extends AbstractCsvCallable {

        private CsvCell rotaIdCell;
        private TppCsvHelper csvHelper;

        public CacheRotaDetailsTask(CsvCurrentState state,
                                    CsvCell rotaIdCell,
                                    TppCsvHelper csvHelper) {
            super(state);

            this.rotaIdCell = rotaIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                String localId = rotaIdCell.getString();
                Schedule schedule = (Schedule)csvHelper.retrieveResource(localId, ResourceType.Schedule);
                if (schedule == null) {
                    return null;
                }

                //find the profile ID this rota was for
                Long profileId = null;
                if (schedule.hasActor()) {
                    Reference practitionerReference = schedule.getActor();
                    practitionerReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(csvHelper, practitionerReference);
                    String profileIdStr = ReferenceHelper.getReferenceId(practitionerReference);
                    profileId = Long.valueOf(profileIdStr);
                }

                //find the start and end date this rota has
                Date startDate = null;
                if (schedule.hasPlanningHorizon()) {
                    Period p = schedule.getPlanningHorizon();
                    startDate = p.getStart();
                }

                //store the above details in a cache, so that when we properly process the SRRota file, we've got those details available
                csvHelper.getRotaDateAndStaffCache().cacheExistingRotaDetails(rotaIdCell, startDate, profileId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

    }
}