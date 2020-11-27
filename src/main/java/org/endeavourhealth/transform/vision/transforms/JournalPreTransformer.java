package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.helpers.VisionCodeHelper;
import org.endeavourhealth.transform.vision.helpers.VisionMappingHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class JournalPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(JournalPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        try {
            //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
            //to parse any record in this file is a critical error
            AbstractCsvParser parser = parsers.get(Journal.class);

            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processLine((Journal) parser, csvHelper, fhirResourceFiler);
                    } catch (Exception ex) {
                        throw new TransformException(parser.getCurrentState().toString(), ex);
                    }
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    private static void processLine(Journal parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            return;
        }


        ResourceType resourceType = JournalTransformer.getTargetResourceType(parser, csvHelper);
        CsvCell observationIdCell = parser.getObservationID();
        CsvCell patientIdCell = parser.getPatientID();

        //all we are interested in are Drug records (non issues)
        //this was used to track the records that represented medication being prescribed, so the
        //issue records could be linked to them, but there is NO link between these records in the data (see SD-105)
        /*if (resourceType == ResourceType.MedicationStatement) {
            CsvCell patientID = parser.getPatientID();
            CsvCell drugRecordID = parser.getObservationID();

            //cache the observation IDs of Drug records (not Issues), so  that we know what is a Drug Record
            //when we run the observation pre and main transformers, i.e. for linking and deriving medications issues
            csvHelper.cacheDrugRecordGuid(patientID, drugRecordID);
        }*/

        //linked consultation encounter record
        CsvCell linkCell = parser.getLinks();
        String consultationId = JournalTransformer.extractEncounterLinkId(linkCell);
        if (!Strings.isNullOrEmpty(consultationId)) {

            csvHelper.cacheNewConsultationChildRelationship(consultationId, patientIdCell, observationIdCell, resourceType, linkCell);
        }

        //if it is not a problem itself, cache the Observation or Medication linked problem to be filed with the condition resource
        if (resourceType != ResourceType.Condition) {

            Set<String> problemLinkIds = JournalTransformer.extractJournalLinkIds(linkCell);
            if (problemLinkIds != null) {

                for (String problemId: problemLinkIds) {

                    //store the problem/observation relationship in the helper
                    csvHelper.cacheProblemRelationship(problemId, patientIdCell, observationIdCell, resourceType, linkCell);
                }
            }
        }



        //medication issue record (subset = S) - set linked drug record first and last issue dates
        //see SD-105: there is no link between medication issues and repeat journal records, so none of this is doing anything
        /*if (resourceType == ResourceType.MedicationOrder) {
            String drugRecordID = JournalTransformer.extractDrugRecordLinkID (parser.getLinks().getString(), patientIdCell.getString(), csvHelper);
            if (!Strings.isNullOrEmpty(drugRecordID)) {
                //note that in this case we only care about the DATE, not the TIME
                Date effectiveDate = parser.getEffectiveDate().getDate();
                String effectiveDatePrecision = "YMD";
                DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
                csvHelper.cacheDrugRecordDate(drugRecordID, patientIdCell, dateTime);
            }
        }*/

        CsvCell readCodeCell = parser.getReadCode();
        String readCode = VisionCodeHelper.formatReadCode(readCodeCell, csvHelper); //use this fn to format the cell into a regular Read2 code
        if (!Strings.isNullOrEmpty(readCode)) {

            //try to get Ethnicity from Journal
            if (VisionMappingHelper.isPotentialEthnicity(readCode)) {
                EthnicCategory ethnicCategory = VisionMappingHelper.findEthnicityCode(readCode);
                if (ethnicCategory != null) {
                    CsvCell dateCell = parser.getEffectiveDate();
                    CsvCell timeCell = parser.getEffectiveTime();
                    Date effectiveDateTime = CsvCell.getDateTimeFromTwoCells(dateCell, timeCell);

                    csvHelper.cacheEthnicity(patientIdCell, effectiveDateTime, ethnicCategory, readCodeCell);
                }
            }

            //NOTE - Vision does not have MaritalStatus records in the Journal file so there is no code
            //here to detect them and cache them for the FHIR Patient (see SD-187)
        }

        //audit the Read2/Local codes and their term
        CsvCell termCell = parser.getRubric();
        csvHelper.cacheCodeAndTermUsed(readCodeCell, termCell);

        //audit the Read2/Local code to Snomed mappings too
        if (resourceType == ResourceType.MedicationOrder
                || resourceType == ResourceType.MedicationStatement) {

            CsvCell dmdCell = parser.getDrugDMDCode();
            CsvCell recordedDateCell = parser.getEnteredDate();
            csvHelper.cacheReadToSnomedMapping(readCodeCell, dmdCell, recordedDateCell);

        } else {
            CsvCell snomedCell = parser.getSnomedCode();
            CsvCell recordedDateCell = parser.getEnteredDate();
            csvHelper.cacheReadToSnomedMapping(readCodeCell, snomedCell, recordedDateCell);
        }

        //only concerned with Problems in this pre-transformer
        if (resourceType == ResourceType.Condition) {

            CsvCell patientID = parser.getPatientID();
            CsvCell observationID = parser.getObservationID();
            CsvCurrentState parserState = parser.getCurrentState();

            //cache the observation IDs of problems, so
            //that we know what is a problem when we run the observation pre-transformer
            //see SD-105 - the links column only ever refers to problems or encounters, so we don't need this
            //csvHelper.cacheProblemObservationGuid(patientID, observationID);

            //also cache the IDs of any child items from previous instances of this problem, but
            //use a thread pool so we can perform multiple lookups in parallel
            LookupTask task = new LookupTask(patientID, observationID, fhirResourceFiler, csvHelper, parserState);
            csvHelper.submitToThreadPool(task);
        }
    }

    /*private static MaritalStatus findMaritalStatus(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.equals("1331.")) {
            //single
            return MaritalStatus.NEVER_MARRIED;
        } else if (readCode.equals("1332.")
            || readCode.equals("133S.")) {
            return MaritalStatus.MARRIED;
        } else if (readCode.equals("1334.")
            || readCode.equals("133T.")) {
            return MaritalStatus.DIVORCED;
        } else if (readCode.equals("1335.")
            || readCode.equals("133C.")
            || readCode.equals("133V.")) {
            return MaritalStatus.WIDOWED;
        } else if (readCode.equals("1333.")) {
            return MaritalStatus.LEGALLY_SEPARATED;
        } else if (readCode.equals("1336.")
            || readCode.equals("133e.")
            || readCode.equals("133G.")
            || readCode.equals("133H.")) {
            return MaritalStatus.DOMESTIC_PARTNER;
        }

        return null;
    }

    //Vision use Ethnic groups and 2001 census categories so map from Read code
    private static EthnicCategory findEthnicityCode(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.startsWith("9S10") || readCode.startsWith("9i0")) {
            return EthnicCategory.WHITE_BRITISH;
        } else if (readCode.startsWith("9S11") || readCode.startsWith("9i10")) {
            return EthnicCategory.WHITE_IRISH;
        } else if (readCode.startsWith("9S12") || readCode.startsWith("9i2") || readCode.startsWith("9S14")) {
            return EthnicCategory.OTHER_WHITE;
        } else if (readCode.startsWith("9SB5") || readCode.startsWith("9i3")) {
            return EthnicCategory.MIXED_CARIBBEAN;
        } else if (readCode.startsWith("9SB6") || readCode.startsWith("9i4")) {
            return EthnicCategory.MIXED_AFRICAN;
        } else if (readCode.startsWith("9SB2") || readCode.startsWith("9i5")) {
            return EthnicCategory.MIXED_ASIAN;
        } else if (readCode.startsWith("9SB") || readCode.startsWith("9i6"))  {
            return EthnicCategory.OTHER_MIXED;
        } else if (readCode.startsWith("9S6") || readCode.startsWith("9i7")) {
            return EthnicCategory.ASIAN_INDIAN;
        } else if (readCode.startsWith("9S7") || readCode.startsWith("9i8")) {
            return EthnicCategory.ASIAN_PAKISTANI;
        } else if (readCode.startsWith("9S8") || readCode.startsWith("9i9")) {
            return EthnicCategory.ASIAN_BANGLADESHI;
        } else if (readCode.startsWith("9SH") || readCode.startsWith("9iA") || readCode.startsWith("9SA8")) {
            return EthnicCategory.OTHER_ASIAN;
        } else if (readCode.startsWith("9S2") || readCode.startsWith("9iB")) {
            return EthnicCategory.BLACK_CARIBBEAN;
        } else if (readCode.startsWith("9S3") || readCode.startsWith("9iC")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else if (readCode.startsWith("9S4") || readCode.startsWith("9iD") || readCode.startsWith("9S5")) {
            return EthnicCategory.OTHER_BLACK;
        } else if (readCode.startsWith("9S9") || readCode.startsWith("9iE")) {
            return EthnicCategory.CHINESE;
        } else if (readCode.startsWith("9SJ") || readCode.startsWith("9iF") ||
                    readCode.startsWith("9SAC") || readCode.startsWith("9SAD") || readCode.startsWith("9SA.")) {
            return EthnicCategory.OTHER;
        } else if (readCode.startsWith("9SE") || readCode.startsWith("9iG") || readCode.startsWith("9SD")) {
            return EthnicCategory.NOT_STATED;
        } else {
            return null;
        }
    }*/


    static class LookupTask extends AbstractCsvCallable {

        private CsvCell patientID;
        private CsvCell observationID;
        private FhirResourceFiler fhirResourceFiler;
        private VisionCsvHelper csvHelper;

        public LookupTask(CsvCell patientID,
                          CsvCell observationID,
                          FhirResourceFiler fhirResourceFiler,
                          VisionCsvHelper csvHelper,
                          CsvCurrentState parserState) {

            super(parserState);
            this.patientID = patientID;
            this.observationID = observationID;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                String locallyUniqueId = EmisCsvHelper.createUniqueId(patientID, observationID);

                //carry over linked items from any previous instance of this problem
                Condition previousVersion = (Condition)csvHelper.retrieveResource(locallyUniqueId, ResourceType.Condition);
                if (previousVersion == null) {
                    //if this is the first time, then we'll have a null resource
                    return null;
                }

                ConditionBuilder conditionBuilder = new ConditionBuilder(previousVersion);
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

                List<Reference> previousReferencesDiscoveryIds = containedListBuilder.getReferences();

                //the references will be mapped to Discovery UUIDs, so we need to convert them back to local IDs
                List<Reference> previousReferencesLocalIds = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, previousReferencesDiscoveryIds);

                csvHelper.cacheProblemPreviousLinkedResources(locallyUniqueId, previousReferencesLocalIds);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


