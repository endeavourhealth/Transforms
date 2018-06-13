package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ConditionResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRProblem;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SRProblemTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRProblemTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRProblem.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRProblem) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(SRProblem parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell problemId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if ((deleteData != null) && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Condition condition
                        = (org.hl7.fhir.instance.model.Condition) csvHelper.retrieveResource(problemId.getString(),
                        ResourceType.Condition,
                        fhirResourceFiler);

                if (condition != null) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder(condition);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
                }
                return;

            }
        }

        //for problems, use the linked observationId as the ID to build up the resource to then add to in SRCode Transformer
        CsvCell linkedObsCodeId = parser.getIDCode();

        ConditionBuilder conditionBuilder
                = ConditionResourceCache.getConditionBuilder(linkedObsCodeId, patientId, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        conditionBuilder.setPatient(patientReference, patientId);

        //the linked SRCode entry - cache the reference for the SRCode transformer to check that it is a problem
        CsvCell readV3Code = parser.getCTV3Code();
        if (!linkedObsCodeId.isEmpty() && ! readV3Code.isEmpty()) {
            csvHelper.cacheProblemObservationGuid(patientId, linkedObsCodeId, readV3Code.getString());
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                conditionBuilder.setRecordedBy(staffReference, recordedBy);
            }
        }

        CsvCell clinicianDoneBy = parser.getIDDoneBy();
        if (!clinicianDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(clinicianDoneBy);
            conditionBuilder.setClinician(staffReference, clinicianDoneBy);
        }

        //status is mandatory, so set the only value we can
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            conditionBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            conditionBuilder.setOnset(dateTimeType, effectiveDate);
        }

        //set the category on the condition, so we know it's a problem
        conditionBuilder.setCategory("complaint", problemId);
        conditionBuilder.setAsProblem(true);

        CsvCell endDate = parser.getDateEnd();
        if (endDate != null) {

            DateType dateType = new DateType(effectiveDate.getDate());
            conditionBuilder.setEndDateOrBoolean(dateType, endDate);
        }

        CsvCell severity = parser.getSeverity();
        if (severity != null) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(severity, parser);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (!mappedTerm.isEmpty()) {
                    if (mappedTerm.equalsIgnoreCase("minor")) {
                        conditionBuilder.setProblemSignificance(ProblemSignificance.NOT_SIGNIFICANT);
                    } else if (mappedTerm.equalsIgnoreCase("major")) {
                        conditionBuilder.setProblemSignificance(ProblemSignificance.SIGNIFICANT);
                    }
                } else {
                    conditionBuilder.setProblemSignificance(ProblemSignificance.UNSPECIIED);
                }
            } else {
                conditionBuilder.setProblemSignificance(ProblemSignificance.UNSPECIIED);
            }
        }
    }
}
