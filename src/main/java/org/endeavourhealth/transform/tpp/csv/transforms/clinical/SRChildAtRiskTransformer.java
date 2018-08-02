package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.FlagBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRChildAtRisk;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRChildAtRiskTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRChildAtRiskTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRChildAtRisk.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRChildAtRisk) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRChildAtRisk parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if ((deleteData != null) && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Flag flag
                        = (org.hl7.fhir.instance.model.Flag) csvHelper.retrieveResource(rowId.getString(),
                        ResourceType.Flag);

                if (flag != null) {
                    FlagBuilder flagBuilder
                            = new FlagBuilder(flag);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), flagBuilder);
                }
                return;

            }
        }

        FlagBuilder flagBuilder = new FlagBuilder();
        flagBuilder.setId(rowId.getString(), rowId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        flagBuilder.setSubject(patientReference, patientId);

        CsvCell dateAdded = parser.getDateAdded();
        if (!dateAdded.isEmpty()) {
            flagBuilder.setStartDate(dateAdded.getDate(), dateAdded);
        }

        CsvCell dateRemoved = parser.getDateRemoved();
        if (!dateRemoved.isEmpty()) {
            flagBuilder.setEndDate(dateRemoved.getDate(), dateRemoved);
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                if (flagBuilder.isIdMapped()) {
                    staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference,fhirResourceFiler);
                }
                flagBuilder.setAuthor(staffReference, recordedBy);
            }
        }

        CsvCell onPlan = parser.getChildProtectionPlan();
        if (!onPlan.isEmpty()) {
            if (onPlan.getBoolean()) {
                flagBuilder.setStatus(Flag.FlagStatus.ACTIVE, onPlan);
            } else {
                flagBuilder.setStatus(Flag.FlagStatus.INACTIVE, onPlan);
            }
        }

        flagBuilder.setCode("On Child Protection Plan");

        flagBuilder.setCategory("Clinical");

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);

    }
}