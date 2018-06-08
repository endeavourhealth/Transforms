package org.endeavourhealth.transform.adastra;

import org.endeavourhealth.common.utility.XmlHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.xml.schema.CodedItem;
import org.endeavourhealth.transform.adastra.xml.transforms.admin.LocationTransform;
import org.endeavourhealth.transform.adastra.xml.transforms.admin.OrganisationTransformer;
import org.endeavourhealth.transform.adastra.xml.transforms.admin.PatientTransformer;
import org.endeavourhealth.transform.adastra.xml.transforms.admin.UserTransform;
import org.endeavourhealth.transform.adastra.xml.transforms.clinical.EncounterTransform;
import org.endeavourhealth.transform.adastra.xml.transforms.clinical.EpisodeTransformer;
import org.endeavourhealth.transform.adastra.xml.transforms.clinical.FlagTransform;
import org.endeavourhealth.transform.adastra.xml.transforms.clinical.ObservationTransformer;
import org.endeavourhealth.transform.common.FhirResourceFiler;

import java.util.List;
import java.util.UUID;

public abstract class AdastraXmlToFhirTransformer {

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String version) throws Exception {

        AdastraCaseDataExport caseReport = XmlHelper.deserialize(exchangeBody, AdastraCaseDataExport.class);

        checkMessageForIssues(caseReport);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        OrganisationTransformer.transform(caseReport, processor);
        LocationTransform.transform(caseReport, processor);
        UserTransform.transform(caseReport, processor);
        PatientTransformer.transform(caseReport, processor);
        EpisodeTransformer.transform(caseReport, processor);
        EncounterTransform.createMainCaseEncounter(caseReport, processor);
        String mainEncounterId = AdastraXmlHelper.uniqueIdMapper.get("caseEncounter");

        if (caseReport.getConsultation() != null) {
            EncounterTransform.createChildEncountersFromConsultations(caseReport, processor);
        }

        if (caseReport.getOutcome() != null) {
            for (CodedItem codedItem : caseReport.getOutcome()) {
                ObservationTransformer.observationFromCodedItem(codedItem, mainEncounterId,
                        caseReport.getActiveDate(), caseReport.getAdastraCaseReference(),
                        "Outcome", processor);
            }
        }

        if (caseReport.getPresentingCondition() != null)
            ObservationTransformer.observationFromPresentingCondition(caseReport, processor);

        if  (caseReport.getQuestions() != null && !caseReport.getQuestions().isEmpty()) {
            ObservationTransformer.observationFromFreeText(caseReport.getQuestions(), mainEncounterId,
                    caseReport.getActiveDate(), caseReport.getAdastraCaseReference(),
                    "Questions", processor);
        }

        if (caseReport.getSpecialNote() != null) {
            for (AdastraCaseDataExport.SpecialNote specialNote : caseReport.getSpecialNote()) {
                FlagTransform.transform(specialNote, caseReport.getAdastraCaseReference(), processor);
            }
        }

    }

    private static void checkMessageForIssues(AdastraCaseDataExport caseReport) throws TransformException {
        if (caseReport.getAdastraCaseReference() == null) {
            throw new TransformException("Case Reference not specified in message");
        }

        if (caseReport.getPatient() == null) {
            throw new TransformException("Patient not specified in message");
        }

        if (caseReport.getPatient().getGpRegistration() == null) {
            throw new TransformException("GP Registration not specified in message");
        }
    }
}
