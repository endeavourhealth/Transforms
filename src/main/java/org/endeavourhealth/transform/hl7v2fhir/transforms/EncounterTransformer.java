package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.TS;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.List_;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EncounterTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    /**
     *
     * @param pv1
     * @param encounter
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @param msgType
     * @param patientGuid
     * @throws Exception
     */
    public static void transformPV1ToEncounter(PV1 pv1, Encounter encounter, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper, String msgType, String patientGuid) throws Exception {
        EncounterBuilder encounterBuilder = createEncountersParentMinimum(pv1, imperialHL7Helper, msgType, patientGuid);
        createChildEncounters(pv1, encounterBuilder, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
    }

    /**
     *
     * @param pv1
     * @param imperialHL7Helper
     * @param msgType
     * @return
     * @throws Exception
     */
    private static EncounterBuilder createEncountersParentMinimum(PV1 pv1, ImperialHL7Helper imperialHL7Helper, String msgType, String patientGuid) throws Exception {
        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        Encounter.EncounterClass classAttr = null;
        if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.EMERGENCY;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Emergency");

        } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.INPATIENT;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Inpatient");

        } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.OUTPATIENT;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Outpatient");
        }
        parentTopEncounterBuilder.setClass(classAttr);
        parentTopEncounterBuilder.setId(String.valueOf(pv1.getVisitNumber().getID()));

        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date stDt = formatter.parse(startDt.substring(0,4)+"-"+startDt.substring(4,6)+"-"+startDt.substring(6,8));
        parentTopEncounterBuilder.setPeriodStart(stDt);

        TS dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt = null;
        if(!dischargeDtTime.isEmpty()) {
            String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
            dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));
        }

        if (("ADT_A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
            parentTopEncounterBuilder.setPeriodEnd(dsDt);
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        setCommonEncounterAttributes(pv1, parentTopEncounterBuilder, imperialHL7Helper, patientGuid, false);
        return parentTopEncounterBuilder;
    }

    /**
     *
     * @param pv1
     * @param builder
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void setCommonEncounterAttributes(PV1 pv1, EncounterBuilder builder, ImperialHL7Helper imperialHL7Helper, String patientGuid, boolean encounterInd) throws Exception {

        String patientId = String.valueOf(patientGuid);
        String patientVisitId = String.valueOf(pv1.getVisitNumber().getID());
        XCN[] consultingDoctor = pv1.getConsultingDoctor();
        String consultingDoctorId = null;
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            consultingDoctorId = String.valueOf(consultingDoctor[0].getIDNumber());
        }

        XCN[] referringDoctor = pv1.getReferringDoctor();
        String referringDoctorId = null;
        if(referringDoctor != null && referringDoctor.length > 0) {
            referringDoctorId = String.valueOf(referringDoctor[0].getIDNumber());
        }

        if (!patientId.isEmpty()) {
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientId);
            if (builder.isIdMapped()) {
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            }
            builder.setPatient(patientReference);
        }

        if (!patientVisitId.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, patientVisitId);
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, imperialHL7Helper);
            }
            builder.setEpisodeOfCare(episodeReference);
        }

        String loc[] = String.valueOf(pv1.getAssignedPatientLocation().getLocationType()).split(",");
        if (!loc[0].isEmpty()) {
            Reference patientAssignedLocReference
                    = ReferenceHelper.createReference(ResourceType.Location, loc[0]);
            if (builder.isIdMapped()) {
                patientAssignedLocReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientAssignedLocReference, imperialHL7Helper);
            }
            builder.addLocation(patientAssignedLocReference);
        }

        //Todo need to verify the practitioner code
        if (!consultingDoctorId.isEmpty()) {
            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, consultingDoctorId);
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }

        if (!referringDoctorId.isEmpty()) {
            Reference practitionerReferenceRd
                    = ReferenceHelper.createReference(ResourceType.Practitioner, referringDoctorId);
            if (builder.isIdMapped()) {

                practitionerReferenceRd
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReferenceRd, imperialHL7Helper);
            }
            builder.addParticipant(practitionerReferenceRd, EncounterParticipantType.SECONDARY_PERFORMER);
        }

        Reference organizationReference
                = ReferenceHelper.createReference(ResourceType.Organization, "Imperial College Healthcare NHS Trust");
        if (builder.isIdMapped()) {

            organizationReference
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, imperialHL7Helper);
        }
        builder.setServiceProvider(organizationReference);

        if ((encounterInd) && (!patientVisitId.isEmpty())) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, patientVisitId);
            if (builder.isIdMapped()) {

                parentEncounter
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, imperialHL7Helper);
            }
            builder.setPartOf(parentEncounter);
        }

    }

    /**
     *
     * @param pv1
     * @param existingParentEncounterBuilder
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void createChildEncounters(PV1 pv1, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper, String msgType, String patientGuid) throws Exception {

        ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        EncounterBuilder childEncounterBuilder = new EncounterBuilder();
        String encounterId = null;
        if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            childEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);
            encounterId = pv1.getVisitNumber().getID() + ":EM";

            CodeableConceptBuilder codeableConceptBuilderAdmission
                    = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAdmission.setText("Emergency Arrival");

        } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            childEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
            encounterId = pv1.getVisitNumber().getID() + ":IP";

            CodeableConceptBuilder codeableConceptBuilderAdmission
                    = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAdmission.setText("Inpatient Attendance");

        } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            childEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);
            encounterId = pv1.getVisitNumber().getID() + ":OP";

            CodeableConceptBuilder codeableConceptBuilderAdmission
                    = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAdmission.setText("Outpatient Attendance");
        }
        childEncounterBuilder.setId(encounterId);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        TS admitDtTime = pv1.getAdmitDateTime();
        if(!admitDtTime.isEmpty()) {
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
            Date stDt = formatter.parse(startDt.substring(0,4)+"-"+startDt.substring(4,6)+"-"+startDt.substring(6,8));
            childEncounterBuilder.setPeriodStart(stDt);
        }

        TS dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt = null;
        if(!dischargeDtTime.isEmpty()) {
            String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
            dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));
        }

        if (("ADT_A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
            childEncounterBuilder.setPeriodEnd(dsDt);
            childEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            childEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        setCommonEncounterAttributes(pv1, childEncounterBuilder, imperialHL7Helper, patientGuid, true);

        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(childEncounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        //and link the parent to this new child encounter
        Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, encounterId);
        if (existingParentEncounterBuilder.isIdMapped()) {
            childDischargeRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, imperialHL7Helper);
        }
        existingEncounterList.addReference(childDischargeRef);

        fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder, childEncounterBuilder);
    }

    /**
     *
     * @param pv1
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    public static void deleteEncounterAndChildren(PV1 pv1, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper) throws Exception {
        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        Encounter existingParentEncounter
                = (Encounter) imperialHL7Helper.retrieveResourceForLocalId(ResourceType.Encounter, String.valueOf(pv1.getVisitNumber().getID()));

        EncounterBuilder parentEncounterBuilder
                = new EncounterBuilder(existingParentEncounter);

        if (existingParentEncounter.hasContained()) {
            ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();

            for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {
                Reference ref = item.getItem();
                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                if (comps.getResourceType() != ResourceType.Encounter) {
                    continue;
                }
                Encounter childEncounter
                        = (Encounter) resourceDal.getCurrentVersionAsResource(imperialHL7Helper.getServiceId(), ResourceType.Encounter, comps.getId());
                if (childEncounter != null) {
                    LOG.debug("Deleting child encounter " + childEncounter.getId());

                    fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                } else {

                    TransformWarnings.log(LOG, imperialHL7Helper, "Cannot find existing child Encounter: {} for deletion", childEncounter.getId());
                }
            }
            //finally, delete the top level parent
            fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

        } else {
            TransformWarnings.log(LOG, imperialHL7Helper, "Cannot find existing Encounter: {} for deletion", String.valueOf(pv1.getVisitNumber().getID()));
        }
    }

}
