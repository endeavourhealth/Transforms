package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.TS;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EncounterTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    /**
     *
     * @param pv1
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @param msgType
     * @param patientGuid
     * @throws Exception
     */
    public static void transformPV1ToEncounter(PV1 pv1, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper, String msgType, String patientGuid) throws Exception {
        CX visitNum = pv1.getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            EncounterBuilder encounterBuilder = null;
            Encounter existingEncounter = null;
            String visitId = String.valueOf(visitNum.getID());
            existingEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitId, ResourceType.Encounter);
            if (existingEncounter != null) {
                encounterBuilder = new EncounterBuilder(existingEncounter);
                encounterBuilder = updateExistingEncounterParent(pv1, imperialHL7Helper, msgType, patientGuid, encounterBuilder);
                createChildEncounters(pv1, encounterBuilder, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);

            } else {
                encounterBuilder = new EncounterBuilder();
                encounterBuilder = createEncountersParentMinimum(pv1, imperialHL7Helper, msgType, patientGuid, encounterBuilder);
                createChildEncounters(pv1, encounterBuilder, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            }
        }
    }

    /**
     *
     * @param pv1
     * @param imperialHL7Helper
     * @param msgType
     * @param patientGuid
     * @param parentTopEncounterBuilder
     * @return
     * @throws Exception
     */
    private static EncounterBuilder createEncountersParentMinimum(PV1 pv1, ImperialHL7Helper imperialHL7Helper, String msgType, String patientGuid, EncounterBuilder parentTopEncounterBuilder) throws Exception {
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
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        Date stDt = formatter.parse(startDt.substring(0,4)+startDt.substring(4,6)+startDt.substring(6,8)+startDt.substring(8,10)+startDt.substring(10,12)+startDt.substring(12,14));
        parentTopEncounterBuilder.setPeriodStart(stDt);

        TS dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt = null;
        if(!dischargeDtTime.isEmpty()) {
            String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
            dsDt = formatter.parse(endDt.substring(0,4)+endDt.substring(4,6)+endDt.substring(6,8)+endDt.substring(8,10)+endDt.substring(10,12)+endDt.substring(12,14));
        }

        if (("A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
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
     * @param imperialHL7Helper
     * @param msgType
     * @param patientGuid
     * @param parentTopEncounterBuilder
     * @return
     * @throws Exception
     */
    private static EncounterBuilder updateExistingEncounterParent(PV1 pv1, ImperialHL7Helper imperialHL7Helper, String msgType, String patientGuid, EncounterBuilder parentTopEncounterBuilder) throws Exception {
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

        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        Date stDt = formatter.parse(startDt.substring(0,4)+startDt.substring(4,6)+startDt.substring(6,8)+startDt.substring(8,10)+startDt.substring(10,12)+startDt.substring(12,14));
        parentTopEncounterBuilder.setPeriodStart(stDt);

        TS dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt = null;
        if(!dischargeDtTime.isEmpty()) {
            String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
            dsDt = formatter.parse(endDt.substring(0,4)+endDt.substring(4,6)+endDt.substring(6,8)+endDt.substring(8,10)+endDt.substring(10,12)+endDt.substring(12,14));

        }

        if (("A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
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

        if (null!=patientId && !patientId.isEmpty()) {
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientId);
            if (builder.isIdMapped()) {
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            }
            builder.setPatient(patientReference);
        }

        if (null!=patientVisitId && !patientVisitId.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, patientVisitId);
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, imperialHL7Helper);
            }
            builder.setEpisodeOfCare(episodeReference);
        }

        String loc = String.valueOf(pv1.getAssignedPatientLocation().getPointOfCare());
        if (null!=loc) {
            Reference patientAssignedLocReference
                    = ReferenceHelper.createReference(ResourceType.Location, loc);
            if (builder.isIdMapped()) {
                patientAssignedLocReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientAssignedLocReference, imperialHL7Helper);
            }
            builder.addLocation(patientAssignedLocReference);
            builder.getLocation().get(0).setStatus(Encounter.EncounterLocationStatus.ACTIVE);
        }

        //Todo need to verify the practitioner code
        if (null!=consultingDoctorId && (!consultingDoctorId.isEmpty())) {
            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, consultingDoctorId);
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }

        if (null!=referringDoctorId && (!referringDoctorId.isEmpty())) {
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

                parentEncounter
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, imperialHL7Helper);

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

        List<String> encounterIds = new ArrayList<String>();
        if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            if ("A03".equalsIgnoreCase(msgType)) {
                encounterIds.add(pv1.getVisitNumber().getID() + ":02:EM");

            } else if ("A08".equalsIgnoreCase(msgType)) {
                Encounter existingChildEncounter = (Encounter) imperialHL7Helper.retrieveResource(pv1.getVisitNumber().getID() + ":02:EM", ResourceType.Encounter);
                encounterIds.add(pv1.getVisitNumber().getID() + ":01:EM");
                if(existingChildEncounter != null) {
                    encounterIds.add(pv1.getVisitNumber().getID() + ":02:EM");
                }

            } else {
                encounterIds.add(pv1.getVisitNumber().getID() + ":01:EM");
            }

        } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            if ("A03".equalsIgnoreCase(msgType)) {
                encounterIds.add(pv1.getVisitNumber().getID() + ":02:IP");

            } else if ("A08".equalsIgnoreCase(msgType)) {
                Encounter existingChildEncounter = (Encounter) imperialHL7Helper.retrieveResource(pv1.getVisitNumber().getID() + ":02:IP", ResourceType.Encounter);
                encounterIds.add(pv1.getVisitNumber().getID() + ":01:IP");
                if(existingChildEncounter != null) {
                    encounterIds.add(pv1.getVisitNumber().getID() + ":02:IP");
                }

            } else {
                encounterIds.add(pv1.getVisitNumber().getID() + ":01:IP");
            }

        } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            encounterIds.add(pv1.getVisitNumber().getID() + ":01:OP");
        }

        if(encounterIds != null) {
            for(String encounterId : encounterIds) {
                ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

                EncounterBuilder childEncounterBuilder = new EncounterBuilder();

                if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
                    childEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);
                    CodeableConceptBuilder codeableConceptBuilderAdmission
                            = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    if ("A03".equalsIgnoreCase(msgType)) {
                        codeableConceptBuilderAdmission.setText("Emergency Discharge");

                    } else {
                        codeableConceptBuilderAdmission.setText("Emergency Admission");
                    }

                } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
                    childEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
                    CodeableConceptBuilder codeableConceptBuilderAdmission
                            = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    if ("A03".equalsIgnoreCase(msgType)) {
                        codeableConceptBuilderAdmission.setText("Inpatient Discharge");

                    } else {
                        codeableConceptBuilderAdmission.setText("Inpatient Admission");
                    }

                } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
                    childEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

                    CodeableConceptBuilder codeableConceptBuilderAdmission
                            = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    codeableConceptBuilderAdmission.setText("Outpatient Attendance");
                }

                childEncounterBuilder.setId(encounterId);

                SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
                TS admitDtTime = pv1.getAdmitDateTime();
                if(!admitDtTime.isEmpty()) {
                    String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
                    Date stDt = formatter.parse(startDt.substring(0,4)+startDt.substring(4,6)+startDt.substring(6,8)+startDt.substring(8,10)+startDt.substring(10,12)+startDt.substring(12,14));

                    childEncounterBuilder.setPeriodStart(stDt);
                }

                TS dischargeDtTime = pv1.getDischargeDateTime();
                Date dsDt = null;
                if(!dischargeDtTime.isEmpty()) {
                    String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
                    dsDt = formatter.parse(endDt.substring(0,4)+endDt.substring(4,6)+endDt.substring(6,8)+endDt.substring(8,10)+endDt.substring(10,12)+endDt.substring(12,14));
                }

                if (("A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
                    childEncounterBuilder.setPeriodEnd(dsDt);
                    childEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
                } else {
                    childEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
                }

                setCommonEncounterAttributes(pv1, childEncounterBuilder, imperialHL7Helper, patientGuid, true);

                //add in additional extended data as Parameters resource with additional extension
                ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(childEncounterBuilder);
                containedParametersBuilder.removeContainedParameters();

                String patientType = pv1.getPatientType().getValue();
                if (!Strings.isNullOrEmpty(patientType)  && !patientType.equalsIgnoreCase("\"\"")) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "patient_type"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);
                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "patient_type", patientType, IMConstant.IMPERIAL_CERNER
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());

                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }


                String treatmentFunctionCode = pv1.getHospitalService().getValue();
                if (!Strings.isNullOrEmpty(treatmentFunctionCode)  && !treatmentFunctionCode.equalsIgnoreCase("\"\"")) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "treatment_function_code"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);
                     MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "treatment_function_code", treatmentFunctionCode, IMConstant.IMPERIAL_CERNER
                    );
                     MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());

                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                String admissionSourceCode = pv1.getAdmitSource().getValue();
                if (!Strings.isNullOrEmpty(admissionSourceCode) && !admissionSourceCode.equalsIgnoreCase("\"\"")) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "admission_source_code"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "admission_source_code", admissionSourceCode, IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                String admissionMethodCode = pv1.getAdmissionType().getValue();
                if (!Strings.isNullOrEmpty(admissionMethodCode) && !admissionMethodCode.equalsIgnoreCase("\"\"")) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "admission_method_code"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "admission_method_code", admissionMethodCode, IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                String dischargeDisposition = pv1.getDischargeDisposition().getValue();
                if (!Strings.isNullOrEmpty(dischargeDisposition) && !dischargeDisposition.equalsIgnoreCase("\"\"")) {
                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "discharge_method"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "discharge_method", dischargeDisposition, IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                String dischargedToLocation = pv1.getDischargedToLocation().toString();
                if (!Strings.isNullOrEmpty(dischargedToLocation) && !dischargedToLocation.equalsIgnoreCase("\"\"")) {
                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "discharge_destination_code"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Imperial","CM_Sys_Cerner","HL7v2", msgType,
                            "discharge_destination_code", dischargedToLocation, IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                //and link the parent to this new child encounter
                Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, encounterId);
                if (existingParentEncounterBuilder.isIdMapped()) {
                    childDischargeRef
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, imperialHL7Helper);
                }
                existingEncounterList.addReference(childDischargeRef);

                fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);
                fhirResourceFiler.savePatientResource(null, childEncounterBuilder);
            }
        }
    }

    /**
     *
     * @param pv1
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @param msgType
     * @throws Exception
     */
    public static void deleteEncounterAndChildren(PV1 pv1, FhirResourceFiler fhirResourceFiler, ImperialHL7Helper imperialHL7Helper, String msgType) throws Exception {
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
                    boolean deleteChild = false;
                    if(("EMERGENCY".equalsIgnoreCase(childEncounter.getClass_().toString())) && ("ADT_A11".equalsIgnoreCase(msgType))) {
                        deleteChild = true;

                    } else if(("INPATIENT".equalsIgnoreCase(childEncounter.getClass_().toString())) && ("ADT_A12".equalsIgnoreCase(msgType))) {
                        deleteChild = true;

                    } else if(("INPATIENT".equalsIgnoreCase(childEncounter.getClass_().toString())) && ("ADT_A11".equalsIgnoreCase(msgType))) {
                        deleteChild = true;

                    } else if("OUTPATIENT".equalsIgnoreCase(childEncounter.getClass_().toString())) {
                        deleteChild = true;
                    }

                    if(deleteChild) {
                        LOG.debug("Deleting child encounter " + childEncounter.getId());
                        fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                    }

                } else {

                    TransformWarnings.log(LOG, imperialHL7Helper, "Cannot find existing child Encounter: {} for deletion", childEncounter.getId());
                }
            }
            //finally, delete the top level parent
            if ("ADT_A11".equalsIgnoreCase(msgType)) {
                fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);
            }

        } else {
            TransformWarnings.log(LOG, imperialHL7Helper, "Cannot find existing Encounter: {} for deletion", String.valueOf(pv1.getVisitNumber().getID()));
        }
    }

}
