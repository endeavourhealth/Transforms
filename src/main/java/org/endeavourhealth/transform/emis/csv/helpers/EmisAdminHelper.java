package org.endeavourhealth.transform.emis.csv.helpers;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.models.TransformWarning;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisAdminCacheDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisLocationDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisOrganisationDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisUserInRoleDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisLocation;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisLocationOrganisation;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisOrganisation;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisUserInRole;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.exceptions.UnmappedValueException;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class EmisAdminHelper {
    private static final Logger LOG = LoggerFactory.getLogger(EmisAdminHelper.class);

    private Set<String> locationIdsChanged = ConcurrentHashMap.newKeySet();
    private Set<String> organisationIdsChanged = ConcurrentHashMap.newKeySet();
    private Set<String> userIdsChanged = ConcurrentHashMap.newKeySet();
    private Set<String> hsRequiredUserIds = ConcurrentHashMap.newKeySet(); //we only create Practitioner resources when needed

    public void addLocationChanged(CsvCell locationIdCell) {
        String id = locationIdCell.getString();
        locationIdsChanged.add(id);
    }

    public void applyAdminResourceCacheIfRequired(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        //if this is the first exchange for this organisation, we need to apply all the content of the admin resource cache
        EmisAdminCacheDalI dal = DalProvider.factoryEmisTransformDal();
        if (dal.wasAdminCacheApplied(fhirResourceFiler.getServiceId())) {
            return;
        }

        //apply the cache
        LOG.trace("Will apply admin resource cache for service " + fhirResourceFiler.getServiceId());

        applyAdminResourceCache();
    }

    private void applyAdminResourceCache() throws Exception {

        //load all location IDs
        EmisLocationDalI locationDal = DalProvider.factoryEmisLocationDal();
        Set<String> allLocationIds = locationDal.retrieveAllIds();
        this.locationIdsChanged.addAll(allLocationIds);

        //load all org IDs
        EmisOrganisationDalI organisationDal = DalProvider.factoryEmisOrganisationDal();
        Set<String> allOrgIds = organisationDal.retrieveAllIds();
        this.organisationIdsChanged.addAll(allOrgIds);

        //load all user IDs
        EmisUserInRoleDalI userInRoleDal = DalProvider.factoryEmisUserInRoleDal();
        Set<String> allUserIds = userInRoleDal.retrieveAllIds();
        this.userIdsChanged.addAll(allUserIds);
    }

    public void addOrganisationChanged(CsvCell organisationGuid) {
        String id = organisationGuid.getString();
        organisationIdsChanged.add(id);
    }

    public void processAdminChanges(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        //locations
        createLocations(fhirResourceFiler, csvHelper);

        //organisations
        createOrganisations(fhirResourceFiler, csvHelper);

        //users in role
        //we don't transform EVERY user - only those that are referenced by data or have previously been transformed
        Set<String> userIdsToTransform = findUserIdsToTransform(csvHelper);
        createPractitioners(fhirResourceFiler, csvHelper, userIdsToTransform);

        //only after actually saving everything, do we flag our admin cache as applied
        setAdminCacheApplied(csvHelper);

        //set everything to null to free up memory and cause exceptions if accessed
        this.locationIdsChanged = null;
        this.organisationIdsChanged = null;
        this.userIdsChanged = null;
        this.hsRequiredUserIds = null;
    }

    private void createPractitioners(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper, Set<String> userIdsToTransform) throws Exception {
        Set<String> batch = new HashSet<>();
        int done = 0;

        for (String userId: userIdsToTransform) {
            batch.add(userId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CreatePractitioners(batch, csvHelper, fhirResourceFiler));
                batch.clear();
            }

            done ++;
            if (done % 10000 == 0) {
                LOG.debug("Submitted " + done + " practitioners to thread pool");
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CreatePractitioners(batch, csvHelper, fhirResourceFiler));
            batch.clear();
        }
        LOG.debug("Submitted " + done + " practitioners to thread pool");

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();
    }

    private void createOrganisations(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        LOG.debug("Creating and saving " + organisationIdsChanged.size() + " location resources");
        Set<String> batch = new HashSet<>();
        int done = 0;

        for (String organisationId: organisationIdsChanged) {
            batch.add(organisationId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CreateOrganisations(batch, csvHelper, fhirResourceFiler));
                batch.clear();
            }

            done ++;
            if (done % 10000 == 0) {
                LOG.debug("Submitted " + done + " organisations to thread pool");
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CreateOrganisations(batch, csvHelper, fhirResourceFiler));
            batch.clear();
        }
        LOG.debug("Submitted " + done + " organisations to thread pool");

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();
    }

    private void createLocations(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        LOG.debug("Creating and saving " + locationIdsChanged.size() + " location resources");
        Set<String> batch = new HashSet<>();
        int done = 0;

        for (String locationId: locationIdsChanged) {
            batch.add(locationId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CreateLocations(batch, csvHelper, fhirResourceFiler));
                batch.clear();
            }

            done ++;
            if (done % 10000 == 0) {
                LOG.debug("Submitted " + done + " locations  to thread pool");
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CreateLocations(batch, csvHelper, fhirResourceFiler));
            batch.clear();
        }
        LOG.debug("Submitted " + done + " locations  to thread pool");

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();
    }


    private Set<String> findUserIdsToTransform(EmisCsvHelper csvHelper) throws Exception {

        Set<String> userIdsToTransform = new HashSet<>();

        //for each CHANGED profile ID, find out if we've previously transformed it or not, then add to TO TRANSFORM list
        LOG.debug("Users changed = " + userIdsChanged.size());
        Set<String> profileIdsPreviouslyTransformed = findUserIdsWithMappings(userIdsChanged, csvHelper, true);
        LOG.debug("User IDs previously transformed = " + profileIdsPreviouslyTransformed.size());
        userIdsToTransform.addAll(profileIdsPreviouslyTransformed);

        //for each REQUIRED profile ID we need to make sure, find if previously transformed - if NOT add to TO TRANSFORM list
        LOG.debug("Profiles required " + hsRequiredUserIds.size());
        Set<String> profileIdsNotPreviouslyTransformed = findUserIdsWithMappings(hsRequiredUserIds, csvHelper, false);
        userIdsToTransform.addAll(profileIdsNotPreviouslyTransformed);

        LOG.debug("Found users to transform " + userIdsToTransform.size());
        return userIdsToTransform;
    }

    /**
     * finds user IDs that we have previously transformed
     */
    private static Set<String> findUserIdsWithMappings(Set<String> userIds, EmisCsvHelper csvHelper, boolean findOnesWithMappings) throws Exception {

        Set<String> ret = ConcurrentHashMap.newKeySet();

        Set<String> batch = new HashSet<>();

        for (String userId: userIds) {
            batch.add(userId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CheckForIdMappings(batch, ret, csvHelper.getServiceId(), findOnesWithMappings));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CheckForIdMappings(batch, ret, csvHelper.getServiceId(), findOnesWithMappings));
            batch.clear();
        }

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();

        return ret;
    }


    /**
     * sets the admin cache as applied if it wasn't already
     */
    private void setAdminCacheApplied(EmisCsvHelper csvHelper) throws Exception {

        EmisAdminCacheDalI dal = DalProvider.factoryEmisTransformDal();
        if (!dal.wasAdminCacheApplied(csvHelper.getServiceId())) {
            dal.adminCacheWasApplied(csvHelper.getServiceId(), csvHelper.getDataSharingAgreementGuid());
        }
    }

    public void addUserInRoleChanged(CsvCell userInRoleGuidCell) {
        String id = userInRoleGuidCell.getString();
        this.userIdsChanged.add(id);
    }

    public void addRequiredUserInRole(CsvCell userInRoleGuidCell) {
        if (userInRoleGuidCell.isEmpty()) {
            return;
        }
        String id = userInRoleGuidCell.getString();
        this.hsRequiredUserIds.add(id);
    }


    /**
     * runnable to check if profile ID -> UUID mappings mappings exist and store the IDs that do (or don't)
     */
    private static class CheckForIdMappings implements Callable {

        private Set<String> userIds;
        private Set<String> ret;
        private UUID serviceId;
        private boolean findOnesWithMappings;

        public CheckForIdMappings(Set<String> userIds, Set<String> ret, UUID serviceId, boolean findOnesWithMappings) {
            this.userIds = new HashSet<>(userIds); //create copy because original will be changed
            this.ret = ret;
            this.serviceId = serviceId;
            this.findOnesWithMappings = findOnesWithMappings;
        }

        @Override
        public Object call() throws Exception {

            try {

                //need to create reference objects for each
                Set<Reference> refSet = new HashSet<>();
                for (String userId: userIds) {
                    Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, userId);
                    refSet.add(ref);
                }

                //look up ID mappings for the references
                Map<Reference, UUID> mappings = IdHelper.getEdsResourceIds(serviceId, refSet);

                //add profile IDs for any mappings found to the result set
                for (Reference ref: refSet) {
                    boolean hasMapping = mappings.containsKey(ref);
                    if (hasMapping == findOnesWithMappings) {
                        String refStr = ReferenceHelper.getReferenceId(ref);
                        ret.add(refStr);
                    }
                }

            } catch (Throwable t) {
                String msg = "Error looking up ID mappings for user IDs " + userIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }


    private class CreateLocations implements Callable {

        private Set<String> locationIds;
        private EmisCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public CreateLocations(Set<String> locationIds, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) {
            this.locationIds = new HashSet<>(locationIds); //copy because the original will keep changing
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }


        @Override
        public Object call() throws Exception {

            try {

                EmisLocationDalI dal = DalProvider.factoryEmisLocationDal();
                Set<EmisLocation> locs = dal.retrieveRecordsForIds(locationIds);
                for (EmisLocation loc: locs) {

                    //audit the file and records we used
                    ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
                    audit.auditRecord(loc.getPublishedFileId(), loc.getPublishedFileRecordNumber());

                    LocationBuilder locationBuilder = new LocationBuilder();

                    String locationGuid = loc.getLocationGuid();
                    locationBuilder.setId(locationGuid);

                    String locationName = loc.getLocationName();
                    if (!Strings.isNullOrEmpty(locationName)) {
                        locationBuilder.setName(locationName);
                    }

                    String type = loc.getLocationTypeDescription();
                    if (!Strings.isNullOrEmpty(type)) {
                        CodeableConceptBuilder cc = new CodeableConceptBuilder(locationBuilder, CodeableConceptBuilder.Tag.Location_Type, true);
                        cc.setText(type);
                    }

                    String parentLocationGuid = loc.getParentLocationGuid();
                    if (!Strings.isNullOrEmpty(parentLocationGuid)) {
                        Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, parentLocationGuid);
                        locationBuilder.setPartOf(locationReference);
                    }

                    Date openDate = loc.getOpenDate();
                    if (openDate != null) {
                        locationBuilder.setOpenDate(openDate);
                    }

                    Date closeDate = loc.getCloseDate();
                    if (closeDate != null) {
                        locationBuilder.setCloseDate(closeDate);
                    }

                    String mainContactName = loc.getMainContactName();
                    if (!Strings.isNullOrEmpty(mainContactName)) {
                        locationBuilder.setMainContactName(mainContactName);
                    }

                    String faxNumber = loc.getFaxNumber();
                    if (!Strings.isNullOrEmpty(faxNumber)) {
                        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(locationBuilder);
                        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.FAX);
                        contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
                        contactPointBuilder.setValue(faxNumber);
                    }

                    String email = loc.getEmailAddress();
                    if (!Strings.isNullOrEmpty(email)) {
                        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(locationBuilder);
                        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
                        contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
                        contactPointBuilder.setValue(email);
                    }

                    String phoneNumber = loc.getPhoneNumber();
                    if (!Strings.isNullOrEmpty(phoneNumber)) {
                        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(locationBuilder);
                        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
                        contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
                        contactPointBuilder.setValue(phoneNumber);
                    }


                    String houseNameFlat = loc.getHouseNameFlatNumber();
                    String numberAndStreet = loc.getNumberAndStreet();
                    String village = loc.getVillage();
                    String town = loc.getTown();
                    String county = loc.getCounty();
                    String postcode = loc.getPostcode();

                    AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
                    addressBuilder.setUse(Address.AddressUse.WORK);
                    addressBuilder.addLine(houseNameFlat);
                    addressBuilder.addLine(numberAndStreet);
                    addressBuilder.addLine(village);
                    addressBuilder.setCity(town);
                    addressBuilder.setDistrict(county);
                    addressBuilder.setPostcode(postcode);

                    if (loc.getOrganisations() != null) {
                        for (EmisLocationOrganisation org: loc.getOrganisations()) {
                            if (!org.isOrganisationLocationDeleted()
                                    && org.isMainLocation()) {
                                audit.auditRecord(org.getPublishedFileId(), org.getPublishedFileRecordNumber());

                                String orgGuid = org.getOrganisationGuid();
                                Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, orgGuid);
                                locationBuilder.setManagingOrganisation(organisationReference);
                            }
                        }
                    }

                    if (loc.isDeleted()) {
                        fhirResourceFiler.deleteAdminResource(null, locationBuilder);

                    } else {
                        fhirResourceFiler.saveAdminResource(null, locationBuilder);
                    }
                }

            } catch (Throwable t) {
                String msg = "Error creating practitioners for profile IDs " + locationIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }

    private class CreateOrganisations implements Callable {

        private Set<String> organisationIds;
        private EmisCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public CreateOrganisations(Set<String> organisationIds, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) {
            this.organisationIds = new HashSet<>(organisationIds); //copy because the original will keep changing
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }


        @Override
        public Object call() throws Exception {

            try {

                EmisOrganisationDalI dal = DalProvider.factoryEmisOrganisationDal();
                Set<EmisOrganisation> orgs = dal.retrieveRecordsForIds(organisationIds);
                for (EmisOrganisation org: orgs) {

                    //audit the file and records we used
                    ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
                    audit.auditRecord(org.getPublishedFileId(), org.getPublishedFileRecordNumber());

                    OrganizationBuilder organizationBuilder = new OrganizationBuilder();

                    String orgGuid = org.getOrganisationGuid();
                    organizationBuilder.setId(orgGuid);

                    String odsCode = org.getOdsCode();
                    if (!Strings.isNullOrEmpty(odsCode)) {
                        organizationBuilder.setOdsCode(odsCode);
                    }

                    String name = org.getOrganisationName();
                    if (!Strings.isNullOrEmpty(name)) {
                        organizationBuilder.setName(name);
                    }

                    String cdbNumber = org.getCdb();
                    if (!Strings.isNullOrEmpty(cdbNumber)) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
                        identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_CDB_NUMBER);
                        identifierBuilder.setValue(cdbNumber);
                    }

                    //FHIR orgs can only support being "part of" one other organisation, so if we have a parent
                    //CCG, then use that as the parent, otherwise use the regular parent column
                    String ccgOrganisationGuid = org.getCcgOrganisationGuid();
                    if (!Strings.isNullOrEmpty(ccgOrganisationGuid)
                            && !ccgOrganisationGuid.equals(orgGuid)) { //some EMIS CCG orgs seem to refer to themselves, so ignore those

                        Reference parentOrgReference = ReferenceHelper.createReference(ResourceType.Organization, ccgOrganisationGuid);
                        organizationBuilder.setParentOrganisation(parentOrgReference);

                    } else {

                        String parentOrganisationGuid = org.getParentOrganisationGuid();
                        if (!Strings.isNullOrEmpty(parentOrganisationGuid)) {
                            Reference parentOrgReference = ReferenceHelper.createReference(ResourceType.Organization, parentOrganisationGuid);
                            organizationBuilder.setParentOrganisation(parentOrgReference);
                        }
                    }

                    String organisationType = org.getOrganisationType();
                    if (!Strings.isNullOrEmpty(organisationType)) {
                        try {
                            OrganisationType fhirOrgType = EmisMappingHelper.findOrganisationType(organisationType);
                            if (fhirOrgType != null) {
                                organizationBuilder.setType(fhirOrgType);
                            }
                        } catch (UnmappedValueException ex) {
                            //getting to many unmapped Emis org types, as they keep adding new ones so just handle them
                            //see https://endeavourhealth.atlassian.net/browse/SD-216
                            TransformWarnings.log(LOG, csvHelper, "Unmapped Emis organisation type {}", organisationType);
                            organizationBuilder.setTypeFreeText(organisationType);
                        }
                    }

                    Date openDate = org.getOpenDate();
                    if (openDate != null) {
                        organizationBuilder.setOpenDate(openDate);
                    }

                    Date closeDate = org.getCloseDate();
                    if (closeDate != null) {
                        organizationBuilder.setCloseDate(openDate);
                    }

                    String mainLocationGuid = org.getMainLocationGuid();
                    if (!Strings.isNullOrEmpty(mainLocationGuid)) {
                        Reference fhirReference = ReferenceHelper.createReference(ResourceType.Location, mainLocationGuid);
                        organizationBuilder.setMainLocation(fhirReference);
                    }

                    fhirResourceFiler.saveAdminResource(null, organizationBuilder);
                }

            } catch (Throwable t) {
                String msg = "Error creating practitioners for profile IDs " + organisationIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }

    private class CreatePractitioners implements Callable {

        private Set<String> practitionerIds;
        private EmisCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public CreatePractitioners(Set<String> practitionerIds, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) {
            this.practitionerIds = new HashSet<>(practitionerIds); //copy because the original will keep changing
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }


        @Override
        public Object call() throws Exception {

            try {

                EmisUserInRoleDalI dal = DalProvider.factoryEmisUserInRoleDal();
                Set<EmisUserInRole> users = dal.retrieveRecordsForIds(practitionerIds);
                for (EmisUserInRole user: users) {

                    //audit the file and records we used
                    ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
                    audit.auditRecord(user.getPublishedFileId(), user.getPublishedFileRecordNumber());

                    PractitionerBuilder practitionerBuilder = new PractitionerBuilder();

                    String userInRoleGuid = user.getUserInRoleGuid();
                    practitionerBuilder.setId(userInRoleGuid);

                    String title = user.getTitle();
                    String givenName = user.getGivenName();
                    String surname = user.getSurname();

                    NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
                    nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
                    nameBuilder.addPrefix(title);
                    nameBuilder.addGiven(givenName);
                    nameBuilder.addFamily(surname);

                    //need to call this to generate the role in the practitioner, as all the following fields are set on that
                    PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

                    Date startDate = user.getContractStartDate();
                    if (startDate != null) {
                        roleBuilder.setRoleStartDate(startDate);
                    }

                    Date endDate = user.getContractEndDate();
                    if (endDate != null) {
                        roleBuilder.setRoleEndDate(endDate);
                    }

                    //after doing the start and end dates, call this to calculate the active state from them
                    practitionerBuilder.calculateActiveFromRoles();

                    String orgUuid = user.getOrganisationGuid();
                    Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, orgUuid);
                    roleBuilder.setRoleManagingOrganisation(organisationReference);

                    CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
                    codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);

                    String roleName = user.getJobCategoryName();
                    if (!Strings.isNullOrEmpty(roleName)) {
                        codeableConceptBuilder.setCodingDisplay(roleName);
                    }

                    String roleCode = user.getJobCategoryCode();
                    if (!Strings.isNullOrEmpty(roleCode)) {
                        codeableConceptBuilder.setCodingCode(roleCode);
                    }

                    fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
                }

            } catch (Throwable t) {
                String msg = "Error creating practitioners for profile IDs " + practitionerIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }
}
