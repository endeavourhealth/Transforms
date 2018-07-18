package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.hl7.fhir.instance.model.DomainResource;
import org.hl7.fhir.instance.model.List_;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;

import java.util.ArrayList;
import java.util.List;

public class ContainedListBuilder {

    private static final String CONTAINED_LIST_ID = "Items";

    private HasContainedListI parentBuilder;

    public ContainedListBuilder(HasContainedListI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    public void addContainedListItem(Reference reference, CsvCell... sourceCells) {

        DomainResource resource = parentBuilder.getResource();

        List_ list = getOrCreateContainedList();

        //avoid having duplicates, so check before we add
        for (List_.ListEntryComponent entry: list.getEntry()) {
            Reference existingReference = entry.getItem();
            if (ReferenceHelper.equals(existingReference, reference)) {
                return;
            }
        }

        List_.ListEntryComponent entry = list.addEntry();
        entry.setItem(reference);

        int listIndex = resource.getContained().indexOf(list);
        int entryIndex = list.getEntry().indexOf(entry);
        parentBuilder.auditValue("contained[" + listIndex + "].entry[" + entryIndex + "].item.reference", sourceCells);
    }

    public void removeContainedList() {

        DomainResource resource = parentBuilder.getResource();
        List_ list = getOrCreateContainedList();

        //remove any audits we've created for the Contained list
        parentBuilder.getAuditWrapper().removeAudit("contained");

        resource.getContained().remove(list);

        //also remove the extension that points to the list
        String extensionUrl = parentBuilder.getContainedListExtensionUrl();
        ExtensionConverter.removeExtension(resource, extensionUrl);
    }

    public List<Reference> getContainedListItems() {

        List<Reference> ret = new ArrayList<>();

        List_ list = getContainedList();
        if (list != null
                && list.hasEntry()) {

            for (List_.ListEntryComponent entry: list.getEntry()) {
                Reference reference = entry.getItem();
                ret.add(reference);
            }
        }

        return ret;
    }

    private List_ getContainedList() {

        DomainResource resource = parentBuilder.getResource();
        if (resource.hasContained()) {
            for (Resource contained: resource.getContained()) {
                if (contained.getId().equals(CONTAINED_LIST_ID)) {
                    return (List_)contained;
                }
            }
        }

        return null;
    }

    private List_ getOrCreateContainedList() {

        DomainResource resource = parentBuilder.getResource();
        List_ list = getContainedList();

        //if the list wasn't there before, create and add it
        if (list == null) {
            list = new List_();
            list.setId(CONTAINED_LIST_ID);
            resource.getContained().add(list);
        }

        //add the extension, unless it's already there
        String extensionUrl = parentBuilder.getContainedListExtensionUrl();
        Reference listReference = ReferenceHelper.createInternalReference(CONTAINED_LIST_ID);
        ExtensionConverter.createOrUpdateExtension(resource, extensionUrl, listReference);

        return list;
    }

    public void addReferences(ReferenceList referenceList) {
        if (referenceList == null) {
            return;
        }

        for (int i=0; i<referenceList.size(); i++) {
            Reference reference = referenceList.getReference(i);
            CsvCell[] sourceCells = referenceList.getSourceCells(i);
            addContainedListItem(reference, sourceCells);
        }
    }

    /**
     * used to copy references from one list to another without any audit info
     */
    public void addReferencesNoAudit(List<Reference> references) {
        for (Reference reference: references) {
            addContainedListItem(reference);
        }
    }

}
