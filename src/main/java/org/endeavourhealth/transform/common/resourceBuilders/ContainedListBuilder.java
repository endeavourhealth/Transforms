package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ContainedListBuilder {

    private static final String CONTAINED_LIST_ID = "Items";

    private HasContainedListI parentBuilder;

    public ContainedListBuilder(HasContainedListI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    public boolean addReference(Reference reference, CsvCell... sourceCells) {

        DomainResource resource = parentBuilder.getResource();

        List_ list = getOrCreateContainedList();

        //avoid having duplicates, so check before we add
        for (List_.ListEntryComponent entry: list.getEntry()) {
            if (entry.hasItem()) {
                Reference existingReference = entry.getItem();
                if (ReferenceHelper.equals(existingReference, reference)) {
                    return false;
                }
            }
        }

        List_.ListEntryComponent entry = list.addEntry();
        entry.setItem(reference);

        int listIndex = resource.getContained().indexOf(list);
        int entryIndex = list.getEntry().indexOf(entry);
        parentBuilder.auditValue("contained[" + listIndex + "].entry[" + entryIndex + "].item.reference", sourceCells);

        return true;
    }

    public boolean removeReference(Reference reference) {

        List_ list = getContainedList();
        if (list == null) {
            return false;
        }

        boolean removed = false;

        List<List_.ListEntryComponent> components = list.getEntry();
        for (int i=components.size()-1; i>=0; i--) {
            List_.ListEntryComponent component = components.get(i);
            if (component.hasItem()) {
                Reference existingReference = component.getItem();
                if (ReferenceHelper.equals(existingReference, reference)) {
                    components.remove(i);
                    removed = true;
                    continue; //don't break, just on the off chance the reference is in multiple times
                }
            }
        }

        return removed;
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

    public List<Reference> getReferences() {

        List<Reference> ret = new ArrayList<>();

        List_ list = getContainedList();
        if (list != null
                && list.hasEntry()) {

            for (List_.ListEntryComponent entry: list.getEntry()) {
                if (entry.hasItem()) {
                    Reference reference = entry.getItem();
                    ret.add(reference);
                }
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
            addReference(reference, sourceCells);
        }
    }

    public void addCodeableConcept(CodeableConcept codeableConcept, CsvCell... sourceCells) {
        DomainResource resource = parentBuilder.getResource();
        List_ list = getOrCreateContainedList();

        List_.ListEntryComponent entry = list.addEntry();
        entry.setFlag(codeableConcept);

        int listIndex = resource.getContained().indexOf(list);
        int entryIndex = list.getEntry().indexOf(entry);
        parentBuilder.auditValue("contained[" + listIndex + "].entry[" + entryIndex + "].flag.text", sourceCells);
    }

    public void addDateToLastItem(Date date, CsvCell... sourceCells) {
        DomainResource resource = parentBuilder.getResource();
        List_ list = getOrCreateContainedList();

        List<List_.ListEntryComponent> entries = list.getEntry();
        List_.ListEntryComponent lastEntry = entries.get(entries.size()-1);

        lastEntry.setDate(date);

        int listIndex = resource.getContained().indexOf(list);
        int entryIndex = list.getEntry().indexOf(lastEntry);
        parentBuilder.auditValue("contained[" + listIndex + "].entry[" + entryIndex + "].date", sourceCells);
    }

    public List<List_.ListEntryComponent> getContainedListItems() {
        DomainResource resource = parentBuilder.getResource();
        List_ list = getContainedList();
        if (list == null
                || !list.hasEntry()) {
            return null;
        }
        return list.getEntry();
    }

    /**
     * finds and returns the contained list indicated by the given extension, null otherwise
     */
    public static List_ findContainedList(DomainResource resource, String extensionUrl) {

        if (!resource.hasContained()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(resource, extensionUrl);
        if (extension == null) {
            return null;
        }

        Reference idReference = (Reference)extension.getValue();
        String idReferenceValue = idReference.getReference();
        idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char which is used for internal references

        for (Resource containedResource: resource.getContained()) {
            if (containedResource.getId().equals(idReferenceValue)) {
                return (List_)containedResource;
            }
        }

        return null;
    }
}
