
package org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Java class for CareAimListType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="CareAimListType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="CareAim" type="{http://www.e-mis.com/emisopen/MedicalRecord}CareAimType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CareAimListType", namespace = "http://www.e-mis.com/emisopen/MedicalRecord", propOrder = {
    "careAim"
})
public class CareAimListType {

    @XmlElement(name = "CareAim", namespace = "http://www.e-mis.com/emisopen/MedicalRecord")
    protected List<CareAimType> careAim;

    /**
     * Gets the value of the careAim property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the careAim property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCareAim().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link CareAimType }
     * 
     * 
     */
    public List<CareAimType> getCareAim() {
        if (careAim == null) {
            careAim = new ArrayList<CareAimType>();
        }
        return this.careAim;
    }

}
