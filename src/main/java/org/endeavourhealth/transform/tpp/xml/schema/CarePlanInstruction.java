
package org.endeavourhealth.transform.tpp.xml.schema;

import javax.xml.bind.annotation.*;
import java.math.BigInteger;


/**
 * <p>Java class for CarePlanInstruction complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="CarePlanInstruction">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="InstructionText" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="Order" type="{http://www.w3.org/2001/XMLSchema}integer"/>
 *         &lt;element name="Responsibility" type="{}CarePlanInstructionResponsibility"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CarePlanInstruction", propOrder = {
    "instructionText",
    "order",
    "responsibility"
})
public class CarePlanInstruction {

    @XmlElement(name = "InstructionText", required = true)
    protected String instructionText;
    @XmlElement(name = "Order", required = true)
    protected BigInteger order;
    @XmlElement(name = "Responsibility", required = true)
    @XmlSchemaType(name = "string")
    protected CarePlanInstructionResponsibility responsibility;

    /**
     * Gets the value of the instructionText property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getInstructionText() {
        return instructionText;
    }

    /**
     * Sets the value of the instructionText property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setInstructionText(String value) {
        this.instructionText = value;
    }

    /**
     * Gets the value of the order property.
     * 
     * @return
     *     possible object is
     *     {@link BigInteger }
     *     
     */
    public BigInteger getOrder() {
        return order;
    }

    /**
     * Sets the value of the order property.
     * 
     * @param value
     *     allowed object is
     *     {@link BigInteger }
     *     
     */
    public void setOrder(BigInteger value) {
        this.order = value;
    }

    /**
     * Gets the value of the responsibility property.
     * 
     * @return
     *     possible object is
     *     {@link CarePlanInstructionResponsibility }
     *     
     */
    public CarePlanInstructionResponsibility getResponsibility() {
        return responsibility;
    }

    /**
     * Sets the value of the responsibility property.
     * 
     * @param value
     *     allowed object is
     *     {@link CarePlanInstructionResponsibility }
     *     
     */
    public void setResponsibility(CarePlanInstructionResponsibility value) {
        this.responsibility = value;
    }

}
