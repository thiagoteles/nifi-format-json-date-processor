package com.apache.nifi.processors.jsondateformat.processors.jsondateformat;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"json", "date", "format"})
@CapabilityDescription("Format provided json date fields with specific format")
public class FormatJsonDateProcessor extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor JSON_PROPERTIES = new PropertyDescriptor.Builder()
            .name("JSON Properties")
            .required(true)
            .description("Specify json properties to format. You can add multiple properties with comma separator")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CURRENT_DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("Current Date Format")
            .required(true)
            .description("Specify the current date format of the field. E.g: d/M/yyyy")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor NEW_DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("New Date Format")
            .required(true)
            .description("Specify the new date format of the field. E.g: dd/MM/yyyy")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship FAIL = new Relationship.Builder()
            .name("failure")
            .description("Fail relationship")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(JSON_PROPERTIES);
        properties.add(CURRENT_DATE_FORMAT);
        properties.add(NEW_DATE_FORMAT);
        this.descriptors = Collections.unmodifiableList(properties);

        Set<Relationship> newRelationships = new HashSet<>();
        newRelationships.add(SUCCESS);
        newRelationships.add(FAIL);
        this.relationships = Collections.unmodifiableSet(newRelationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> json = new AtomicReference<>();
        String[] properties = context.getProperty(JSON_PROPERTIES).getValue().split(",");
        FlowFile flowfile = session.get();

        session.read(flowfile, in -> json.set(IOUtils.toString(in, StandardCharsets.UTF_8.name())));

        JsonObject jsonObject = new Gson().fromJson(json.get(), JsonObject.class).getAsJsonObject();

        for (String property : properties) {
            JsonElement jsonElement = jsonObject.get(property.trim());
            String currentJsonValue = jsonElement.getAsString();

            if (currentJsonValue != null && !currentJsonValue.isEmpty()) {
                String formattedDate = getFormattedDate(context, currentJsonValue);

                if (formattedDate == null) {
                    session.transfer(flowfile, FAIL);
                    return;
                }

                jsonObject.addProperty(property.trim(), formattedDate);
            }
        }

        json.set(jsonObject.toString());
        flowfile = session.write(flowfile, out -> out.write(json.get().getBytes()));
        session.transfer(flowfile, SUCCESS);
    }

    private String getFormattedDate(ProcessContext context, String oldDateString) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(context.getProperty(CURRENT_DATE_FORMAT)
                                                        .getValue());
            Date date = simpleDateFormat.parse(oldDateString);
            simpleDateFormat.applyPattern(context.getProperty(NEW_DATE_FORMAT).getValue());
            return simpleDateFormat.format(date);
        } catch (ParseException e) {
            getLogger().error("Failed to convert one or more fields with provided format");
        }

        return null;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        FormatJsonDateProcessor that = (FormatJsonDateProcessor) o;
        return Objects.equals(descriptors, that.descriptors) && Objects.equals(relationships, that.relationships);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), descriptors, relationships);
    }
}
