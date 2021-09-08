package com.apache.nifi.processors.jsondateformat.processors.jsondateformat;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"json", "date", "format"})
@CapabilityDescription("Format provided json date fields with specific format")
public class FormatJsonDateProcessor extends AbstractProcessor {

    private static String COMMA = ",";

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
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship FAIL = new Relationship.Builder()
            .name("FAIL")
            .description("Fail relationship")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(JSON_PROPERTIES);
        properties.add(CURRENT_DATE_FORMAT);
        properties.add(NEW_DATE_FORMAT);
        this.descriptors = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        FlowFile currentFlowFile = flowfile;
        session.read(flowfile, in -> {
            try{
                String json = IOUtils.toString(in, StandardCharsets.UTF_8.name());
                JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class).getAsJsonObject();

                String[] properties = context.getProperty(JSON_PROPERTIES).getValue().split(COMMA);

                for (String property : properties) {
                    JsonElement je = jsonObject.get(property.trim());
                    String currentJsonValue = je.getAsString();

                    if (currentJsonValue != null && !currentJsonValue.isEmpty()) {
                        jsonObject.addProperty(property.trim(), getFormattedDate(context, currentJsonValue));
                    }
                }

                value.set(jsonObject.toString());
            } catch(Exception ex){
                ex.printStackTrace();
                getLogger().error("Failed to format date of json object.");
                session.transfer(currentFlowFile, FAIL);
            }
        });

        flowfile = session.write(flowfile, out -> out.write(value.get().getBytes()));

        session.transfer(flowfile, SUCCESS);
    }

    private String getFormattedDate(ProcessContext context, String oldDateString) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(context.getProperty(CURRENT_DATE_FORMAT).getValue());
        Date d = sdf.parse(oldDateString);
        sdf.applyPattern(context.getProperty(NEW_DATE_FORMAT).getValue());
        return sdf.format(d);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
}
