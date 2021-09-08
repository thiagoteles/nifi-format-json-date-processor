package com.apache.nifi.processors.jsondateformat.processors.jsondateformat;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.Assert.*;

public class FormatJsonDateTest {
    @Test
    public void testOnTrigger_success() {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream(("{ \"name\": \"Thiago Teles\", \"eventDate\": \"1/10/2021\", " +
                "\"lastDate\": \"1/9/2020\", \"nonFormattedDate\": \"5/8/2021\" }").getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new FormatJsonDateProcessor());

        // Add properties
        runner.setProperty(FormatJsonDateProcessor.JSON_PROPERTIES, "eventDate, lastDate");
        runner.setProperty(FormatJsonDateProcessor.CURRENT_DATE_FORMAT, "d/M/yyyy");
        runner.setProperty(FormatJsonDateProcessor.NEW_DATE_FORMAT, "yyyy-MM-dd");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(FormatJsonDateProcessor.SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result),
                StandardCharsets.UTF_8.name()));

        // Test attributes and content
        result.assertContentEquals("{\"name\":\"Thiago Teles\",\"eventDate\":\"2021-10-01\",\"lastDate\":\"2020-09-01\",\"nonFormattedDate\":\"5/8/2021\"}");
    }

    @Test
    public void testOnTrigger_failure() {
        // Content to be mock a json file
        String originalJsonValue = "{ \"name\": \"Thiago Teles\", \"eventDate\": \"1/10/2021\", " +
                "\"lastDate\": \"1/9/2020\", \"nonFormattedDate\": \"5/8/2021\" }";
        InputStream content = new ByteArrayInputStream((originalJsonValue).getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new FormatJsonDateProcessor());

        // Add properties
        runner.setProperty(FormatJsonDateProcessor.JSON_PROPERTIES, "eventDate, lastDate");
        runner.setProperty(FormatJsonDateProcessor.CURRENT_DATE_FORMAT, "-1");
        runner.setProperty(FormatJsonDateProcessor.NEW_DATE_FORMAT, "yyyy-MM-dd");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(FormatJsonDateProcessor.FAIL);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result), StandardCharsets.UTF_8.name()));

        // Test attributes and content
        result.assertContentEquals(originalJsonValue);
    }

    @Test
    public void testOnTrigger_with_null_dates() {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream(("{ \"name\": \"Thiago Teles\", \"eventDate\": null, " +
                "\"lastDate\": \"1/9/2020\", \"nonFormattedDate\": \"5/8/2021\" }").getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new FormatJsonDateProcessor());

        // Add properties
        runner.setProperty(FormatJsonDateProcessor.JSON_PROPERTIES, "eventDate, lastDate");
        runner.setProperty(FormatJsonDateProcessor.CURRENT_DATE_FORMAT, "d/M/yyyy");
        runner.setProperty(FormatJsonDateProcessor.NEW_DATE_FORMAT, "yyyy-MM-dd");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(FormatJsonDateProcessor.SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result),
                StandardCharsets.UTF_8.name()));

        // Test attributes and content
        result.assertContentEquals("{\"name\":\"Thiago Teles\",\"eventDate\":null," +
                "\"lastDate\":\"2020-09-01\",\"nonFormattedDate\":\"5/8/2021\"}");
    }

    @Test
    public void testOnTrigger_with_empty_dates() {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream(("{ \"name\": \"Thiago Teles\", \"eventDate\": \"\", " +
                "\"lastDate\": \"1/9/2020\", \"nonFormattedDate\": \"5/8/2021\" }").getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new FormatJsonDateProcessor());

        // Add properties
        runner.setProperty(FormatJsonDateProcessor.JSON_PROPERTIES, "eventDate, lastDate");
        runner.setProperty(FormatJsonDateProcessor.CURRENT_DATE_FORMAT, "d/M/yyyy");
        runner.setProperty(FormatJsonDateProcessor.NEW_DATE_FORMAT, "yyyy-MM-dd");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(FormatJsonDateProcessor.SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result),
                StandardCharsets.UTF_8.name()));

        // Test attributes and content
        result.assertContentEquals("{\"name\":\"Thiago Teles\",\"eventDate\":\"\"," +
                "\"lastDate\":\"2020-09-01\",\"nonFormattedDate\":\"5/8/2021\"}");
    }
}
