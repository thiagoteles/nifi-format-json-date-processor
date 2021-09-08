package com.apache.nifi.processors.jsondateformat.processors.jsondateformat;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import static org.junit.Assert.*;

public class JsonProcessorTest {
    /**
     * Test of onTrigger method, of class JsonProcessor.
     */
    @org.junit.Test
    public void testOnTrigger() throws IOException {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream(("{ \"name\": \"Thiago Teles\", \"eventDate\": \"1/10/2021\", " +
                "\"lastDate\": \"1/9/2020\", \"nonFormattedDate\": \"5/8/2021\" }").getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new JsonProcessor());

        // Add properties
        runner.setProperty(JsonProcessor.JSON_PROPERTIES, "eventDate, lastDate");
        runner.setProperty(JsonProcessor.CURRENT_DATE_FORMAT, "d/M/yyyy");
        runner.setProperty(JsonProcessor.NEW_DATE_FORMAT, "yyyy-MM-dd");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(JsonProcessor.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result)));

        // Test attributes and content

        result.assertContentEquals("{\"name\":\"Thiago Teles\",\"eventDate\":\"2021-10-01\",\"lastDate\":\"2020-09-01\",\"nonFormattedDate\":\"5/8/2021\"}");
    }
}
