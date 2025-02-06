package com.google.refine.expr.util;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class CustomTests {
    @Test
    public void testFillMissingData() {
        String input = null;
        String defaultValue = "NULL";
        String result = fillMissingData(input, defaultValue);
        assertEquals(defaultValue, result);
    }
    
    private String fillMissingData(String input, String defaultValue) {
        return (input == null || input.isEmpty()) ? defaultValue : input;
    }
}