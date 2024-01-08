package com.google.migration.dto;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class IntermediateCoGBKResult {
} // class