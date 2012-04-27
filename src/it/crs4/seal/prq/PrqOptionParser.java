// Copyright (C) 2011-2012 CRS4.
//
// This file is part of Seal.
//
// Seal is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// Seal is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
//
// You should have received a copy of the GNU General Public License along
// with Seal.  If not, see <http://www.gnu.org/licenses/>.

package it.crs4.seal.prq;

import it.crs4.seal.common.SealToolParser;
import it.crs4.seal.common.ClusterUtils;
import it.crs4.seal.common.Utils;

import fi.tkk.ics.hadoop.bam.FastqInputFormat;
import fi.tkk.ics.hadoop.bam.QseqInputFormat;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PrqOptionParser extends SealToolParser {

	private static final Log LOG = LogFactory.getLog(PairReadsQSeq.class);

	public static final String ConfigSection = "Prq";

	public enum InputFormat {
		qseq,
		fastq
	};

	public static final String InputFormatDefault = InputFormat.qseq.toString();
	public static final String InputFormatConfigName = "seal.prq.input-format";

	public static final int DefaultMinBasesThreshold = 30;
	public static final String MinBasesThresholdConfigName = "seal.prq.min-bases-per-read";
	public static final String MinBasesThresholdConfigName_deprecated = "bl.prq.min-bases-per-read";

	public static final boolean DropFailedFilterDefault = true;
	public static final String DropFailedFilterConfigName = "seal.prq.drop-failed-filter";
	public static final String DropFailedFilterConfigName_deprecated = "bl.prq.drop-failed-filter";

	public static final boolean WarningOnlyIfUnpairedDefault = false;
	public static final String WarningOnlyIfUnpairedConfigName = "seal.prq.warning-only-if-unpaired";
	public static final String WarningOnlyIfUnpairedConfigName_deprecated = "bl.prq.warning-only-if-unpaired";
	private InputFormat inputFormat;

	public PrqOptionParser()
	{
		super(ConfigSection, "seal_prq");
		this.setMinReduceTasks(1);
		this.setAcceptedOutputFormats(new String[] { "prq" });
	}

	@Override
	protected CommandLine parseOptions(Configuration conf, String[] args)
	  throws IOException, ParseException
	{
		conf.setInt(MinBasesThresholdConfigName, DefaultMinBasesThreshold);
		conf.setBoolean(DropFailedFilterConfigName, DropFailedFilterDefault);
		conf.setBoolean(WarningOnlyIfUnpairedConfigName, WarningOnlyIfUnpairedDefault);
		conf.set(InputFormatConfigName, InputFormatDefault);

		CommandLine line = super.parseOptions(conf, args);

		/* **** handle deprected properties **** */
		Utils.checkDeprecatedProp(conf, LOG, MinBasesThresholdConfigName_deprecated, MinBasesThresholdConfigName);
		Utils.checkDeprecatedProp(conf, LOG, DropFailedFilterConfigName_deprecated, DropFailedFilterConfigName);
		Utils.checkDeprecatedProp(conf, LOG, WarningOnlyIfUnpairedConfigName_deprecated, WarningOnlyIfUnpairedConfigName);

		// Let the deprecated properties override the new ones, unless the new ones have a non-default value.
		// If the new property has a non-default value, it must have been set by the user.
		// If, on the other hand, the deprecated property has a value, it must have been set by the user since
		// we're not setting them here.
		if (conf.get(MinBasesThresholdConfigName_deprecated) != null &&
				conf.getInt(MinBasesThresholdConfigName, DefaultMinBasesThreshold) == DefaultMinBasesThreshold)
		{
			conf.setInt(MinBasesThresholdConfigName, conf.getInt(MinBasesThresholdConfigName_deprecated, DefaultMinBasesThreshold));
		}

		if (conf.get(DropFailedFilterConfigName_deprecated) != null &&
				conf.getBoolean(DropFailedFilterConfigName, DropFailedFilterDefault) == DropFailedFilterDefault)
		{
			conf.setBoolean(DropFailedFilterConfigName, conf.getBoolean(DropFailedFilterConfigName_deprecated, DropFailedFilterDefault));
		}

		if (conf.get(WarningOnlyIfUnpairedConfigName_deprecated) != null &&
				conf.getBoolean(WarningOnlyIfUnpairedConfigName, WarningOnlyIfUnpairedDefault) == WarningOnlyIfUnpairedDefault)
		{
			conf.setBoolean(WarningOnlyIfUnpairedConfigName, conf.getBoolean(WarningOnlyIfUnpairedConfigName_deprecated, WarningOnlyIfUnpairedDefault));
		}

		/* **** end handle deprected properties **** */

		String input = conf.get(InputFormatConfigName);
		try {
			inputFormat = Enum.valueOf(InputFormat.class, input); // throws IllegalArgumentException
		}
		catch (IllegalArgumentException e) {
			throw new ParseException("Unknown input format name " + input + ". Try 'qseq' or 'fastq'");
		}

		if (inputFormat == InputFormat.fastq && conf.get(QseqInputFormat.CONF_BASE_QUALITY_ENCODING) != null && conf.get(FastqInputFormat.CONF_BASE_QUALITY_ENCODING) == null)
		{
			throw new ParseException(
					"Input format set to fastq, but you're also setting " + QseqInputFormat.CONF_BASE_QUALITY_ENCODING + "\n" +
					"and not setting " + FastqInputFormat.CONF_BASE_QUALITY_ENCODING + ".\n" +
					"Perhaps you've made an error and set the wrong property?  Set both\n" +
					QseqInputFormat.CONF_BASE_QUALITY_ENCODING + " and " + FastqInputFormat.CONF_BASE_QUALITY_ENCODING + " to avoid this safety check.");
		}
		else if (inputFormat == InputFormat.qseq && conf.get(QseqInputFormat.CONF_BASE_QUALITY_ENCODING) == null && conf.get(FastqInputFormat.CONF_BASE_QUALITY_ENCODING) != null)
		{
			throw new ParseException(
					"Input format set to qseq, but you're also setting " + FastqInputFormat.CONF_BASE_QUALITY_ENCODING + "\n" +
					"and not setting " + QseqInputFormat.CONF_BASE_QUALITY_ENCODING + ".\n" +
					"Perhaps you've made an error and set the wrong property?  Set both\n" +
					QseqInputFormat.CONF_BASE_QUALITY_ENCODING + " and " + FastqInputFormat.CONF_BASE_QUALITY_ENCODING + " to avoid this safety check.");
		}

		// set number of reduce tasks to use
		conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, String.valueOf(getNReduceTasks()));
		return line;
	}

	// XXX: This is already implemented in a way different from how
	// SealToolParser currently does things for input and output formats.
	// I'll leave this as is for now and might refactor it in the future.
	public InputFormat getSelectedInputFormat() { return inputFormat; }

	public String getOutputFormatName()
	{
		String name = super.getOutputFormatName();
		if (name == null)
			return "prq"; // return default
		else
			return name;
	}
}
