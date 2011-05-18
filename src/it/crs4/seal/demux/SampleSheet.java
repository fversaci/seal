// Copyright (C) 2011 CRS4.
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

package it.crs4.seal.demux;

import it.crs4.seal.common.CutString;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.IOException;

public class SampleSheet
{
	public static class FormatException extends Exception {
		private static final long serialVersionUID = 1L;
		public FormatException(String msg) {
			super(msg);
		}
	}

	// defaults
	private static final int InitNumLanes = 8;
	private static final int InitNumIndexTags = 12;

	// private fields
	private ArrayList< HashMap<String, String> > table;
	private int nSamples = 0;

	public SampleSheet() 
	{
	 // Create an empty table for consistency.  It will be trashed when
	 // a file is loaded.
		table = new ArrayList< HashMap<String, String> >(0);
 	}

	public SampleSheet(Reader in) throws IOException, FormatException
	{
		loadTable(in);
	}


	public void loadTable(Reader in) throws IOException, FormatException
	{
		table = new ArrayList< HashMap<String, String> >(InitNumLanes);
		nSamples = 0;

		String line = null;
		CutString scanner = new CutString(",", 1, 2, 4);
		LineNumberReader input = new LineNumberReader(in);

		line = input.readLine();
		if (line == null)
			throw new FormatException("Empty sample sheet");
		// else this should be the heading line, so drop it.

		try
		{
			line = input.readLine();
			while (line != null)
			{
				scanner.loadRecord(line);
				insertRecord( Integer.parseInt(scanner.getField(0)), scanner.getField(1), scanner.getField(2));
				line = input.readLine();
			}
		}
		catch (CutString.FormatException e)
		{
			throw new FormatException(e.getMessage());
		}
	}

	private void insertRecord(int lane, String sample, String tag) throws FormatException
	{
		// remove quotes and turn index tag to uppercase
		sample = sample.replaceAll("\"", "");
		tag = tag.replaceAll("\"", "").toUpperCase();

		// validate inputs
		if (lane <= 0)
			throw new FormatException("Invalid lane number " + lane);
		if (sample.isEmpty())
			throw new FormatException("Invalid blank sample name");
		if (tag.isEmpty())
			throw new FormatException("Invalid blank index");

		int index = lane - 1;
		if (table.size() < lane)
		{
			int grow_by = lane - table.size();
			for (int i = 0; i < grow_by; ++i)
				table.add(new HashMap<String, String>(InitNumIndexTags));
		}

		HashMap<String, String> map = table.get(index);

		// check for duplicates
		if (map.get(tag) != null)
			throw new FormatException("index " + tag + " appears twice for the same lane " + lane);

		// and finally insert
		map.put(tag, sample);
		nSamples += 1;
	}

	public String getSampleId(int lane, String indexSeq)
	{
		if (lane <= 0)
			throw new IllegalArgumentException("Invalid negative lane number " + lane);
		if (indexSeq.isEmpty())
			throw new IllegalArgumentException("Invalid blank index");

		// turn tag to uppercase
		indexSeq = indexSeq.toUpperCase();

		int index = lane - 1;
		if (index < table.size())
			return table.get(index).get(indexSeq); // will return null if the indexSeq isn't in the Map
		else
			return null;
	}

	public int getNumSamples() { return nSamples; }
	public boolean isEmpty() { return nSamples == 0; }
}
