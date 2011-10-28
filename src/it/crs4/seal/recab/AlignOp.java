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

package it.crs4.seal.recab;

public class AlignOp {

	public static enum AlignOpType {
		Match,
		Insert,
		Delete,
		SoftClip,
		HardClip,
		Skip,
		Pad;

		public static AlignOpType fromSymbol(String sym) {
			if (sym.length() != 1)
				throw new IllegalArgumentException("Unrecognized alignment operation symbol " + sym);
			else
				return fromSymbol(sym.charAt(0));
		}

		public static AlignOpType fromSymbol(char sym) {
			switch (sym) {
			case 'M':
				return AlignOpType.Match;
			case 'I':
				return AlignOpType.Insert;
			case 'D':
				return AlignOpType.Delete;
			case 'S':
				return AlignOpType.SoftClip;
			case 'H':
				return AlignOpType.HardClip;
			case 'N':
				return AlignOpType.Skip;
			case 'P':
				return AlignOpType.Pad;
			default:
				throw new IllegalArgumentException("Unrecognized alignment operation symbol " + sym);
			}
		}
	}

	private AlignOpType op;
	private int len;

	public AlignOp(AlignOpType op, int len) {
		this.op = op;
		this.len = len;
	}
	
	public AlignOpType getOp() { return op; }
	public int getLen() { return len; }
}
