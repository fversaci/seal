# Copyright (C) 2011-2012 CRS4.
#
# This file is part of Seal.
#
# Seal is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Seal is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Seal.  If not, see <http://www.gnu.org/licenses/>.

import logging
import os
import random

from pydoop.pipes import Mapper
from pydoop.utils import jc_configure, jc_configure_int, jc_configure_bool

from seal.lib.aligner.hirapi import HiRapiAligner
import seal.lib.io.protobuf_mapping as protobuf_mapping
import seal.lib.mr.utils as utils
from seal.lib.mr.hit_processor_chain_link import HitProcessorChainLink
from seal.lib.mr.emit_sam_link import RapiEmitSamLink
from seal.lib.mr.filter_link import RapiFilterLink
from seal.lib.mr.hadoop_event_monitor import HadoopEventMonitor
import seal.lib.deprecation_utils as deprecation_utils
from seal.seqal import seqal_app

_BWA_INDEX_MANDATORY_EXT = set(["amb", "ann", "bwt", "pac", "rbwt", "rpac"])
_BWA_INDEX_MMAP_EXT = set(["rsax", "sax"])
_BWA_INDEX_NORM_EXT = set(["rsa", "sa"])
_BWA_INDEX_EXT = _BWA_INDEX_MANDATORY_EXT | _BWA_INDEX_MMAP_EXT | _BWA_INDEX_NORM_EXT


class MarkDuplicatesEmitter(HitProcessorChainLink):
    def __init__(self, context, event_monitor, next_link = None):
        super(MarkDuplicatesEmitter, self).__init__(next_link)
        self.ctx = context
        self.event_monitor = event_monitor

    def __order_pair(self, pair):
        # Order pair such that left-most read is at pos 0.
        # Unmapped reads come after all positions.  None values are last.

        if pair[1] is None:
            ordered_pair = pair
        elif pair[0] is None:
            ordered_pair = (pair[1], pair[0])
        elif pair[1].is_unmapped(): # there are no None values
            ordered_pair = pair
        elif pair[0].is_unmapped():
            ordered_pair = (pair[1], pair[0])
        #else there are no unmapped reads
        elif (pair[0].ref_id, pair[0].get_untrimmed_left_pos(), pair[0].is_on_reverse()) > \
             (pair[1].ref_id, pair[1].get_untrimmed_left_pos(), pair[1].is_on_reverse()):
            ordered_pair = (pair[1], pair[0])
        else:
            ordered_pair = pair

        return ordered_pair

    def process(self, pair):
        if any(pair):
            # order pair such that left-most read is at pos 0
            ordered_pair = self.__order_pair(pair)

            record = protobuf_mapping.serialize_pair(ordered_pair)
            # emit with the left coord
            key = self.get_hit_key(ordered_pair[0])
            self.ctx.emit(key, record)
            if ordered_pair[0].is_mapped():
                self.event_monitor.count("mapped coordinates", 1)
                # since we ordered the pair, if ordered_pair[0] is unmapped
                # ordered_pair[1] will not be mapped.
                if ordered_pair[1]:
                    if ordered_pair[1].is_mapped():
                        # a full pair. We emit the coordinate, but with PAIR_STRING as the value
                        key = self.get_hit_key(ordered_pair[1])
                        self.ctx.emit(key, seqal_app.PAIR_STRING)
                        self.event_monitor.count("mapped coordinates", 1)
                    else:
                        self.event_monitor.count("unmapped reads", 1)
            else:
                self.event_monitor.count("unmapped reads", len(pair))

        # in all cases, forward the original pair to the link in the chain
        if self.next_link:
            self.next_link.process(pair)

    @staticmethod
    def get_hit_key(hit):
        """Build a key to identify a read hit.
           To get the proper order in the lexicographic sort, we use a 12-digit
             field for the position, padding the left with 0s.  12 digits should
             be enough for any genome :-)

             We do the same thing for the contig, using its id instead of its name (the tid field).
             This gives us almost a sorted order in the reducer output; the only entries
             out of place are the reversed and trimmed reads, since their untrimmed position
             (used in the key) is different from the reference position.

             On the other hand, if the read is unmapped, we make a key that starts with the string
             'unmapped:' and then has a 10-digit random number.  The randomness is inserted so that
             the unmapped reads may be distributed to the various reducers, instead of having them
             all send to the same one (since they would all have the same key).
        """
        if hit.is_mapped():
            if hit.is_on_reverse():
                pos = hit.get_untrimmed_right_pos()
                orientation = 'R'
            else:
                pos = hit.get_untrimmed_left_pos()
                orientation = 'F'
            values = ("%04d" % hit.ref_id, "%012d" % pos, orientation)
        else:
            values = (seqal_app.UNMAPPED_STRING, "%010d" % random.randint(0, 9999999999))
        return ':'.join( values )

class mapper(Mapper):
    """
    Aligns sequences to a reference genome.

    @input-record: C{key} does not matter (standard LineRecordReader);
    C{value} is a tab-separated text line with 5 fields: ID, read_seq,
    read_qual, mate_seq, mate_qual.

    @output-record: protobuf-serialized mapped pairs (map-reduce job) or alignment
    records in SAM format (map-only job).

    @jobconf-param: C{mapred.reduce.tasks} number of Hadoop reduce tasks to launch.
    If the value of this property is set to 0, then the mapper will directly output
    the mappings in SAM format, like BWA.  If set to a value > 0 the mapper will output
    mappings in the protobuf serialized format for the rmdup reducer.

    @jobconf-param: C{seal.seqal.log.level} logging level,
    specified as a logging module literal.

    @jobconf-param: C{mapred.cache.archives} distributed
    cache entry for the bwa index archive. The entry
    is of the form HDFS_PATH#LINK_NAME. The archive for a given
    chromosome must contain (at the top level, i.e., no directories) all
    files generated by 'bwa index' for that chromosome.

    @jobconf-param: C{seal.seqal.alignment.max.isize}: if the
    inferred isize is greater than this value, Smith-Waterman alignment
    for unmapped reads will be skipped.

    @jobconf-param: C{seal.seqal.pairing.batch.size}: how many
    sequences should be processed at a time by the pairing
    function. Status will be updated at each new batch: therefore,
    lowering this value can help avoid timeouts.

    @jobconf-param: C{seal.seqal.fastq-subformat} Specifies base quality
    score encoding.  Supported types are: 'fastq-sanger' and 'fastq-illumina'.

    @jobconf-param: C{mapred.create.symlink} must be set to 'yes'.

    @jobconf-param: C{seal.seqal.min_hit_quality} mapping quality
    threshold below which the mapping will be discarded.
    """
    SUPPORTED_FORMATS = "fastq-illumina", "fastq-sanger"
    DEFAULT_FORMAT = "fastq-sanger"
    COUNTER_CLASS = "SEQAL"
    DeprecationMap = {
      "seal.seqal.log.level":           "bl.seqal.log.level",
      "seal.seqal.alignment.max.isize": "bl.seqal.alignment.max.isize",
      "seal.seqal.pairing.batch.size":  "bl.seqal.pairing.batch.size",
      "seal.seqal.fastq-subformat":     "bl.seqal.fastq-subformat",
      "seal.seqal.min_hit_quality":     "bl.seqal.min_hit_quality",
      "seal.seqal.remove_unmapped":     "bl.seqal.remove_unmapped",
      "seal.seqal.discard_duplicates":  "bl.seqal.discard_duplicates",
      "seal.seqal.nthreads":            "bl.seqal.nthreads",
      "seal.seqal.trim.qual":           "bl.seqal.trim.qual",
    }

    def __get_configuration(self, ctx):
        # TODO:  refactor settings common to mapper and reducer
        jc = ctx.getJobConf()

        jobconf = deprecation_utils.convert_job_conf(jc, self.DeprecationMap, self.logger)

        jc_configure(self, jobconf, 'seal.seqal.log.level', 'log_level', 'INFO')
        jc_configure(self, jobconf, "seal.seqal.fastq-subformat", "format", self.DEFAULT_FORMAT)
        jc_configure_int(self, jobconf, 'seal.seqal.alignment.max.isize', 'max_isize', 1000)
        jc_configure_int(self, jobconf, 'seal.seqal.alignment.min.isize', 'min_isize', None)
        jc_configure_int(self, jobconf, 'seal.seqal.pairing.batch.size', 'batch_size', 10000)
        jc_configure_int(self, jobconf, 'seal.seqal.min_hit_quality', 'min_hit_quality', 0)
        jc_configure_bool(self, jobconf, 'seal.seqal.remove_unmapped', 'remove_unmapped', False)
        jc_configure_int(self, jobconf, 'seal.seqal.nthreads', 'nthreads', 1)
        jc_configure_int(self, jobconf, 'seal.seqal.trim.qual', 'trim_qual', 0)

        try:
            self.log_level = getattr(logging, self.log_level)
        except AttributeError:
            raise ValueError("Unsupported log level: %r" % self.log_level)

        if self.format not in self.SUPPORTED_FORMATS:
            raise_pydoop_exception(
              "seal.seqal.fastq-subformat must be one of %r" %
              (self.SUPPORTED_FORMATS,)
              )

        if self.remove_unmapped:
            raise NotImplementedError("seal.seqal.remove_unmapped is currently unsupported")
        if self.min_hit_quality > 0:
            raise NotImplementedError("seal.seqal.min_hit_quality is currently unsupported")
        if self.trim_qual > 0:
            raise NotImplementedError("seal.seqal.trim_qual is currently unsupported")

        if self.max_isize <= 0:
            raise ValueError("'seal.seqal.alignment.max.isize' must be > 0, if specified [1000]")

        if self.batch_size <= 0:
            raise ValueError("'seal.seqal.pairing.batch.size' must be > 0, if specified [10000]")

        # minimum qual value required for a hit to be kept.  By default outputs all the
        # hits BWA returns.
        if self.min_hit_quality < 0:
            raise ValueError("'seal.seqal.min_hit_quality' must be >= 0, if specified [0]")

        # number of concurrent threads for main alignment operation
        if self.nthreads <= 0:
            raise ValueError("'seal.seqal.nthreads' must be > 0, if specified [1]")

        # trim quality parameter used by BWA from read trimming.  Equivalent to
        # the -q parameter for bwa align
        if self.trim_qual < 0:
            raise ValueError("'seal.seqal.trim.qual' must be >= 0, if specified [0]")

        if jc.hasKey('mapred.reduce.tasks') and jc.getInt('mapred.reduce.tasks') > 0:
            self.__map_only = False
        else:
            self.__map_only = True


    def get_reference_root_from_archive(self, ref_dir):
        """
        Given a directory containing an indexed reference,
        such that all its files have a common name (except the extension),
        this method find the path to the reference including the common name.
         e.g. my_reference/hg_18.bwt
              my_reference/hg_18.rsax
              my_reference/hg_18.sax   => "my_references/hg_18"
              my_reference/hg_18.pac
              my_reference/.irrelevant_file
        """
        file_list = [ p for p in os.listdir(ref_dir) ]

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("file_list extracted from reference archive: %s", file_list)

        filtered_file_list = [ p for p in file_list if not p.startswith('.') and os.path.splitext(p)[1].lstrip('.') in _BWA_INDEX_EXT ]
        prefix = os.path.commonprefix(filtered_file_list).rstrip('.')
        if not prefix:
            raise RuntimeError("Could not determine common prefix from list of files (%s)" %\
                    filtered_file_list if len(filtered_file_list) < 15 else "{}, ...".format(', '.join(filtered_file_list[0:15])))
        full_prefix = os.path.join(ref_dir, prefix)
        return full_prefix

    def __init__(self, ctx):
        super(mapper, self).__init__(ctx)
        self.logger = logging.getLogger("seqal")
        self.__get_configuration(ctx)
        logging.basicConfig(level=self.log_level)
        self.event_monitor = HadoopEventMonitor(self.COUNTER_CLASS, logging.getLogger("mapper"), ctx)

        pe = True # single-end sequencen alignment not yet supported by Seqal
        self.hi_rapi = HiRapiAligner('rapi_bwa', paired=pe)

        # opts
        self.hi_rapi.opts.n_threads = self.nthreads
        self.hi_rapi.opts.isize_max = self.max_isize
        if self.min_isize is not None:
            self.hi_rapi.opts.isize_min = self.min_isize
        self.hi_rapi.qoffset = self.hi_rapi.Qenc_Illumina if self.format == "fastq-illumina" else self.hi_rapi.Qenc_Sanger
        # end opts

        self.logger.info("Using the %s aligner plugin, aligner version %s, plugin version %s",
                self.hi_rapi.aligner_name, self.hi_rapi.aligner_version, self.hi_rapi.plugin_version)
        self.logger.info("Working in %s mode", 'paired-end' if pe else 'single-end')

        # allocate space for reads
        self.logger.debug("Reserving batch space for %s reads", self.batch_size)
        self.hi_rapi.reserve_space(self.batch_size) 

        # load reference
        reference_root = self.get_reference_root_from_archive(utils.get_ref_archive(ctx.getJobConf()))
        self.logger.info("Full reference path (prefix): %s", reference_root)
        with self.event_monitor.time_block("Loading reference %s" % reference_root):
            self.hi_rapi.load_ref(reference_root)

        ######## assemble hit processor chain
        chain = RapiFilterLink(self.event_monitor)
        chain.remove_unmapped = self.remove_unmapped
        chain.min_hit_quality = self.min_hit_quality
        if self.__map_only:
            chain.set_next( RapiEmitSamLink(ctx, self.event_monitor, self.hi_rapi) )
        else:
            raise NotImplementedError("Only mapping mode is supported at the moment")
        self.hit_visitor_chain = chain

    def _visit_hits(self):
        for read_tpl in self.hi_rapi.ifragments():
            self.hit_visitor_chain.process(read_tpl)

    def map(self, ctx):
        # Accumulates reads in self.pairs, until batch size is reached.
        # At that point it calls run_alignment and emits the output.
        v = ctx.value
        f_id, r1, q1, r2, q2 = v.split("\t")
        self.hi_rapi.load_pair(f_id, r1, q1, r2, q2)
        if self.hi_rapi.batch_size >= self.batch_size:
            self.hi_rapi.align_batch()
            self._visit_hits()
            self.hi_rapi.clear_batch()

    def close(self):
        # If there are any reads left in the aligner batch,
        # align them too
        if self.hi_rapi.batch_size > 0:
            self.hi_rapi.align_batch()
            self._visit_hits()
            self.hi_rapi.clear_batch()
        self.hi_rapi.release_resources()

