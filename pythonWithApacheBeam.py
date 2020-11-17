from __future__ import absolute_import
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import argparse
from apache_beam.io import WriteToText, ReadFromText


class SplitRecords(beam.DoFn):
    """Spilt the element into records, return rideable_type record."""
    def process(self, element):
        records = element.split(",")
        return [records[1]]

argv = None

parser = argparse.ArgumentParser()
parser.add_argument(
  '--input',
  dest='input',
  default='test.txt',
  help='Input file(s) to process.')
parser.add_argument(
  '--output',
  dest='output',
  required=True,
  help='Output file to write results to.')

opts, pipeline_args = parser.parse_known_args(argv)

pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(SetupOptions).save_main_session = True

p = beam.Pipeline(options=pipeline_options)
lines = (p | ReadFromText(opts.input, skip_header_lines=1))
records = (lines | beam.ParDo(SplitRecords()))
groups = (records | beam.Map(lambda x: (x, 1)) | beam.CombinePerKey(sum))
groups | 'Write' >> WriteToText(opts.output)
p.run()
