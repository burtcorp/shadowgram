require 'aws-sdk-xray'
require 'aws-sdk-s3'
require 'json'
require 'tmpdir'
require 'time'

module Shadowgram
  class TraceCollector
    MAX_BATCH_SIZE = 5

    def initialize(xray_client:, s3_client_factory:, history_base_uri:, window_size: 3, tmp_dir: nil, clock: nil, logger: nil)
      @xray_client = xray_client
      @s3_client_factory = s3_client_factory
      @history_base_uri = URI(history_base_uri)
      @window_size = window_size
      @tmp_dir = tmp_dir ||Dir.mktmpdir
      @clock = clock || Time
      @logger = logger || Logger.new(File.open(IO::NULL, 'w'))
    end

    def self.handler(event: nil, context: nil)
      xray_client = Aws::XRay::Client.new
      s3_client_factory = Aws::S3::Client
      logger = Logger.new($stderr)
      logger.level = Logger::DEBUG
      handler = new(
        xray_client: xray_client,
        s3_client_factory: s3_client_factory,
        history_base_uri: ENV['HISTORY_BASE_URI'],
        window_size: Integer(ENV.fetch('WINDOW_SIZE', 3)),
        logger: logger,
      )
      handler.collect_traces
    end

    def collect_traces
      now = @clock.at(@clock.now.to_i/3600 * 3600).utc
      @window_size.times do |n|
        start_time = now - ((n + 1) * 3600)
        end_time = now - (n * 3600)
        @logger.info(format('Loading summaries and segments for %s/P1H', start_time.iso8601))
        trace_count = 0
        segment_count = 0
        skipped_count = 0
        summary_batch = []
        summary_io = Zlib::GzipWriter.open(File.join(@tmp_dir, start_time.strftime('trace-summary-%Y%m%d%H.json.gz')))
        @logger.debug(format('Buffering trace summaries for %s/P1H in %s', start_time.iso8601, summary_io.path))
        segment_io = Zlib::GzipWriter.open(File.join(@tmp_dir, start_time.strftime('segment-%Y%m%d%H.json.gz')))
        @logger.debug(format('Buffering trace segments for %s/P1H in %s', start_time.iso8601, segment_io.path))
        @xray_client.get_trace_summaries(start_time: start_time, end_time: end_time).each do |response|
          response.trace_summaries.each do |trace_summary|
            if empty_trace?(trace_summary)
              skipped_count += 1
            else
              summary_io.puts(JSON.dump(trace_summary.to_h))
              trace_count += 1
              summary_batch << trace_summary
              if summary_batch.size == MAX_BATCH_SIZE
                segment_count += process_trace_batch(summary_batch.map(&:id), segment_io)
                summary_batch = []
              end
            end
          end
        end
        segment_count += process_trace_batch(summary_batch.map(&:id), segment_io)
        summary_io.close
        segment_io.close
        store_object(summary_io.path, 'summary', 'summaries', trace_count, start_time)
        store_object(segment_io.path, 'segment', 'segments', segment_count, start_time)
        if skipped_count > 0
          @logger.debug(format('Skipped %d empty trace summaries for %s/P1H', skipped_count, start_time.iso8601))
        end
      end
      nil
    end

    private def empty_trace?(trace_summary)
      trace_summary.duration.nil? && trace_summary.entry_point.nil?
    end

    private def process_trace_batch(trace_ids, segment_io)
      segment_count = 0
      response = @xray_client.batch_get_traces(trace_ids: trace_ids)
      response.traces.each do |trace|
        trace.segments.each do |segment|
          document = JSON.load(segment.document)
          document['type'] = 'segment'
          if (subsegments = document.delete('subsegments'))
            document['subsegments'] = subsegments.map { |s| s['id'] }
            segment_count += process_subsegments(document['trace_id'], [document['id']], subsegments, segment_io)
          end
          segment_io.puts(JSON.dump(document))
          segment_count += 1
        end
      end
      segment_count
    end

    private def process_subsegments(trace_id, parent_ids, subsegments, segment_io)
      subsegment_count = subsegments.size
      subsegments.each do |subsegment|
        subsegment['trace_id'] = trace_id
        subsegment['parent_id'] = parent_ids.last
        subsegment['parent_ids'] = parent_ids
        subsegment['type'] = 'subsegment'
        if (subsubsegments = subsegment.delete('subsegments'))
          subsegment['subsegments'] = subsubsegments.map { |s| s['id'] }
          subsegment_count += process_subsegments(trace_id, [*parent_ids, subsegment['id']], subsubsegments, segment_io)
        end
        segment_io.puts(JSON.dump(subsegment))
      end
      subsegment_count
    end

    private def history_s3_client
      @history_s3_client ||= create_s3_client(@history_base_uri.host)
    end

    private def create_s3_client(bucket)
      region = @s3_client_factory.new.get_bucket_location(bucket: bucket).location_constraint
      region = 'us-east-1' if region.empty?
      @logger.debug(format('Detected region of bucket %s as %s', bucket, region))
      @s3_client_factory.new(region: region)
    end

    private def store_object(data_path, type, pluralized_type, count, start_time)
      region = @xray_client.config.region
      bucket = @history_base_uri.host
      key = [@history_base_uri.path[1..-1].chomp('/'), type, region, start_time.strftime("%Y/%m/%d/%H/#{type}-%Y%m%d%H.json.gz")].join('/')
      @logger.debug(format('Storing %d trace %s for %s/P1H to s3://%s/%s', count, pluralized_type, start_time.iso8601, bucket, key))
      File.open(data_path, 'r') do |io|
        history_s3_client.put_object(bucket: bucket, key: key, body: io)
      end
      @logger.info(format('Stored trace %s to s3://%s/%s', pluralized_type, bucket, key))
    end
  end
end
