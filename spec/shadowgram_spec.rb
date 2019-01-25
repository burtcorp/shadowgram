module Shadowgram
  describe TraceCollector do
    subject :handler do
      described_class.new(
        xray_client: xray_client,
        s3_client_factory: s3_client_factory,
        history_base_uri: history_base_uri,
        window_size: 3,
        tmp_dir: tmp_dir,
        clock: clock,
        logger: logger,
      )
    end

    let :xray_client do
      Aws::XRay::Client.new(stub_responses: true).tap do |xc|
        allow(xc).to receive(:get_trace_summaries) do |parameters|
          summaries_for_hour = Array(trace_summaries[parameters[:start_time]])
          summaries_for_hour.each_slice(3).map do |ts|
            xc.stub_data(:get_trace_summaries, trace_summaries: ts)
          end
        end
        allow(xc).to receive(:batch_get_traces) do |parameters|
          traces = parameters[:trace_ids].map do |id|
            {segments: trace_segments[id].map { |s| {document: JSON.dump(s)} }}
          end
          xc.stub_data(:batch_get_traces, traces: traces)
        end
      end
    end

    let :s3_client_factory do
      class_double(Aws::S3::Client, new: s3_client)
    end

    let :s3_client do
      Aws::S3::Client.new(stub_responses: true).tap do |sc|
        allow(sc).to receive(:get_bucket_location) do |parameters|
          if parameters[:bucket] == URI(history_base_uri).host
            sc.stub_data(:get_bucket_location, location_constraint: 'hi-story-3')
          else
            sc.stub_data(:get_bucket_location, location_constraint: 'no-region-1')
          end
        end
        allow(sc).to receive(:put_object) do |parameters|
          if parameters[:bucket] == URI(history_base_uri).host
            stored_objects[parameters[:key]] = parameters[:body]
          end
        end
      end
    end

    let :history_base_uri do
      's3://xray-history/base/uri'
    end

    let :tmp_dir do
      Dir.mktmpdir
    end

    let :clock do
      class_double(Time).tap do |t|
        allow(t).to receive(:now).and_return(now)
        allow(t).to receive(:at, &Time.method(:at))
      end
    end

    let :now do
      Time.new(2019, 1, 18, 19, 20, 21, '-09:00')
    end

    let :logger do
      instance_double(Logger, debug: nil, info: nil, warn: nil)
    end

    let :trace_summaries do
      {
        Time.utc(2019, 1, 19, 3) => %w[11 10 9 8].map { |id| make_trace_summary(id) },
        Time.utc(2019, 1, 19, 2) => %w[7 6].map { |id| make_trace_summary(id) },
        Time.utc(2019, 1, 19, 1) => %w[5 4 3 2 1 0].map { |id| make_trace_summary(id) },
      }
    end

    let :trace_segments do
      {
        '0' => [{id: 'a', trace_id: '0'}],
        '1' => [{id: 'b', trace_id: '1'}],
        '2' => [{id: 'c', trace_id: '2'}],
        '3' => [{id: 'd', trace_id: '3'}],
        '4' => [{id: 'e', trace_id: '4'}],
        '5' => [{id: 'f', trace_id: '5'}],
        '6' => [{id: 'g', trace_id: '6'}],
        '7' => [{id: 'h', trace_id: '7'}, {id: 'i', trace_id: '7'}, {id: 'j', trace_id: '7'}],
        '8' => [{id: 'k', trace_id: '8'}],
        '9' => [{id: 'l', trace_id: '9'}, {id: 'm', trace_id: '9'}],
        '10' => [{id: 'n', trace_id: '10'}, {id: 'o', trace_id: '10'}, {id: 'p', trace_id: '10'}],
        '11' => [{id: 'q', trace_id: '11'}],
      }
    end

    let :stored_objects do
      {}
    end

    def make_trace_summary(trace_id)
      {id: trace_id, duration: rand * 10, entry_point: {name: 'something'}}
    end

    def make_empty_trace_summary(trace_id)
      {id: trace_id}
    end

    describe '.handle' do
      before do
        allow(logger).to receive(:level=)
        allow(Aws::XRay::Client).to receive(:new).and_return(xray_client)
        allow(Aws::S3::Client).to receive(:new).and_return(s3_client)
        allow(Logger).to receive(:new).and_return(logger)
      end

      before do
        ENV['HISTORY_BASE_URI'] = 's3://history/base/uri'
        ENV['WINDOW_SIZE'] = '5'
      end

      after do
        ENV.delete('HISTORY_BASE_URI')
        ENV.delete('WINDOW_SIZE')
      end

      it 'initializes the handler with its dependencies and calls #collect_traces' do
        described_class.handler(event: nil, context: nil)
        expect(xray_client).to have_received(:get_trace_summaries).at_least(:once)
      end

      it 'returns nil' do
        expect(described_class.handler(event: nil, context: nil)).to be_nil
      end

      it 'picks up the history URI from the environment' do
        described_class.handler(event: nil, context: nil)
        expect(s3_client).to have_received(:put_object).with(hash_including(bucket: 'history', key: including('base/uri/'))).at_least(:once)
      end

      it 'picks up the window size from the environment' do
        described_class.handler(event: nil, context: nil)
        expect(xray_client).to have_received(:get_trace_summaries).exactly(5).times
      end
    end

    describe '#collect_traces' do
      it 'logs when it starts processing the traces for an hour' do
        handler.collect_traces
        expect(logger).to have_received(:info).with('Loading summaries and segments for 2019-01-19T03:00:00Z/P1H')
        expect(logger).to have_received(:info).with('Loading summaries and segments for 2019-01-19T02:00:00Z/P1H')
        expect(logger).to have_received(:info).with('Loading summaries and segments for 2019-01-19T01:00:00Z/P1H')
      end

      it 'looks up the region of the history URI\'s bucket and creates an S3 client for that region' do
        handler.collect_traces
        expect(s3_client_factory).to have_received(:new).with(region: 'hi-story-3')
      end

      it 'logs the region it detects for the bucket' do
        handler.collect_traces
        expect(logger).to have_received(:debug).with('Detected region of bucket xray-history as hi-story-3')
      end

      it 'loads trace summaries for the last full hour' do
        handler.collect_traces
        expect(xray_client).to have_received(:get_trace_summaries).with(
          start_time: Time.utc(2019, 1, 19, 3),
          end_time: Time.utc(2019, 1, 19, 4),
        )
      end

      it 'loads the trace summaries for the number of hours given by the window size parameter' do
        handler.collect_traces
        expect(xray_client).to have_received(:get_trace_summaries).with(
          start_time: Time.utc(2019, 1, 19, 3),
          end_time: Time.utc(2019, 1, 19, 4),
        )
        expect(xray_client).to have_received(:get_trace_summaries).with(
          start_time: Time.utc(2019, 1, 19, 2),
          end_time: Time.utc(2019, 1, 19, 3),
        )
        expect(xray_client).to have_received(:get_trace_summaries).with(
          start_time: Time.utc(2019, 1, 19, 1),
          end_time: Time.utc(2019, 1, 19, 2),
        )
      end

      it 'loads the trace segments using the IDs from the trace summaries, in batches of five, delimited by the hours' do
        handler.collect_traces
        expect(xray_client).to have_received(:batch_get_traces).with(trace_ids: %w[11 10 9 8])
        expect(xray_client).to have_received(:batch_get_traces).with(trace_ids: %w[7 6])
        expect(xray_client).to have_received(:batch_get_traces).with(trace_ids: %w[5 4 3 2 1])
        expect(xray_client).to have_received(:batch_get_traces).with(trace_ids: %w[0])
      end

      it 'stores all the trace summaries for the same hour in a compressed JSON stream file' do
        handler.collect_traces
        expect(stored_objects.keys).to include(
          'base/uri/summary/us-stubbed-1/2019/01/19/03/summary-2019011903.json.gz',
          'base/uri/summary/us-stubbed-1/2019/01/19/02/summary-2019011902.json.gz',
          'base/uri/summary/us-stubbed-1/2019/01/19/01/summary-2019011901.json.gz',
        )
        stored_object = stored_objects['base/uri/summary/us-stubbed-1/2019/01/19/02/summary-2019011902.json.gz']
        summaries = Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line) }
        expect(summaries).to include(
          hash_including('id' => '6'),
          hash_including('id' => '7'),
        )
      end

      it 'adds a "region" property to the trace summaries' do
        handler.collect_traces
        stored_object = stored_objects['base/uri/summary/us-stubbed-1/2019/01/19/02/summary-2019011902.json.gz']
        summaries = Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line) }
        expect(summaries).to all(include('region' => 'us-stubbed-1'))
      end

      it 'stores all the trace segments for the same hour in a compressed JSON stream file' do
        handler.collect_traces
        expect(stored_objects.keys).to include(
          'base/uri/segment/us-stubbed-1/2019/01/19/03/segment-2019011903.json.gz',
          'base/uri/segment/us-stubbed-1/2019/01/19/02/segment-2019011902.json.gz',
          'base/uri/segment/us-stubbed-1/2019/01/19/01/segment-2019011901.json.gz',
        )
        stored_object = stored_objects['base/uri/segment/us-stubbed-1/2019/01/19/02/segment-2019011902.json.gz']
        segment_ids = Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line)['id'] }
        expect(segment_ids).to contain_exactly(*%w[g h i j])
      end

      it 'adds a "type" property to the trace segments' do
        handler.collect_traces
        stored_object = stored_objects['base/uri/segment/us-stubbed-1/2019/01/19/02/segment-2019011902.json.gz']
        segments = Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line) }
        expect(segments).to contain_exactly(
          hash_including('id' => 'g', 'type' => 'segment'),
          hash_including('id' => 'h', 'type' => 'segment'),
          hash_including('id' => 'i', 'type' => 'segment'),
          hash_including('id' => 'j', 'type' => 'segment'),
        )
      end

      it 'adds a "region" property to the trace segments' do
        handler.collect_traces
        handler.collect_traces
        stored_object = stored_objects['base/uri/segment/us-stubbed-1/2019/01/19/02/segment-2019011902.json.gz']
        segments = Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line) }
        expect(segments).to contain_exactly(
          hash_including('id' => 'g', 'region' => 'us-stubbed-1'),
          hash_including('id' => 'h', 'region' => 'us-stubbed-1'),
          hash_including('id' => 'i', 'region' => 'us-stubbed-1'),
          hash_including('id' => 'j', 'region' => 'us-stubbed-1'),
        )
      end

      it 'logs the temporary location it uses for buffering data' do
        handler.collect_traces
        expect(logger).to have_received(:debug).with(including(tmp_dir)).at_least(:once)
      end

      it 'logs when it stores an object to S3' do
        handler.collect_traces
        expect(logger).to have_received(:debug).with('Storing 4 trace summaries for 2019-01-19T03:00:00Z/P1H to s3://xray-history/base/uri/summary/us-stubbed-1/2019/01/19/03/summary-2019011903.json.gz')
        expect(logger).to have_received(:debug).with('Storing 7 trace segments for 2019-01-19T03:00:00Z/P1H to s3://xray-history/base/uri/segment/us-stubbed-1/2019/01/19/03/segment-2019011903.json.gz')
        expect(logger).to have_received(:info).with('Stored trace summaries to s3://xray-history/base/uri/summary/us-stubbed-1/2019/01/19/03/summary-2019011903.json.gz')
        expect(logger).to have_received(:info).with('Stored trace segments to s3://xray-history/base/uri/segment/us-stubbed-1/2019/01/19/03/segment-2019011903.json.gz')
      end

      context 'when a trace summary is empty' do
        let :trace_summaries do
          super().tap do |ts|
            ts[ts.keys[1]] << make_empty_trace_summary('6b')
          end
        end

        let :trace_segments do
          super().tap do |ts|
            ts['6b'] = []
          end
        end

        it 'skips the trace summary' do
          handler.collect_traces
          stored_object = stored_objects['base/uri/summary/us-stubbed-1/2019/01/19/02/summary-2019011902.json.gz']
          summaries = Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line) }
          expect(summaries).to_not include(hash_including('id' => '6b'))
          expect(xray_client).to_not have_received(:batch_get_traces).with(trace_ids: include('6b'))
        end

        it 'logs the number of skipped trace summaries' do
          handler.collect_traces
          expect(logger).to have_received(:debug).with('Skipped 1 empty trace summaries for 2019-01-19T02:00:00Z/P1H')
          expect(logger).to_not have_received(:debug).with('Skipped 0 empty trace summaries for 2019-01-19T03:00:00Z/P1H')
        end
      end

      context 'when a segment has subsegments' do
        let :trace_segments do
          super().tap do |ts|
            ts['6'][0] = {id: 'g', trace_id: '6', subsegments: [{id: 'g1'}, {id: 'g2'}]}
          end
        end

        let :stored_segments do
          stored_object = stored_objects['base/uri/segment/us-stubbed-1/2019/01/19/02/segment-2019011902.json.gz']
          Zlib::GzipReader.new(File.open(stored_object.path)).each_line.map { |line| JSON.load(line) }
        end

        it 'stores the subsegments as their own entries' do
          handler.collect_traces
          expect(stored_segments.map { |s| s['id'] }).to contain_exactly(*%w[g g1 g2 h i j])
        end

        it 'adds a "parent_id" property with the ID of the parent segment' do
          handler.collect_traces
          expect(stored_segments.map { |s| s['parent_id'] }).to contain_exactly('g', 'g', nil, nil, nil, nil)
        end

        it 'adds a "parent_ids" property with the ID of the parent segment' do
          handler.collect_traces
          expect(stored_segments.map { |s| s['parent_ids'] }).to contain_exactly(%w[g], %w[g], nil, nil, nil, nil)
        end

        it 'adds a "trace_id" property with the ID of the trace' do
          handler.collect_traces
          expect(stored_segments.map { |s| s['trace_id'] }).to contain_exactly('6', '6', '6', '7', '7', '7')
        end

        it 'sets the "type" property to "subsegment"' do
          handler.collect_traces
          expect(stored_segments.select { |s| s['parent_id'] == 'g' }).to all(include('type' => 'subsegment'))
        end

        it 'replaces the "subsegments" property with a list of the IDs of the subsegments' do
          handler.collect_traces
          expect(stored_segments.find { |s| s['id'] == 'g' }).to include('subsegments' => %w[g1 g2])
        end

        it 'logs the number of segments including subsegments' do
          handler.collect_traces
          expect(logger).to have_received(:debug).with('Storing 6 trace segments for 2019-01-19T02:00:00Z/P1H to s3://xray-history/base/uri/segment/us-stubbed-1/2019/01/19/02/segment-2019011902.json.gz')
        end

        context 'and a subsegment has its own subsegments' do
          let :trace_segments do
            super().tap do |ts|
              ts['6'][0] = {
                id: 'g',
                trace_id: '6',
                subsegments: [
                  {
                    id: 'g1',
                    subsegments: [
                      {id: 'g1a'},
                      {id: 'g1b'},
                    ],
                  },
                  {
                    id: 'g2',
                    subsegments: [
                      {id: 'g2a'},
                      {id: 'g2b'},
                      {id: 'g2c'},
                    ],
                  },
                ],
              }
            end
          end

          it 'stores the subsegments of the subsegments as their own entries and adds parent IDs' do
            handler.collect_traces
            expect(stored_segments).to contain_exactly(
              hash_including('id' => 'g', 'subsegments' => %w[g1 g2], 'type' => 'segment', 'trace_id' => '6'),
              hash_including('id' => 'g1', 'parent_id' => 'g', 'parent_ids' => %w[g], 'subsegments' => %w[g1a g1b], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'g1a', 'parent_id' => 'g1', 'parent_ids' => %w[g g1], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'g1b', 'parent_id' => 'g1', 'parent_ids' => %w[g g1], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'g2', 'parent_id' => 'g', 'parent_ids' => %w[g], 'subsegments' => %w[g2a g2b g2c], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'g2a', 'parent_id' => 'g2', 'parent_ids' => %w[g g2], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'g2b', 'parent_id' => 'g2', 'parent_ids' => %w[g g2], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'g2c', 'parent_id' => 'g2', 'parent_ids' => %w[g g2], 'type' => 'subsegment', 'trace_id' => '6'),
              hash_including('id' => 'h', 'type' => 'segment', 'trace_id' => '7'),
              hash_including('id' => 'i', 'type' => 'segment', 'trace_id' => '7'),
              hash_including('id' => 'j', 'type' => 'segment', 'trace_id' => '7'),
            )
          end

          context 'and a subsegment of a subsegment of a subsegment of a subsegment has its own subsegments' do
            let :trace_segments do
              super().tap do |ts|
                ts['6'][0] = {
                  id: 'g', trace_id: '6', subsegments: [
                    {id: 'g1', subsegments: [
                      {id: 'g1a', subsegments: [
                        {id: 'g1a1', subsegments: [
                          {id: 'g1a1a', subsegments: [
                            {id: 'g1a1a1', subsegments: [
                              {id: 'g1a1a1a'},
                              {id: 'g1a1a1b'},
                            ]},
                          ]},
                        ]},
                      ]},
                    ]},
                  ],
                }
              end
            end

            it 'stores all levels as their own entries and adds parent IDs' do
              handler.collect_traces
              expect(stored_segments).to contain_exactly(
                hash_including('id' => 'g', 'subsegments' => %w[g1], 'type' => 'segment', 'trace_id' => '6'),
                hash_including('id' => 'g1', 'parent_id' => 'g', 'parent_ids' => %w[g], 'subsegments' => %w[g1a], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'g1a', 'parent_id' => 'g1', 'parent_ids' => %w[g g1], 'subsegments' => %w[g1a1], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'g1a1', 'parent_id' => 'g1a', 'parent_ids' => %w[g g1 g1a], 'subsegments' => %w[g1a1a], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'g1a1a', 'parent_id' => 'g1a1', 'parent_ids' => %w[g g1 g1a g1a1], 'subsegments' => %w[g1a1a1], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'g1a1a1', 'parent_id' => 'g1a1a', 'parent_ids' => %w[g g1 g1a g1a1 g1a1a], 'subsegments' => %w[g1a1a1a g1a1a1b], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'g1a1a1a', 'parent_id' => 'g1a1a1', 'parent_ids' => %w[g g1 g1a g1a1 g1a1a g1a1a1], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'g1a1a1b', 'parent_id' => 'g1a1a1', 'parent_ids' => %w[g g1 g1a g1a1 g1a1a g1a1a1], 'type' => 'subsegment', 'trace_id' => '6'),
                hash_including('id' => 'h', 'type' => 'segment', 'trace_id' => '7'),
                hash_including('id' => 'i', 'type' => 'segment', 'trace_id' => '7'),
                hash_including('id' => 'j', 'type' => 'segment', 'trace_id' => '7'),
              )
            end
          end
        end
      end

      context 'when a bucket is located in us-east-1' do
        let :s3_client do
          super().tap do |sc|
            allow(sc).to receive(:get_bucket_location) do |parameters|
              sc.stub_data(:get_bucket_location, location_constraint: '')
            end
          end
        end

        it 'converts the empty location constraint returned by GetBucketLocation to "us-east-1"' do
          handler.collect_traces
          expect(s3_client_factory).to have_received(:new).with(region: 'us-east-1')
        end
      end
    end
  end
end
