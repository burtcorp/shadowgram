# Shadowgram

Shadowgram snapshots your X-Ray history to S3.

## Usage

Shadowgram requires AWS credentials to run, and also needs to know which region to use when talking to the X-Ray API. Make sure that `AWS_REGION` is set, and that AWS SDK has access to credentials, either through environment variables or an EC2 metadata server.

Shadowgram will write the history to the S3 location given by the `HISTORY_BASE_URI` environment variable.

You can run Shadowgram from a checkout, like this:

```shell
$ bundle install
$ HISTORY_BASE_URI=s3://my-xray-history/data/ bundle exec bin/shadowgram collect-traces
```

or with Docker, like this:

```shell
$ docker run -it --rm -e AWS_REGION=us-east-2 -e HISTORY_BASE_URI=s3://my-xray-history/data/ burtcorp/shadowgram
```

## Development

You run the tests with:

```shell
$ bundle exec rake spec
```

# Copyright

© 2019 Burt AB, see LICENSE.txt (BSD 3-Clause).
