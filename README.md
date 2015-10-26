# s3sync

## Basic Usage - The "full-sync" job

Edit config.js to set your AWS credentials and set up the source and destination buckets and paths.

In the supplied config, there are two syncs set up:

The first will use a bucket named "srcbucket" in region us-west-2 as the origin,
and synchronize the contents of "srcfolder" inside that bucket.  The destination
buckets will be "destbucket_prefix.usw2", "destbucket_prefix.euw1" and
"destbucket.cnn1" in us-west-2, eu-west-1 and cn-north-1 respectively.  The
cn-north-1 bucket will use a seperate set of AWS keys (any of the buckets could
use seperate keys if needed).  Further the files will contain the same directory
structure as the source beginning from "srcfolder/" except that they will be
written under "destfolder/" in the destination buckets.

The second job will use a bucket named "srcbucket2" in region us-east-1 (S3's
US-STANDARD region) as the origin, and synchronize the contents of "srcfolder2"
inside that bucket to "destfolder2" in buckets "destbucket_prefix.use1", also
located in us-east-1, and the same "destbucket_prefix.euw1" in eu-west-1 from the
first job.

## Advanced Usage

### fullsync-interval

In addition to the above example, you can set up s3sync to automatically run
a full sync job every *n* seconds by configuring the ```fullsync.interval``` config
option in config.js (or by passing it on the command line as
```--fullsync.interval=n``` ).

### daemon (SQS) mode

In some cases, it may be inconvenient or inefficient to use the normal *full-sync*
mode of operation.  For example, you may have writes which needed to be propogated
more often than the *full-sync* can run.  Or you can have so many items stored in
a bucket that the time-cost (or API cost) of running full-scans on the source
and destination buckets is prohibatively high.  As a final example, we can combine
the two scenarios above: you may have ephemeral data which can be deleted between
the time the bucket full-scan and the sync operations, causing Amazon S3 errors
when trying to copy non-existant files.

For any high-volume S3 setup, it may be more convenient to use a primarily
push-based solution.  Fortunately, Amazon supports writing S3 events to an SQS
queue, and s3sync can be configured to monitor such an SQS queue for
near-real-time synchronization between the source and destination buckets.

Before you can use SQS mode, you'll need to set up event reporting from S3 to
SQS as [documented here](http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html)

The configuration for running in SQS mode is identical to that of running in
*full-sync* mode, with an additional configuration section for the SQS information.

```js
s3sync:{
    regions: [
    // ...
    ],
    buckets: [
    // ...
    ],
    sqs: {
        url: 'https://sqs.us-west-2.amazonaws.com/xxxxxxxxxx/queue_name',
        region: 'us-west-2'
    }
}
```

```s3sync.sqs.url``` should be the HTTP endpoint for the SQS URL.
```s3sync.sqs.region``` should be the region that the SQS queue is created in.

### skip-fullsync

By default, no matter how it's run s3sync does a full-sync at startup.  To disable
this, set the ```skip-fullsync``` configuration option to ```true``` or pass it
on the command line as ```--skip-fullsync```

## Rationale

Why make another S3 sync tool?  AWS supports S3 replication as part of S3, and
s3tools already includes a sync script.

I had 3 use-casees that weren't covered by either of those solutions (so far)

Firstly, I needed to be able to synchronize between AWS and AWS-China, which as
yet can't share IAM privileges between the China and non-China regions, meaning
I'd need a tool which accepts multiple AWS keys, and doesn't rely on IAM or bucket
policies - meaning S3 built-in replication won't do.  s3tools similary won't support
this (unless I spool to a local disk by syncing account1 --> disk and then disk
--> account2) which I really didn't want to do.

Secondly, I wanted to be able to support multiple and distinct folder patterns in
the source and destinations, which also would have required s3tools to spool via
disk, and S3 doesn't support built-in.

Eventually I realized that the full-sync wasn't enough given the volume and traffic
in my S3 accounts and required SQS integration.  While that could have been done
in AWS Lambda, that would require maintaining the script and config in several
Lambda scripts per bucket, rather than a single always-on script to deal with
all buckets.

Thus s3sync was born.

## LICENSE

Copyright 2015 Issac Goldstand <issac@ironsrc.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.