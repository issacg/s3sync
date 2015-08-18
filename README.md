# s3sync

## Usage

Edit config.js to set your AWS credentials and set up the source and destination buckets and paths.

## Rationale

Why make another S3 sync tool?  AWS supports S3 replication as part of S3, and
s3tools already includes a sync script.

I had 2 use-casees that weren't covered by either of those solutions (so far)

Firstly, I needed to be able to synchronize between AWS and AWS-China, which as
yet can't share IAM privileges between the China and non-China regions, meaning
I'd need a tool which accepts multiple AWS keys, and doesn't rely on IAM or bucket
policies - meaning S3 built-in replication won't do.  s3tools similary won't support
this (unless I spool to a local disk by syncing account1 --> disk and then disk
--> account2) which I really didn't want to do.

Secondly, I wanted to be able to support multiple and distinct folder patterns in
the source and destinations, which also would have required s3tools to spool via
disk, and S3 doesn't support built-in.

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