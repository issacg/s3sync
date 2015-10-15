var AWS = require('aws-sdk'),
    async = require('async'),
    config = require('./config'),
    _  = require('lodash'),
    logging = require('./logging'),
    moment = require('moment'),
    sqs = require('sqs-consumer'),
    logger = logging.getLogger('s3sync');

AWS.config.update(config.aws);

var MAX_COPY_SIZE = 5 * 1024 * 1024 * 1024; // 5GB

var getAWSLogger = _.memoize(function (category) {
    var logger = logging.getLogger(category);
    return {log:function(o) {logger.trace(o)}}
});

var commonParams = {
    ServerSideEncryption: "AES256",
    StorageClass: "REDUCED_REDUNDANCY"
};

var regions = config.s3sync.regions,
    buckets = config.s3sync.buckets;

var getS3Obj = function (region) {
    var regionObj = _.find(regions,{'region':region});
    if (!regionObj) throw(new Error("Invalid region " + region));
    var params = {apiVersion: '2012-06-01', region: regionObj.region, sslEnabled: true, logger:getAWSLogger('AWS-'+regionObj.region)};
    regionObj.account = 'BASE';
    if (regionObj.access) {
        regionObj.account = params.accessKeyId = regionObj.access;
        params.secretAccessKey = regionObj.secret;
    }
    regionObj.s3 = new AWS.S3(params);
    return regionObj;
};

function destName(key, bucket) {
    return key.replace(bucket.srcprefix, bucket.destprefix);
}

function listObjects(bucket,prefix,s3,cb) {
    logger.debug("Scanning bucket [" + [bucket,prefix].join("/") + "]");
    var params = {
        Bucket: bucket,
        Prefix: prefix
    };
    var res = {};
    var more = true;
    var icount = 1;
    function getBatch(cb) {
        s3.s3.listObjects(params, function(err, data) {
            if (err) return cb(err);
            more = data.IsTruncated;
            _.each(data.Contents, function(obj) {
                res[obj.Key] = {Key:obj.Key,LastModified:obj.LastModified,ETag:obj.ETag,Size:obj.Size};
            });
            if (more) {
                params.Marker = _.last(data.Contents)['Key'];
            }
            cb();
        });
    };
    async.whilst(
        function(){return more},
        getBatch,
        function(err) {
            if (err) return cb(err);
            cb(null, res);
        }
    );
}

function touchObject(dest, key, cb) {
    var destKey = destName(key, dest.bucketObj);
    logger.debug("Touching [" +[dest.bucket, destKey].join("/") + "]");
    var params = _.clone(commonParams);
    params.CopySource = encodeURIComponent([dest.bucket, destKey].join("/"));
    params.Bucket = dest.bucket;
    params.Key = destKey;
    params.MetadataDirective = "REPLACE";
    dest.s3.s3.copyObject(params, cb);
}

function copyObject(src, dest, key, cb) {
    var express = (src.s3.account == dest.s3.account && src.keys[key].Size <= MAX_COPY_SIZE);
    var destKey = destName(key, dest.bucketObj);
    logger.debug((express ? "Express" : "Slow") + " copying [" + [[src.bucket, key].join("/"),[dest.bucket, destKey].join("/")].join(" --> ") + "]");
    if (express) {
        var params = _.clone(commonParams);
        params.CopySource = encodeURIComponent([src.bucket, key].join("/"));
        params.Bucket = dest.bucket;
        params.Key = destKey;
        params.MetadataDirective = "COPY";
        dest.s3.s3.copyObject(params, cb);
    } else {
        var params = _.clone(commonParams);
        params.Bucket = dest.bucket;
        params.Key = destKey;
        params.Body = src.s3.s3.getObject({Bucket: src.bucket, Key: key}).createReadStream();
        params.ContentLength = src.keys[key].Size; // What happens if the file is updated between the listObjects and here?
        dest.s3.s3.upload(params, cb);
    }
}

function deleteObject(dest, key, cb) {
    var destKey = destName(key, dest.bucketObj);
    logger.debug("Deleting [" +[dest.bucket, destKey].join("/") + "]");
    dest.s3.s3.deleteObject({Bucket: dest.bucket, Key: destKey}, cb);
}

function syncJobObject(bucket) {
    // This makes an object looking like this:
    //{
    //    src: {
    //        s3: s3Object
    //        bucket: bucketName
    //        bucketObj: bucketObj
    //        keys: listOfObjects
    //    },
    //    dest: [
    //        {
    //            s3: s3Object
    //            bucket: bucketname
    //            bucketObj: bucketObj
    //            keys: listOfObjects
    //        }, ...
    //    ]
    //}
    return {
        src:{s3:getS3Obj(bucket.srcregion),bucket:bucket.src,bucketObj:bucket,keys:[]},
        dest : _.map(_.map(bucket.destregions, getS3Obj), function(s3) {return {s3:s3,bucket:bucket.dest + s3.suffix,bucketObj:bucket,keys:[]}})
    }
}

function getSrc(bucket, cb) {
    var obj = syncJobObject(bucket);
    function getSrc(cb) {
        listObjects(bucket.src, bucket.srcprefix, src.s3, function(err, res) {
            obj.src.keys = res;
            cb(err);
        });
    }
    function getDest(cb) {
        async.map(
            obj.dest,
            function(region, cb) {
                listObjects(bucket.dest + region.s3.suffix, bucket.destprefix, region.s3, function(err, res) {
                    region.keys = res;
                    cb(err);
                });
            },
            cb
        );
    }
    async.parallel([getSrc,getDest], function(err) {
        cb(err, {src:src,dest:dest});
    });
}

function compareBucketRegions(src, dest, cb) {
    // AWS should give us basically unlimited concurent requests, but each of these closures costs RAM.  This seems the most effective chokehold to stop leaks
    function copyForward(cb) {
        async.eachLimit(Object.keys(src.keys), 100, async.ensureAsync(function(key, cb) {
            var skey = src.keys[key];
            var dkey = (dest.keys.hasOwnProperty(key) ? dest.keys[key] : undefined);
            if (!dkey) {
                // Exists in src, but not dest.  Copy
                return copyObject(src, dest, key, cb);
            }
            if (skey.LastModified.getTime() <= dkey.LastModified.getTime()) {
                // Files are the same.  No action.
                return cb();
            }
            if (skey.ETag == dkey.ETag) {
                // Files are the same, but date mismatches.  Copy metaobject
                return touchObject(dest, key, cb);
            }
            // Files differ.  Copy.
            return copyObject(src, dest, key, cb);
        }), cb);
    }
    function cleanUp(cb) {
        async.eachLimit(Object.keys(dest.keys), 50, async.ensureAsync(function(key, cb) {
            var skey = (src.keys.hasOwnProperty(key) ? src.keys[key] : undefined);
            if (!skey) {
                // Deleted in src.  Remove from dest.
                return deleteObject(dest, key, cb);
            }
            // Exists in src. No action.
            return cb();
        }), cb);
    }
    logger.info("Syncing [" + src.bucket + " --> " + dest.bucket + "]");
    async.parallel([copyForward, cleanUp], cb);
}
//enum src (listObjects)
//enum dest

//foreach destregion
//p1
//foreach file in src
  // if timestamps differ
     // if content differs
        // copy file (copyObject or getObject+putObject)?
     // else
        // touch dest (copyObject using dest as source+destination and REPLACE mode)

//p2
//foreach file in dest
  // if doesn't exist in src
     // deleteobject

function fullSync(cb) {
    var starttime = new moment();
    logger.info("Starting");
    async.each(buckets, function(bucket, cb) {
        getSrc(bucket, function(err, toc) {
            if (err) return cb(err);
            logger.info("" + bucket.src + "/" + bucket.srcprefix +  " contains " + Object.keys(toc.src.keys).length + " keys");
            toc.dest.forEach(function(dest) {
                logger.info("" + bucket.dest + dest.s3.suffix  + "/" + bucket.destprefix + " contains " + Object.keys(dest.keys).length + " keys");
            });
            async.each(toc.dest, async.apply(compareBucketRegions, toc.src), cb);
        });
    }, function(err) {
        var endtime = new moment();
        logger.info("Operation took " + moment.duration(endtime - starttime).humanize() + " to complete");
        if (err) {
            logger.warn("Operation completed with errors");
            logger.error(err);
            cb(err);
        } else {
            logger.info("Operation completed successfully");
            cb();
        }
    });
}

function handleMessage(message, done) {
    // Parse & validate the event
    if (!(message && message.Body)) return done (new Error("missing message body: " + message));
    var body;
    try {
        body = JSON.parse(message.Body);
    } catch(e) {
        return done(e);
    }

    if (body && body.Event && body.Event == "s3:TestEvent") {
        logger.trace("Got S3 test event from Amazon.  Ignoring.");
        return done();
    }

    if (!(body.Records && body.Records.length && body.Records.length == 1 &&
          body.Records[0].eventSource == "aws:s3" && body.Records[0].s3 &&
          body.Records[0].awsRegion && body.Records[0].eventName)) return done (new Error("invalid or missing s3 message: " + message.Body));

    // Find a matching action or ignore.  We know how to deal with all actions, so error if unknown
    var action;

    if (_.startsWith(body.Records[0].eventName,"ObjectCreated"))
        action = "ObjectCreated"
    else if (_.startsWith(body.Records[0].eventName,"ObjectRemoved"))
        action = "ObjectRemoved"
    else if (_.startsWith(body.Records[0].eventName,"ReducedRedundancyLostObject"))
        action = "ReducedRedundancyLostObject"

    if (!action) {
        done("Unknown action: " + body.Records[0].eventName)
    }

    // Find a matching config rule, or ignore if none matched
    var rule;
    if (action == "ObjectCreated" || action == "ObjectRemoved") {
        var rule = _.find(config.s3sync.buckets, function(obj) {
            return ((obj.src == body.Records[0].s3.bucket.name) &&
                    (_.startsWith(body.Records[0].s3.object.key, obj.srcprefix)))
            });
    } else {
        // TODO - generate a rule for ReducedRedundancyLostObject
    }

    if (!rule) {
        logger.trace("No rule found.  Ignoring " + action + " event for " + body.Records[0].s3.object.key);
        return done();
    }

    // Deal with the event
    if (action == "ObjectCreated") {
        var obj = syncJobObject(rule);
        obj.src.keys[body.Records[0].s3.object.key] = {Size: body.Records[0].s3.object.size};
        async.each(obj.dest, function(dest, cb) {
            copyObject(obj.src, dest, body.Records[0].s3.object.key, cb);
        }, done);
        return;
    }

    if (action == "ObjectRemoved") {
        var obj = syncJobObject(rule);
        async.each(obj.dest, function(dest, cb) {
            deleteObject(dest, body.Records[0].s3.object.key, cb);
        }, done);
        return;
    }

    done(new Error("Missing action"));
}

function sqsSync(cb) {
    var q = sqs.create({
        queueUrl: config.s3sync.sqs.url,
        region: config.s3sync.sqs.region,
        handleMessage: handleMessage,
        batchSize:10
    });
    q.start();
    q.on("error", function(er) {
        logger.error("caught " + er);
    });
    function stop() {
        logger.info("Shutting down (this may take up to 30 seconds)...");
        q.stop();
        cb();
    }
    //process.on('SIGINT', stop);
    //process.on('SIGHUP', stop);
}