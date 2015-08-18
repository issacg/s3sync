var AWS = require('aws-sdk'),
    async = require('async'),
    config = require('./config'),
    _  = require('lodash'),
    logging = require('./logging'),
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
        params.ContentLength = src.keys[key].Size;
        dest.s3.s3.upload(params, cb);
    }
}

function deleteObject(dest, key, cb) {
    var destKey = destName(key, dest.bucketObj);
    logger.debug("Deleting [" +[dest.bucket, destKey].join("/") + "]");
    dest.s3.s3.deleteObject({Bucket: dest.bucket, Key: destKey}, cb);
}

function getSrc(bucket, cb) {
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
    var src = {s3:getS3Obj(bucket.srcregion),bucket:bucket.src,bucketObj:bucket};
    var dest = _.map(_.map(bucket.destregions, getS3Obj), function(s3) {return {s3:s3,bucket:bucket.dest + s3.suffix,bucketObj:bucket}});

    function getSrc(cb) {
        listObjects(bucket.src, bucket.srcprefix, src.s3, function(err, res) {
            src.keys = res;
            cb(err);
        });
    }
    function getDest(cb) {
        async.map(
            dest,
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

async.each(buckets, function(bucket, cb) {
    getSrc(bucket, function(err, toc) {
        if (err) return cb(err);
        async.each(toc.dest, async.apply(compareBucketRegions, toc.src), cb);
    });
}, function(err) {
    if (err) {
        logger.warn("Operation completed with errors");
        logger.error(err);
    } else {
        logger.info("Operation completed successfully");
    }
});
