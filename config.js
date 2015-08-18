var rc = require("rc");

var defaults = {
    aws: {
        accessKeyId: "",
        secretAccessKey: ""
    },

    log4js: {
        "level": "INFO",
        "replaceConsole": true,
        "appenders": [
            {"type": "console"}
        ]
    },

    s3sync:{
        regions: [
            {region:'us-east-1',suffix:'.use1'},
            {region:'us-west-2',suffix:'.usw2'},
            {region:'eu-west-1',suffix:'.euw1'},
            {region:'cn-north-1',suffix:'.cnn1',access:'',secret:''}
        ],
        buckets: [
            {src:'srcbucket',srcprefix:'srcfolder/',srcregion:'us-west-2',dest:'destbucket_prefix',destprefix:'destfolder/',destregions:["us-west-2","eu-west-1","cn-north-1"]},
            {src:'srcbucket2',srcprefix:'srcfolder2/',srcregion:'us-east-1',dest:'destbucket_prefix',destprefix:'destfolder2/',destregions:["us-east-1","eu-west-1"]},
        ]
    }

};

module.exports = rc("s3sync", defaults);
