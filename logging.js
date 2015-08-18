var log4js = require('log4js');
var config = require('./config');

var getLogger = function(name, level) {
    if (!name) name = __filename;
    var logger = log4js.getLogger(name);
    if (level && log4js.levels.hasOwnProperty(level))
        logger.setLevel(level);
    return logger;
};


log4js.configure(config.log4js);
log4js.setGlobalLogLevel(config.log4js.level);
logger = getLogger('Logging');
logger.debug("Configured log4js with default level: " + config.log4js.level);

module.exports.getLogger = getLogger;