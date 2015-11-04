gulp            = require 'gulp'
gutil           = require 'gulp-util'
coffeelint      = require 'gulp-coffeelint'
runSequence     = require 'run-sequence'

csv             = require 'csv'
coffee          = require 'gulp-coffee'
appnexus        = require 'anx-cli'
fs              = require 'fs'
moment          = require 'moment'
gzip            = require 'gulp-gzip'
glob            = require 'glob'
path            = require 'path'
mkdirp          = require 'mkdirp'
nop             = require 'gulp-nop'
debounce        = require 'debounce'
azure           = require 'azure-storage'

# CONFIG ---------------------------------------------------------

config =
  # path to upload files
  files: glob.sync '../up/*.anx'
  workbase: path.resolve '../up'
  isProd: gutil.env.type is 'prod'

  anx: require '../anx.json'
  today: moment(new Date())

gulp.task 'createUploadFile', (cb) ->
  
gulp.task 'compressFile', (cb) ->
  
gulp.task 'uploadAppNexus', (cb) ->
  

gulp.task 'default', (cb) ->
  runSequence 'createUploadFile', 'compressFile', 'uploadAppNexus', cb
