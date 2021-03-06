gulp            = require 'gulp'
gutil           = require 'gulp-util'
coffeelint      = require 'gulp-coffeelint'
runSequence     = require 'run-sequence'

csv             = require 'csv'
coffee          = require 'gulp-coffee'
appnexus        = require 'adnxs-api'
fs              = require 'fs'
moment          = require 'moment'
gzip            = require 'gulp-gzip'
glob            = require 'glob'
path            = require 'path'
mkdirp          = require 'mkdirp'
nop             = require 'gulp-nop'
debounce        = require 'debounce'
requestJson     = require 'request-json'
deployCdn       = require 'gulp-deploy-azure-cdn'
azure           = require 'azure-storage'
http            = require 'http'
request         = require 'request'
exec            = require('child_process').exec;

AnxClient       = appnexus.Client
AnxEndpoint     = appnexus.endpoints

# CONFIG ---------------------------------------------------------

config =
  # path to upload files
  workbase: path.resolve '../sandbox'
  isProd: gutil.env.type is 'prod'

  anx: require '../appnexus.json'
  azure: require '../azure.json'
  today: moment(new Date())
  count: 0

config.azure.container = 'archiveanx'

formatString = (s) =>
  return null unless s?

  result = (s + '').replace(/\W+/gi, '_')

  result.toLowerCase()

formatData = (data) =>
  return null unless data.AppNexusId?
  return null unless data.AppNexusId != '0'
  result = "#{data.AppNexusId},"
  isValid = false

  if (data.DepartmentId?)
    isValid = true
    result = "#{result};dacs#{formatString(data.DepartmentId)}"

  if (data.AisleId?)
    isValid = true
    result = "#{result};dacs#{formatString(data.AisleId)}"

  if (data.CategoryId?)
    isValid = true
    result = "#{result};dacs#{formatString(data.CategoryId)}"

  if (data.ShelfId?)
    isValid = true
    result = "#{result};dacs#{formatString(data.ShelfId)}"

  return null unless isValid

  result = result + '\n'
  result

# upload Blob
waitUntil = (cb, myAction) =>
  if cb()
    myAction()
  else
    setTimeout ->
      waitUntil(cb, myAction)
    , 500

getAnxUploadUrl = () =>
  gutil.log '>get AppNexus uploadUrl'

  url = if config.isProd then config.anx.url else config.anx.sandboxUrl
  client = new AnxClient(url)
  client
    .authorize(config.anx.user, config.anx.pass)
    .then (token) ->
      config.anx.token = token
      # upload to new url
      client.post(AnxEndpoint.BATCH_SEGMENT_SERVICE + '?member_id=' + config.anx.member_id)
        .then (rsp) ->
          config.anx.rsp = rsp
          config.anx.uploadUrl = rsp.batch_segment_upload_job.upload_url
        .catch (err) -> 
          gutil.log err
    .catch (err) ->
      gutil.log err

compressFile = () =>
  gutil.log '>count: ' + config.count
  gutil.log '>compressing: ' + config.outFile
  config.outFileZip = config.outFile + '.gz'
  gulp.src(config.outFile)
    .pipe(gzip({ append: true }))
    .pipe(gulp.dest(config.workbase))
    
uploadAppNexus = (cb) =>
  gutil.log 'Uploading anx: ' + config.outFileZip
  options = 
    url: config.anx.uploadUrl
    headers: 
      'Authorization': config.anx.token
      'Content-Encoding': 'text/*'
  outText = "curl -v -H \"Content-Type:application/octet-stream\" --data-binary \"@#{config.outFileZip}\" \"#{config.anx.uploadUrl}\""
  gutil.log outText
  child = exec(outText, (error, stdout, stderr) =>
    try 
      
      gutil.log 'stdout:' + stdout
      gutil.log 'stderr:' + stderr
      if (error)
        gutil.log 'exec error:' + error

    catch ex
  )

gulp.task 'createUploadFile', () =>
  mkdirp config.workbase
  config.filePath = config.today.format('YYYYMMDDHH') + '.anx'
  config.outFile = path.join config.workbase, config.filePath
  getAnxUploadUrl()

  # create upload file
  gutil.log '>contacting brick api'
  requestPath = 'api/v1/partner/GetItemsCreatedSince?minutesAgo=' + config.anx.timespan
  client = requestJson.createClient 'http://clientapix.gsn2.com/'
  client.get requestPath, (err, res, body) ->
    gutil.log '>creating file' + config.outFile
    outStream = fs.createWriteStream(config.outFile)
    for v, k in body
      data = formatData v
      if (data?)
        config.count++
        # gutil.log data
        outStream.write(data)

    outStream.end()
    gutil.log 'created ' + config.outFile
    compressFile()

gulp.task 'uploadToAnx', (cb) =>
  waitUntil () =>
    return false unless config.outFileZip
    exists = fs.existsSync(config.outFileZip) and config.anx.rsp
    gutil.log ">#{exists} #{config.outFileZip}"
    return exists
  , () =>
    uploadAppNexus(cb)

gulp.task 'default', (cb) =>
  runSequence 'createUploadFile', 'uploadToAnx', cb
