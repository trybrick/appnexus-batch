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

AnxClient       = appnexus.Client
AnxEndpoint     = appnexus.endpoints

# CONFIG ---------------------------------------------------------

config =
  # path to upload files
  workbase: path.resolve '../sandbox'
  isProd: gutil.env.type is 'prod'
  uploadSuccess: true

  anx: require '../appnexus.json'
  azure: require '../azure.json'
  today: moment(new Date())

config.azure.container = 'archiveanx'

formatString = (s, prefixChar) =>
  return null unless s?

  result = s.replace(/\W+/gi, '_')

  if (prefixChar)
    result = "#{prefixChar}_#{result}"

  result.toLowerCase()

formatData = (data) =>
  return null unless data.AppNexusId?
  return null unless data.AppNexusId != '0'
  result = "#{data.AppNexusId},site_#{formatString(data.SiteName)};store_#{formatString(data.StoreName)}"
  isValid = false

  if (data.Department?)
    isValid = true
    result = "#{result};dept_#{formatString(data.Department)}"

  if (data.Aisle?)
    isValid = true
    result = "#{result};aisle_#{formatString(data.Aisle)}"

  if (data.Category?)
    isValid = true
    result = "#{result};cat_#{formatString(data.Category)}"

  if (data.Shelf?)
    isValid = true
    result = "#{result};shelf_#{formatString(data.Shelf)}"

  if (data.BrandName?)
    isValid = true
    result = "#{result};brand_#{formatString(data.BrandName)}"

  return null unless isValid

  result = result + '\n'
  result

# upload Blob
uploadBlob = () =>
  mySources = [config.outFile]
  gutil.log 'uploading file: ' + config.outFile
  return gulp.src(mySources).pipe(deployCdn({
      containerName: config.azure.container,
      serviceOptions: [config.azure.account, config.azure.key], 
      containerOptions: {},
      folder: config.today.format('YYYYMM/DD'), 
      zip: true,
      deleteExistingBlobs: true, 
      metadata: {
          cacheControl: 'public, max-age=31530000', 
          cacheControlHeader: 'public, max-age=31530000' 
      },
      concurrentUploadThreads: 2,
      testRun: !config.isProd 
  })).on('error', (err) ->
    gutil.log err
    config.uploadSuccess = false
  );

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
  gutil.log '>compressing: ' + config.outFile
  config.outFileZip = config.outFile + '.gz'
  gulp.src(config.outFile)
    .pipe(gzip({ append: true }))
    .pipe(gulp.dest(config.workbase))

uploadAppNexus = () =>
  gutil.log 'Uploading anx: ' + config.outFileZip
  options = 
    url: config.anx.uploadUrl
    headers: 
      Authorization: config.anx.token
  
  fs.createReadStream(config.outFileZip)
    .pipe request.post options, (err, resp, body) ->

      gutil.log '>uploaded anx: ' + config.outFileZip

      if err
        gutil.log err

      config.anx.uploadResult = {
        err: err
        resp: resp
        body: body
      }

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
        # gutil.log data
        outStream.write(data)
    outStream.end()
    gutil.log 'created ' + config.outFile
    compressFile()
    uploadBlob()

gulp.task 'insertQueue', (cb) =>

  return unless config.uploadSuccess
  myTimeout = 15000
  gutil.log '>sleep ' + myTimeout
  setTimeout( () ->
    gutil.log '>wake ' + myTimeout
    uploadAppNexus()
    queueService = azure.createQueueService(config.azure.account, config.azure.key)
    queueName = config.azure.queueName ? 'highpriority'
    queueService.createQueueIfNotExists( queueName, (err1) ->
      if (err1)
        gutil.log err1
        cb()
      else
        myTimeout = myTimeout * 1
        gutil.log '>sleep2 ' + myTimeout
        setTimeout( () ->
          gutil.log '>wake2 ' + myTimeout
          # queue exists
          msg = {
            "qtype" : "appnexus-segment-upload",
            "filename" : config.filePath
            "foldername" : config.today.format('YYYYMM/DD')
            "container" : config.azure.container
            "fullpath": null,
            "token": config.anx.token,
            "uploadUrl": config.anx.uploadUrl
            "uploadResult": config.anx.uploadResult
          }
          gutil.log '> queue: '
          gutil.log msg

          queueService.createMessage queueName, JSON.stringify(msg, null), (err2) ->
            if (err2)
              gutil.log err2
            else
              gutil.log '>insert queue success'
            cb()

        myTimeout)
      )
    myTimeout)

gulp.task 'default', (cb) =>
  runSequence 'createUploadFile', 'insertQueue', cb
