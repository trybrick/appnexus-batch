gulp            = require 'gulp'
gutil           = require 'gulp-util'
coffeelint      = require 'gulp-coffeelint'
runSequence     = require 'run-sequence'

csv             = require 'csv'
coffee          = require 'gulp-coffee'
deployCdn       = require 'gulp-deploy-azure-cdn'
fs              = require 'fs'
moment          = require 'moment'
azureTables     = require 'azure-table-client'
glob            = require 'glob'
path            = require 'path'
mkdirp          = require 'mkdirp'
nop             = require 'gulp-nop'
createBatchRequestStream           = require 'batch-request-stream'

# CONFIG ---------------------------------------------------------

config =
  chains: 
    127: ["StoreNumber", "PurchaseDateRaw", "Quantity", "UPC", "PurchasePrice", "ExternalId"]
    182: ["StoreNumber", "UPC", "PurchasePrice",  "PurchaseDateRaw", "ExternalId", "Quantity", "Weight"]
    215: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]
    216: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]
    217: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]
    218: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]

  # path to pos files
  files: glob.sync './*.hif'
  workbase: path.resolve './test'
  isProd: gutil.env.type is 'prod'

  azure: require './azure.json'
  azureConfig: 
    batchSize: 100
    concurrency: 100   # batchSize * concurrency = 10K which is half of 20K message azure limit
  today: moment(new Date())
  batchCount: 0

  # resulting output schema
  output: [
    "ChainId",
    "StoreNumber", 
    "UPC", 
    "PurchaseDate", 
    "PurchasePrice", 
    "ExternalId", 
    "Quantity", 
    "Weight", 
    "TransactionType",
    "Id",
    "TableName"
  ]

# initializing azure table config
azureTables.config(config.azure.account, config.azure.key)

batchInsert = (items, cb) ->
  gutil.log config.batchCount
  config.batchCount++

  firstItem = items[0]

  MyAzureRecord = azureTables.define
    ChainId: String
    StoreNumber: String
    UPC: String
    PurchaseDate: String
    PurchasePrice: String
    ExternalId: String
    Quantity: String
    Weight: String
    TransactionType: String
    Id: String
    PartitionKey: (model) ->
      model.ChainId
    RowKey: (model) ->
      model.Id
    TableName: () ->
      firstItem.TableName

  batchItems = []
  existingItems = {}
  for v, k in items
    if (existingItems[v.Id]?)
      # gutil.log 'existingItem ' + v.Id
      continue
    existingItems[v.Id] = v
    batchItems.push MyAzureRecord.build(v)
  
  # gutil.log batchItems.length
  # gutil.log batch

  MyAzureRecord.store(batchItems, 1).then cb

  return

# create stream request to definition for batch
batchRequestStream = createBatchRequestStream({
  request: batchInsert,
  batchSize: config.azureConfig.batchSize,         
  maxLiveRequests: config.azureConfig.concurrency,    
  streamOptions: { objectMode: true }
})

getWorkPath = (fileName) ->
  fullPath = path.resolve(fileName)
  extension = path.extname(fileName)
  fileNameNoExtension = path.basename(fullPath, extension)
  return path.join(config.workbase, fileNameNoExtension)

# doUpload to azure table
doUploadTable = (fullPath, cb) ->
  # gutil.log fullPath
  statFileName = fullPath.replace('.csv', '.json')
  stat = 
    currentBatch: 0

  try
    stat = require statFileName
  catch err
    # gutil.log err
  
  gutil.log fullPath
  readStream = fs.createReadStream(fullPath)
  readStream.on 'open', () ->
    readStream.pipe(csv.parse({ delimiter: ',', rowDelimiter: '\n', trim: true, columns: true}))
      .pipe(batchRequestStream)

  readStream.on 'end', () ->
    cb?()
    cb = null

  return

# separate each file into purchase dates for Table Partition
transform = (fullPath, cb) ->
  fileName = path.basename(fullPath)
  fileNameNoExtension = fileName.replace('.hif', '')
  fileParts = fileName.split('-')
  chainId = fileParts[0]
  schemaIdx = config.chains[chainId]
  outPath = getWorkPath(fullPath)
  if !fs.existsSync(outPath)
    mkdirp(outPath)
  
  if fs.existsSync(path.join(outPath, '_.csv'))
    cb()
    return

  # gutil.log chainId
  schema = {}

  # gutil.log schemaIdx
  for v, k in schemaIdx
    schema[v] = k

  #gutil.log schema
  recordCount = 0
  outStreams = {}
  readStream = fs.createReadStream(fullPath)
  writeStream = fs.createWriteStream(outPath + '/_.csv')
  writeStream.write(config.output.join(',') + '\n')
  readStream.on 'open', () ->
    readStream.pipe(csv.parse({ delimiter: '|', rowDelimiter: '\n', trim: true}))
      .pipe(csv.transform (record) ->
        # find the stream to write to
        return record unless record.length > 3

        newRecord = {}
        for v, k in schemaIdx
          newRecord[v] = record[k]

        theDate = moment(newRecord.PurchaseDateRaw, 'MM/DD/YYYY')
        newRecord.PurchaseDate = theDate.format("YYYY-MM-DD")
        newRecord.UPC = ('00000000000' + newRecord.UPC).slice(-11)
        newRecord.Quantity = newRecord.Quantity || 1
        newRecord.ChainId = chainId
        newRecord.Id = "#{newRecord.ExternalId}__#{theDate.format('YYYYMMDD')}__#{newRecord.UPC}"
        newRecord.TableName = "pos#{theDate.format('YYYYMM')}"

        #gutil.log newRecord
        result = []
        for v, k in config.output
          result.push newRecord[v]

        # determine stream
        recordCount = recordCount + 1
        outFile = "#{outPath}/pos#{theDate.format('YYYYMMDD')}__#{fileNameNoExtension}.csv"

        #gutil.log outFile

        if !outStreams[outFile]
          outStreams[outFile] = fs.createWriteStream(outFile)
          outStreams[outFile].write(config.output.join(',') + '\n')
        
        outStreams[outFile].write(result.join(',') + '\n')
        result
      )
      .pipe(nop())

  readStream.on 'end', () ->
    cb?()
    cb = null

  return

# transform and separate file by purchase date
gulp.task 'transform', (cb) ->  
  transformTasks = []
  for v, k in config.files
    taskName = 'transform: ' + path.basename(v)
    transformTasks.push(taskName)
    fullPath = path.resolve(v)
    gulp.task taskName, (myCb) ->
      transform(fullPath, myCb)
  runSequence transformTasks, cb

# upload Blob
gulp.task 'uploadBlob', () ->
  mySources = []
  for v, k in config.files
    # gzip and upload
    dirName = getWorkPath(v)
    gutil.log dirName

    searchName = path.join(dirName, 'pos*.csv')
    mySources.push(searchName)
    # gutil.log searchName

  return gulp.src(mySources).pipe(deployCdn({
      containerName: config.azure.container,
      serviceOptions: [config.azure.account, config.azure.key], 
      folder: config.today.format('YYYYMM/DD'), 
      zip: true, 
      deleteExistingBlobs: false, 
      concurrentUploadThreads: 10, 
      metadata: {
          cacheControl: 'public, max-age=31530000', 
          cacheControlHeader: 'public, max-age=31530000' 
      },
      testRun: !config.isProd 
  })).on('error', gutil.log);

# upload Table
gulp.task 'uploadTable', () ->
  tableTasks = []
  # collect upload tasks
  for v, k in config.files
    dirName = getWorkPath(v)
    gutil.log dirName

    searchName = path.join(dirName, 'pos20*.csv')
    gutil.log searchName

    # glob and create a task for each file
    files = glob.sync searchName

    for v, k in files
      taskName = 'azure-table-upload-' + k
      tableTasks.push(taskName)
      gulp.task taskName, (myCb) ->
        doUploadTable(v, myCb)

  runSequence.apply(null, tableTasks);

gulp.task 'default', (cb) ->
  runSequence 'transform', 'uploadBlob', 'uploadTable', cb
