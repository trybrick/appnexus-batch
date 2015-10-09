gulp            = require 'gulp'
gutil           = require 'gulp-util'
coffeelint      = require 'gulp-coffeelint'
runSequence     = require 'run-sequence'

csv             = require 'csv'
coffee          = require 'gulp-coffee'
gzip            = require 'gulp-gzip'
deployCdn       = require 'gulp-deploy-azure-cdn'
fs              = require 'fs'
moment          = require 'moment'
azure           = require 'azure-storage'
glob            = require 'glob'
path            = require 'path'
mkdirp          = require 'mkdirp'

# CONFIG ---------------------------------------------------------

isProd = gutil.env.type is 'prod'

config =
  files: glob.sync './*.hif'
  outStreams: {}
  outPath: []
  azure: require './azure.json'
  tasks: ['lint']
  today: moment(new Date())
  uploadTasks: []
  output: [
    "PartitionKey",
    "RowKey",
    "ChainId",
    "StoreNumber", 
    "UPC", 
    "PurchaseDate", 
    "PurchasePrice", 
    "ExternalId", 
    "Quantity", 
    "Weight", 
    "TransactionType"
  ]
  chains: 
    127: ["StoreNumber", "PurchaseDateRaw", "Quantity", "UPC", "PurchasePrice", "ExternalId"]
    182: ["StoreNumber", "UPC", "PurchasePrice",  "PurchaseDateRaw", "ExternalId", "Quantity", "Weight"]
    215: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]
    216: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]
    217: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]
    218: ["StoreNumber", "UPC", "PurchaseDateRaw", "PurchasePrice", "ExternalId", "Quantity", "Weight", "TransactionType"]


# Transform and Upload Azure Blob
uploadBlob = () ->
  mySources = []
  for v, k in config.files
    # gzip and upload
    dirName = v.replace('.hif', '')
    console.log dirName

    searchName = path.join(dirName, 'pos*.csv')
    mySources.push(searchName)
    # console.log searchName

  gulp.src(mySources).pipe(deployCdn({
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
      testRun: true 
  })).on('error', gutil.log);

createTableStorageUploadTasks = () ->
  for v, k in config.files
    # gzip and upload
    dirName = v.replace('.hif', '')
    console.log dirName

    searchName = path.join(dirName, 'pos*.csv')
    
    # glob and create a task for each file


compress = () ->
  for v, k in config.files
    # gzip and upload
    dirName = v.replace('.hif', '')
    console.log dirName

    searchName = path.join(dirName, 'pos*.csv')
    console.log searchName

    gulp.src(searchName).pipe(gzip({
      append: true,
      threshold: false,
      extension: 'gz'
      gzipOptions: {
          level: 9,
          memLevel: 9
      }
    }))
    .pipe(gulp.dest(dirName))

transform = (v, k) ->
  fullPath = path.resolve(v)
  fileName = path.basename(fullPath)
  fileNameNoExtension = fileName.replace('.hif', '')
  fileParts = fileName.split('-')
  chainId = fileParts[0]
  schemaIdx = config.chains[chainId]
  outPath = fullPath.replace('.hif', '')
  if !fs.existsSync(outPath)
    mkdirp(outPath)
  
  return unless !fs.existsSync(path.join(outPath, '_.csv'))

  # console.log chainId
  schema = {}

  # console.log schemaIdx
  for v, k in schemaIdx
    schema[v] = k

  #console.log schema
  recordCount = 0
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
        newRecord.PartitionKey = chainId
        newRecord.RowKey = "#{newRecord.ExternalId}/#{theDate.format('YYYYMMDD')}/#{newRecord.UPC}"
        newRecord.ChainId = chainId
        #console.log newRecord
        result = []
        for v, k in config.output
          result.push newRecord[v]

        # determine stream
        recordCount = recordCount + 1
        outFile = "#{outPath}/pos#{theDate.format('YYYYMMDD')}__#{fileNameNoExtension}.csv"

        #console.log outFile

        if !config.outStreams[outFile]
          config.outStreams[outFile] = fs.createWriteStream(outFile)
          config.outStreams[outFile].write(config.output.join(',') + '\n')
        
        config.outStreams[outFile].write(result.join(',') + '\n')
        return result
      )
      .pipe(csv.stringify())
      .pipe(writeStream)

  readStream.on 'end', uploadBlob

# Azure Table Insert

gulp.task 'transform', () ->  
  for v, k in config.files
    transform(v, k)

gulp.task 'upload', () ->
  uploadBlob()

gulp.task 'default', () ->
  runSequence('transform')
