csv             = require 'csv'
gulp            = require 'gulp'
gutil           = require 'gulp-util'

coffee          = require 'gulp-coffee'
concat          = require 'gulp-concat'
uglify          = require 'gulp-uglify'
runSequence     = require 'run-sequence'
gzip            = require 'gulp-gzip'
uploadAzure     = require 'gulp-upload-azure'
fs              = require 'fs'
moment          = require 'moment'
azure           = require 'azure-storage'

gulp.task 'gzip127', ->
    return gulp.src('./127*.hif').pipe(gzip({
        append: true,
        threshold: false,
        extension: 'gz'
        gzipOptions: {
            level: 9,
            memLevel: 9
        }
    }))
    .pipe(gulp.dest('.'))


config =
  sources: 'gulp-tasks/**/*.coffee'
  azure: require './azure.json'
  srcRoot: './workspace'
  tasks: ['lint']
  
tableSvc = azure.createTableService()

gulp.task 'transform127', () ->
  columns = 'StoreNumber,PurchaseDate,Quantity,UPC,PurchasePrice,ExternalId,TransactionType,Weight'
  writeStream = fs.createWriteStream('./127.csv')
  writeStream.write(columns)
  writeStream.write(',PartitionKey,RowKey\n')
  fs.createReadStream('127-2015811-1_remkemarkets.hif')
    .pipe(csv.parse({ delimiter: '|', rowDelimiter: '\n', trim: true}))
    .pipe(csv.transform (record) ->
       theDate = moment(record[1], 'MM/DD/YYYY')
       record[1] = theDate.format("YYYY/MM/DD")
       record[3] = ('00000000000' + record[3]).slice(-11)
       record[2] = record[2] || 1
       record.push(null)
       record.push(null)
       record.push('127')
       record.push(record[5] + '/' + theDate.format('YYYYMMDD') + '/' + record[3])
       # console.log record
       record
    )
    .pipe(csv.stringify())
    .pipe(writeStream);

runSequence(['transform127'])