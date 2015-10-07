CSVTransform    = require 'csv-transform'
gulp            = require 'gulp'
gutil           = require 'gulp-util'

coffeelint      = require 'gulp-coffeelint'
coffee          = require 'gulp-coffee'
concat          = require 'gulp-concat'
uglify          = require 'gulp-uglify'
runSequence     = require 'run-sequence'


# CONFIG ---------------------------------------------------------

isProd = gutil.env.type is 'prod'

sources =
  coffee: 'src/**/*.coffee'


# TASKS -------------------------------------------------------------

gulp.task 'lint', ->
  gulp.src(sources.coffee)
  .pipe(coffeelint())
  .pipe(coffeelint.reporter())

gulp.task 'default', [
  'lint'
]
