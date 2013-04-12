
var getResource = require('async-resource').get
var ensureResource = require('async-resource').ensure
var http = require('http')
var _ = require('underscore')

var initIndex = function(callback) {
  if (!this.options.index.settings) return callback()
  var options = _.pick(this.options, 'host', 'port')
  // check to see if it already exists
  request.call({options: options},{method:'head',path: '/' + this.options.index.name}, function(err, code) {
    if (err) return callback(err)
    // return if already exists
    if (code == 200) return callback()
    // else setup index
    request.call(
      {options: options}, 
      {method: 'put', path: '/' + this.options.index.name, body: _.pick(this.options.index, 'settings')},
      callback
    )
  }.bind(this))
}

// an elasticsearch index
var ElasticSearch = function(options) {
  this.options = options
  this.ensureIndex = getResource(initIndex.bind(this))
}

ElasticSearch.prototype.addType = function(type,options) {
  options = _.defaults(options || {}, this.options)
  if (!this[type]) this[type] = new SearchType(type, options, this.ensureIndex)
  return this
}

ElasticSearch.prototype.deleteIndex = function(callback) {
  if (!this.options.index.settings) return callback()
  var options = _.pick(this.options, 'host', 'port')
  // check to see if it already exists
  request.call({options: options},{method:'delete',path: '/' + this.options.index.name}, callback)
}

// an elasticsearch type
var SearchType = function(type, options, ensureIndex) {
  this.type = type
  this.index = options.index.name
  this.options = _.pick(options, 'host', 'port')
  this.ensureType = getResource(function(callback) {
    ensureIndex(function(err) {
      if (!options.mapping) return callback()
      if (err) return callback(err)
      request.call(this, {method: 'put', path: '/' + this.index + '/' + this.type + '/_mapping', body: options.mapping}, callback)
    }.bind(this))
  }.bind(this))
}

// get a document
SearchType.prototype.get = function(id, callback) {
  request.call(this, {method: 'get', path: '/' + this.index + '/' + this.type + '/' + id}, callback)
}

// add a document
SearchType.prototype.add = function(doc, callback) {
  var id = doc._id
  doc = _.omit(doc, '_id')
  request.call(this, {method: 'post', path: '/' + this.index + '/' + this.type + '/' + id, body: doc}, callback)
}

SearchType.prototype.update = function(id, options, callback) {
  request.call(this, {method: 'post', path: '/' + this.index + '/' + this.type + '/' + id + '/_update', body: options}, callback)
}

// search for documents
SearchType.prototype.search = function(query, options, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  _.defaults(query, options)
  request.call(this, {method: 'post', path: '/' + this.index + '/' + this.type + '/_search', body: query}, function(err, result) {
    if (err) return callback(err)
    callback(null, {
      total: result.hits.total,
      max_score: result.hits.max_score,
      hits: result.hits.hits.map(function(h) { return _.defaults({ _id: h._id, _score: h._score }, h._source) })
    })
  })
}

// make sure the index and the type are configured before calling get, add, update and search
ensureResource(SearchType.prototype, 'ensureType', ['get','add','update','search'])


// export
var indexes = module.exports = {
  config: function(config) {
    if (!indexes[config.index.name]) indexes[config.index.name] = new ElasticSearch(config)
    return indexes[config.index.name]
  },
  ElasticSearch: ElasticSearch
}

// utils
var request = function(options, callback) {
  var req = http.request(_.defaults(options, this.options), function(res) {
    parse(res, function(err, result) {
      if (err) return callback(err)
      if (_.isObject(result) && result.error) return callback(new Error(JSON.stringify(result)))
      callback(null, result)
    })
  })
  if (options.body) {
    req.end(JSON.stringify(options.body), 'utf8')
  } else {
    req.end()
  }
}

var parse = function(stream, callback) {
  toString(stream, 'utf8', function(err, str) {
    if (err) return callback(err)
    callback(null, str ? JSON.parse(str) : stream.statusCode)
  })
}

var toString = function(stream, encoding, callback) {
  if (!callback) {
    callback = encoding
    encoding = 'utf8'
  }
  var result = ''
  stream.setEncoding(encoding)
  stream.on('data', function(data) {
    result += data
  })
  stream.on('end', function() {
    callback(null, result)
  })
  stream.on('error', callback)
}

