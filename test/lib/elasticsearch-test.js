var assert = require('assert')
var async = require('async')

var options = {
  host: 'localhost',
  port: 9200,
  index: {
    name: 'twitter',
    settings: {
      index: {
        analysis: {
          analyzer: {
            snowball: {
              type: "snowball",
              language: "English"
            }
          }
        }
      }
    }
  }
}

var es = require('../../lib/elasticsearch').config(options).addType('tweet', {
  mapping: {
    tweet: {
      properties: {
        message: {type: 'string', analyzer: 'snowball'}
       }
     }
  }
})

var tweets = [
  {
    _id: '1',
    user: 'dan',
    message: 'this is my first tweet, i hope people enjoy it and its searchable',
    postDate: new Date()
  },
  {
    _id: '2',
    user: 'dan',
    message: 'you know, so you can search the site',
    postDate: new Date()
  },
  {
    _id: '4',
    user: 'dan',
    message: 'this is my third tweet. is it indexed?',
    postDate: new Date()
  }
]

describe('elasticsearch', function() {
  before(function(done) {
    es.deleteIndex(done)
  })
  it('adds documents to the index', function(done) {
    async.forEach(tweets, function(t, done) {
      es.tweet.add(t, function(err) {
        assert.ifError(err)
        es.tweet.get(t._id, function(err, indexedTweet) {
          assert.ifError(err)
          assert.equal(t.message, indexedTweet._source.message)
          done()
        })
      })
    }, done)
  })
  it('is searchable', function(done) {
    setTimeout(function() {
      var query = {
        query: {
          multi_match: {
            query: 'index',
            fields: ['message^2','user']
          }
        },
        sort: [
          "_score",
          { postDate: "desc" }
        ]
      }
      es.tweet.search(query, function(err, result) {
        assert.ifError(err)
        assert.equal(result.hits[0].message, 'this is my third tweet. is it indexed?')
        done()
      })
    }, 1500)
  })
})

