/**
 * Class to export MySQL rows as individual JSON documents.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module mysql-export-json
 */

const EventEmitter = require('events')
const Dot = require('dot-object')

const BOOL_REGEX = /^(false|true)$/i

function removeNull (row, key, val) {
  if (val === null) {
    delete row[key]
    return true
  }
}

function convertTrueFalse (row, key, val) {
  if ((typeof val === 'string') && BOOL_REGEX.test(val)) {
    row[key] = (val === 'true')
    return true
  }
}

class Exporter extends EventEmitter {
  constructor (connection, options) {
    super()

    if (!connection) throw new Error('Required: connection')

    this.connection = connection
    this.options = options = Object.assign({
      convertTrueFalse: false,
      dotSeparator: '.',
      expand: false,
      keepNulls: false,
      modifiers: []
    }, options)

    this._mods = options.modifiers

    if (!options.keepNulls) this._mods.push(removeNull)
    if (options.convertTrueFalse) this._mods.push(convertTrueFalse)
    if (options.expand) this._dot = new Dot(options.dotSeparator)

    let query = options.query

    if (!query) {
      if (options.sqlString) {
        query = connection.query(options.sqlString)
      } else if (options.tableName) {
        const limit = typeof options.limit === 'number' ? ` LIMIT ${options.limit}` : ''
        query = connection.query(`SELECT * FROM ${connection.escapeId(options.tableName)}${limit}`)
      } else {
        throw new Error('Required: sqlString or tableName')
      }
    }

    if (!query) throw new Error('Required: query')

    this.query = query

    query.once('end', this._onEndHandler.bind(this))
    query.once('error', this._onErrorHandler.bind(this))
    query.on('result', this._onResultHandler.bind(this))
  }

  destroy () {
    this._dot = this._mods = this.connection = this.options = this.query = null
  }

  _onEndHandler () {
    this.query.removeAllListeners()
    this.emit('end')
  }

  _onErrorHandler (err) {
    this.emit('error', err)
  }

  _onResultHandler (row) {
    this.connection.pause()

    try {
      Object.keys(row).forEach(key => this._mods.find(mod => mod(row, key, row[key])))
      this.emit('data', this._dot ? this._dot.object(row) : row)
    } catch (err) {
      this.emit('error', err)
    }

    this.connection.resume()
  }
}

module.exports = Exporter
