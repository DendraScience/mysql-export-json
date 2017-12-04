/**
 * Main tests
 */

const mysql = require('mysql')
const {Writable} = require('stream')

describe('Module', function () {
  this.timeout(60000)

  const connection = mysql.createConnection({
    host: 'localhost',
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASS,
    database: 'dev_dendro_odm'
  })

  let Exporter

  before(function () {
    return new Promise((resolve, reject) => {
      connection.connect(err => (err ? reject(err) : resolve()))
    })
  })

  after(function () {
    return new Promise((resolve, reject) => {
      connection.end(err => (err ? reject(err) : resolve()))
    })
  })

  it('should import', function () {
    Exporter = require('../../dist')

    expect(Exporter).to.respondTo('constructor')
  })

  it('should export using query', function () {
    const res = []
    const exp = new Exporter(connection, {
      query: connection.query('SELECT geo$coordinates$0, geo$type FROM vw_export_stations LIMIT 1')
    })

    return new Promise((resolve, reject) => {
      exp.on('end', resolve)
      exp.on('error', reject)
      exp.on('data', data => (res.push(JSON.stringify(data))))
      exp.start()
    }).then(() => {
      expect(res).to.deep.equal([
        '{"geo$coordinates$0":-123.652408,"geo$type":"Point"}'
      ])
    })
  })

  it('should export using sqlString', function () {
    const res = []
    const exp = new Exporter(connection, {
      sqlString: 'SELECT geo$coordinates$0, geo$type FROM vw_export_stations LIMIT 1'
    })

    return new Promise((resolve, reject) => {
      exp.on('end', resolve)
      exp.on('error', reject)
      exp.on('data', data => (res.push(JSON.stringify(data))))
      exp.start()
    }).then(() => {
      expect(res).to.deep.equal([
        '{"geo$coordinates$0":-123.652408,"geo$type":"Point"}'
      ])
    })
  })

  it('should export using keepNulls, sqlString', function () {
    const res = []
    const exp = new Exporter(connection, {
      keepNulls: true,
      sqlString: 'SELECT media$0$type FROM vw_export_stations LIMIT 1'
    })

    return new Promise((resolve, reject) => {
      exp.on('end', resolve)
      exp.on('error', reject)
      exp.on('data', data => (res.push(JSON.stringify(data))))
      exp.start()
    }).then(() => {
      expect(res).to.deep.equal([
        '{"media$0$type":null}'
      ])
    })
  })

  it('should export using dotSeparator, expand, sqlString', function () {
    const res = []
    const exp = new Exporter(connection, {
      dotSeparator: '$',
      expand: true,
      sqlString: 'SELECT enabled, name FROM vw_export_stations LIMIT 1'
    })

    return new Promise((resolve, reject) => {
      exp.on('end', resolve)
      exp.on('error', reject)
      exp.on('data', data => (res.push(JSON.stringify(data))))
      exp.start()
    }).then(() => {
      expect(res).to.deep.equal([
        '{"enabled":"true","name":"Angelo HQ WS"}'
      ])
    })
  })

  it('should export using convertTrueFalse, dotSeparator, expand, sqlString', function () {
    const res = []
    const exp = new Exporter(connection, {
      convertTrueFalse: true,
      dotSeparator: '$',
      expand: true,
      sqlString: 'SELECT enabled, name FROM vw_export_stations LIMIT 1'
    })

    return new Promise((resolve, reject) => {
      exp.on('end', resolve)
      exp.on('error', reject)
      exp.on('data', data => (res.push(JSON.stringify(data))))
      exp.start()
    }).then(() => {
      expect(res).to.deep.equal([
        '{"enabled":true,"name":"Angelo HQ WS"}'
      ])
    })
  })

  it('should export using convertTrueFalse, dotSeparator, expand, limit, tableName', function () {
    const res = []
    const exp = new Exporter(connection, {
      convertTrueFalse: true,
      dotSeparator: '$',
      expand: true,
      limit: 2,
      tableName: 'vw_export_stations'
    })

    return new Promise((resolve, reject) => {
      exp.on('end', resolve)
      exp.on('error', reject)
      exp.on('data', data => (res.push(JSON.stringify(data))))
      exp.start()
    }).then(() => {
      expect(res).to.deep.equal([
        '{"enabled":true,"is_active":true,"is_stationary":true,"name":"Angelo HQ WS","station_type":"weather","time_zone":"PST","utc_offset":"-28800","external_refs":[{"identifier":2,"type":"odm.station.StationID","url":"file://WSHQ_CR1000_AR_HWS.dat"}],"geo":{"coordinates":[-123.652408,39.718186,405.9],"type":"Point"}}',
        '{"enabled":true,"is_active":true,"is_stationary":true,"name":"Angelo HQ SF Eel Gage","station_type":"weather","time_zone":"PST","utc_offset":"-28800","external_refs":[{"identifier":3,"type":"odm.station.StationID","url":"file://GHQ_CR1000_GHQTable.dat"}],"geo":{"coordinates":[-123.652277,39.718947,394],"type":"Point"}}'
      ])
    })
  })

  it('should export using stream', function () {
    const res = []
    const exp = new Exporter(connection, {
      convertTrueFalse: true,
      dotSeparator: '$',
      expand: true,
      limit: 2,
      tableName: 'vw_export_stations'
    })

    const writable = new Writable({
      objectMode: true,
      write (data, encoding, callback) {
        res.push(JSON.stringify(data))
        callback()
      }
    })

    const transform = exp.stream()
    transform.pipe(writable)

    return new Promise((resolve, reject) => {
      transform.on('error', reject)
      writable.on('error', reject)
      writable.on('finish', resolve)
    }).then(() => {
      expect(res).to.deep.equal([
        '{"enabled":true,"is_active":true,"is_stationary":true,"name":"Angelo HQ WS","station_type":"weather","time_zone":"PST","utc_offset":"-28800","external_refs":[{"identifier":2,"type":"odm.station.StationID","url":"file://WSHQ_CR1000_AR_HWS.dat"}],"geo":{"coordinates":[-123.652408,39.718186,405.9],"type":"Point"}}',
        '{"enabled":true,"is_active":true,"is_stationary":true,"name":"Angelo HQ SF Eel Gage","station_type":"weather","time_zone":"PST","utc_offset":"-28800","external_refs":[{"identifier":3,"type":"odm.station.StationID","url":"file://GHQ_CR1000_GHQTable.dat"}],"geo":{"coordinates":[-123.652277,39.718947,394],"type":"Point"}}'
      ])
    })
  })
})
