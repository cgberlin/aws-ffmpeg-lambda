'use strict';
const AWS = require('aws-sdk')
const { spawn, spawnSync } = require('child_process')
const { createReadStream, createWriteStream } = require('fs')
const fs = require('fs');
var path = require('path');
const fsPromise = require('fs').promises;
const { resolve } = require('path');
const ffprobePath = '/opt/nodejs/ffprobe'
const ffmpegPath = '/opt/bin/ffmpeg'
const allowedTypes = ['mov', 'mpg', 'mpeg', 'mp4', 'wmv', 'avi', 'webm']
const width = process.env.WIDTH
const height = process.env.HEIGHT

const S3 = new AWS.S3({
  signatureVersion: 'v4',
});

const URL = process.env.URL;
const ALLOWED_RESOLUTIONS = process.env.ALLOWED_RESOLUTIONS ? new Set(process.env.ALLOWED_RESOLUTIONS.split(/\s*,\s*/)) : new Set([]);
let new_bucket = 'gandesign-images'
exports.handler = function (event, context, callback) {
  const srcKey = decodeURIComponent(event.Records[0].s3.object.key).replace(/\+/g, ' ')
  const bucket = event.Records[0].s3.bucket.name

  let fileType = srcKey.match(/\.\w+$/)

  if (!fileType) {
    throw new Error(`invalid file type found for key: ${srcKey}`)
  }

  fileType = fileType[0].slice(1)

  if (allowedTypes.indexOf(fileType) === -1) {
    throw new Error(`filetype: ${fileType} is not an allowed type`)
  }

  fs.readdirSync('/tmp', (err, files) => {
    if (err) throw err;
    console.log('found these files inside of the container')
    console.log(files)
  
    for (const file of files) {
    
      fs.unlink(path.join('/tmp', file), err => {
        if (err) throw err;
      });
    }
  });

  var tempFileName = path.join('/tmp', `input.${fileType}`);
  var tempFile = fs.createWriteStream(tempFileName);
  let file_stream = S3.getObject({ Bucket: bucket, Key: srcKey }).createReadStream().pipe(tempFile)
  file_stream.on('finish', () => {
    extractFrames(tempFileName)
    .then(buffer => uploadAllToS3())
    .then(() => callback(null, {
      statusCode: '301',
      headers: { 'location': `${srcKey}` },
      body: '',
    })
    )
    .catch(err => callback(err))
  })
  

  function readFiles(dirname, onFileContent, onError) {
    fs.readdir(dirname, (err, filenames) => {
      console.log(filenames)
      if (err) {
        onError(err);
        return;
      }

      filenames.forEach((filename) => {
        if (filename != 'input.mp4') {
          fs.readFile(path.resolve(dirname, filename), function (err, content) {
            if (err) {
              onError(err);
              return;
            }
            onFileContent(filename, content);
          });
        }
      });
    });
  }


  function uploadAllToS3() {
    return new Promise((resolve, reject) => {
      let split_key = srcKey.split('/')
      readFiles(path.resolve(__dirname, '/tmp/'), (filename, content) => {
        
        let dstKey = `${split_key[1]}/${split_key[2]}/${filename}`
        let params = {
          Bucket: new_bucket,
          Key: dstKey,
          Body: content,
          ContentType: `image/png`
        }
        S3.upload(params, function (err, data) {
          if (err) {
            console.log(err)
          }
          console.log(`successful upload to ${new_bucket}/${dstKey}`)
        })
      }, function (error) {
        console.log(error)
      });

    })

  }

  function extractFrames(target) {
    return new Promise((resolve, reject) => {
      //let tmpFile = createWriteStream(`/tmp/logs.txt`)
      const ffmpeg = spawn(ffmpegPath, [
        '-i',
        target,
        '-r',
        30,
        '/tmp/%d.png'
      ])

      //ffmpeg.stdout.pipe(tmpFile)

      ffmpeg.on('close', function (code) {
        console.log('ffmpeg close')
        console.log(code)
        resolve()
      })

      ffmpeg.on('error', function (err) {
        console.log(err)
        console.log('ffmpeg err')
        console.log(err.message)
        reject(err.message)
      })
    })
  }
}

