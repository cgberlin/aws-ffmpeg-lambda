'use strict';
/**
 * AWS Lambda function for extraction images from uploaded videos triggered by an s3 bucket upload
 * Allowed video types are defined in allowedTypes const
 * Resulting images will be written to the S3 bucket defined as process.env.NEW_BUCKET
 */
// import the aws sdk and node packages needed
const AWS = require('aws-sdk');
const { spawn } = require('child_process');
const { createReadStream, createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const fsPromise = require('fs').promises;
const { resolve } = require('path');

// define our ffmpeg path and other constants
const ffprobePath = '/opt/nodejs/ffprobe';
const ffmpegPath = '/opt/bin/ffmpeg';
const allowedTypes = ['mov', 'mpg', 'mpeg', 'mp4', 'wmv', 'avi', 'webm']

// read our environment variables
const width = process.env.WIDTH;
const height = process.env.HEIGHT;
const URL = process.env.URL;
let newBucket = process.env.NEW_BUCKET;
const framerate = process.env.FRAMERATE
// check our list of allowed resolutions
const ALLOWED_RESOLUTIONS = process.env.ALLOWED_RESOLUTIONS ? new Set(process.env.ALLOWED_RESOLUTIONS.split(/\s*,\s*/)) : new Set([]);

// initialize s3
const S3 = new AWS.S3({
  signatureVersion: 'v4',
});


// main lambda function, ran everytime a video with type: allowedTypes is uploaded to the attached bucket
exports.handler = function (event, context, callback) {
  // since this is triggered from an s3 bucket upload event we can get the file key and bucket name
  const srcKey = decodeURIComponent(event.Records[0].s3.object.key).replace(/\+/g, ' ');
  const bucket = event.Records[0].s3.bucket.name;

  // extract the file type
  let fileType = srcKey.match(/\.\w+$/);

  // check if we can read the file type, return an error if it doesnt exist (usually file didnt have an extension)
  if (!fileType) {
    throw new Error(`invalid file type found for key: ${srcKey}`);
  }

  fileType = fileType[0].slice(1)

  // check our file type against the allowed types, return an error if not allowed 
  if (allowedTypes.indexOf(fileType) === -1) {
    throw new Error(`filetype: ${fileType} is not an allowed type`);
  }

  // lambda is strange, sometimes doesn't get garbage collected. Check our tmp folder here and clear it
  fs.readdirSync('/tmp', (err, files) => {
    if (err) throw err;
    // these are nice as a logging mechanism, can be left in but should be moved to a logging package in prod
    console.log('found these files inside of the container');
    console.log(files);
    // iterate the found files and unlink them from the filesystem
    for (const file of files) {
      fs.unlink(path.join('/tmp', file), err => {
        if (err) throw err;
      });
    }
  });

  // create our temporary filename and file object
  let tempFileName = path.join('/tmp', `input.${fileType}`);
  let tempFile = fs.createWriteStream(tempFileName);
  // retrieve filestream from s3 and pipe to our temporary file
  let fileStream = S3.getObject({ Bucket: bucket, Key: srcKey }).createReadStream().pipe(tempFile);
  fileStream.on('finish', () => {
    // when we finish streaming our file, pass it to extractFrames() for further processing
    extractFrames(tempFileName)
      // when extractFrames() finishes processing our images -> uploadAllToS3()
      .then(buffer => uploadAllToS3())
      // finally, pass our resulting new key to the lambda callback
      .then(() => callback(null, {
        statusCode: '301',
        headers: { 'location': `${srcKey}` },
        body: '',
      })
      )
      // catch any errors and pass to lambda callback
      .catch(err => callback(err))
  });

  // function to read the files from a directory
  const readFiles = (dirname, onFileContent, onError) => {
    // accepts a directory name, onFileContent callback func, and onError callback func
    fs.readdir(dirname, (err, filenames) => {
      console.log(filenames);
      if (err) {
        onError(err);
        return;
      }
      // check our filenames, make sure they aren't of type video (we only want the resulting images)
      filenames.forEach((filename) => {
        // get our filetype from the filename by splitting on period and popping last item from resulting array
        let fileType = filename.split('.').pop();
        // we can check against the allowed file types, since the video will be one of those types
        // if it is not in the array indexOf will return -1
        if (allowedTypes.indexOf(fileType) === -1) {
          // read our file
          fs.readFile(path.resolve(dirname, filename), (err, content) => {
            if (err) {
              // if error call the error callback
              onError(err);
              return;
            }
            // else call our onFileContent callback with the filename and resulting data stream
            onFileContent(filename, content);
          });
        }
      });
    });
  }

  // function to loop over the lambdas tmp directory and upload and found files (that aren't the input video) to a new s3 bucket
  const uploadAllToS3 = () => {
    // return a promise
    return new Promise((resolve, reject) => {
      // split our keys path
      let splitKey = srcKey.split('/');
      // read the files and loop, extacting the filename and content
      readFiles(path.resolve(__dirname, '/tmp/'), (filename, content) => {
        // get our destination key and construct params for s3 bucket upload
        let dstKey = `${splitKey[1]}/${splitKey[2]}/${filename}`;
        let params = {
          Bucket: newBucket,
          Key: dstKey,
          Body: content,
          ContentType: `image/png`
        }
        // finally, upload to s3, for now just console logging and results or errors but should eventually go to a log package
        S3.upload(params, (err, data) => {
          if (err) {
            console.log(err);
          }
          console.log(`successful upload to ${newBucket}/${dstKey}`);
        })
      }, (error) => {
        console.log(error);
      });
    })
  }

  // function to spawn local ffmpeg, run on video -> extract frames as pngs
  const extractFrames = (target) => {
    return new Promise((resolve, reject) => {
      // using node spawn for process management
      // hard-coded to pngs for now but framerate is set by the environment variable FRAMERATE
      const ffmpeg = spawn(ffmpegPath, [
        '-i',
        target,
        '-r',
        framerate,
        '/tmp/%d.png'
      ]);

      // on close resolve the promise and log
      ffmpeg.on('close', (code) => {
        console.log('ffmpeg close');
        console.log(code);
        resolve();
      })
      // on error reject the promise and log
      ffmpeg.on('error', (err) => {
        console.log(err);
        console.log('ffmpeg err');
        console.log(err.message);
        reject(err.message);
      })
    })
  }
}

