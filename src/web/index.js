const express = require('express');
const dotenvConfigResult = require('dotenv').config();
const app = express();
const asyncHandler = require('express-async-handler');
const port = 6000;
const { exec } = require('node:child_process');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');


// Middleware
app.use(express.json({ limit: '1mb' }));

if (dotenvConfigResult.error) {
  console.warn('No .env file found or has initialization errors - will use default environment');
}

const statusMap = {};
const indraProcessedMap = {};

const STATE = {
  IN_PROGRESS: 'In progress',
  FAILED: 'Failed',
  COMPLETED: 'Completed'
};

app.get('/', (req, res) => {
  res.json({ message: 'Anansi web-service!' });
});

// POST knowledge ingestion
app.post('/kb', asyncHandler(async (req, res) => {
  const trackId = uuidv4();
  statusMap[trackId] = STATE.IN_PROGRESS;

  const indra = req.body.indra;
  const dart = req.body.dart;

  exec(`cd ../ && INDRA_DATASET=${indra} DART_DATA=${dart} python3 knowledge_pipeline.py`, (err, stdout, stderr) => {
    console.log(err);
    console.log(stdout);
    console.log(stderr);
    if (err) {
      statusMap[trackId] = STATE.FAILED;
    } else {
      statusMap[trackId] = STATE.COMPLETED;
    }
  });
  res.json({ trackId });
}));


// POST incremental knowledge ingestion
app.post('/byod', asyncHandler(async (req, res) => {
  const trackId = uuidv4();
  statusMap[trackId] = STATE.IN_PROGRESS;

  const payload = req.body;
  const id = payload.id;
  console.log(payload, id);
  exec(`cd ../ && ASSEMBLY_REQUEST_ID=${id} python3 incremental_pipeline_web.py`, (err, stdout, stderr) => {
    console.log(err);
    console.log(stdout);
    console.log(stderr);
    if (err) {
      statusMap[trackId] = STATE.FAILED;
    } else {
      statusMap[trackId] = STATE.COMPLETED;
    }
  });
  res.json({ trackId });
}));

app.get('/status/:id', asyncHandler(async (req, res) => {
  const id = req.params.id;
  res.json({ status: statusMap[id] || `${id} not found` });
}));


/**
 * Check a directory/folder at regular intervals. This is contorlled by three env variables
 * - WATCH_FOLDER 
 * - WATCH_INTERVAL
 * - DART_CDR_URL
*/
const WATCH_FOLDER = process.env.WATCH_FOLDER;
const WATCH_INTERVAL = +(process.env.WATCH_INTERVAL) || 30000;
const DART_CDR_URL = process.env.DART_CDR_URL || 'http://10.64.16.209:4005/dart-may-2021/auto-test.zip';

if (WATCH_FOLDER) {
  console.log(`Watching ${WATCH_FOLDER}`);
  setInterval(async () => {
    console.log(`checking ${WATCH_FOLDER} for new arrivals`);
    const files = fs.readdirSync(WATCH_FOLDER);

    const newList = [];
    for (const file of files) {
      if (!fs.lstatSync(`${WATCH_FOLDER}/${file}`).isDirectory()) {
        continue;
      }
      if (file.startsWith('__PROCESSED__')) {
        continue;
      }

      if (!indraProcessedMap[file]) {
        newList.push(file);
        indraProcessedMap[file] = true; // Flag as read
      }
    }

    if (newList.length === 0) return;
    for (const indra of newList) {
      console.log(`Processing new data set ${indra}`);

      // Download a dart CDR set per indra
      exec(`DART_CDR_URL=${DART_CDR_URL} WATCH_FOLDER=${WATCH_FOLDER} NAME=${indra} ./dart.sh`, (errDart, stdoutDart, stderrDart) => {
        console.log(errDart);
        console.log(stdoutDart);
        console.log(stderrDart);

        // kick off knowledge pipeline
        console.log('Loading knowledge base into Causemos');
        exec(`cd ../ && INDRA_DATASET=${WATCH_FOLDER}/${indra} DART_DATA=${WATCH_FOLDER}/${indra}-dart_cdr.json  python3 knowledge_pipeline.py`, (err, stdout, stderr) => {
          console.log(err);
          console.log(stdout);
          console.log(stderr);

          // Makr dataset as processed
          exec(`mv ${WATCH_FOLDER}/${indra} ${WATCH_FOLDER}/__PROCESSED__${indra}`);

        });
      });
    }
  }, WATCH_INTERVAL);
}


app.listen(port, () => {
  console.log(`Anansi web-service listening on port ${port}`)
});
