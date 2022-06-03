const express = require('express');
const dotenvConfigResult = require('dotenv').config();
const app = express();
const asyncHandler = require('express-async-handler');
const port = 6000;
const { exec } = require('node:child_process');
const { v4: uuidv4 } = require('uuid');

// Middleware
app.use(express.json({ limit: '1mb' }));

if (dotenvConfigResult.error) {
  console.warn('No .env file found or has initialization errors - will use default environment');
}

const statusMap = {};

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

app.listen(port, () => {
  console.log(`Anansi web-service listening on port ${port}`)
});
