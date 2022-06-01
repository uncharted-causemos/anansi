const express = require('express');
const dotenvConfigResult = require('dotenv').config();
const app = express();
const asyncHandler = require('express-async-handler');
const port = 6000;
const { exec } = require('node:child_process');

app.use(express.json({ limit: '1mb' }));

if (dotenvConfigResult.error) {
  console.warn('No .env file found or has initialization errors - will use default environment');
}

let runningKB = false;
let runningBYOD = false;

app.get('/', (req, res) => {
  res.json({ message: 'Anansi web-service!' })
});

app.post('/kb', asyncHandler(async (req, res) => {
  runningKB = false;
  exec('cd ../ && INDRA_DATASET=aaa python knowledge_pipeline.py', (err, stdout, stderr) => {
    console.log(err);
    console.log(stdout);
    console.log(stderr);
    runningKB = false;
  });
  res.json({});
}))

app.post('/byod', asyncHandler(async (req, res) => {
  const payload = req.body;
  const id = payload.id;
  runningBYOD = true;
  console.log(payload, id);
  exec(`cd ../ && ASSEMBLY_REQUEST_ID=${id} python incremental_pipeline_web.py`, (err, stdout, stderr) => {
    console.log(err);
    console.log(stdout);
    console.log(stderr);
    runningBYOD = false;
  });

  res.json({});
}));

app.listen(port, () => {
  console.log(`App listening on port ${port}`)
});
