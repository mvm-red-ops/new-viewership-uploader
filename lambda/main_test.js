const mainModule = require('./index');

const event ={
  record_count: 206148,
  tot_hov: 6412508.334782927,
  platform: 'Wurl',
  domain: 'Distribution Partners',
  filename: 'Distribution_Partners_viewership_wurl_2025-08-29T9:42:49.upload',
  userEmail: 'dilshad@mvmediasales.com',
  type: 'Viewership',
  LAMBDA_TASK_ORCHESTRATOR: 'register-start-viewership-data-processing-dev'
}

mainModule.handler(event);