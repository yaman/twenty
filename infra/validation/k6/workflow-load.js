// Tests workflow trigger throughput

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('workflow_errors');
const workflowsTriggered = new Counter('workflows_triggered');

export const options = {
  stages: [
    { duration: '30s', target: 3 },
    { duration: '2m', target: 15 },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<5000'],
    workflow_errors: ['rate<0.10'],
  },
};

export default function () {
  const body = graphql(`
    query WorkflowRuns($first: Int) {
      workflowRuns(first: $first) {
        edges {
          node { id status }
        }
      }
    }
  `, { first: 20 });

  const res = http.post(`${BASE_URL}/api`, body, { headers });

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (success) workflowsTriggered.add(1);
  errorRate.add(!success);
  sleep(0.3);
}
